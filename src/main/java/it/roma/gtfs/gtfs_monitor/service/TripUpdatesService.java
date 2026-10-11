package it.roma.gtfs.gtfs_monitor.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static it.roma.gtfs.gtfs_monitor.utils.DelayFmt.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class TripUpdatesService {
    private static final long DEFAULT_REFRESH_MILLIS = 5_000L;
    private static final int MAX_LIMIT = 5_000;
    private static final long RETRY_BACKOFF_MILLIS = 400L;

    /**
     * Tetto all'attesa di un poll. Il client HTTP ha timeout di 30 s, troppi per
     * un feed che si rilegge ogni 5: un upstream lento non deve tenere fermo il
     * thread dello scheduler (e prima ancora le richieste HTTP) per mezzo minuto.
     */
    private static final Duration FEED_BLOCK_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Un feed valido ma senza entita' si accetta solo se dura: i dati
     * precedenti restano serviti finche' sono piu' giovani di cosi'.
     *
     * <p>Prima un feed vuoto non veniva mai accettato, e i mezzi (o gli arrivi)
     * dell'ultimo feed pieno restavano in cache per sempre: a fine servizio
     * l'app avrebbe mostrato i bus della sera fermi sulla mappa. Accettarlo
     * subito pero' e' rischioso: un feed vuoto per un giro e' piu' spesso un
     * singhiozzo dell'upstream che una rete davvero ferma (a Roma di notte
     * girano le linee N). Un minuto di feed vuoti di fila e' un segnale vero.
     */
    static final long EMPTY_FEED_GRACE_MILLIS = 60_000L;

    private final GtfsProperties props;
    private final WebClient webClient;
    private final GtfsIndexService indexService;
    private final AtomicReference<CacheEntry<List<TripUpdateDTO>>> cacheRef = new AtomicReference<>(CacheEntry.empty());
    private final Object refreshLock = new Object();
    private static final DateTimeFormatter ROME_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    public List<TripUpdateDTO> fetch(String linea, Integer limit) {
        List<TripUpdateDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) {
            return List.of();
        }

        int max = normalizeLimit(limit);
        List<TripUpdateDTO> out = new ArrayList<>(max > 0 ? Math.min(max, base.size()) : base.size());
        for (TripUpdateDTO dto : base) {
            if (linea != null && !Objects.equals(dto.getLinea(), linea)) {
                continue;
            }
            out.add(dto);
            if (max > 0 && out.size() >= max) {
                break;
            }
        }
        return out;
    }

    public long countDelayedVehicles(String linea) {
        List<TripUpdateDTO> items = fetch(linea, null);
        Set<String> delayed = new HashSet<>();
        for (TripUpdateDTO dto : items) {
            Double arr = dto.getRitardoArrivoMin();
            Double dep = dto.getRitardoPartenzaMin();
            boolean hasDelay = (arr != null && arr > 0d) || (dep != null && dep > 0d);
            if (!hasDelay) continue;

            String key = firstNonBlank(dto.getVeicolo(), dto.getCorsa());
            if (key != null) {
                delayed.add(key);
            }
        }
        return delayed.size();
    }

    public List<TripUpdateDTO> nextStopsByVehicle(String vehicleId, Integer limit) {
        if (vehicleId == null || vehicleId.isBlank()) return List.of();

        String target = vehicleId.trim();
        long now = System.currentTimeMillis();
        int max = normalizeLimit(limit);

        List<TripUpdateDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) return List.of();

        List<TripUpdateDTO> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (TripUpdateDTO dto : base) {
            if (!target.equals(dto.getVeicolo())) continue;
            Long bestTimeMs = bestTimeMillis(dto);
            if (bestTimeMs != null && bestTimeMs < (now - 5 * 60_000L)) continue;

            String dedupKey = (dto.getCorsa() == null ? "" : dto.getCorsa()) + "|" +
                    (dto.getFermataId() == null ? "" : dto.getFermataId()) + "|" +
                    (bestTimeMs == null ? "" : bestTimeMs);
            if (!seen.add(dedupKey)) continue;
            out.add(dto);
        }

        out.sort(Comparator.comparingLong(dto -> Optional.ofNullable(bestTimeMillis(dto)).orElse(Long.MAX_VALUE)));
        if (max > 0 && out.size() > max) {
            return List.copyOf(out.subList(0, max));
        }
        return List.copyOf(out);
    }

    /**
     * L'ultima istantanea gia' in cache, senza mai scatenare un download.
     *
     * Per chi la legge con un ritmo proprio (PunctualityService): passare da
     * fetch() potrebbe avviare un refresh fuori turno, cioe' un poll in piu'
     * verso romamobilita.it, che ha un filtro anti-bot.
     */
    public List<TripUpdateDTO> snapshot() {
        return cacheRef.get().data();
    }

    @Scheduled(
            fixedDelayString = "${gtfs.realtime.refresh-millis:5000}",
            initialDelayString = "${gtfs.realtime.refresh-millis:5000}"
    )
    public void scheduledRefresh() {
        refreshCache(true);
    }

    /**
     * Con dati in cache si serve quello che c'e': a scaricare ci pensa solo il
     * poll schedulato. Stesso motivo di {@code VehiclePositionsService}: prima,
     * con l'upstream giu', ogni richiesta HTTP rifaceva il suo download
     * bloccante in fila dietro il lock. La richiesta scarica solo a freddo,
     * e al massimo una volta per intervallo.
     */
    private List<TripUpdateDTO> getCachedOrRefresh() {
        CacheEntry<List<TripUpdateDTO>> current = cacheRef.get();
        if (!current.isEmpty()) {
            return current.data();
        }
        refreshCache(false);
        List<TripUpdateDTO> data = cacheRef.get().data();
        return data != null ? data : List.of();
    }

    private void refreshCache(boolean force) {
        synchronized (refreshLock) {
            CacheEntry<List<TripUpdateDTO>> current = cacheRef.get();
            if (!force && !isExpired(current)) {
                return;
            }
            long now = System.currentTimeMillis();
            Optional<List<TripUpdateDTO>> fetched = Optional.empty();
            try {
                fetched = fetchRemoteSnapshot();
            } catch (Exception e) {
                log.warn("[TripUpdates] refresh cache fallito: {}", e.toString());
            }
            if (fetched.isEmpty()) {
                // Fallimento: si annota il tentativo, cosi' la cache conta come
                // fresca per un altro intervallo e nessuno ritenta prima.
                cacheRef.set(current.withAttempt(now));
                return;
            }
            List<TripUpdateDTO> fresh = fetched.get();
            boolean emptyForLong = now - current.loadedAtMillis() > EMPTY_FEED_GRACE_MILLIS;
            if (!fresh.isEmpty() || current.isEmpty() || emptyForLong) {
                cacheRef.set(new CacheEntry<>(List.copyOf(fresh), now, now));
            } else {
                cacheRef.set(current.withAttempt(now));
            }
        }
    }

    /** @return l'istantanea scaricata, oppure vuoto se il download e' fallito. */
    private Optional<List<TripUpdateDTO>> fetchRemoteSnapshot() {
        String url = props.realtime().tripUpdatesUrl();
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("gtfs.realtime.tripUpdatesUrl mancante");
        }

        int attempt = 1;
        while (true) {
            try {
                return fetchRemoteSnapshotOnce(url);
            } catch (InvalidProtocolBufferException e) {
                if (attempt >= 2) {
                    log.error("[TripUpdates] parse error anche al retry: {}", e.toString());
                    return Optional.empty();
                }
                log.warn("[TripUpdates] protobuf corrotto (tentativo {}), retry immediato: {}", attempt, e.toString());
            } catch (WebClientResponseException.BadGateway | WebClientRequestException e) {
                if (attempt >= 2) {
                    log.warn("[TripUpdates] refresh cache fallito anche al retry: {}", e.toString());
                    return Optional.empty();
                }
                log.warn("[TripUpdates] upstream temporaneamente non disponibile (tentativo {}), retry immediato: {}", attempt, e.toString());
            } catch (WebClientResponseException.Forbidden | WebClientResponseException.TooManyRequests e) {
                /*
                 * Il filtro anti-bot davanti al feed ogni tanto respinge il poll.
                 * Non si ritenta: insistere su un rate limit lo peggiora, e il
                 * giro successivo fra 5 secondi e' gia' il tentativo dopo. La
                 * cache precedente resta servita, quindi nessuno se ne accorge.
                 */
                log.warn("[TripUpdates] poll respinto dall'upstream, servo la cache precedente: {}", e.toString());
                return Optional.empty();
            } catch (Exception e) {
                log.error("[TripUpdates] fetch/parsing error: {}", e.toString());
                return Optional.empty();
            }

            attempt++;
            sleepBeforeRetry();
        }
    }

    private Optional<List<TripUpdateDTO>> fetchRemoteSnapshotOnce(String url) throws InvalidProtocolBufferException {
        byte[] body = webClient.get().uri(url)
                .retrieve()
                .bodyToMono(byte[].class)
                .block(FEED_BLOCK_TIMEOUT);

        if (body == null || body.length == 0) {
            // Un feed valido ha sempre almeno l'header: un body vuoto e' un guasto.
            log.warn("[TripUpdates] body vuoto da {}", url);
            return Optional.empty();
        }

        var feed = GtfsRealtime.FeedMessage.parseFrom(body);
        List<TripUpdateDTO> out = new ArrayList<>(Math.max(32, feed.getEntityCount()));

        for (var ent : feed.getEntityList()) {
            if (!ent.hasTripUpdate()) continue;

            var tu = ent.getTripUpdate();
            String routeId = tu.getTrip().getRouteId();
            String tripId  = tu.getTrip().getTripId();
            String vehId   = tu.hasVehicle() ? tu.getVehicle().getId() : null;

            for (var stu : tu.getStopTimeUpdateList()) {
                String stopId = stu.getStopId();

                Long arrEpoch = (stu.hasArrival() && stu.getArrival().hasTime()) ? stu.getArrival().getTime() : null;
                Long depEpoch = (stu.hasDeparture() && stu.getDeparture().hasTime()) ? stu.getDeparture().getTime() : null;

                String arrivo   = toRomeIso(arrEpoch);
                String partenza = toRomeIso(depEpoch);

                Double ritArrMin = (stu.hasArrival() && stu.getArrival().hasDelay()) ? round1(stu.getArrival().getDelay() / 60.0) : null;
                Double ritPartMin = (stu.hasDeparture() && stu.getDeparture().hasDelay()) ? round1(stu.getDeparture().getDelay() / 60.0) : null;

                String descArr   = formatDelay(ritArrMin);
                String descPart  = formatDelay(ritPartMin);

                GtfsIndexService.Stop stop = indexService.stopByIdOrNull(stopId);
                String nomeFermata = (stop != null) ? stop.name() : null;

                out.add(TripUpdateDTO.builder()
                        .linea(routeId)
                        .corsa(tripId)
                        .veicolo(vehId)
                        .fermataId(stopId)
                        .fermataNome(nomeFermata)
                        .arrivo(arrivo)
                        .partenza(partenza)
                        .ritardoArrivoMin(ritArrMin)
                        .ritardoPartenzaMin(ritPartMin)
                        .descArrivo(descArr)
                        .descPartenza(descPart)
                        .build());
            }
        }
        return Optional.of(out);
    }

    private static void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** Scaduta = l'ultimo tentativo, riuscito o no, e' piu' vecchio di un intervallo. */
    private boolean isExpired(CacheEntry<?> entry) {
        return (System.currentTimeMillis() - entry.attemptedAtMillis()) > refreshMillis();
    }

    private long refreshMillis() {
        long configured = props.realtime() != null ? props.realtime().refreshMillis() : 0L;
        return configured > 0 ? configured : DEFAULT_REFRESH_MILLIS;
    }

    private static int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) return -1;
        return Math.min(limit, MAX_LIMIT);
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return null;
    }

    private static Long bestTimeMillis(TripUpdateDTO dto) {
        Long arr = parseIsoMillis(dto.getArrivo());
        Long dep = parseIsoMillis(dto.getPartenza());
        if (arr == null) return dep;
        if (dep == null) return arr;
        return Math.min(arr, dep);
    }

    private static Long parseIsoMillis(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return ZonedDateTime.parse(value, ROME_TS).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * @param loadedAtMillis    quando sono arrivati i dati serviti
     * @param attemptedAtMillis ultimo tentativo di download, anche fallito
     */
    private record CacheEntry<T>(T data, long loadedAtMillis, long attemptedAtMillis) {
        private static <T> CacheEntry<T> empty() {
            return new CacheEntry<>(null, 0L, 0L);
        }

        private CacheEntry<T> withAttempt(long attemptedAt) {
            return new CacheEntry<>(data, loadedAtMillis, attemptedAt);
        }

        private boolean isEmpty() {
            return data == null;
        }
    }
}
