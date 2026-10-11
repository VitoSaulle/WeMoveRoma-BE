package it.roma.gtfs.gtfs_monitor.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.text.Normalizer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsService {
    private static final long DEFAULT_REFRESH_MILLIS = 5_000L;

    /** Pausa fra un tentativo e l'altro, come in TripUpdatesService. */
    private static final long RETRY_BACKOFF_MILLIS = 400L;
    private static final int MAX_LIMIT = 5_000;

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

    private static final ZoneId ROME = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ISO_ROME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ROME);

    private final GtfsProperties props;
    private final WebClient webClient;
    private final GtfsIndexService indexService;
    private final VehiclePositionsSseService vehiclePositionsSseService;
    private final AtomicReference<CacheEntry> cacheRef = new AtomicReference<>(CacheEntry.empty());
    private final Object refreshLock = new Object();


    /**
     * @param linea opzionale: filtra per route_id (match esatto)
     * @param limit opzionale: max righe (<=0 o null = nessun limite)
     */
    public List<VehiclePositionDTO> fetch(String linea, Integer limit) {
        return fetch(linea, null, limit);
    }

    public List<VehiclePositionDTO> fetch(String linea, String capolinea, Integer limit) {
        List<VehiclePositionDTO> base = getCachedOrRefresh().dataOrEmpty();
        String normalizedCapolinea = normalizeText(capolinea);
        int max = normalizeLimit(limit);
        if (linea == null && normalizedCapolinea == null && max <= 0) {
            // Senza filtri si restituisce lo snapshot stesso, immutabile: niente
            // copia di migliaia di mezzi a ogni richiesta.
            return base;
        }
        List<VehiclePositionDTO> out = new ArrayList<>(max > 0 ? Math.min(max, base.size()) : base.size());
        for (VehiclePositionDTO dto : base) {
            if (linea != null && !indexService.matchesLine(linea, dto.getLinea())) {
                continue;
            }
            if (normalizedCapolinea != null && !Objects.equals(normalizeText(dto.getCapolinea()), normalizedCapolinea)) {
                continue;
            }
            out.add(dto);
            if (max > 0 && out.size() >= max) {
                break;
            }
        }
        if (!out.isEmpty()) {
            return out;
        }
        if (linea == null || linea.isBlank()) {
            return List.of();
        }
        List<VehiclePositionDTO> simulated = simulateVehicles(linea, capolinea);
        return max > 0 && simulated.size() > max ? simulated.subList(0, max) : simulated;
    }

    public Instant lastSnapshotAt() {
        CacheEntry current = getCachedOrRefresh();
        return current.isEmpty() ? null : Instant.ofEpochMilli(current.loadedAtMillis());
    }

    /**
     * L'ultima istantanea gia' in cache, senza mai scatenare un download: come
     * {@code TripUpdatesService.snapshot()}, e per lo stesso lettore
     * (PunctualityService, che ne prende l'affollamento dei mezzi).
     */
    public List<VehiclePositionDTO> snapshot() {
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
     * Con dati in cache si serve quello che c'e', anche se di qualche secondo piu'
     * vecchio dell'intervallo: a scaricare ci pensa solo il poll schedulato.
     *
     * <p>Prima ogni richiesta che trovava la cache scaduta scaricava da se',
     * in fila dietro il lock. Bastava un upstream giu': il fallimento non
     * aggiornava l'ora di caricamento, la cache restava "scaduta" e ogni
     * richiesta HTTP rifaceva il suo download bloccante (con retry), una dopo
     * l'altra. Ora la richiesta scarica solo a freddo, quando non c'e' ancora
     * niente da servire, e anche allora al massimo un tentativo per intervallo.
     */
    private CacheEntry getCachedOrRefresh() {
        CacheEntry current = cacheRef.get();
        if (!current.isEmpty()) {
            return current;
        }
        refreshCache(false);
        return cacheRef.get();
    }

    /**
     * Il mezzo che sta facendo la corsa, dall'indice costruito una volta per
     * snapshot (a parita' di corsa vince il primo del feed). Prima chi cercava
     * gli arrivi copiava e indicizzava l'intero elenco a ogni richiesta.
     */
    public VehiclePositionDTO vehicleByTrip(String tripId) {
        if (tripId == null || tripId.isBlank()) return null;
        return getCachedOrRefresh().indici().perCorsa().get(tripId);
    }

    /** Il mezzo con questo id veicolo, dall'indice per snapshot. */
    public VehiclePositionDTO vehicleById(String vehicleId) {
        if (vehicleId == null || vehicleId.isBlank()) return null;
        return getCachedOrRefresh().indici().perVeicolo().get(vehicleId);
    }

    private void refreshCache(boolean force) {
        synchronized (refreshLock) {
            CacheEntry current = cacheRef.get();
            if (!force && !isExpired(current)) {
                return;
            }
            long now = System.currentTimeMillis();
            Optional<List<VehiclePositionDTO>> fetched = Optional.empty();
            try {
                fetched = fetchRemoteSnapshot();
            } catch (Exception e) {
                log.warn("[VehiclePositions] refresh cache fallito: {}", e.toString());
            }
            if (fetched.isEmpty()) {
                // Fallimento: si annota il tentativo, cosi' la cache conta come
                // fresca per un altro intervallo e nessuno ritenta prima.
                cacheRef.set(current.withAttempt(now));
                return;
            }
            List<VehiclePositionDTO> fresh = fetched.get();
            boolean emptyForLong = now - current.loadedAtMillis() > EMPTY_FEED_GRACE_MILLIS;
            if (!fresh.isEmpty() || current.isEmpty() || emptyForLong) {
                List<VehiclePositionDTO> immutable = List.copyOf(fresh);
                cacheRef.set(new CacheEntry(immutable, Indici.di(immutable), now, now));
                vehiclePositionsSseService.publish(immutable, Instant.ofEpochMilli(now));
            } else {
                cacheRef.set(current.withAttempt(now));
            }
        }
    }

    /**
     * Stessa gestione dei tentativi di {@code TripUpdatesService}.
     *
     * Prima il ritentativo valeva solo per il protobuf corrotto: un 502 di
     * Roma Mobilita' finiva nel catch generico, si arrendeva subito e loggava
     * ERROR. Gli aggiornamenti delle corse invece ritentavano e passavano.
     * Stesso feed, stesso singhiozzo, due comportamenti: le posizioni dei mezzi
     * saltavano un ciclo e il log si riempiva di ERROR per un guasto passeggero
     * di cui il codice sa gia' riprendersi.
     */
    /** @return l'istantanea scaricata, oppure vuoto se il download e' fallito. */
    private Optional<List<VehiclePositionDTO>> fetchRemoteSnapshot() {
        int attempt = 1;
        while (true) {
            try {
                return fetchRemoteSnapshotOnce();
            } catch (InvalidProtocolBufferException e) {
                if (attempt >= 2) {
                    log.error("[VehiclePositions] feed protobuf corrotto anche al retry: {}", e.toString());
                    return Optional.empty();
                }
                log.warn("[VehiclePositions] protobuf corrotto (tentativo {}), retry immediato: {}", attempt, e.toString());
            } catch (WebClientResponseException.BadGateway | WebClientRequestException e) {
                if (attempt >= 2) {
                    log.warn("[VehiclePositions] refresh cache fallito anche al retry: {}", e.toString());
                    return Optional.empty();
                }
                log.warn("[VehiclePositions] upstream temporaneamente non disponibile (tentativo {}), retry immediato: {}", attempt, e.toString());
            } catch (WebClientResponseException.Forbidden | WebClientResponseException.TooManyRequests e) {
                /*
                 * Il filtro anti-bot davanti al feed ogni tanto respinge il poll.
                 * Non si ritenta: insistere su un rate limit lo peggiora, e il
                 * giro successivo fra 5 secondi e' gia' il tentativo dopo. La
                 * cache precedente resta servita, quindi nessuno se ne accorge.
                 */
                log.warn("[VehiclePositions] poll respinto dall'upstream, servo la cache precedente: {}", e.toString());
                return Optional.empty();
            } catch (Exception e) {
                log.error("[VehiclePositions] fetch/parsing error: {}", e.toString());
                return Optional.empty();
            }

            attempt++;
            sleepBeforeRetry();
        }
    }

    private static void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private Optional<List<VehiclePositionDTO>> fetchRemoteSnapshotOnce() throws InvalidProtocolBufferException {
        String url = Objects.requireNonNull(props.realtime().vehiclePositionsUrl(),
                "gtfs.realtime.vehiclePositionsUrl mancante");

        byte[] body = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(byte[].class)
                .block(FEED_BLOCK_TIMEOUT);

        if (body == null || body.length == 0) {
            // Un feed valido ha sempre almeno l'header: un body vuoto e' un guasto.
            log.warn("[VehiclePositions] body vuoto da {}", url);
            return Optional.empty();
        }

        var feed = GtfsRealtime.FeedMessage.parseFrom(body);
        List<VehiclePositionDTO> out = new ArrayList<>(Math.max(32, feed.getEntityCount()));

        for (var ent : feed.getEntityList()) {
            if (!ent.hasVehicle()) continue;
            var v = ent.getVehicle();

            String routeId = v.hasTrip() ? v.getTrip().getRouteId() : null;
            String tripId  = v.hasTrip() ? v.getTrip().getTripId()  : null;
            String vehId   = v.hasVehicle() ? v.getVehicle().getId() : null;

            Double lat = (v.hasPosition() && v.getPosition().hasLatitude()) ? (double) v.getPosition().getLatitude() : null;
            Double lon = (v.hasPosition() && v.getPosition().hasLongitude()) ? (double) v.getPosition().getLongitude() : null;

            Double kmh = null;
            if (v.hasPosition() && v.getPosition().hasSpeed()) {
                kmh = round3(v.getPosition().getSpeed());
            }

            String ts = null;
            if (v.hasTimestamp()) {
                ts = ISO_ROME.format(Instant.ofEpochSecond(v.getTimestamp()));
            }

            GtfsIndexService.Trip trip = indexService.tripByIdOrNull(tripId);
            String capolinea = (trip != null) ? trip.headsign() : null;
            Boolean wheelchairAccessible = wheelchairAccessible(trip);
            String occupancyStatus = v.hasOccupancyStatus() ? v.getOccupancyStatus().name() : null;

            out.add(VehiclePositionDTO.builder()
                    .linea(routeId)
                    .corsa(tripId)
                    .veicolo(vehId)
                    .lat(lat)
                    .lon(lon)
                    .velocitaKmh(kmh)
                    .timestamp(ts)
                    .capolinea(capolinea)
                    .occupancyStatus(occupancyStatus)
                    .wheelchairAccessible(wheelchairAccessible)
                    .build());
        }
        return Optional.of(out);
    }

    private static Boolean wheelchairAccessible(GtfsIndexService.Trip trip) {
        if (trip == null || trip.wheelchair() == null) return null;
        if (trip.wheelchair() == 1) return Boolean.TRUE;
        if (trip.wheelchair() == 2) return Boolean.FALSE;
        return null;
    }

    /** Scaduta = l'ultimo tentativo, riuscito o no, e' piu' vecchio di un intervallo. */
    private boolean isExpired(CacheEntry entry) {
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

    private static double round3(double value) {
        return Math.round(value * 1000.0d) / 1000.0d;
    }

    private static String normalizeText(String value) {
        if (value == null) return null;
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{Alnum}]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
        return normalized.isEmpty() ? null : normalized;
    }

    /** Linea senza posizioni nel feed: un mezzo per ogni corsa che l'orario da' in viaggio. */
    private List<VehiclePositionDTO> simulateVehicles(String linea, String capolinea) {
        long nowEpochSeconds = Instant.now().getEpochSecond();
        String timestamp = ISO_ROME.format(Instant.ofEpochSecond(nowEpochSeconds));
        List<VehiclePositionDTO> out = new ArrayList<>();
        Instant now = Instant.ofEpochSecond(nowEpochSeconds);
        List<GtfsIndexService.SimulatedTrip> simulatedTrips = indexService.simulatedTripsInService(linea, capolinea, now);
        if (simulatedTrips.isEmpty() && capolinea != null && !capolinea.isBlank()) {
            simulatedTrips = indexService.simulatedTripsInService(linea, null, now);
        }

        for (GtfsIndexService.SimulatedTrip simulated : simulatedTrips) {
            double progress = tripProgress(nowEpochSeconds, simulated.startEpochSeconds(), simulated.endEpochSeconds());
            Position pos = interpolateOnShape(simulated.shape(), progress);
            if (pos == null) continue;

            out.add(VehiclePositionDTO.builder()
                    .linea(simulated.line())
                    .corsa(simulated.tripId())
                    .veicolo("sim-" + simulated.tripId())
                    .lat(pos.lat())
                    .lon(pos.lon())
                    .velocitaKmh(null)
                    .timestamp(timestamp)
                    .capolinea(simulated.destination())
                    .occupancyStatus(null)
                    .wheelchairAccessible(simulated.wheelchairAccessible())
                    .build());
        }
        return List.copyOf(out);
    }

    private static double tripProgress(long nowEpochSeconds, long startEpochSeconds, long endEpochSeconds) {
        if (endEpochSeconds <= startEpochSeconds) return 0d;
        double raw = (double) (nowEpochSeconds - startEpochSeconds) / (double) (endEpochSeconds - startEpochSeconds);
        return Math.max(0d, Math.min(1d, raw));
    }

    private static Position interpolateOnShape(List<GtfsIndexService.ShapePoint> shape, double progress) {
        if (shape == null || shape.isEmpty()) return null;
        if (shape.size() == 1) {
            GtfsIndexService.ShapePoint point = shape.get(0);
            return new Position(point.lat(), point.lon());
        }

        double scaled = progress * (shape.size() - 1);
        int lowerIndex = (int) Math.floor(scaled);
        int upperIndex = Math.min(shape.size() - 1, lowerIndex + 1);
        double fraction = scaled - lowerIndex;

        GtfsIndexService.ShapePoint a = shape.get(lowerIndex);
        GtfsIndexService.ShapePoint b = shape.get(upperIndex);
        double lat = a.lat() + (b.lat() - a.lat()) * fraction;
        double lon = a.lon() + (b.lon() - a.lon()) * fraction;
        return new Position(lat, lon);
    }

    /**
     * @param indici            gli stessi mezzi per corsa e per veicolo, una volta per snapshot
     * @param loadedAtMillis    quando sono arrivati i dati serviti
     * @param attemptedAtMillis ultimo tentativo di download, anche fallito
     */
    private record CacheEntry(List<VehiclePositionDTO> data, Indici indici, long loadedAtMillis, long attemptedAtMillis) {
        private static CacheEntry empty() {
            return new CacheEntry(null, Indici.VUOTI, 0L, 0L);
        }

        private CacheEntry withAttempt(long attemptedAt) {
            return new CacheEntry(data, indici, loadedAtMillis, attemptedAt);
        }

        private List<VehiclePositionDTO> dataOrEmpty() {
            return data != null ? data : List.of();
        }

        private boolean isEmpty() {
            return data == null;
        }
    }

    private record Position(double lat, double lon) {}

    /** Mezzi per corsa e per id veicolo; a parita' di chiave vince il primo del feed. */
    private record Indici(Map<String, VehiclePositionDTO> perCorsa, Map<String, VehiclePositionDTO> perVeicolo) {
        private static final Indici VUOTI = new Indici(Map.of(), Map.of());

        private static Indici di(List<VehiclePositionDTO> mezzi) {
            Map<String, VehiclePositionDTO> perCorsa = new HashMap<>();
            Map<String, VehiclePositionDTO> perVeicolo = new HashMap<>();
            for (VehiclePositionDTO v : mezzi) {
                if (v.getCorsa() != null && !v.getCorsa().isBlank()) perCorsa.putIfAbsent(v.getCorsa(), v);
                if (v.getVeicolo() != null && !v.getVeicolo().isBlank()) perVeicolo.putIfAbsent(v.getVeicolo(), v);
            }
            return new Indici(Map.copyOf(perCorsa), Map.copyOf(perVeicolo));
        }
    }
}
