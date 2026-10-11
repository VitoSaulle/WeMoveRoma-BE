package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stream SSE delle posizioni dei mezzi.
 *
 * Due livelli, perche' un client che tiene aperta la connessione ma non legge non
 * rallenti gli altri:
 * <ul>
 *   <li>dispatcher (un thread): filtra, simula le corse senza GPS e serializza. Non
 *       scrive mai su un socket, quindi non si blocca su un client lento. Dei tick
 *       arrivati mentre lavora tiene solo l'ultimo: quelli vecchi non servono a nessuno;</li>
 *   <li>invio: per ogni client al massimo un virtual thread alla volta, che scrive il
 *       payload piu' recente. Se il client e' fermo, il suo thread resta bloccato sulla
 *       write (fino al timeout di Tomcat, server.tomcat.connection-timeout) e intanto
 *       i payload nuovi sostituiscono quello in attesa invece di accodarsi.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsSseService {
    private static final ZoneId ROME = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ISO_ROME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ROME);

    /**
     * Senza eventi per questo tempo (feed GTFS-RT fermo) il client riceve un commento SSE:
     * le connessioni morte emergono alla scrittura e i proxy non chiudono quelle inattive.
     */
    static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(15);

    /**
     * Un invio fermo da piu' di cosi' e' un client che non legge (o legge a gocce, senza mai
     * far scattare il timeout di scrittura): lo togliamo dai destinatari.
     */
    static final Duration SLOW_SEND_LIMIT = Duration.ofSeconds(30);

    private final GtfsIndexService gtfsIndexService;
    private final ObjectMapper objectMapper;
    private final Set<Subscription> emitters = ConcurrentHashMap.newKeySet();

    /** Oltre questo numero di client contemporanei lo stream risponde 503. */
    @Value("${vehicles.sse.max-subscribers:500}")
    private int maxSubscribers = 500;

    /** Ultimo snapshot pubblicato e non ancora distribuito: un tick nuovo sostituisce quello in attesa. */
    private final AtomicReference<Tick> pendingTick = new AtomicReference<>();

    private final ScheduledExecutorService dispatcher = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "vehicle-positions-sse-dispatch");
        t.setDaemon(true);
        return t;
    });

    private final ExecutorService sendExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("vehicle-positions-sse-send-", 0).factory());

    @PostConstruct
    void startHeartbeat() {
        long periodMillis = HEARTBEAT_INTERVAL.toMillis();
        dispatcher.scheduleWithFixedDelay(() -> {
            try {
                sendHeartbeats(System.nanoTime());
            } catch (RuntimeException e) {
                log.warn("[VehiclePositionsSse] heartbeat fallito: {}", e.toString());
            }
        }, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    public SseEmitter subscribe(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        return subscribe(snapshot, generatedAt, null, null, null);
    }

    public SseEmitter subscribe(
            List<VehiclePositionDTO> snapshot,
            Instant generatedAt,
            String linea,
            String destination,
            String vehicleId
    ) {
        if (emitters.size() >= maxSubscribers) {
            log.warn("[VehiclePositionsSse] {} client gia' collegati, nuova sottoscrizione rifiutata", emitters.size());
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Troppi client collegati allo stream dei mezzi");
        }
        SseEmitter emitter = newEmitter();
        Subscription subscription = new Subscription(emitter, linea, normalizeText(destination), vehicleId);
        emitters.add(subscription);

        emitter.onCompletion(() -> emitters.remove(subscription));
        emitter.onTimeout(() -> {
            emitters.remove(subscription);
            emitter.complete();
        });
        emitter.onError((ex) -> emitters.remove(subscription));

        // Snapshot iniziale preparato sul dispatcher come i tick, e solo dopo la
        // sottoscrizione entra nei tick: un tick piu' recente non puo' precederlo.
        runOnDispatcher(() -> {
            String json = toJson(toApiResponse(filterSnapshot(snapshot, subscription, generatedAt), generatedAt));
            if (json != null) {
                offer(subscription, new Outgoing(json));
            }
            subscription.ready = true;
        });
        return emitter;
    }

    public void publish(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        if (emitters.isEmpty()) {
            return;
        }
        // Il chiamante (thread di polling) ritorna subito. Se il dispatcher e' ancora
        // indietro, lo snapshot sostituisce quello in attesa invece di accodarsi.
        if (pendingTick.getAndSet(new Tick(snapshot, generatedAt)) == null) {
            runOnDispatcher(this::dispatchPendingTick);
        }
    }

    @PreDestroy
    public void shutdown() {
        dispatcher.shutdownNow();
        sendExecutor.shutdownNow();
    }

    /** Punto di estensione per i test, che sostituiscono la scrittura sul socket. */
    SseEmitter newEmitter() {
        return new SseEmitter(0L);
    }

    /** Per i test: attende che il dispatcher abbia smaltito il lavoro gia' in coda. */
    void awaitDispatcherIdle() throws Exception {
        dispatcher.submit(() -> { }).get(5, TimeUnit.SECONDS);
    }

    private void runOnDispatcher(Runnable task) {
        try {
            dispatcher.execute(task);
        } catch (RejectedExecutionException e) {
            log.debug("[VehiclePositionsSse] dispatcher spento, invio scartato: {}", e.toString());
        }
    }

    private void dispatchPendingTick() {
        Tick tick = pendingTick.getAndSet(null);
        if (tick != null) {
            dispatch(List.copyOf(emitters), tick.snapshot(), tick.generatedAt());
        }
    }

    private void dispatch(List<Subscription> targets, List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        Map<String, String> normalizedCapolineaCache = buildNormalizedCapolineaCache(targets, snapshot);
        // Un solo payload serializzato per gruppo di filtri identici: nel caso comune
        // (nessun filtro) tutti i subscriber condividono la stessa stringa JSON.
        Map<FilterKey, String> payloadByFilter = new HashMap<>();
        long now = System.nanoTime();
        for (Subscription subscription : targets) {
            if (!subscription.ready || !emitters.contains(subscription)) {
                continue; // snapshot iniziale non ancora pronto, o rimosso nel frattempo
            }
            long sendStarted = subscription.sendStartedNanos;
            if (sendStarted != 0L && now - sendStarted > SLOW_SEND_LIMIT.toNanos()) {
                log.info("[VehiclePositionsSse] client fermo da oltre {}s, scollegato", SLOW_SEND_LIMIT.toSeconds());
                emitters.remove(subscription);
                continue;
            }
            FilterKey key = subscription.filterKey();
            String json = payloadByFilter.get(key);
            if (json == null) {
                json = toJson(toApiResponse(
                        filterSnapshot(snapshot, subscription, generatedAt, normalizedCapolineaCache),
                        generatedAt));
                if (json == null) {
                    continue; // errore di serializzazione già loggato
                }
                payloadByFilter.put(key, json);
            }
            offer(subscription, new Outgoing(json));
        }
    }

    /** Un commento SSE a chi non riceve nulla da HEARTBEAT_INTERVAL. Gira sul dispatcher. */
    void sendHeartbeats(long nowNanos) {
        for (Subscription subscription : emitters) {
            if (subscription.ready && nowNanos - subscription.lastOfferNanos >= HEARTBEAT_INTERVAL.toNanos()) {
                offer(subscription, Outgoing.HEARTBEAT);
            }
        }
    }

    /**
     * Consegna il messaggio al client senza bloccare il chiamante. Al massimo un invio
     * per client e' in corso: quello che arriva nel frattempo prende il posto del
     * messaggio in attesa (un heartbeat no: non scavalca dati veri).
     */
    private void offer(Subscription subscription, Outgoing message) {
        subscription.lastOfferNanos = System.nanoTime();
        if (message == Outgoing.HEARTBEAT) {
            if (!subscription.pending.compareAndSet(null, message)) {
                return;
            }
        } else {
            subscription.pending.set(message);
        }
        if (subscription.sending.compareAndSet(false, true)) {
            try {
                sendExecutor.execute(() -> drain(subscription));
            } catch (RejectedExecutionException e) {
                subscription.sending.set(false);
                log.debug("[VehiclePositionsSse] executor spento, invio scartato: {}", e.toString());
            }
        }
    }

    /** Gira sul virtual thread del client: scrive finche' c'e' un messaggio in attesa. */
    private void drain(Subscription subscription) {
        while (true) {
            Outgoing next = subscription.pending.getAndSet(null);
            if (next == null) {
                subscription.sending.set(false);
                // Un offer arrivato fra il getAndSet e qui ha trovato sending=true e non ha
                // avviato nessuno: il suo messaggio lo mandiamo noi.
                if (subscription.pending.get() == null || !subscription.sending.compareAndSet(false, true)) {
                    return;
                }
                continue;
            }
            if (!emitters.contains(subscription)) {
                // Scollegato perche' troppo lento (o gia' chiuso): sending resta true,
                // cosi' nessuno avvia piu' invii verso questo client.
                completeQuietly(subscription.emitter());
                return;
            }
            if (!send(subscription, next)) {
                emitters.remove(subscription);
                return;
            }
        }
    }

    private boolean send(Subscription subscription, Outgoing message) {
        subscription.sendStartedNanos = System.nanoTime();
        try {
            if (message == Outgoing.HEARTBEAT) {
                subscription.emitter().send(SseEmitter.event().comment("ping"));
            } else {
                subscription.emitter().send(SseEmitter.event()
                        .name("vehicles")
                        .data(message.json(), MediaType.APPLICATION_JSON));
            }
            return true;
        } catch (Exception e) {
            try {
                subscription.emitter().completeWithError(e);
            } catch (Exception ignored) {
                // no-op
            }
            log.debug("[VehiclePositionsSse] emitter closed: {}", e.toString());
            return false;
        } finally {
            subscription.sendStartedNanos = 0L;
        }
    }

    private static void completeQuietly(SseEmitter emitter) {
        try {
            emitter.complete();
        } catch (Exception ignored) {
            // no-op
        }
    }

    /**
     * Precalcola una volta per tick la normalizzazione (regex NFD) del capolinea di ogni
     * veicolo, invece di rifarla per ogni subscriber. Costruita solo se serve davvero.
     */
    private Map<String, String> buildNormalizedCapolineaCache(List<Subscription> targets, List<VehiclePositionDTO> snapshot) {
        boolean needed = targets.stream().anyMatch(s -> s.normalizedDestination() != null);
        if (!needed) {
            return Map.of();
        }
        Map<String, String> cache = new HashMap<>();
        for (VehiclePositionDTO dto : snapshot) {
            String capolinea = dto.getCapolinea();
            if (capolinea != null && !cache.containsKey(capolinea)) {
                cache.put(capolinea, normalizeText(capolinea));
            }
        }
        return cache;
    }

    private String toJson(ApiListResponseDTO<ApiVehicleDTO> response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            log.error("[VehiclePositionsSse] serializzazione payload SSE fallita: {}", e.toString());
            return null;
        }
    }

    private ApiListResponseDTO<ApiVehicleDTO> toApiResponse(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        List<ApiVehicleDTO> items = snapshot.stream().map(this::toApiDto).toList();
        return new ApiListResponseDTO<>(items, items.size(), generatedAt != null ? generatedAt : Instant.now());
    }

    private ApiVehicleDTO toApiDto(VehiclePositionDTO dto) {
        return new ApiVehicleDTO(
                dto.getVeicolo(),
                gtfsIndexService.publicLineByRouteId(dto.getLinea()),
                dto.getCapolinea(),
                dto.getCorsa(),
                dto.getLat(),
                dto.getLon(),
                dto.getVelocitaKmh(),
                dto.getTimestamp(),
                dto.getOccupancyStatus(),
                dto.getWheelchairAccessible()
        );
    }

    private List<VehiclePositionDTO> filterSnapshot(List<VehiclePositionDTO> snapshot, Subscription subscription, Instant generatedAt) {
        return filterSnapshot(snapshot, subscription, generatedAt, Map.of());
    }

    private List<VehiclePositionDTO> filterSnapshot(
            List<VehiclePositionDTO> snapshot,
            Subscription subscription,
            Instant generatedAt,
            Map<String, String> normalizedCapolineaCache
    ) {
        List<VehiclePositionDTO> filtered = snapshot.stream().filter(dto -> {
            if (subscription.vehicleId() != null && !subscription.vehicleId().isBlank()) {
                if (!subscription.vehicleId().equals(dto.getVeicolo())) return false;
            }
            if (subscription.linea() != null && !subscription.linea().isBlank()) {
                if (!gtfsIndexService.matchesLine(subscription.linea(), dto.getLinea())) return false;
            }
            if (subscription.normalizedDestination() != null) {
                if (!subscription.normalizedDestination().equals(
                        normalizedCapolinea(dto.getCapolinea(), normalizedCapolineaCache))) return false;
            }
            return true;
        }).toList();

        if (!filtered.isEmpty()) {
            return filtered;
        }
        if (subscription.vehicleId() != null && !subscription.vehicleId().isBlank()) {
            if (subscription.vehicleId().startsWith("sim-")) {
                VehiclePositionDTO predicted = simulateVehicleByTripId(subscription.vehicleId().substring(4), generatedAt);
                return predicted != null ? List.of(predicted) : List.of();
            }
            return List.of();
        }
        if (subscription.linea() == null || subscription.linea().isBlank()) {
            return List.of();
        }
        return simulateVehicles(subscription.linea(), subscription.normalizedDestination(), generatedAt);
    }

    private static String normalizedCapolinea(String capolinea, Map<String, String> cache) {
        if (capolinea == null) {
            return null;
        }
        String cached = cache.get(capolinea);
        return cached != null ? cached : normalizeText(capolinea);
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

    /** Come in VehiclePositionsService: un mezzo per ogni corsa che l'orario da' in viaggio. */
    private List<VehiclePositionDTO> simulateVehicles(String linea, String destination, Instant generatedAt) {
        Instant now = generatedAt != null ? generatedAt : Instant.now();
        List<VehiclePositionDTO> out = new ArrayList<>();
        List<GtfsIndexService.SimulatedTrip> simulatedTrips = gtfsIndexService.simulatedTripsInService(linea, destination, now);
        if (simulatedTrips.isEmpty() && destination != null && !destination.isBlank()) {
            simulatedTrips = gtfsIndexService.simulatedTripsInService(linea, null, now);
        }

        for (GtfsIndexService.SimulatedTrip simulated : simulatedTrips) {
            VehiclePositionDTO dto = toPredictedVehicle(simulated, now);
            if (dto != null) {
                out.add(dto);
            }
        }

        return List.copyOf(out);
    }

    private VehiclePositionDTO simulateVehicleByTripId(String tripId, Instant generatedAt) {
        Instant now = generatedAt != null ? generatedAt : Instant.now();
        return gtfsIndexService.simulatedTripById(tripId, now)
                .map(simulated -> toPredictedVehicle(simulated, now))
                .orElse(null);
    }

    private VehiclePositionDTO toPredictedVehicle(GtfsIndexService.SimulatedTrip simulated, Instant now) {
        long nowEpochSeconds = now.getEpochSecond();
        double progress = tripProgress(nowEpochSeconds, simulated.startEpochSeconds(), simulated.endEpochSeconds());
        Position position = interpolateOnShape(simulated.shape(), progress);
        if (position == null) {
            return null;
        }

        return VehiclePositionDTO.builder()
                .linea(simulated.line())
                .corsa(simulated.tripId())
                .veicolo("sim-" + simulated.tripId())
                .lat(position.lat())
                .lon(position.lon())
                .velocitaKmh(null)
                .timestamp(ISO_ROME.format(now))
                .capolinea(simulated.destination())
                .occupancyStatus(null)
                .wheelchairAccessible(simulated.wheelchairAccessible())
                .build();
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


    private static final class Subscription {
        private final SseEmitter emitter;
        private final String linea;
        private final String normalizedDestination;
        private final String vehicleId;

        /** Messaggio in attesa di invio: ne resta al massimo uno, il piu' recente. */
        private final AtomicReference<Outgoing> pending = new AtomicReference<>();
        /** true mentre un virtual thread sta scrivendo verso questo client. */
        private final AtomicBoolean sending = new AtomicBoolean();
        /** Diventa true dopo lo snapshot iniziale: prima i tick non lo includono. */
        private volatile boolean ready;
        private volatile long lastOfferNanos = System.nanoTime();
        /** Inizio della scrittura in corso, 0 se nessuna. */
        private volatile long sendStartedNanos;

        private Subscription(SseEmitter emitter, String linea, String normalizedDestination, String vehicleId) {
            this.emitter = emitter;
            this.linea = linea;
            this.normalizedDestination = normalizedDestination;
            this.vehicleId = vehicleId;
        }

        private SseEmitter emitter() { return emitter; }
        private String linea() { return linea; }
        private String normalizedDestination() { return normalizedDestination; }
        private String vehicleId() { return vehicleId; }

        private FilterKey filterKey() {
            return new FilterKey(linea, normalizedDestination, vehicleId);
        }
    }

    /** Chiave di raggruppamento: subscriber con filtri identici condividono lo stesso payload JSON. */
    private record FilterKey(String linea, String normalizedDestination, String vehicleId) {}

    private record Tick(List<VehiclePositionDTO> snapshot, Instant generatedAt) {}

    /** Evento da scrivere: il JSON dei mezzi, oppure (json null) l'heartbeat. */
    private record Outgoing(String json) {
        private static final Outgoing HEARTBEAT = new Outgoing(null);
    }

    private record Position(double lat, double lon) {}
}
