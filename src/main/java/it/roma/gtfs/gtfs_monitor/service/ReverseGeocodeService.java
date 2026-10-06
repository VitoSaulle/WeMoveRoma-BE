package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ReverseGeocodeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Da coordinate a indirizzo. Una fermata ha la via gia' calcolata (vedi
 * {@link StopAddressService}); per ogni altro punto si chiede a Nominatim e,
 * se non risponde o non puo' essere interrogato, a Photon: sono due servizi
 * pubblici e gratuiti, ognuno dietro il suo freno (vedi {@link UpstreamGate}).
 */
@Slf4j
@Service
public class ReverseGeocodeService {
    private static final long TTL_MILLIS = Duration.ofHours(6).toMillis();
    /*
     * Un tentativo fallito non si ripete subito. Prima non veniva ricordato
     * affatto: ogni apertura della stessa fermata richiedeva di nuovo, e
     * proprio mentre il servizio stava rifiutando.
     */
    private static final long FAILURE_TTL_MILLIS = Duration.ofMinutes(2).toMillis();
    /** I mezzi seguiti chiedono un punto nuovo a ogni spostamento: senza tetto la cache cresce e basta. */
    private static final int MAX_CACHE_ENTRIES = 20_000;
    private static final long PHOTON_MIN_INTERVAL_MILLIS = 500L;
    /** Meta' ciascuno del tetto che aveva Nominatim da solo: nel caso peggiore si aspetta quanto prima. */
    private static final Duration UPSTREAM_TIMEOUT = Duration.ofSeconds(4);

    private final WebClient webClient;
    private final StopAddressService stopAddresses;
    private final UpstreamGate nominatimGate;
    private final UpstreamGate photonGate;
    private final LongSupplier clock;
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    @Autowired
    public ReverseGeocodeService(WebClient webClient, StopAddressService stopAddresses, NominatimGate nominatimGate) {
        this(webClient, stopAddresses, nominatimGate,
                new UpstreamGate("Photon", PHOTON_MIN_INTERVAL_MILLIS), System::currentTimeMillis);
    }

    ReverseGeocodeService(WebClient webClient, StopAddressService stopAddresses,
                          UpstreamGate nominatimGate, UpstreamGate photonGate, LongSupplier clock) {
        this.webClient = webClient;
        this.stopAddresses = stopAddresses;
        this.nominatimGate = nominatimGate;
        this.photonGate = photonGate;
        this.clock = clock;
    }

    public ReverseGeocodeDTO reverse(double lat, double lon) {
        Optional<ReverseGeocodeDTO> atStop = stopAddresses.find(lat, lon);
        if (atStop.isPresent()) {
            return atStop.get();
        }

        String key = cacheKey(lat, lon);
        CacheEntry cached = cache.get(key);
        if (cached != null && clock.getAsLong() < cached.expiresAtMillis) {
            return cached.value;
        }

        ReverseGeocodeDTO empty = new ReverseGeocodeDTO(lat, lon, null, null, null, null);
        Attempt nominatim = askNominatim(lat, lon);
        if (nominatim.found()) {
            return remember(key, nominatim.value(), TTL_MILLIS);
        }
        Attempt photon = askPhoton(lat, lon);
        if (photon.found()) {
            return remember(key, photon.value(), TTL_MILLIS);
        }
        // Qualcuno ha risposto, e li' un indirizzo non c'e': vale quanto un indirizzo.
        if (nominatim.outcome() == Outcome.ANSWERED || photon.outcome() == Outcome.ANSWERED) {
            return remember(key, empty, TTL_MILLIS);
        }
        if (nominatim.outcome() == Outcome.FAILED || photon.outcome() == Outcome.FAILED) {
            return remember(key, empty, FAILURE_TTL_MILLIS);
        }
        // Nessuno e' stato interrogato (freni chiusi): la prossima richiesta puo' passare.
        return empty;
    }

    @SuppressWarnings("unchecked")
    private Attempt askNominatim(double lat, double lon) {
        if (!nominatimGate.tryEnter()) {
            return Attempt.SKIPPED;
        }
        try {
            Map<String, Object> payload = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .scheme("https")
                            .host("nominatim.openstreetmap.org")
                            .path("/reverse")
                            .queryParam("format", "jsonv2")
                            .queryParam("lat", lat)
                            .queryParam("lon", lon)
                            .queryParam("addressdetails", 1)
                            .queryParam("accept-language", "it")
                            .build())
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(UPSTREAM_TIMEOUT);
            nominatimGate.succeeded();
            return Attempt.answered(fromNominatim(lat, lon, payload));
        } catch (Exception e) {
            // Mai le coordinate nei log: possono essere il punto scelto da un utente.
            if (!nominatimGate.refusedBy(e)) {
                log.warn("[ReverseGeocode] Nominatim failed: {}", e.toString());
            }
            return Attempt.FAILED;
        }
    }

    @SuppressWarnings("unchecked")
    private Attempt askPhoton(double lat, double lon) {
        if (!photonGate.tryEnter()) {
            return Attempt.SKIPPED;
        }
        try {
            Map<String, Object> payload = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .scheme("https")
                            .host("photon.komoot.io")
                            .path("/reverse")
                            .queryParam("lat", lat)
                            .queryParam("lon", lon)
                            .queryParam("limit", 1)
                            // Solo vie e civici: senza, il piu' vicino puo' essere un parco o un negozio.
                            .queryParam("layer", "street", "house")
                            .build())
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(UPSTREAM_TIMEOUT);
            photonGate.succeeded();
            return Attempt.answered(fromPhoton(lat, lon, payload));
        } catch (Exception e) {
            if (!photonGate.refusedBy(e)) {
                log.warn("[ReverseGeocode] Photon failed: {}", e.toString());
            }
            return Attempt.FAILED;
        }
    }

    @SuppressWarnings("unchecked")
    private static ReverseGeocodeDTO fromNominatim(double lat, double lon, Map<String, Object> payload) {
        if (payload == null) {
            return null;
        }
        String displayName = asString(payload.get("display_name"));
        Map<String, Object> address = payload.get("address") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        String road = firstNonBlank(
                asString(address.get("road")),
                asString(address.get("pedestrian")),
                asString(address.get("footway")),
                asString(address.get("path")),
                asString(address.get("residential"))
        );
        String city = firstNonBlank(
                asString(address.get("city")),
                asString(address.get("town")),
                asString(address.get("village")),
                asString(address.get("municipality")),
                asString(address.get("county"))
        );
        String country = asString(address.get("country"));

        String label = firstNonBlank(compactLabel(road, city), displayName);
        return new ReverseGeocodeDTO(lat, lon, label, road, city, country);
    }

    /**
     * Photon risponde in GeoJSON. Di una via il nome sta in {@code name}; di un
     * civico o di un negozio {@code name} e' il suo, e la via sta in {@code street}.
     */
    @SuppressWarnings("unchecked")
    private static ReverseGeocodeDTO fromPhoton(double lat, double lon, Map<String, Object> payload) {
        if (payload == null
                || !(payload.get("features") instanceof List<?> features)
                || features.isEmpty()
                || !(features.get(0) instanceof Map<?, ?> feature)
                || !(feature.get("properties") instanceof Map<?, ?> rawProperties)) {
            return null;
        }
        Map<String, Object> properties = (Map<String, Object>) rawProperties;
        String name = asString(properties.get("name"));
        String road = firstNonBlank(
                asString(properties.get("street")),
                "street".equals(asString(properties.get("type"))) ? name : null
        );
        String city = firstNonBlank(
                asString(properties.get("city")),
                asString(properties.get("county"))
        );
        String country = asString(properties.get("country"));

        String label = firstNonBlank(compactLabel(road, city), name);
        return new ReverseGeocodeDTO(lat, lon, label, road, city, country);
    }

    private ReverseGeocodeDTO remember(String key, ReverseGeocodeDTO value, long ttlMillis) {
        long now = clock.getAsLong();
        if (cache.size() >= MAX_CACHE_ENTRIES) {
            cache.values().removeIf(entry -> now >= entry.expiresAtMillis);
            if (cache.size() >= MAX_CACHE_ENTRIES) {
                cache.clear();
            }
        }
        cache.put(key, new CacheEntry(value, now + ttlMillis));
        return value;
    }

    private static String compactLabel(String street, String city) {
        if (street == null && city == null) return null;
        if (street == null) return city;
        if (city == null) return street;
        return street + ", " + city;
    }

    private static String asString(Object value) {
        if (value == null) return null;
        String s = Objects.toString(value, null);
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static String cacheKey(double lat, double lon) {
        double latR = Math.round(lat * 10000.0d) / 10000.0d;
        double lonR = Math.round(lon * 10000.0d) / 10000.0d;
        return latR + "|" + lonR;
    }

    private enum Outcome { ANSWERED, FAILED, SKIPPED }

    /** Com'e' andata con una fonte: {@code value} c'e' solo se ha risposto con qualcosa. */
    private record Attempt(Outcome outcome, ReverseGeocodeDTO value) {
        static final Attempt SKIPPED = new Attempt(Outcome.SKIPPED, null);
        static final Attempt FAILED = new Attempt(Outcome.FAILED, null);

        static Attempt answered(ReverseGeocodeDTO value) {
            return new Attempt(Outcome.ANSWERED, value);
        }

        boolean found() {
            return value != null && (value.label() != null || value.street() != null);
        }
    }

    private record CacheEntry(ReverseGeocodeDTO value, long expiresAtMillis) {}
}
