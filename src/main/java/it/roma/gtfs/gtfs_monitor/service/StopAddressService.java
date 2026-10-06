package it.roma.gtfs.gtfs_monitor.service;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import it.roma.gtfs.gtfs_monitor.model.dto.ReverseGeocodeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * La via delle fermate, senza chiederla a nessuno.
 *
 * Le fermate non si spostano, eppure la loro via veniva chiesta a un geocoder
 * pubblico a ogni apertura: sono la gran parte delle richieste, e sono quelle
 * che hanno fatto limitare il server. La via di ognuna e' calcolata una volta
 * dall'estratto OpenStreetMap ({@code scripts/build-stop-addresses.py}) e sta
 * in un file caricato all'avvio.
 *
 * Si cerca per coordinate e non per codice di fermata: chi chiede l'indirizzo
 * manda un punto, e cosi' sito e app non cambiano. Una fermata che nel file
 * manca, o che il feed ha spostato, semplicemente non si trova: per quella si
 * chiede ancora fuori.
 */
@Slf4j
@Service
public class StopAddressService {
    private static final String RESOURCE = "geocode/stop-addresses.csv";
    /**
     * Quanto puo' distare il punto chiesto dalla fermata del file. Le
     * coordinate che serviamo sono quelle del feed a meno dell'arrotondamento,
     * quindi basterebbe un metro: dieci coprono un ritocco del feed senza
     * arrivare alla fermata di fronte.
     */
    private static final double MAX_METERS = 10.0;
    private static final double CELL_METERS = 25.0;
    // Metri per grado alla latitudine di Roma, come nello script.
    private static final double M_PER_DEG_LAT = 110_574.0;
    private static final double M_PER_DEG_LON = 111_320.0 * Math.cos(Math.toRadians(41.9));

    private final Map<Long, List<Entry>> cells = new HashMap<>();
    private int size;

    public StopAddressService() {
        this(new ClassPathResource(RESOURCE));
    }

    StopAddressService(Resource resource) {
        if (!resource.exists()) {
            log.warn("[StopAddress] {} not found: every address will be asked upstream", RESOURCE);
            return;
        }
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setLineSeparatorDetectionEnabled(true);
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
            for (Record row : new CsvParser(settings).iterateRecords(reader)) {
                String street = row.getString("street");
                Double lat = row.getDouble("lat");
                Double lon = row.getDouble("lon");
                if (street == null || street.isBlank() || lat == null || lon == null) {
                    continue;
                }
                String city = row.getString("city");
                Entry entry = new Entry(x(lon), y(lat), street.trim(), city == null || city.isBlank() ? null : city.trim());
                cells.computeIfAbsent(cell(entry.x, entry.y), k -> new ArrayList<>(2)).add(entry);
                size++;
            }
            log.info("[StopAddress] loaded {} stop addresses", size);
        } catch (Exception e) {
            // Senza file il servizio funziona lo stesso, solo chiedendo fuori.
            log.warn("[StopAddress] could not read {}: {}", RESOURCE, e.toString());
            cells.clear();
            size = 0;
        }
    }

    /** L'indirizzo della fermata che sta nel punto, se li' ce n'e' una. */
    public Optional<ReverseGeocodeDTO> find(double lat, double lon) {
        double px = x(lon);
        double py = y(lat);
        Entry nearest = null;
        double nearestMeters = MAX_METERS;
        for (int dx = -1; dx <= 1; dx++) {
            for (int dy = -1; dy <= 1; dy++) {
                List<Entry> entries = cells.get(cell(px + dx * CELL_METERS, py + dy * CELL_METERS));
                if (entries == null) continue;
                for (Entry entry : entries) {
                    double meters = Math.hypot(entry.x - px, entry.y - py);
                    if (meters <= nearestMeters) {
                        nearest = entry;
                        nearestMeters = meters;
                    }
                }
            }
        }
        if (nearest == null) {
            return Optional.empty();
        }
        String label = nearest.city == null ? nearest.street : nearest.street + ", " + nearest.city;
        return Optional.of(new ReverseGeocodeDTO(lat, lon, label, nearest.street, nearest.city, "Italia"));
    }

    int size() {
        return size;
    }

    private static double x(double lon) {
        return lon * M_PER_DEG_LON;
    }

    private static double y(double lat) {
        return lat * M_PER_DEG_LAT;
    }

    private static long cell(double x, double y) {
        return (((long) Math.floor(x / CELL_METERS)) << 32) ^ (((long) Math.floor(y / CELL_METERS)) & 0xffffffffL);
    }

    private record Entry(double x, double y, String street, String city) {}
}
