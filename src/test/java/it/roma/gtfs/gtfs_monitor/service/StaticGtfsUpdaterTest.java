package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lo stato del client (ETag, Last-Modified) cambia solo quando il feed nuovo e'
 * davvero in uso; un feed scartato lascia file e indici di prima.
 */
class StaticGtfsUpdaterTest {

    private static final String STOPS = """
            stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
            S1,C1,Prima,,41.9,12.5,,1,,0,
            """;
    private static final String ROUTES = """
            route_id,route_short_name,route_long_name
            R1,64,Linea 64
            """;
    private static final String TRIPS = """
            route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
            R1,SVC1,T1,Termini,,0,,,1,0
            """;
    private static final String TRIPS_VUOTO = """
            route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
            """;
    private static final String STOP_TIMES = """
            trip_id,arrival_time,departure_time,stop_id,stop_sequence
            T1,08:00:00,08:00:00,S1,1
            """;
    private static final String LAST_MODIFIED = "Thu, 01 Oct 2026 10:00:00 GMT";

    @TempDir
    Path root;

    private Path dataDir;
    private GtfsIndexService indexService;
    private StaticGtfsUpdater updater;

    @BeforeEach
    void setUp() throws IOException {
        dataDir = root.resolve("gtfs");
        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps("http://localhost/unused", dataDir.toString(), 0L), null);
        indexService = new GtfsIndexService(props);
        indexService.init();
        updater = new StaticGtfsUpdater(props, WebClient.create(), indexService);

        // Feed vecchio gia' sul disco e in memoria.
        Files.writeString(dataDir.resolve("stops.txt"), STOPS);
        Files.writeString(dataDir.resolve("routes.txt"), ROUTES);
        Files.writeString(dataDir.resolve("trips.txt"), TRIPS);
        Files.writeString(dataDir.resolve("stop_times.txt"), STOP_TIMES);
        indexService.rebuildIndexes();
    }

    @Test
    void zipIllegibileNonAggiornaLoStato() {
        updater.handleZipResponse(risposta("non sono uno zip".getBytes(StandardCharsets.UTF_8))).block();

        // Prima ETag e Last-Modified erano gia' quelli nuovi: il retry riceveva
        // 304 e smetteva di riprovare, sui dati vecchi.
        assertNull(updater.feedLastModified());
        assertFalse(Files.exists(dataDir.resolve(".gtfs_static_state.json")));
        assertTrue(Files.isRegularFile(dataDir.resolve("trips.txt")));
    }

    @Test
    void feedSenzaCorseRipristinaIlBackupELoStato() throws IOException {
        updater.handleZipResponse(risposta(zip(TRIPS_VUOTO))).block();

        assertNull(updater.feedLastModified());
        assertEquals(TRIPS, Files.readString(dataDir.resolve("trips.txt")), "deve tornare il feed di prima");
        assertTrue(indexService.isStaticDataLoaded(), "gli indici di prima restano in uso");
        assertTrue(indexService.tripById("T1").isPresent());
    }

    @Test
    void feedValidoAggiornaLoStato() throws IOException {
        updater.handleZipResponse(risposta(zip(TRIPS))).block();

        assertEquals(LAST_MODIFIED, updater.feedLastModified());
        String stato = Files.readString(dataDir.resolve(".gtfs_static_state.json"));
        assertTrue(stato.contains("v2"), stato);
        assertTrue(stato.contains(LAST_MODIFIED), stato);
    }

    private static ResponseEntity<byte[]> risposta(byte[] body) {
        return ResponseEntity.ok()
                .header(HttpHeaders.ETAG, "\"v2\"")
                .header(HttpHeaders.LAST_MODIFIED, LAST_MODIFIED)
                .body(body);
    }

    private static byte[] zip(String trips) throws IOException {
        Map<String, String> files = new LinkedHashMap<>();
        files.put("stops.txt", STOPS);
        files.put("routes.txt", ROUTES);
        files.put("trips.txt", trips);
        files.put("stop_times.txt", STOP_TIMES);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            for (Map.Entry<String, String> f : files.entrySet()) {
                zip.putNextEntry(new ZipEntry(f.getKey()));
                zip.write(f.getValue().getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
        }
        return bytes.toByteArray();
    }
}
