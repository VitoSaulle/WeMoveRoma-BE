package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * La cache degli indici per-data tiene ieri, oggi e domani rispetto a oggi, non
 * rispetto all'ultima data chiesta.
 */
class GtfsIndexServicePerDateCacheTest {

    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");
    private static final LocalDate OGGI = LocalDate.of(2026, 7, 21);

    @TempDir
    Path dataDir;

    private GtfsIndexService service;

    @BeforeEach
    void setUp() throws Exception {
        write("stops.txt", """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                S1,C1,Prima,,41.9,12.5,,1,,0,
                """);
        write("routes.txt", """
                route_id,route_short_name,route_long_name
                R1,64,Linea 64
                """);
        write("trips.txt", """
                route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
                R1,SVC1,T1,Termini,,0,,,1,0
                """);
        write("calendar_dates.txt", """
                service_id,date,exception_type
                SVC1,20260720,1
                SVC1,20260721,1
                SVC1,20260722,1
                SVC1,20260730,1
                """);
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                T1,08:00:00,08:00:00,S1,1
                """);
        service = new GtfsIndexService(new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L), null));
        service.setClock(Clock.fixed(OGGI.atTime(12, 0).atZone(ROME_ZONE).toInstant(), ROME_ZONE));
        service.init();
        service.rebuildIndexes();
        service.awaitWarmup();
    }

    @Test
    void ieriEDomaniNonSiEspellonoAVicenda() throws Exception {
        assertEquals(List.of("64"), service.linesServingStop("S1", OGGI.minusDays(1)));
        assertEquals(List.of("64"), service.linesServingStop("S1", OGGI.plusDays(1)));

        // Prima la finestra era intorno all'ultima data inserita: chiedere il 22
        // buttava fuori il 20, e viceversa.
        assertEquals(Set.of(OGGI.minusDays(1), OGGI, OGGI.plusDays(1)), dateInCache());
    }

    @Test
    void unaDataLontanaSiCalcolaMaNonEntraInCache() throws Exception {
        service.linesServingStop("S1", OGGI.minusDays(1));
        Set<LocalDate> prima = dateInCache();

        assertEquals(List.of("64"), service.linesServingStop("S1", LocalDate.of(2026, 7, 30)));

        assertEquals(prima, dateInCache());
    }

    @SuppressWarnings("unchecked")
    private Set<LocalDate> dateInCache() throws Exception {
        Field f = GtfsIndexService.class.getDeclaredField("scheduledByDateRef");
        f.setAccessible(true);
        return Set.copyOf(((AtomicReference<Map<LocalDate, ?>>) f.get(service)).get().keySet());
    }

    private void write(String nome, String contenuto) throws IOException {
        Files.writeString(dataDir.resolve(nome), contenuto);
    }
}
