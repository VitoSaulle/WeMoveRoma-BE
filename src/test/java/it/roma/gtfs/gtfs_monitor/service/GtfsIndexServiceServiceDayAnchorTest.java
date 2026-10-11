package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Gli orari GTFS si contano da "mezzogiorno meno 12 ore", non dalla mezzanotte:
 * nei giorni del cambio d'ora le due cose differiscono di un'ora.
 */
class GtfsIndexServiceServiceDayAnchorTest {

    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");

    @TempDir
    Path dataDir;

    @Test
    void ritornoAllOraSolare() {
        LocalDate giorno = LocalDate.of(2026, 10, 25);
        long ancora = GtfsIndexService.serviceDayAnchorEpochSeconds(giorno);
        // Mezzogiorno CET meno 12h = 01:00 CEST, un'ora dopo la mezzanotte.
        assertEquals(giorno.atStartOfDay(ROME_ZONE).toEpochSecond() + 3600, ancora);
        assertEquals(giorno.atTime(8, 0).atZone(ROME_ZONE).toEpochSecond(), ancora + 8 * 3600);
    }

    @Test
    void passaggioAllOraLegale() {
        LocalDate giorno = LocalDate.of(2026, 3, 29);
        long ancora = GtfsIndexService.serviceDayAnchorEpochSeconds(giorno);
        // Mezzogiorno CEST meno 12h = 23:00 CET del giorno prima.
        assertEquals(giorno.atStartOfDay(ROME_ZONE).toEpochSecond() - 3600, ancora);
        assertEquals(giorno.atTime(8, 0).atZone(ROME_ZONE).toEpochSecond(), ancora + 8 * 3600);
    }

    @Test
    void giornoNormaleCoincideConLaMezzanotte() {
        LocalDate giorno = LocalDate.of(2026, 7, 21);
        assertEquals(giorno.atStartOfDay(ROME_ZONE).toEpochSecond(),
                GtfsIndexService.serviceDayAnchorEpochSeconds(giorno));
    }

    @Test
    void lePartenzeDelGiornoDelCambioOraSonoAllOraScritta() throws IOException {
        write("stops.txt", """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                S1,C1,Prima,,41.9,12.5,,1,,0,
                S2,C2,Seconda,,41.91,12.51,,1,,0,
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
                SVC1,20261025,1
                """);
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                T1,08:00:00,08:00:00,S1,1
                T1,08:10:00,08:10:00,S2,2
                """);
        GtfsIndexService service = new GtfsIndexService(new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L), null));
        service.init();
        service.rebuildIndexes();

        LocalDate giorno = LocalDate.of(2026, 10, 25);
        Instant quando = giorno.atTime(7, 0).atZone(ROME_ZONE).toInstant();
        List<GtfsIndexService.ScheduledTripStop> fermate = service.scheduledStopsForTrip("T1", quando);

        assertEquals(giorno.atTime(8, 0).atZone(ROME_ZONE).toInstant(), fermate.getFirst().arrivalTime());
    }

    private void write(String nome, String contenuto) throws IOException {
        Files.writeString(dataDir.resolve(nome), contenuto);
    }
}
