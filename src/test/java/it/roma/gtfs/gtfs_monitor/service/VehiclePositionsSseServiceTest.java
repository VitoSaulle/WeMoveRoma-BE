package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fallback "veicolo predetto": quando lo snapshot GTFS-RT non ha mezzi per una
 * sottoscrizione, il servizio ne sintetizza uno dagli orari programmati,
 * interpolandone la posizione sullo shape della corsa.
 *
 * Il feed e' una fixture minima in @TempDir, come in GtfsIndexServiceScheduledStopsTest.
 * La versione precedente girava sul feed reale in data/gtfs_static — cartella gitignorata
 * da ~260 MB, quindi assente su un clone pulito — con data di servizio e trip_id copiati
 * da uno snapshot di allora. Ogni riscaricamento del feed li invalidava: quello del
 * 25/08/2026 copre 05/08-30/09, per cui il 07/03 non aveva piu' nessuna corsa attiva e
 * i due test diventavano rossi senza che la logica fosse cambiata.
 */
class VehiclePositionsSseServiceTest {

    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");
    private static final LocalDate SERVICE_DATE = LocalDate.of(2026, 3, 7);
    private static final String LINE = "990L";
    private static final String DESTINATION = "STAZ.NE METRO CIPRO (MA)";
    private static final String TRIP_ID = "0#3734-31";
    private static final String VEHICLE_ID = "sim-" + TRIP_ID;

    /** 20:14:06 a Roma: la corsa (20:00 -> 20:30) e' in viaggio da 14 minuti e 6 secondi. */
    private static final Instant NOW = SERVICE_DATE.atTime(20, 14, 6).atZone(ROME_ZONE).toInstant();

    @TempDir
    Path dataDir;

    private VehiclePositionsSseService sseService;

    @BeforeEach
    void setUp() throws IOException {
        write("stops.txt", """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                S1,C1,Cornelia,,41.9020,12.4300,,1,,0,
                S2,C2,Via Anastasio II,,41.9060,12.4405,,1,,0,
                S3,C3,Staz.ne Metro Cipro,,41.9075,12.4462,,1,,0,
                """);
        // La 990L ha una corsa sola; la MEA e' la linea fitta: due treni per
        // direzione a un quarto d'ora l'uno dall'altro, piu' una corsa che parte
        // alle 24:10 del giorno di servizio, cioe' dopo mezzanotte.
        write("routes.txt", """
                route_id,route_short_name,route_long_name
                R990,990L,Linea 990L
                RMEA,MEA,Metro A
                """);
        write("trips.txt", """
                route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
                R990,SVC1,0#3734-31,STAZ.NE METRO CIPRO (MA),,0,,SH990,1,0
                RMEA,SVC1,A-AND-1,ANAGNINA (MA),,0,,SH990,1,0
                RMEA,SVC1,A-AND-2,ANAGNINA (MA),,0,,SH990,1,0
                RMEA,SVC1,A-RIT-1,BATTISTINI (MA),,1,,SH990,1,0
                RMEA,SVC1,A-RIT-2,BATTISTINI (MA),,1,,SH990,1,0
                RMEA,SVC1,A-NOTTE,ANAGNINA (MA),,0,,SH990,1,0
                """);
        write("calendar_dates.txt", """
                service_id,date,exception_type
                SVC1,20260307,1
                """);
        // Tre punti equidistanti in sequenza: bastano a rendere verificabile l'interpolazione.
        write("shapes.txt", """
                shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
                SH990,41.9020,12.4300,1
                SH990,41.9060,12.4405,2
                SH990,41.9075,12.4462,3
                """);
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                0#3734-31,20:00:00,20:00:00,S1,1
                0#3734-31,20:15:00,20:15:00,S2,2
                0#3734-31,20:30:00,20:30:00,S3,3
                A-AND-1,10:00:00,10:00:00,S1,1
                A-AND-1,10:30:00,10:30:00,S3,2
                A-AND-2,10:15:00,10:15:00,S1,1
                A-AND-2,10:45:00,10:45:00,S3,2
                A-RIT-1,10:05:00,10:05:00,S3,1
                A-RIT-1,10:35:00,10:35:00,S1,2
                A-RIT-2,10:20:00,10:20:00,S3,1
                A-RIT-2,10:50:00,10:50:00,S1,2
                A-NOTTE,24:10:00,24:10:00,S1,1
                A-NOTTE,24:40:00,24:40:00,S3,2
                """);

        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L),
                null
        );
        GtfsIndexService indexService = new GtfsIndexService(props);
        indexService.init();
        indexService.rebuildIndexes();
        sseService = new VehiclePositionsSseService(indexService, new ObjectMapper());
    }

    @Test
    void simulateVehiclesReturnsPredictedVehicleForLineAndDestination() throws Exception {
        List<VehiclePositionDTO> vehicles = simulateVehicles(LINE, DESTINATION, NOW);

        assertFalse(vehicles.isEmpty());
        VehiclePositionDTO predicted = vehicles.getFirst();
        assertEquals(LINE, predicted.getLinea());
        assertEquals(TRIP_ID, predicted.getCorsa());
        assertEquals(VEHICLE_ID, predicted.getVeicolo());
        assertEquals(DESTINATION, predicted.getCapolinea());
        assertEquals(Boolean.TRUE, predicted.getWheelchairAccessible());
        assertInterpolatedAt47Percent(predicted);
    }

    @Test
    void simulateVehiclesIgnoresTripsOutsideTheVisibilityWindow() throws Exception {
        // La corsa parte alle 20:00, oltre i 45 minuti di anticipo ammessi: alle 18:00
        // non deve ancora comparire, ne' per oggi ne' ripiegando sul giorno dopo.
        Instant tooEarly = SERVICE_DATE.atTime(18, 0).atZone(ROME_ZONE).toInstant();

        assertTrue(simulateVehicles(LINE, DESTINATION, tooEarly).isEmpty());
    }

    @Test
    void unaLineaFittaHaUnMezzoPerOgniCorsaInViaggio() throws Exception {
        // Alle 10:25 sono in viaggio tutti e quattro i treni, due per direzione.
        Instant now = SERVICE_DATE.atTime(10, 25).atZone(ROME_ZONE).toInstant();

        List<VehiclePositionDTO> treni = simulateVehicles("MEA", null, now);

        assertEquals(
                List.of("sim-A-AND-1", "sim-A-RIT-1", "sim-A-AND-2", "sim-A-RIT-2"),
                treni.stream().map(VehiclePositionDTO::getVeicolo).toList());
        // E con la direzione scelta, solo quelli che ci vanno.
        assertEquals(
                List.of("sim-A-AND-1", "sim-A-AND-2"),
                simulateVehicles("MEA", "ANAGNINA (MA)", now).stream().map(VehiclePositionDTO::getVeicolo).toList());
    }

    @Test
    void leCorseNonAncoraPartiteNonSonoInViaggio() throws Exception {
        // Alle 10:10 la seconda verso Anagnina (10:15) e la seconda verso
        // Battistini (10:20) devono ancora partire: i treni sono due, non quattro.
        Instant now = SERVICE_DATE.atTime(10, 10).atZone(ROME_ZONE).toInstant();

        assertEquals(
                List.of("sim-A-AND-1", "sim-A-RIT-1"),
                simulateVehicles("MEA", null, now).stream().map(VehiclePositionDTO::getVeicolo).toList());
    }

    @Test
    void seNessunaCorsaEInViaggioRestaLaPiuVicinaAPartire() throws Exception {
        // Alle 19:40 la 990L non ha corse in viaggio, ma una parte fra venti
        // minuti: la mappa non deve svuotarsi fra una corsa e l'altra.
        Instant now = SERVICE_DATE.atTime(19, 40).atZone(ROME_ZONE).toInstant();

        List<VehiclePositionDTO> vehicles = simulateVehicles(LINE, null, now);

        assertEquals(List.of(VEHICLE_ID), vehicles.stream().map(VehiclePositionDTO::getVeicolo).toList());
    }

    @Test
    void dopoMezzanotteContaIlGiornoDiServizioDiIeri() throws Exception {
        // 00:20 dell'8 marzo: la corsa delle "24:10" appartiene ancora al 7.
        Instant now = SERVICE_DATE.plusDays(1).atTime(0, 20).atZone(ROME_ZONE).toInstant();

        assertEquals(
                List.of("sim-A-NOTTE"),
                simulateVehicles("MEA", null, now).stream().map(VehiclePositionDTO::getVeicolo).toList());
    }

    @Test
    void filterSnapshotKeepsSpecificPredictedVehicleAliveForSimulatedVehicleSubscriptions() throws Exception {
        Object subscription = newSubscription(LINE, "staz ne metro cipro ma", VEHICLE_ID);

        List<VehiclePositionDTO> vehicles = filterSnapshot(List.of(), subscription, NOW);

        assertFalse(vehicles.isEmpty());
        VehiclePositionDTO predicted = vehicles.getFirst();
        assertEquals(VEHICLE_ID, predicted.getVeicolo());
        assertEquals(LINE, predicted.getLinea());
        assertEquals(TRIP_ID, predicted.getCorsa());
        assertInterpolatedAt47Percent(predicted);
    }

    @Test
    void filterSnapshotDropsSubscriptionsToVehiclesThatAreNotSimulated() throws Exception {
        // Senza prefisso "sim-" non c'e' nessuna corsa da ricostruire: il veicolo reale
        // e' semplicemente sparito dallo snapshot e la lista resta vuota.
        Object subscription = newSubscription(LINE, null, "12345");

        assertTrue(filterSnapshot(List.of(), subscription, NOW).isEmpty());
    }

    /**
     * now = 20:14:06 su una corsa 20:00 -> 20:30 => progress 846/1800 = 0,47, che su
     * tre punti cade al 94% del primo segmento.
     */
    private static void assertInterpolatedAt47Percent(VehiclePositionDTO predicted) {
        assertEquals(41.9020 + (41.9060 - 41.9020) * 0.94, predicted.getLat(), 1e-4);
        assertEquals(12.4300 + (12.4405 - 12.4300) * 0.94, predicted.getLon(), 1e-4);
    }

    @SuppressWarnings("unchecked")
    private List<VehiclePositionDTO> simulateVehicles(String line, String destination, Instant now) throws Exception {
        Method simulateVehicles = VehiclePositionsSseService.class.getDeclaredMethod(
                "simulateVehicles", String.class, String.class, Instant.class);
        simulateVehicles.setAccessible(true);
        return (List<VehiclePositionDTO>) simulateVehicles.invoke(sseService, line, destination, now);
    }

    @SuppressWarnings("unchecked")
    private List<VehiclePositionDTO> filterSnapshot(List<VehiclePositionDTO> snapshot, Object subscription, Instant now)
            throws Exception {
        Method filterSnapshot = VehiclePositionsSseService.class.getDeclaredMethod(
                "filterSnapshot", List.class, subscriptionClass(), Instant.class);
        filterSnapshot.setAccessible(true);
        return (List<VehiclePositionDTO>) filterSnapshot.invoke(sseService, snapshot, subscription, now);
    }

    private static Object newSubscription(String line, String normalizedDestination, String vehicleId) throws Exception {
        Constructor<?> ctor = subscriptionClass()
                .getDeclaredConstructor(SseEmitter.class, String.class, String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(new SseEmitter(0L), line, normalizedDestination, vehicleId);
    }

    private static Class<?> subscriptionClass() throws ClassNotFoundException {
        return Class.forName("it.roma.gtfs.gtfs_monitor.service.VehiclePositionsSseService$Subscription");
    }

    private void write(String fileName, String content) throws IOException {
        Files.writeString(dataDir.resolve(fileName), content);
    }
}
