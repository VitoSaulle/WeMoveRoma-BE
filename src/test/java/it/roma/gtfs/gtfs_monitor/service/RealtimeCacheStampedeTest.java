package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Con l'upstream giu' le richieste HTTP non devono mettersi a scaricare ognuna
 * per conto suo: un fallimento conta come tentativo per un intervallo, e con
 * dati in cache la richiesta serve quelli senza scaricare.
 */
class RealtimeCacheStampedeTest {

    private static GtfsRealtime.FeedMessage feedConUnaCorsa() {
        return FeedStub.feed().addEntity(GtfsRealtime.FeedEntity.newBuilder()
                .setId("e1")
                .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                        .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("T1").setRouteId("64"))
                        .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                                .setStopId("S1")
                                .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(2_000_000_000L)))))
                .build();
    }

    private static GtfsRealtime.FeedMessage feedConUnMezzo() {
        return FeedStub.feed().addEntity(GtfsRealtime.FeedEntity.newBuilder()
                .setId("v1")
                .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                        .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("T1").setRouteId("64"))
                        .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId("V1"))))
                .build();
    }

    @Test
    @DisplayName("arrivi: avvio a freddo con upstream giu', un solo tentativo per intervallo")
    void tripUpdatesFallimentoAFreddo() {
        FeedStub stub = new FeedStub();
        stub.down();
        TripUpdatesService svc = new TripUpdatesService(FeedStub.props(60_000), stub.webClient, mock(GtfsIndexService.class));

        assertThat(svc.fetch(null, null)).isEmpty();
        assertThat(svc.fetch(null, null)).isEmpty();
        assertThat(svc.nextStopsByVehicle("V1", null)).isEmpty();

        assertThat(stub.calls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("arrivi: con dati in cache la richiesta non scarica, nemmeno se scaduti")
    void tripUpdatesRichiestaServeLaCache() throws Exception {
        FeedStub stub = new FeedStub();
        stub.serve(feedConUnaCorsa());
        TripUpdatesService svc = new TripUpdatesService(FeedStub.props(1), stub.webClient, mock(GtfsIndexService.class));

        svc.scheduledRefresh();
        assertThat(stub.calls.get()).isEqualTo(1);

        stub.down();
        Thread.sleep(10);
        for (int i = 0; i < 5; i++) {
            assertThat(svc.fetch(null, null)).hasSize(1);
        }
        assertThat(stub.calls.get()).isEqualTo(1);

        // Il poll schedulato fallisce: i dati vecchi restano serviti.
        svc.scheduledRefresh();
        assertThat(stub.calls.get()).isEqualTo(2);
        assertThat(svc.fetch(null, null)).hasSize(1);
    }

    @Test
    @DisplayName("mezzi: avvio a freddo con upstream giu', un solo tentativo per intervallo")
    void vehiclesFallimentoAFreddo() {
        FeedStub stub = new FeedStub();
        stub.down();
        VehiclePositionsService svc = new VehiclePositionsService(FeedStub.props(60_000), stub.webClient,
                mock(GtfsIndexService.class), mock(VehiclePositionsSseService.class));

        assertThat(svc.fetch(null, null)).isEmpty();
        assertThat(svc.lastSnapshotAt()).isNull();
        assertThat(svc.fetch(null, null)).isEmpty();

        assertThat(stub.calls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("mezzi: con dati in cache la richiesta non scarica, nemmeno se scaduti")
    void vehiclesRichiestaServeLaCache() throws Exception {
        FeedStub stub = new FeedStub();
        stub.serve(feedConUnMezzo());
        VehiclePositionsService svc = new VehiclePositionsService(FeedStub.props(1), stub.webClient,
                mock(GtfsIndexService.class), mock(VehiclePositionsSseService.class));

        svc.scheduledRefresh();
        stub.down();
        Thread.sleep(10);
        assertThat(svc.fetch(null, null)).hasSize(1);
        assertThat(svc.lastSnapshotAt()).isNotNull();
        assertThat(stub.calls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("avvisi: avvio a freddo con upstream giu', un solo tentativo per intervallo")
    void alertsFallimentoAFreddo() {
        FeedStub stub = new FeedStub();
        stub.down();
        ServiceAlertsService svc = new ServiceAlertsService(FeedStub.props(60_000), stub.webClient,
                mock(FcmDispatcherService.class), mock(AlertsDispatchTracker.class));

        assertThat(svc.fetchActiveNow(null, null)).isEmpty();
        assertThat(svc.fetchActiveNow(null, null)).isEmpty();

        assertThat(stub.calls.get()).isEqualTo(1);
    }
}
