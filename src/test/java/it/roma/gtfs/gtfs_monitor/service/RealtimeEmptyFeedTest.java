package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Cosa succede quando il feed arriva letto bene ma senza entita'. */
class RealtimeEmptyFeedTest {

    @Test
    @DisplayName("avvisi: un feed senza avvisi svuota la lista, non lascia gli ultimi per sempre")
    void avvisiFeedVuotoAccettato() {
        FeedStub stub = new FeedStub();
        stub.serve(FeedStub.feed().addEntity(GtfsRealtime.FeedEntity.newBuilder()
                .setId("A1")
                .setAlert(GtfsRealtime.Alert.newBuilder()
                        .setEffect(GtfsRealtime.Alert.Effect.DETOUR)
                        .addInformedEntity(GtfsRealtime.EntitySelector.newBuilder().setRouteId("64"))))
                .build());
        ServiceAlertsService svc = new ServiceAlertsService(FeedStub.props(60_000), stub.webClient,
                mock(FcmDispatcherService.class), mock(AlertsDispatchTracker.class));

        svc.scheduledRefresh();
        assertThat(svc.fetchActiveNow(null, null)).hasSize(1);

        stub.serve(FeedStub.feed().build());
        svc.scheduledRefresh();
        assertThat(svc.fetchActiveNow(null, null)).isEmpty();
    }

    @Test
    @DisplayName("mezzi: un solo feed vuoto e' un singhiozzo, i mezzi restano")
    void mezziFeedVuotoIsolatoIgnorato() {
        FeedStub stub = new FeedStub();
        stub.serve(FeedStub.feed().addEntity(GtfsRealtime.FeedEntity.newBuilder()
                .setId("v1")
                .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                        .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId("V1"))))
                .build());
        VehiclePositionsService svc = new VehiclePositionsService(FeedStub.props(60_000), stub.webClient,
                mock(GtfsIndexService.class), mock(VehiclePositionsSseService.class));

        svc.scheduledRefresh();
        stub.serve(FeedStub.feed().build());
        svc.scheduledRefresh();

        assertThat(svc.fetch(null, null)).hasSize(1);
    }
}
