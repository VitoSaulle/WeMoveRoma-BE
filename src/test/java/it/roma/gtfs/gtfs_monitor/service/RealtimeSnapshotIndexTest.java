package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Gli indici per fermata, corsa, veicolo e linea costruiti una volta per snapshot. */
class RealtimeSnapshotIndexTest {

    private static final long FUTURO = 4_000_000_000L;

    private static GtfsRealtime.FeedEntity corsa(String tripId, String routeId, String vehicleId, String... fermate) {
        GtfsRealtime.TripUpdate.Builder tu = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId(tripId).setRouteId(routeId))
                .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId(vehicleId));
        long t = FUTURO;
        for (String f : fermate) {
            tu.addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId(f)
                    .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(t += 60)));
        }
        return GtfsRealtime.FeedEntity.newBuilder().setId(tripId).setTripUpdate(tu).build();
    }

    private static GtfsRealtime.FeedEntity mezzo(String tripId, String vehicleId) {
        return GtfsRealtime.FeedEntity.newBuilder()
                .setId(vehicleId)
                .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                        .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId(tripId).setRouteId("64"))
                        .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId(vehicleId)))
                .build();
    }

    @Test
    @DisplayName("arrivi: per fermata, per linea e per veicolo dallo stesso snapshot")
    void indiciArrivi() {
        FeedStub stub = new FeedStub();
        stub.serve(FeedStub.feed()
                .addEntity(corsa("T1", "64", "V1", "S1", "S2"))
                .addEntity(corsa("T2", "40", "V2", "S2", "S3"))
                .build());
        TripUpdatesService svc = new TripUpdatesService(FeedStub.props(60_000), stub.webClient, mock(GtfsIndexService.class));
        svc.scheduledRefresh();

        assertThat(svc.updatesForStop("S2")).extracting(TripUpdateDTO::getCorsa).containsExactly("T1", "T2");
        assertThat(svc.updatesForStop("S9")).isEmpty();
        assertThat(svc.fetch("40", null)).extracting(TripUpdateDTO::getFermataId).containsExactly("S2", "S3");
        assertThat(svc.fetch(null, null)).hasSize(4);
        assertThat(svc.fetch(null, 1)).hasSize(1);
        assertThat(svc.nextStopsByVehicle("V1", null)).extracting(TripUpdateDTO::getFermataId).containsExactly("S1", "S2");

        List<TripUpdateDTO> s2 = svc.updatesForStop("S2");
        assertThatThrownBy(() -> s2.add(s2.get(0))).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("mezzi: per corsa e per veicolo, a parita' vince il primo del feed")
    void indiciMezzi() {
        FeedStub stub = new FeedStub();
        stub.serve(FeedStub.feed()
                .addEntity(mezzo("T1", "V1"))
                .addEntity(mezzo("T1", "V9"))
                .addEntity(mezzo("T2", "V2"))
                .build());
        VehiclePositionsService svc = new VehiclePositionsService(FeedStub.props(60_000), stub.webClient,
                mock(GtfsIndexService.class), mock(VehiclePositionsSseService.class));
        svc.scheduledRefresh();

        assertThat(svc.vehicleByTrip("T1")).extracting(VehiclePositionDTO::getVeicolo).isEqualTo("V1");
        assertThat(svc.vehicleById("V2")).extracting(VehiclePositionDTO::getCorsa).isEqualTo("T2");
        assertThat(svc.vehicleByTrip("T7")).isNull();
        assertThat(svc.vehicleById(null)).isNull();

        List<VehiclePositionDTO> tutti = svc.fetch(null, null);
        assertThat(tutti).hasSize(3);
        assertThatThrownBy(() -> tutti.add(tutti.get(0))).isInstanceOf(UnsupportedOperationException.class);
    }
}
