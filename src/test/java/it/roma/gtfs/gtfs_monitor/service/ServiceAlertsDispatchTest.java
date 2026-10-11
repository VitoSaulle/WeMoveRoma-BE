package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Da dove e quando partono le push degli avvisi. */
class ServiceAlertsDispatchTest {

    private FeedStub stub;
    private FcmDispatcherService fcm;
    private AlertsDispatchTracker tracker;
    private ServiceAlertsService svc;

    private static GtfsRealtime.FeedEntity sospensione(String id) {
        return GtfsRealtime.FeedEntity.newBuilder()
                .setId(id)
                .setAlert(GtfsRealtime.Alert.newBuilder()
                        .setEffect(GtfsRealtime.Alert.Effect.NO_SERVICE)
                        .addInformedEntity(GtfsRealtime.EntitySelector.newBuilder().setRouteId("64")))
                .build();
    }

    @BeforeEach
    void setUp() {
        stub = new FeedStub();
        fcm = mock(FcmDispatcherService.class);
        tracker = mock(AlertsDispatchTracker.class);
        when(fcm.topicAlerts()).thenReturn("wemoveroma-alerts");
        when(fcm.isEnabled()).thenReturn(true);
        svc = new ServiceAlertsService(FeedStub.props(60_000), stub.webClient, fcm, tracker);
        ReflectionTestUtils.setField(svc, "alertSeveritiesCsv", "SEVERE");
        ReflectionTestUtils.setField(svc, "minRoutesForPush", 5);
        ReflectionTestUtils.setField(svc, "maxAlertsPerRefresh", 5);
    }

    @Test
    @DisplayName("una richiesta HTTP che carica la cache a freddo non spedisce push")
    void richiestaNonSpedisce() {
        stub.serve(FeedStub.feed().addEntity(sospensione("A1")).build());

        assertThat(svc.fetchActiveNow(null, null)).hasSize(1);

        verify(fcm, never()).sendToTopic(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    @DisplayName("il poll schedulato spedisce, fuori dal lock del refresh")
    void pollSpedisceFuoriDalLock() {
        stub.serve(FeedStub.feed().addEntity(sospensione("A1")).build());
        Object lock = ReflectionTestUtils.getField(svc, "refreshLock");
        AtomicBoolean dentroIlLock = new AtomicBoolean();
        when(fcm.sendToTopic(anyString(), anyString(), anyString(), any())).thenAnswer(inv -> {
            dentroIlLock.set(Thread.holdsLock(lock));
            return true;
        });

        svc.scheduledRefresh();

        verify(fcm, times(1)).sendToTopic(anyString(), anyString(), anyString(), anyMap());
        assertThat(dentroIlLock).isFalse();
    }

    @Test
    @DisplayName("una push riuscita marca l'avviso, una fallita no e si ritenta al giro dopo")
    void marcaSoloDopoInvioRiuscito() {
        stub.serve(FeedStub.feed().addEntity(sospensione("A1")).build());
        when(fcm.sendToTopic(anyString(), anyString(), anyString(), any())).thenReturn(false, true);

        svc.scheduledRefresh();
        verify(tracker, never()).markSent(anyString());

        svc.scheduledRefresh();
        verify(fcm, times(2)).sendToTopic(anyString(), anyString(), anyString(), anyMap());
        verify(tracker, times(1)).markSent("A1@");
    }

    @Test
    @DisplayName("un avviso gia' notificato non riparte")
    void giaNotificatoNonRiparte() {
        stub.serve(FeedStub.feed().addEntity(sospensione("A1")).build());
        when(tracker.isSent("A1@")).thenReturn(true);

        svc.scheduledRefresh();

        verify(fcm, never()).sendToTopic(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    @DisplayName("una push che fallisce sempre si abbandona dopo qualche giro")
    void pushSempreFallitaSiAbbandona() {
        stub.serve(FeedStub.feed().addEntity(sospensione("A1")).build());
        when(fcm.sendToTopic(anyString(), anyString(), anyString(), any())).thenReturn(false);

        for (int i = 0; i < ServiceAlertsService.MAX_SEND_ATTEMPTS; i++) {
            svc.scheduledRefresh();
        }

        verify(tracker, times(1)).markSent("A1@");
    }

    @Test
    @DisplayName("con FCM spento non si tenta e non si marca niente")
    void fcmSpento() {
        stub.serve(FeedStub.feed().addEntity(sospensione("A1")).build());
        when(fcm.isEnabled()).thenReturn(false);

        svc.scheduledRefresh();

        verify(fcm, never()).sendToTopic(anyString(), anyString(), anyString(), anyMap());
        verify(tracker, never()).markSent(anyString());
    }
}
