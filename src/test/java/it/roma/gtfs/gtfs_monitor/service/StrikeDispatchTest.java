package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Quando partono, e quando no, le push degli scioperi. */
class StrikeDispatchTest {

    private static final String RSS = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0"><channel>
              <item>
                <guid>https://scioperi.mit.gov.it/mit2/public/scioperi/123</guid>
                <title>Settore: Trasporto Pubblico Locale - Regione: Lazio</title>
                <link>https://scioperi.mit.gov.it/123</link>
                <description>Settore: Trasporto Pubblico Locale&lt;br/&gt;Regione: Lazio&lt;br/&gt;Rilevanza: Regionale&lt;br/&gt;Data inizio: 12/06/2026</description>
              </item>
            </channel></rss>
            """;

    private FeedStub stub;
    private FcmDispatcherService fcm;
    private StrikesDispatchTracker tracker;
    private StrikeService svc;

    @BeforeEach
    void setUp() {
        stub = new FeedStub();
        stub.serve(RSS.strip().getBytes(StandardCharsets.UTF_8));
        fcm = mock(FcmDispatcherService.class);
        tracker = mock(StrikesDispatchTracker.class);
        when(fcm.isEnabled()).thenReturn(true);
        when(fcm.topicStrikes()).thenReturn("wemoveroma-strikes");
        svc = new StrikeService(stub.webClient, fcm, tracker);
        ReflectionTestUtils.setField(svc, "rssUrl", "http://feed/rss");
        ReflectionTestUtils.setField(svc, "sectorsCsv", "Trasporto Pubblico Locale");
        ReflectionTestUtils.setField(svc, "regionsCsv", "Italia,Lazio");
        ReflectionTestUtils.setField(svc, "dispatchOnStartup", true);
    }

    @Test
    @DisplayName("warmup con dispatch-on-startup=false: marca gli scioperi presenti senza spedire")
    void warmupSoloMarca() {
        ReflectionTestUtils.setField(svc, "dispatchOnStartup", false);

        svc.warmup();

        verify(fcm, never()).sendToTopic(anyString(), anyString(), anyString(), anyMap());
        verify(tracker).markAllSent(List.of("123"));

        // Il cron dopo spedisce di nuovo normalmente.
        when(fcm.sendToTopic(anyString(), anyString(), anyString(), any())).thenReturn(true);
        svc.scheduledRefresh();
        verify(fcm, times(1)).sendToTopic(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    @DisplayName("il refresh manuale (endpoint non autenticato) aggiorna i dati ma non spedisce")
    void refreshManualeNonSpedisce() {
        when(fcm.sendToTopic(anyString(), anyString(), anyString(), any())).thenReturn(true);

        assertThat(svc.refreshNow()).hasSize(1);

        verify(fcm, never()).sendToTopic(anyString(), anyString(), anyString(), anyMap());
        verify(tracker, never()).markSent(anyString());
        verify(tracker, never()).markAllSent(any());
    }

    @Test
    @DisplayName("una push fallita non marca lo sciopero: si ritenta al refresh dopo")
    void marcaSoloDopoInvioRiuscito() {
        when(fcm.sendToTopic(anyString(), anyString(), anyString(), any())).thenReturn(false, true);

        svc.scheduledRefresh();
        verify(tracker, never()).markSent(anyString());

        svc.scheduledRefresh();
        verify(fcm, times(2)).sendToTopic(anyString(), anyString(), anyString(), anyMap());
        verify(tracker).markSent("123");
    }

    @Test
    @DisplayName("uno sciopero gia' notificato non riparte")
    void giaNotificato() {
        when(tracker.isSent("123")).thenReturn(true);

        svc.scheduledRefresh();

        assertThat(svc.snapshot()).hasSize(1);
        verify(fcm, never()).sendToTopic(anyString(), anyString(), anyString(), anyMap());
    }
}
