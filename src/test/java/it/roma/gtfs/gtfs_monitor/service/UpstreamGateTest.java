package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Il freno davanti ai geocoder pubblici: quante richieste lascia passare e
 * quanto resta chiuso dopo un rifiuto.
 */
class UpstreamGateTest {

    private static final long MINUTO = 60_000L;

    private final AtomicLong adesso = new AtomicLong(1_000_000L);
    private final UpstreamGate freno = new UpstreamGate("Nominatim", 1_000L, adesso::get);

    private static WebClientResponseException risposta(int stato, String retryAfter) {
        HttpHeaders headers = new HttpHeaders();
        if (retryAfter != null) {
            headers.add(HttpHeaders.RETRY_AFTER, retryAfter);
        }
        return WebClientResponseException.create(stato, "", headers, new byte[0], null);
    }

    @Test
    @DisplayName("passa una richiesta per intervallo, le altre no")
    void unaPerIntervallo() {
        assertThat(freno.tryEnter()).isTrue();
        assertThat(freno.tryEnter()).isFalse();

        adesso.addAndGet(999);
        assertThat(freno.tryEnter()).isFalse();

        adesso.addAndGet(1);
        assertThat(freno.tryEnter()).isTrue();
    }

    @Test
    @DisplayName("dopo un 429 non passa niente per cinque minuti")
    void pausaDopoUnRifiuto() {
        assertThat(freno.refusedBy(risposta(429, null))).isTrue();

        adesso.addAndGet(5 * MINUTO - 1);
        assertThat(freno.tryEnter()).isFalse();

        adesso.addAndGet(1);
        assertThat(freno.tryEnter()).isTrue();
    }

    @Test
    @DisplayName("anche un 403 e' un rifiuto: e' cosi' che Nominatim blocca chi esagera")
    void ancheIl403() {
        assertThat(freno.refusedBy(risposta(403, null))).isTrue();
        assertThat(freno.tryEnter()).isFalse();
    }

    @Test
    @DisplayName("ogni rifiuto di fila raddoppia la pausa, una risposta buona la riporta a cinque minuti")
    void laPausaRaddoppia() {
        freno.refusedBy(risposta(429, null));
        adesso.addAndGet(5 * MINUTO);
        freno.refusedBy(risposta(429, null));

        adesso.addAndGet(10 * MINUTO - 1);
        assertThat(freno.tryEnter()).isFalse();
        adesso.addAndGet(1);
        assertThat(freno.tryEnter()).isTrue();

        freno.succeeded();
        freno.refusedBy(risposta(429, null));
        adesso.addAndGet(5 * MINUTO);
        assertThat(freno.tryEnter()).isTrue();
    }

    @Test
    @DisplayName("se il servizio dice quanto aspettare, si aspetta almeno quello, ma non piu' di un'ora")
    void retryAfter() {
        freno.refusedBy(risposta(429, "900"));
        adesso.addAndGet(15 * MINUTO - 1);
        assertThat(freno.tryEnter()).isFalse();
        adesso.addAndGet(1);
        assertThat(freno.tryEnter()).isTrue();

        freno.refusedBy(risposta(429, "86400"));
        adesso.addAndGet(60 * MINUTO);
        assertThat(freno.tryEnter()).isTrue();
    }

    @Test
    @DisplayName("un errore che non e' un rifiuto non chiude il freno")
    void gliAltriErroriNo() {
        assertThat(freno.refusedBy(risposta(500, null))).isFalse();
        assertThat(freno.refusedBy(new IllegalStateException("Timeout on blocking read"))).isFalse();
        assertThat(freno.tryEnter()).isTrue();
    }
}
