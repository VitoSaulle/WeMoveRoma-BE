package it.roma.gtfs.gtfs_monitor.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Il freno davanti a un geocoder pubblico e gratuito.
 *
 * Tutte le richieste degli utenti escono dallo stesso indirizzo, e questi
 * servizi mettono un tetto per chiamante (Nominatim: una al secondo). Senza
 * freno il server finiva limitato con un 429 e, continuando a chiedere a ogni
 * fermata aperta, ci restava: in produzione gli indirizzi erano spariti del
 * tutto. Qui si lascia passare una richiesta per intervallo, e dopo un rifiuto
 * non si chiede piu' niente per un po'.
 *
 * Chi non passa non aspetta: ripiega su un'altra fonte o risponde senza. Sono
 * richieste fatte mentre l'utente guarda, e una risposta vuota subito vale
 * piu' di una buona dopo una coda.
 */
@Slf4j
public class UpstreamGate {
    private static final long FIRST_PAUSE_MILLIS = Duration.ofMinutes(5).toMillis();
    private static final long MAX_PAUSE_MILLIS = Duration.ofHours(1).toMillis();

    private final String name;
    private final long minIntervalMillis;
    private final LongSupplier clock;

    private long nextAllowedAtMillis;
    private long pausedUntilMillis;
    private long nextPauseMillis = FIRST_PAUSE_MILLIS;

    public UpstreamGate(String name, long minIntervalMillis) {
        this(name, minIntervalMillis, System::currentTimeMillis);
    }

    UpstreamGate(String name, long minIntervalMillis, LongSupplier clock) {
        this.name = name;
        this.minIntervalMillis = minIntervalMillis;
        this.clock = clock;
    }

    /** Vero se la richiesta puo' partire adesso: chi lo riceve la fa. */
    public synchronized boolean tryEnter() {
        long now = clock.getAsLong();
        if (now < pausedUntilMillis || now < nextAllowedAtMillis) {
            return false;
        }
        nextAllowedAtMillis = now + minIntervalMillis;
        return true;
    }

    /** Una risposta buona: il prossimo rifiuto riparte dalla pausa breve. */
    public synchronized void succeeded() {
        nextPauseMillis = FIRST_PAUSE_MILLIS;
    }

    /**
     * Se l'errore e' un rifiuto del servizio (429, 403) mette in pausa e
     * risponde vero. Gli altri errori non dicono "state chiedendo troppo", e
     * restano di chi chiama.
     */
    public boolean refusedBy(Throwable error) {
        if (!(error instanceof WebClientResponseException response)) {
            return false;
        }
        int status = response.getStatusCode().value();
        if (status != 429 && status != 403) {
            return false;
        }
        pause(status, retryAfter(response.getHeaders()));
        return true;
    }

    private synchronized void pause(int status, Duration retryAfter) {
        // Ogni rifiuto di fila raddoppia: chi insiste con un servizio che ha
        // gia' detto no si fa tenere fuori piu' a lungo.
        long pauseMillis = nextPauseMillis;
        if (retryAfter != null) {
            pauseMillis = Math.max(pauseMillis, retryAfter.toMillis());
        }
        pauseMillis = Math.min(pauseMillis, MAX_PAUSE_MILLIS);
        pausedUntilMillis = clock.getAsLong() + pauseMillis;
        nextPauseMillis = Math.min(nextPauseMillis * 2, MAX_PAUSE_MILLIS);
        log.warn("[{}] HTTP {}: no requests for {} min", name, status, Duration.ofMillis(pauseMillis).toMinutes());
    }

    private static Duration retryAfter(HttpHeaders headers) {
        String value = headers == null ? null : headers.getFirst(HttpHeaders.RETRY_AFTER);
        if (value == null) {
            return null;
        }
        try {
            return Duration.ofSeconds(Long.parseLong(value.trim()));
        } catch (NumberFormatException e) {
            // La forma con la data e' ammessa ma qui non serve: vale la pausa nostra.
            return null;
        }
    }
}
