package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Le informazioni di un treno da Viaggiatreno: la cache e il confronto con
 * l'ora chiesta. Nessuna richiesta esce davvero.
 */
class RailTrainInfoServiceTest {

    private final List<String> chiamate = new ArrayList<>();
    private Function<String, ClientResponse> risposte = path -> testo("");

    private final WebClient webClient = WebClient.builder()
            .exchangeFunction(request -> {
                String path = request.url().getPath();
                chiamate.add(path);
                return Mono.just(risposte.apply(path));
            })
            .build();
    private final RailTrainInfoService servizio = new RailTrainInfoService(webClient, new ObjectMapper());

    @BeforeEach
    void configura() {
        ReflectionTestUtils.setField(servizio, "viaggiatrenoEnabled", true);
        ReflectionTestUtils.setField(servizio, "viaggiatrenoBaseUrl", "https://www.viaggiatreno.it");
        ReflectionTestUtils.setField(servizio, "viaggiatrenoApiBaseUrl", "http://www.viaggiatreno.it");
    }

    private static ClientResponse testo(String body) {
        return ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN_VALUE)
                .body(body)
                .build();
    }

    @Test
    @DisplayName("la chiave della cache e' l'ora di Roma al minuto, qualunque sia il fuso del client")
    void chiaveAlMinutoSuRoma() {
        String daRoma = RailTrainInfoService.normalizeReferenceTimeKey(OffsetDateTime.parse("2026-07-15T10:30:12.345+02:00"));
        String daUtc = RailTrainInfoService.normalizeReferenceTimeKey(OffsetDateTime.parse("2026-07-15T08:30:59Z"));

        assertThat(daRoma).isEqualTo("2026-07-15T10:30");
        assertThat(daUtc).isEqualTo(daRoma);
    }

    @Test
    @DisplayName("lo stesso treno chiesto due volte nello stesso minuto esce una volta sola")
    void stessoMinutoInCache() {
        servizio.lookupTrainInfo("2135", "Roma Termini", OffsetDateTime.parse("2020-07-15T10:30:12+02:00"));
        servizio.lookupTrainInfo("2135", "Roma Termini", OffsetDateTime.parse("2020-07-15T08:30:48.5Z"));

        assertThat(chiamate).hasSize(1);
    }

    @Test
    @DisplayName("la cache non cresce oltre il tetto")
    void cacheLimitata() {
        OffsetDateTime inizio = OffsetDateTime.parse("2020-01-01T00:00:00+01:00");
        for (int i = 0; i < RailTrainInfoService.MAX_CACHE_ENTRIES + 100; i++) {
            servizio.lookupTrainInfo("2135", "Roma Termini", inizio.plusMinutes(i));
        }

        assertThat(servizio.cacheSize()).isLessThanOrEqualTo(RailTrainInfoService.MAX_CACHE_ENTRIES);
    }
}
