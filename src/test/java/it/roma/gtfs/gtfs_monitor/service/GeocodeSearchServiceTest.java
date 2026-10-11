package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.GeocodeSearchResultDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La ricerca degli indirizzi: quanto tiene in memoria e cosa risponde quando
 * Photon non c'e'. Nessuna richiesta esce davvero.
 */
class GeocodeSearchServiceTest {

    private static final String PHOTON = "photon.komoot.io";
    private static final long MINUTO = 60_000L;

    private static final String PHOTON_GRACCHI = """
            {"features":[{"geometry":{"type":"Point","coordinates":[12.4630,41.9070]},
             "properties":{"type":"street","name":"Via dei Gracchi","city":"Roma","country":"Italia","osm_key":"highway"}}]}""";

    private final AtomicLong adesso = new AtomicLong(1_000_000L);
    private final List<String> chiamate = new ArrayList<>();
    private final AtomicReference<HttpStatus> statoPhoton = new AtomicReference<>(HttpStatus.OK);

    private final WebClient webClient = WebClient.builder()
            .exchangeFunction(request -> {
                String host = request.url().getHost();
                chiamate.add(host);
                if (PHOTON.equals(host)) {
                    if (statoPhoton.get() != HttpStatus.OK) {
                        return Mono.just(ClientResponse.create(statoPhoton.get()).build());
                    }
                    return Mono.just(json(PHOTON_GRACCHI));
                }
                return Mono.just(json("[]"));
            })
            .build();
    // Il freno di Nominatim non si riapre mai (il tempo dei test avanza solo a mano): resta Photon.
    private final UpstreamGate frenoNominatim = new UpstreamGate("Nominatim", Long.MAX_VALUE / 2, adesso::get);
    // Qui Photon passa sempre; il freno vero e' provato a parte.
    private final UpstreamGate frenoPhotonAperto = new UpstreamGate("Photon", 0L, adesso::get);
    private final GeocodeSearchService servizio =
            new GeocodeSearchService(webClient, frenoNominatim, frenoPhotonAperto, adesso::get);

    private static ClientResponse json(String body) {
        return ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }

    @Test
    @DisplayName("le cache non crescono oltre il loro tetto, anche con una chiave nuova a ogni tasto")
    void cacheLimitate() {
        int ricerche = GeocodeSearchService.MAX_SEARCH_CACHE_ENTRIES + 200;
        for (int i = 0; i < ricerche; i++) {
            servizio.search("via dei gracchi " + i, 5, 41.9 + i * 0.001, 12.46);
        }

        assertThat(servizio.searchCacheSize()).isEqualTo(GeocodeSearchService.MAX_SEARCH_CACHE_ENTRIES);
        assertThat(servizio.upstreamCacheSize()).isEqualTo(GeocodeSearchService.MAX_UPSTREAM_CACHE_ENTRIES);
    }

    @Test
    @DisplayName("se Photon non risponde vale l'ultima risposta buona, anche scaduta")
    void ripiegoSullaRispostaScaduta() {
        List<GeocodeSearchResultDTO> prima = servizio.search("via dei grac", 5, null, null);
        assertThat(prima).extracting(GeocodeSearchResultDTO::label).anyMatch(l -> l.contains("Via dei Gracchi"));

        adesso.addAndGet(10 * MINUTO);
        statoPhoton.set(HttpStatus.INTERNAL_SERVER_ERROR);

        List<GeocodeSearchResultDTO> dopo = servizio.search("via dei grac", 5, null, null);
        assertThat(dopo).extracting(GeocodeSearchResultDTO::label).anyMatch(l -> l.contains("Via dei Gracchi"));
    }

    @Test
    @DisplayName("Photon passa dal freno condiviso con la geocodifica inversa: una richiesta ogni mezzo secondo")
    void photonFrenato() {
        UpstreamGate frenoPhoton = new UpstreamGate("Photon", 500L, adesso::get);
        GeocodeSearchService frenato = new GeocodeSearchService(webClient, frenoNominatim, frenoPhoton, adesso::get);

        // La geocodifica inversa ha appena chiamato Photon: la ricerca non lo chiama.
        assertThat(frenoPhoton.tryEnter()).isTrue();
        frenato.search("via dei gracchi", 5, null, null);
        assertThat(chiamate).doesNotContain(PHOTON);

        adesso.addAndGet(500);
        frenato.search("via dei grac", 5, null, null);
        frenato.search("via dei gr", 5, null, null);
        assertThat(chiamate.stream().filter(PHOTON::equals)).hasSize(1);
    }
}
