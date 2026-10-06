package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ReverseGeocodeDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Da dove arriva l'indirizzo di un punto, e cosa succede quando una delle due
 * fonti rifiuta o non risponde. Nessuna richiesta esce davvero: le risposte
 * sono quelle vere dei due servizi, ridotte ai campi che servono.
 */
class ReverseGeocodeServiceTest {

    private static final String NOMINATIM = "nominatim.openstreetmap.org";
    private static final String PHOTON = "photon.komoot.io";
    private static final long MINUTO = 60_000L;

    private static final String NOMINATIM_CLODIO = """
            {"display_name":"Piazzale Clodio, Della Vittoria, Roma, Lazio, Italia",
             "address":{"road":"Piazzale Clodio","suburb":"Della Vittoria","city":"Roma","country":"Italia"}}""";
    private static final String PHOTON_VIA = """
            {"features":[{"properties":{"type":"street","name":"Viale Palmiro Togliatti",
             "district":"Don Bosco","city":"Roma","country":"Italia","osm_key":"highway"}}]}""";
    private static final String PHOTON_NEGOZIO = """
            {"features":[{"properties":{"type":"house","name":"Nashi","street":"Piazza dei Cinquecento",
             "city":"Roma","country":"Italia","osm_key":"shop"}}]}""";
    private static final String PHOTON_NIENTE = """
            {"features":[]}""";

    private final AtomicLong adesso = new AtomicLong(1_000_000L);
    private final List<String> chiamate = new ArrayList<>();
    private final Map<String, Supplier<ClientResponse>> risposte = new HashMap<>();

    private final WebClient webClient = WebClient.builder()
            .exchangeFunction(request -> {
                String host = request.url().getHost();
                chiamate.add(host);
                return Mono.just(risposte.get(host).get());
            })
            .build();
    private final UpstreamGate frenoNominatim = new UpstreamGate("Nominatim", 1_000L, adesso::get);
    private final UpstreamGate frenoPhoton = new UpstreamGate("Photon", 500L, adesso::get);
    // Una sola fermata nel file, in piazzale degli Eroi: gli altri punti dei test ne stanno lontani.
    private final StopAddressService fermate = new StopAddressService(new ByteArrayResource("""
            stop_id,lat,lon,street,city
            70001,41.907500,12.446900,Piazzale degli Eroi,Roma
            """.getBytes(StandardCharsets.UTF_8)));
    private final ReverseGeocodeService servizio =
            new ReverseGeocodeService(webClient, fermate, frenoNominatim, frenoPhoton, adesso::get);

    private void risponde(String host, String json) {
        risposte.put(host, () -> ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(json)
                .build());
    }

    private void rifiuta(String host, HttpStatus stato) {
        risposte.put(host, () -> ClientResponse.create(stato).build());
    }

    @Test
    @DisplayName("la via di una fermata c'e' gia': non parte nessuna richiesta")
    void fermataSenzaChiedere() {
        ReverseGeocodeDTO indirizzo = servizio.reverse(41.9075, 12.4469);

        assertThat(indirizzo.label()).isEqualTo("Piazzale degli Eroi, Roma");
        assertThat(chiamate).isEmpty();
    }

    @Test
    @DisplayName("se Nominatim risponde l'indirizzo e' il suo, e Photon non viene disturbato")
    void nominatimBasta() {
        risponde(NOMINATIM, NOMINATIM_CLODIO);

        ReverseGeocodeDTO indirizzo = servizio.reverse(41.91716, 12.45504);

        assertThat(indirizzo.label()).isEqualTo("Piazzale Clodio, Roma");
        assertThat(indirizzo.street()).isEqualTo("Piazzale Clodio");
        assertThat(chiamate).containsExactly(NOMINATIM);
    }

    @Test
    @DisplayName("se Nominatim rifiuta con un 429 risponde Photon, e Nominatim non viene piu' chiamato")
    void photonQuandoNominatimRifiuta() {
        rifiuta(NOMINATIM, HttpStatus.TOO_MANY_REQUESTS);
        risponde(PHOTON, PHOTON_VIA);

        ReverseGeocodeDTO indirizzo = servizio.reverse(41.8719, 12.5674);

        assertThat(indirizzo.label()).isEqualTo("Viale Palmiro Togliatti, Roma");
        assertThat(chiamate).containsExactly(NOMINATIM, PHOTON);

        // Un altro punto, un minuto dopo: Nominatim e' in pausa, si va dritti a Photon.
        chiamate.clear();
        adesso.addAndGet(MINUTO);
        servizio.reverse(41.9009, 12.5016);
        assertThat(chiamate).containsExactly(PHOTON);

        // Passata la pausa si riprova.
        chiamate.clear();
        adesso.addAndGet(5 * MINUTO);
        risponde(NOMINATIM, NOMINATIM_CLODIO);
        servizio.reverse(41.91716, 12.45504);
        assertThat(chiamate).containsExactly(NOMINATIM);
    }

    @Test
    @DisplayName("di un negozio Photon da' la via in street, non in name")
    void photonCivicoONegozio() {
        rifiuta(NOMINATIM, HttpStatus.TOO_MANY_REQUESTS);
        risponde(PHOTON, PHOTON_NEGOZIO);

        ReverseGeocodeDTO indirizzo = servizio.reverse(41.9009, 12.5016);

        assertThat(indirizzo.street()).isEqualTo("Piazza dei Cinquecento");
        assertThat(indirizzo.label()).isEqualTo("Piazza dei Cinquecento, Roma");
    }

    @Test
    @DisplayName("un indirizzo trovato non si richiede per sei ore")
    void indirizzoInCache() {
        risponde(NOMINATIM, NOMINATIM_CLODIO);

        servizio.reverse(41.91716, 12.45504);
        adesso.addAndGet(5 * 60 * MINUTO);
        servizio.reverse(41.91716, 12.45504);

        assertThat(chiamate).containsExactly(NOMINATIM);
    }

    @Test
    @DisplayName("se nessuna delle due risponde non si richiede subito, ma dopo due minuti si'")
    void fallimentoRicordatoPerPoco() {
        rifiuta(NOMINATIM, HttpStatus.BAD_GATEWAY);
        rifiuta(PHOTON, HttpStatus.BAD_GATEWAY);

        ReverseGeocodeDTO indirizzo = servizio.reverse(41.91716, 12.45504);
        assertThat(indirizzo.label()).isNull();
        assertThat(indirizzo.lat()).isEqualTo(41.91716);
        assertThat(chiamate).containsExactly(NOMINATIM, PHOTON);

        chiamate.clear();
        adesso.addAndGet(MINUTO);
        servizio.reverse(41.91716, 12.45504);
        assertThat(chiamate).isEmpty();

        adesso.addAndGet(MINUTO);
        risponde(NOMINATIM, NOMINATIM_CLODIO);
        assertThat(servizio.reverse(41.91716, 12.45504).label()).isEqualTo("Piazzale Clodio, Roma");
    }

    @Test
    @DisplayName("con i freni chiusi non parte nessuna richiesta, e la successiva puo' passare")
    void freniChiusi() {
        risponde(NOMINATIM, NOMINATIM_CLODIO);
        risponde(PHOTON, PHOTON_NIENTE);
        // Due punti diversi nello stesso istante consumano i due passaggi.
        servizio.reverse(41.80, 12.40);
        servizio.reverse(41.81, 12.41);
        chiamate.clear();

        ReverseGeocodeDTO senza = servizio.reverse(41.91716, 12.45504);
        assertThat(senza.label()).isNull();
        assertThat(chiamate).isEmpty();

        adesso.addAndGet(1_000L);
        assertThat(servizio.reverse(41.91716, 12.45504).label()).isEqualTo("Piazzale Clodio, Roma");
    }
}
