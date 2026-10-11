package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.JourneyPlanResponseDTO;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Il planner davanti a un OTP finto: niente 500 per parametri mancanti
 * o per OTP giu', ma la risposta con il messaggio d'errore giusto.
 */
class JourneyPlannerServicePlanTest {

    private static JourneyPlannerService planner(ExchangeFunction otp) {
        JourneyPlannerService service = new JourneyPlannerService(
                WebClient.builder().exchangeFunction(otp).build(),
                mock(GtfsIndexService.class));
        ReflectionTestUtils.setField(service, "otpEnabled", true);
        ReflectionTestUtils.setField(service, "otpBaseUrl", "http://otp.invalid:8081");
        ReflectionTestUtils.setField(service, "otpTimeoutSeconds", 1L);
        ReflectionTestUtils.setField(service, "searchWindow", "PT60M");
        ReflectionTestUtils.setField(service, "searchWindowFallback", "PT120M");
        ReflectionTestUtils.setField(service, "maxItineraries", 10);
        ReflectionTestUtils.setField(service, "maxUpstreamItineraries", 20);
        ReflectionTestUtils.setField(service, "railInjectionEnabled", true);
        return service;
    }

    private static JourneyPlanResponseDTO daTerminiAPiazzaVenezia(JourneyPlannerService service) {
        return service.plan(41.9013, 12.5004, "Termini", 41.8964, 12.4823, "Piazza Venezia",
                5, null, null, null);
    }

    private static ClientResponse nessunPercorso() {
        return ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body("{\"data\":{\"planConnection\":{\"edges\":[]}}}")
                .build();
    }

    /** Senza il parametro modes (o con modes vuoto) si cercano tutti i mezzi, senza NPE. */
    @Test
    void senzaModesSiCercaConTuttiIMezzi() {
        JourneyPlannerService service = planner(request -> Mono.just(nessunPercorso()));
        ReflectionTestUtils.setField(service, "railInjectionEnabled", false);

        JourneyPlanResponseDTO senza = daTerminiAPiazzaVenezia(service);
        JourneyPlanResponseDTO vuoto = service.plan(41.9013, 12.5004, "Termini", 41.8964, 12.4823,
                "Piazza Venezia", 5, null, null, "  ");

        assertEquals("Nessun percorso trovato.", senza.error());
        assertEquals("Nessun percorso trovato.", vuoto.error());
    }
}
