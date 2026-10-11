package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.JourneyLegDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.JourneyOptionDTO;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * L'iniezione del treno fa le chiamate indipendenti a OTP in parallelo
 * e con OTP muto si arrende entro un tempo limitato.
 */
class JourneyPlannerServiceRailInjectionTest {

    /** Da via Ostiense a viale Trastevere: Roma Ostiense vicino alla partenza, Trastevere all'arrivo. */
    private static final double FROM_LAT = 41.8720, FROM_LON = 12.4860;
    private static final double TO_LAT = 41.8730, TO_LON = 12.4680;

    /**
     * Una risposta buona per tutte le query dell'iniezione: le due stazioni nel
     * raggio (ognuno sceglie la piu' vicina) e un regionale fra le due. I cammini
     * non trovano itinerari solo a piedi e ricadono sulla stima dalla distanza.
     */
    private static final String RISPOSTA_OTP = """
            {"data":{
              "stopsByRadius":{"edges":[
                {"node":{"distance":100,"stop":{"name":"Roma Ostiense","lat":41.8722,"lon":12.4865,"vehicleMode":"RAIL"}}},
                {"node":{"distance":100,"stop":{"name":"Roma Trastevere","lat":41.8735,"lon":12.4675,"vehicleMode":"RAIL"}}}
              ]},
              "planConnection":{"edges":[{"node":{"legs":[
                {"mode":"RAIL","duration":360,"from":{"name":"Roma Ostiense"},"to":{"name":"Roma Trastevere"},
                 "route":{"shortName":"FL1"},
                 "start":{"scheduledTime":"2026-10-12T08:30:00Z"},"end":{"scheduledTime":"2026-10-12T08:36:00Z"}}
              ]}}]}
            }}
            """;

    private static JourneyPlannerService planner(ExchangeFunction otp) {
        JourneyPlannerService service = new JourneyPlannerService(
                WebClient.builder().exchangeFunction(otp).build(),
                mock(GtfsIndexService.class));
        ReflectionTestUtils.setField(service, "otpBaseUrl", "http://otp.invalid:8081");
        ReflectionTestUtils.setField(service, "otpTimeoutSeconds", 1L);
        ReflectionTestUtils.setField(service, "searchWindow", "PT60M");
        ReflectionTestUtils.setField(service, "walkSpeed", 1.34);
        ReflectionTestUtils.setField(service, "railInjectionMaxStationMeters", 1200);
        return service;
    }

    private static JourneyPlannerService.TimePreference alle8() {
        return new JourneyPlannerService.TimePreference("earliestDeparture", Instant.parse("2026-10-12T08:00:00Z"));
    }

    @Test
    void leChiamateIndipendentiPartonoInsieme() {
        AtomicInteger chiamate = new AtomicInteger();
        AtomicInteger inVolo = new AtomicInteger();
        AtomicInteger maxInVolo = new AtomicInteger();
        JourneyPlannerService service = planner(request -> Mono.delay(Duration.ofMillis(150))
                .map(t -> ClientResponse.create(HttpStatus.OK)
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .body(RISPOSTA_OTP)
                        .build())
                .doOnSubscribe(s -> {
                    chiamate.incrementAndGet();
                    maxInVolo.accumulateAndGet(inVolo.incrementAndGet(), Math::max);
                })
                .doFinally(s -> inVolo.decrementAndGet()));

        JourneyOptionDTO treno = service.tryBuildInjectedRailOption(FROM_LAT, FROM_LON, TO_LAT, TO_LON, alle8());

        assertNotNull(treno);
        assertEquals(List.of("WALK", "RAIL", "WALK"), treno.legs().stream().map(JourneyLegDTO::mode).toList());
        assertEquals("Roma Ostiense", treno.legs().get(0).toName());
        assertEquals("Roma Trastevere", treno.legs().get(2).fromName());
        // Due stazioni, due cammini, i treni: cinque chiamate, ma i due cammini
        // e i treni sono in volo nello stesso momento.
        assertEquals(5, chiamate.get());
        assertEquals(3, maxInVolo.get());
    }

    @Test
    void conOtpMutoSiRinunciaEntroIlTimeout() {
        JourneyPlannerService service = planner(request -> Mono.never());

        long inizio = System.nanoTime();
        JourneyOptionDTO treno = service.tryBuildInjectedRailOption(FROM_LAT, FROM_LON, TO_LAT, TO_LON, alle8());
        Duration durata = Duration.ofNanos(System.nanoTime() - inizio);

        assertNull(treno);
        // Timeout OTP di 1 secondo: le stazioni non arrivano e ci si ferma li'.
        assertTrue(durata.compareTo(Duration.ofSeconds(3)) < 0, "durata " + durata);
    }
}
