package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * L'affollamento di un giorno si somma una volta sola: non finche' in coda ci
 * sono ancora suoi passaggi da salvare.
 */
class PunctualityServiceCodaTest {

    private static final ZoneId ROMA = ZoneId.of("Europe/Rome");
    private static final LocalDate IERI = LocalDate.of(2026, 10, 11);

    /** Un passaggio di una corsa comparsa nel feed a quell'ora di Roma. */
    private static PunctualityCollector.Passaggio passaggio(LocalDate giorno, int ora) {
        Instant primaVista = giorno.atTime(ora, 30).atZone(ROMA).toInstant();
        return new PunctualityCollector.Passaggio("R", "T", "S", "V", 1.0, primaVista.plusSeconds(600), primaVista, 1);
    }

    @Test
    void codaVuota() {
        assertFalse(PunctualityService.inCodaFinoA(List.of(), IERI));
    }

    @Test
    void soloPassaggiDiOggiNonFermanoLaSomma() {
        assertFalse(PunctualityService.inCodaFinoA(
                List.of(passaggio(IERI.plusDays(1), 0), passaggio(IERI.plusDays(1), 9)), IERI));
    }

    /** Una corsa di ieri alle 23:30, ancora in coda: anche se il passaggio e' dopo mezzanotte, e' di ieri. */
    @Test
    void unPassaggioDiIeriFermaLaSomma() {
        assertTrue(PunctualityService.inCodaFinoA(
                List.of(passaggio(IERI.plusDays(1), 9), passaggio(IERI, 23)), IERI));
    }

    /** Dopo un database spento in coda restano giorni anche piu' vecchi. */
    @Test
    void ancheUnGiornoPrecedente() {
        assertTrue(PunctualityService.inCodaFinoA(List.of(passaggio(IERI.minusDays(3), 12)), IERI));
    }
}
