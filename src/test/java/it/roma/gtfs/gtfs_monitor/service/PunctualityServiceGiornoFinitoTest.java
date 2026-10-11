package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Quale giorno di servizio si puo' sommare nell'affollamento: ieri dalle 4 del
 * mattino di Roma, l'altro ieri prima.
 */
class PunctualityServiceGiornoFinitoTest {

    private static final ZoneId ROMA = ZoneId.of("Europe/Rome");

    private static ZonedDateTime aRoma(String giorno, int ora, int minuto) {
        return LocalDate.parse(giorno).atTime(ora, minuto).atZone(ROMA);
    }

    @Test
    void dalleQuattroIeriEFinito() {
        assertEquals(LocalDate.parse("2026-10-11"), PunctualityService.ultimoGiornoFinito(aRoma("2026-10-12", 4, 0)));
        assertEquals(LocalDate.parse("2026-10-11"), PunctualityService.ultimoGiornoFinito(aRoma("2026-10-12", 15, 30)));
        assertEquals(LocalDate.parse("2026-10-11"), PunctualityService.ultimoGiornoFinito(aRoma("2026-10-12", 23, 59)));
    }

    /** Le ultime corse di ieri passano ancora dopo mezzanotte: fino alle 4 si resta all'altro ieri. */
    @Test
    void primaDelleQuattroSiRestaAllAltroIeri() {
        assertEquals(LocalDate.parse("2026-10-10"), PunctualityService.ultimoGiornoFinito(aRoma("2026-10-12", 0, 5)));
        assertEquals(LocalDate.parse("2026-10-10"), PunctualityService.ultimoGiornoFinito(aRoma("2026-10-12", 3, 59)));
    }

    /** Il server gira in UTC: conta l'ora di Roma, non la sua. */
    @Test
    void contaLOraDiRomaNonQuellaDelServer() {
        // Le 02:30 UTC di ottobre sono le 04:30 a Roma.
        ZonedDateTime utc = ZonedDateTime.of(2026, 10, 12, 2, 30, 0, 0, ZoneOffset.UTC);
        assertEquals(LocalDate.parse("2026-10-11"), PunctualityService.ultimoGiornoFinito(utc));
        // Le 23:30 UTC dell'11 sono l'01:30 del 12 a Roma: ancora l'altro ieri, cioe' il 10.
        assertEquals(LocalDate.parse("2026-10-10"),
                PunctualityService.ultimoGiornoFinito(ZonedDateTime.of(2026, 10, 11, 23, 30, 0, 0, ZoneOffset.UTC)));
    }

    @Test
    void aCavalloDelMeseEDellAnno() {
        assertEquals(LocalDate.parse("2026-09-30"), PunctualityService.ultimoGiornoFinito(aRoma("2026-10-01", 9, 0)));
        assertEquals(LocalDate.parse("2026-12-30"), PunctualityService.ultimoGiornoFinito(aRoma("2027-01-01", 1, 0)));
    }
}
