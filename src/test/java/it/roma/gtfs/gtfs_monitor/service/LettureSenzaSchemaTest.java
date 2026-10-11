package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Finche' la migrazione non e' riuscita (Postgres spento all'avvio) le letture
 * di affollamento e tratti rispondono vuoto subito, come la puntualita', senza
 * chiedere una connessione al pool.
 */
class LettureSenzaSchemaTest {

    @Test
    void affollamentoVuotoSenzaToccareIlDatabase() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        GtfsIndexService gtfs = mock(GtfsIndexService.class);
        CrowdingService service = new CrowdingService(jdbc, gtfs, null);
        ReflectionTestUtils.setField(service, "enabled", true);

        assertTrue(service.tratti(ServiceDayType.FERIALE, 2, null).isEmpty());
        verifyNoInteractions(jdbc);
    }

    @Test
    void trattiVuotiSenzaToccareIlDatabase() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        GtfsIndexService gtfs = mock(GtfsIndexService.class);
        SlowSegmentsService service = new SlowSegmentsService(jdbc, gtfs);
        ReflectionTestUtils.setField(service, "enabled", true);

        LocalDate oggi = LocalDate.of(2026, 10, 12);
        assertTrue(service.tratti(ServiceDayType.FERIALE, SlowSegmentsService.TUTTE, oggi.minusDays(7), oggi, null)
                .isEmpty());
        verifyNoInteractions(jdbc);
    }
}
