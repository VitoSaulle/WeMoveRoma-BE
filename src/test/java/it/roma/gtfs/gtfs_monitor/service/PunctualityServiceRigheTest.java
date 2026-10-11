package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Le righe della puntualita': i giorni prima di oggi restano in memoria finche'
 * lo storico non cambia, oggi si legge sempre, e il risultato e' lo stesso
 * della lettura di tutto l'intervallo in una volta.
 */
class PunctualityServiceRigheTest {

    private static final LocalDate OGGI = LocalDate.of(2026, 10, 12);

    /** Un database finto: tiene le righe gia' raggruppate e conta le letture. */
    static class Storico extends JdbcTemplate {
        final List<PunctualityService.RigaOraria> righe = new ArrayList<>();
        final List<String> letture = new ArrayList<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
            LocalDate dal = ((Date) args[args.length - 2]).toLocalDate();
            LocalDate al = ((Date) args[args.length - 1]).toLocalDate();
            letture.add(dal + ".." + al);
            return (List<T>) righe.stream()
                    .filter(r -> !r.giorno().isBefore(dal) && !r.giorno().isAfter(al))
                    .toList();
        }
    }

    private Storico storico;
    private PunctualityService service;

    @BeforeEach
    void setUp() {
        storico = new Storico();
        for (int i = 0; i < 30; i++) {
            storico.righe.add(new PunctualityService.RigaOraria(OGGI.minusDays(i), 8, 10 + i, 1, 6, 2, 1));
        }
        service = new PunctualityService(null, null, null, storico, null, null, null);
    }

    private static List<PunctualityService.RigaOraria> ordinate(List<PunctualityService.RigaOraria> righe) {
        return righe.stream().sorted(Comparator.comparing(PunctualityService.RigaOraria::giorno)).toList();
    }

    @Test
    void stesseRigheDellaLetturaUnica() {
        List<PunctualityService.RigaOraria> attese = storico.righe.stream()
                .filter(r -> !r.giorno().isBefore(OGGI.minusDays(27))).toList();
        assertEquals(ordinate(attese), ordinate(service.righe("64", OGGI.minusDays(27), OGGI, OGGI)));
        assertEquals(List.of(OGGI.minusDays(27) + ".." + OGGI.minusDays(1), OGGI + ".." + OGGI), storico.letture);
    }

    @Test
    void iGiorniChiusiSiLeggonoUnaVoltaOggiSempre() {
        service.righe("64", OGGI.minusDays(27), OGGI, OGGI);
        storico.righe.add(new PunctualityService.RigaOraria(OGGI, 9, 5, 0, 5, 0, 0));
        List<PunctualityService.RigaOraria> seconda = service.righe("64", OGGI.minusDays(27), OGGI, OGGI);

        assertEquals(3, storico.letture.size());
        assertEquals(OGGI + ".." + OGGI, storico.letture.getLast());
        assertEquals(29, seconda.size()); // il passaggio nuovo di oggi c'e'
    }

    @Test
    void unPassaggioInRitardoInvalidaIGiorniChiusi() {
        service.righe("64", OGGI.minusDays(27), OGGI.minusDays(1), OGGI);
        storico.righe.add(new PunctualityService.RigaOraria(OGGI.minusDays(1), 23, 3, 0, 3, 0, 0));
        assertEquals(27, service.righe("64", OGGI.minusDays(27), OGGI.minusDays(1), OGGI).size());

        ((AtomicLong) ReflectionTestUtils.getField(service, "versioneStorico")).incrementAndGet();
        assertEquals(28, service.righe("64", OGGI.minusDays(27), OGGI.minusDays(1), OGGI).size());
        assertEquals(2, storico.letture.size());
    }

    @Test
    void soloOggiNonPassaDallaMemoria() {
        service.righe("64", OGGI, OGGI, OGGI);
        service.righe("64", OGGI, OGGI, OGGI);
        assertEquals(List.of(OGGI + ".." + OGGI, OGGI + ".." + OGGI), storico.letture);
    }

    /** A mezzanotte ieri diventa un giorno chiuso: la chiave cambia, niente righe vecchie. */
    @Test
    void ilGiornoDopoLaChiaveCambia() {
        service.righe("64", OGGI.minusDays(27), OGGI, OGGI);
        service.righe("64", OGGI.minusDays(27), OGGI.plusDays(1), OGGI.plusDays(1));
        assertEquals(OGGI.minusDays(27) + ".." + OGGI, storico.letture.get(2));
    }
}
