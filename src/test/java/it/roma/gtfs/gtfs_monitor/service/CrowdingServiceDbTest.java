package it.roma.gtfs.gtfs_monitor.service;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * L'aggregato dell'affollamento su un Postgres vero, come SlowSegmentsServiceDbTest
 * e con lo stesso interruttore:
 *
 *   ./mvnw test -Dtest=CrowdingServiceDbTest -Dstorico.db=jdbc:postgresql://127.0.0.1:5433/wemoveroma_verifica
 */
class CrowdingServiceDbTest {

    private static final ZoneId ROMA = ZoneId.of("Europe/Rome");
    /** Martedi' e mercoledi' feriali, e il sabato della stessa settimana. */
    private static final LocalDate MARTEDI = LocalDate.of(2026, 7, 21);
    private static final LocalDate MERCOLEDI = LocalDate.of(2026, 7, 22);
    private static final LocalDate SABATO = LocalDate.of(2026, 7, 25);
    private static final YearMonth LUGLIO = YearMonth.of(2026, 7);

    private static final Integer POSTI = 1;
    private static final Integer IN_PIEDI = 3;
    private static final Integer NESSUN_DATO = 7;

    private String schema;
    private JdbcTemplate jdbc;
    private CrowdingService service;

    @BeforeEach
    void setUp() {
        String url = System.getProperty("storico.db");
        Assumptions.assumeTrue(url != null, "Abilitare con -Dstorico.db=jdbc:postgresql://...");
        String utente = System.getProperty("storico.user", "wemoveroma");
        String password = System.getProperty("storico.password", "");
        schema = "prova_affollamento_" + UUID.randomUUID().toString().substring(0, 8);

        Flyway.configure().dataSource(url, utente, password).schemas(schema)
                .locations("classpath:db/migration").load().migrate();
        DriverManagerDataSource ds = new DriverManagerDataSource(
                url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema, utente, password);
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE passaggi_2026_07 PARTITION OF passaggi FOR VALUES FROM ('2026-07-01') TO ('2026-08-01')");

        service = new CrowdingService(jdbc, null, null);
        ReflectionTestUtils.setField(service, "mesiConservati", 6);
        service.passaggiMinimiTratto = 1;

        // Martedi' alle 8, tratto S2 -> S3: due corse della 64 in piedi, una della 85 seduta.
        // S1 e' la prima fermata registrata: non ha una provenienza, non entra.
        corsa(MARTEDI, "T1", "64", "08:00", "S1", POSTI, "S2", POSTI, "S3", IN_PIEDI, "S4", IN_PIEDI);
        corsa(MARTEDI, "T2", "64", "08:10", "S1", POSTI, "S2", IN_PIEDI, "S3", IN_PIEDI, "S4", POSTI);
        corsa(MARTEDI, "T3", "85", "08:20", "S1", null, "S2", POSTI, "S3", POSTI, "S4", NESSUN_DATO);
    }

    @AfterEach
    void tearDown() {
        if (jdbc != null) {
            jdbc.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    @Test
    void contaPerLineaTrattoEOraSoloIValoriUtili() {
        service.aggregaGiorno(MARTEDI);

        assertEquals(List.of(
                        "64 S1>S2 8: 2/1", "64 S2>S3 8: 2/2", "64 S3>S4 8: 2/1",
                        "85 S1>S2 8: 1/0", "85 S2>S3 8: 1/0"),
                jdbc.queryForList("""
                        SELECT linea || ' ' || da || '>' || a || ' ' || ora || ': ' || passaggi || '/' || in_piedi
                        FROM affollamento_mensile WHERE mese = '2026-07-01' AND tipo = 'feriale' ORDER BY 1
                        """, String.class));
        // Nel giorno i passaggi con un valore utile sono 10; in tabella ne entrano 8,
        // perche' i due a S1 un valore lo hanno ma, senza provenienza, non hanno un tratto.
        assertEquals(10, jdbc.queryForObject("SELECT passaggi FROM affollamento_giorni", Integer.class));
        assertEquals(8, jdbc.queryForObject("SELECT sum(passaggi) FROM affollamento_mensile", Integer.class));
    }

    @Test
    void unGiornoNonSiContaDueVolteEDueGiorniSiSommano() {
        assertTrue(service.aggregaGiorno(MARTEDI) > 0);
        assertEquals(0, service.aggregaGiorno(MARTEDI));
        assertEquals("2/2", cella("64", "S2", "S3", 8));

        corsa(MERCOLEDI, "T1", "64", "08:00", "S1", POSTI, "S2", POSTI, "S3", POSTI);
        service.aggregaGiorno(MERCOLEDI);
        assertEquals("3/2", cella("64", "S2", "S3", 8));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM affollamento_giorni", Integer.class));
    }

    /** Il feed salta una fermata: il passaggio a S3 arriva dopo S1, ma il tratto resta S2 -> S3. */
    @Test
    void ilPassaggioVaSulTrattoDellaProvenienzaPiuFrequente() {
        corsa(MARTEDI, "T4", "64", "08:30", "S0", POSTI, "S1", POSTI, "S3", IN_PIEDI);
        service.aggregaGiorno(MARTEDI);

        assertEquals("3/3", cella("64", "S2", "S3", 8));
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM affollamento_mensile WHERE da = 'S1' AND a = 'S3'", Integer.class));
    }

    @Test
    void sabatoEFerialeRestanoSeparati() {
        corsa(SABATO, "T1", "64", "08:00", "S1", POSTI, "S2", POSTI, "S3", POSTI);
        service.aggregaGiorno(MARTEDI);
        service.aggregaGiorno(SABATO);

        assertEquals("2/2", cella("64", "S2", "S3", 8));
        assertEquals("1/0", jdbc.queryForObject("""
                SELECT passaggi || '/' || in_piedi FROM affollamento_mensile
                WHERE tipo = 'sabato' AND linea = '64' AND da = 'S2' AND a = 'S3'
                """, String.class));
    }

    @Test
    void unGiornoSenzaAffollamentoSiSegnaESiSalta() {
        corsa(MERCOLEDI, "T1", "64", "08:00", "S1", null, "S2", null, "S3", NESSUN_DATO);
        assertEquals(0, service.aggregaGiorno(MERCOLEDI));

        assertEquals(0, jdbc.queryForObject("SELECT passaggi FROM affollamento_giorni WHERE giorno = ?",
                Integer.class, Date.valueOf(MERCOLEDI)));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM affollamento_mensile", Integer.class));
    }

    @Test
    void trovaIGiorniDaSommareEToglieIMesiVecchi() {
        jdbc.update("INSERT INTO corse_osservate (giorno, corsa, linea, route_id, prima_vista, ultima_vista) "
                        + "VALUES (?, 'T1', '64', '64', now(), now()), (?, 'X1', '64', '64', now(), now())",
                Date.valueOf(MARTEDI), Date.valueOf(MERCOLEDI));

        assertEquals(List.of(MARTEDI, MERCOLEDI), service.giorniMancanti(MERCOLEDI));
        service.aggregaGiorno(MARTEDI);
        assertEquals(List.of(MERCOLEDI), service.giorniMancanti(MERCOLEDI));
        assertEquals(List.of(), service.giorniMancanti(MARTEDI));

        // Sei mesi dopo luglio 2026 e' gennaio 2027: luglio esce a febbraio.
        assertEquals(0, service.eliminaVecchi(LocalDate.of(2027, 1, 31)));
        assertTrue(service.eliminaVecchi(LocalDate.of(2027, 2, 1)) > 0);
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM affollamento_giorni", Integer.class));
    }

    @Test
    void laLetturaSommaLeLineeSulTrattoOraPerOra() {
        corsa(MARTEDI, "T5", "64", "17:00", "S1", POSTI, "S2", POSTI, "S3", IN_PIEDI);
        service.aggregaGiorno(MARTEDI);

        CrowdingService.Periodo periodo = service.leggi("feriale", LUGLIO, LUGLIO, null);
        assertEquals(1, periodo.giorni());
        // Dal piu' pieno: S2 -> S3 ha tre passaggi in piedi.
        CrowdingService.Tratto t = periodo.tratti().getFirst();
        assertEquals("S2", t.da());
        assertEquals("S3", t.a());
        assertEquals(3, t.serie().passaggi()[8]);
        assertEquals(2, t.serie().inPiedi()[8]);
        assertEquals(1, t.serie().passaggi()[17]);
        assertEquals(1, t.serie().inPiedi()[17]);
        assertEquals(List.of("64", "85"), List.copyOf(t.linee()));

        // Le linee, ora per ora, su tutti i loro tratti.
        assertEquals(6, periodo.linee().get("64").passaggi()[8]);
        assertEquals(4, periodo.linee().get("64").inPiedi()[8]);
        assertEquals(2, periodo.linee().get("85").passaggi()[8]);
        assertEquals(0, periodo.linee().get("85").inPiedi()[8]);
    }

    @Test
    void conLaLineaSoloISuoiPassaggi() {
        service.aggregaGiorno(MARTEDI);

        CrowdingService.Periodo periodo = service.leggi("feriale", LUGLIO, LUGLIO, "85");
        assertEquals(List.of("S1>S2", "S2>S3"),
                periodo.tratti().stream().map(t -> t.da() + ">" + t.a()).sorted().toList());
        CrowdingService.Tratto t = periodo.tratti().stream().filter(x -> x.a().equals("S3")).findFirst().orElseThrow();
        assertEquals(1, t.serie().passaggi()[8]);
        assertEquals(0, t.serie().inPiedi()[8]);
        assertEquals(List.of("85"), List.copyOf(t.linee()));
        assertEquals(List.of("85"), List.copyOf(periodo.linee().keySet()));
    }

    @Test
    void sottoLaSogliaIlTrattoNonSiMandaESenzaGiorniNonCeNiente() {
        service.aggregaGiorno(MARTEDI);
        service.passaggiMinimiTratto = 3;

        // S3 -> S4 ha due passaggi utili (il terzo dice "nessun dato"): resta fuori.
        // Gli altri in ordine di passaggi in piedi.
        assertEquals(List.of("S2>S3", "S1>S2"), service.leggi("feriale", LUGLIO, LUGLIO, null).tratti().stream()
                .map(t -> t.da() + ">" + t.a()).toList());
        // Di sabato in luglio non c'e' nessun giorno con dati.
        CrowdingService.Periodo sabato = service.leggi("sabato", LUGLIO, LUGLIO, null);
        assertEquals(0, sabato.giorni());
        assertTrue(sabato.tratti().isEmpty());
        // Agosto non c'e'.
        assertEquals(0, service.leggi("feriale", LUGLIO.plusMonths(1), LUGLIO.plusMonths(1), null).giorni());
    }

    // ---- dati ----------------------------------------------------------------

    /** Una corsa: fermate a tre minuti l'una dall'altra, con l'affollamento dichiarato andando verso ciascuna. */
    private void corsa(LocalDate giorno, String id, String linea, String partenza, Object... fermateEAffollamento) {
        LocalTime ora = LocalTime.parse(partenza);
        for (int i = 0; i < fermateEAffollamento.length; i += 2) {
            jdbc.update("""
                    INSERT INTO passaggi (giorno, osservato_il, linea, route_id, corsa, fermata, veicolo, ritardo_sec, affollamento)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, 0, ?)
                    """,
                    Date.valueOf(giorno),
                    OffsetDateTime.ofInstant(giorno.atTime(ora.plusMinutes(3L * i / 2)).atZone(ROMA).toInstant(), ZoneOffset.UTC),
                    linea, linea, id, (String) fermateEAffollamento[i], (Integer) fermateEAffollamento[i + 1]);
        }
    }

    private String cella(String linea, String da, String a, int ora) {
        return jdbc.queryForObject("""
                SELECT passaggi || '/' || in_piedi FROM affollamento_mensile
                WHERE tipo = 'feriale' AND linea = ? AND da = ? AND a = ? AND ora = ?
                """, String.class, linea, da, a, ora);
    }
}
