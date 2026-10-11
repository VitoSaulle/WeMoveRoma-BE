package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiCrowdingSegmentsDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Quanto sono pieni i bus: l'aggregato mensile per linea, tratto e ora, e la
 * sua lettura per la mappa dell'affollamento.
 *
 * COSA SI MISURA. Ogni passaggio porta quanto era pieno il mezzo mentre andava
 * verso quella fermata (PunctualityCollector). Qui si contano, per linea,
 * tratto e ora, i passaggi con un valore utile e quanti di loro erano "solo
 * posti in piedi" o peggio. Il feed di Roma ha due soli livelli, posti a sedere
 * e in piedi (ottobre 2026): la misura e' quante volte su cento si viaggia in
 * piedi, non quante persone ci sono a bordo.
 *
 * IL TRATTO E' QUELLO CHE ARRIVA ALLA FERMATA. Il passaggio dice a quale fermata
 * si andava, non da quale si veniva: la fermata precedente e' quella del
 * passaggio prima, ma il feed ne salta una su quattordici (PunctualityCollector)
 * e allora il passaggio prima e' due fermate indietro. Per ogni linea e fermata
 * si prende quindi la provenienza piu' frequente, e tutti i passaggi a quella
 * fermata vanno sul suo tratto: quello che dichiarano e' comunque l'affollamento
 * andando li'. Dove una linea arriva alla stessa fermata da due strade (un
 * ramo, una deviazione) si disegna solo la piu' battuta.
 *
 * UN GIORNO SI SOMMA UNA VOLTA SOLA, quando e' finito: l'altro ieri, perche' le
 * corse notturne di ieri finiscono oggi. La tabella e' per mese (il perche' e'
 * nella migrazione V4) e sommare non si puo' ripetere come un ricalcolo: lo
 * impedisce affollamento_giorni, scritta nella stessa transazione. Per rifare un
 * mese si cancellano le sue righe da tutte e due le tabelle: al riavvio i giorni
 * ancora nei passaggi tornano fra i mancanti.
 *
 * Come SlowSegmentsService, l'aggregato gira sul thread del database di
 * PunctualityService e la lettura resta in memoria finche' non cambiano
 * l'aggregato o il GTFS.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CrowdingService {

    /** Da "solo posti in piedi" (3) in su il viaggio e' in piedi. */
    static final int IN_PIEDI_DA = 3;
    /** Oltre questo intervallo dal passaggio prima, quello non dice da dove si veniva. */
    static final int INTERVALLO_MASSIMO_MIN = 20;

    /** In lettura, sotto questo numero di passaggi in tutto il periodo il tratto non si manda. */
    static final int PASSAGGI_MINIMI_TRATTO = 20;
    /** Sotto questo numero di passaggi un'ora o una fascia non ha un valore: lo dice la risposta a chi la disegna. */
    public static final int PASSAGGI_MINIMI = 10;
    public static final int MESI_PREDEFINITI = 2;
    public static final int MESI_MASSIMI = 6;

    /** Una risposta con tutta la rete pesa qualche MB: se ne tengono poche. */
    private static final int RISPOSTE_RETE_IN_MEMORIA = 6;
    private static final int RISPOSTE_LINEA_IN_MEMORIA = 48;

    private final JdbcTemplate jdbc;
    private final GtfsIndexService gtfs;
    private final SlowSegmentsService strade;
    /** Campo e non costante solo perche' i test possano abbassarla. */
    int passaggiMinimiTratto = PASSAGGI_MINIMI_TRATTO;

    @Value("${punctuality.enabled:true}")
    private boolean enabled;

    /**
     * Misurato sommando tre giorni feriali di ottobre 2026: 193 mila righe, 39 MB
     * con l'indice, e un mese intero ha piu' o meno le stesse celle. Con sabati
     * e festivi sono 70-80 MB al mese: sei mesi stanno sotto il mezzo GB, e sono
     * anche il massimo che la lettura somma (MESI_MASSIMI).
     */
    @Value("${punctuality.crowding-retention-months:6}")
    private int mesiConservati;

    /** Cresce a ogni giorno sommato: invalida le risposte in memoria. */
    private final AtomicLong versioneAggregato = new AtomicLong();
    private final Map<String, Risposta> risposteRete = new ConcurrentHashMap<>();
    private final Map<String, Risposta> risposteLinea = new ConcurrentHashMap<>();
    private volatile TransactionTemplate transazione;

    private record Risposta(long aggregato, Instant gtfs, ApiCrowdingSegmentsDTO dto) {}

    /** Passaggi con un valore utile e, fra loro, quelli in piedi: 24 valori, uno per ora. */
    record Serie(int[] passaggi, int[] inPiedi) {
        Serie() {
            this(new int[24], new int[24]);
        }

        int totale() {
            return IntStream.of(passaggi).sum();
        }

        int totaleInPiedi() {
            return IntStream.of(inPiedi).sum();
        }
    }

    record Tratto(String da, String a, Serie serie, TreeSet<String> linee) {}

    /** I tratti e le linee di un periodo, e in quanti giorni di quel tipo c'erano dati. */
    record Periodo(int giorni, List<Tratto> tratti, Map<String, Serie> linee) {}

    // ---- aggregato -------------------------------------------------------------

    /**
     * Somma un giorno di servizio nel suo mese. Chiamarla due volte per lo stesso
     * giorno non lo conta due volte: la seconda non fa niente.
     *
     * @return righe scritte; 0 se il giorno era gia' sommato o nessun mezzo
     *         dichiarava l'affollamento
     */
    public int aggregaGiorno(LocalDate giorno) {
        Date data = Date.valueOf(giorno);
        Integer righe = transazione().execute(stato -> {
            int nuovo = jdbc.update(
                    "INSERT INTO affollamento_giorni (giorno, passaggi) VALUES (?, 0) ON CONFLICT (giorno) DO NOTHING",
                    data);
            if (nuovo == 0) {
                return 0;
            }
            // I giorni prima della colonna affollamento, e quelli senza posizioni:
            // la query delle finestre su 700 mila passaggi non serve.
            Integer utili = jdbc.queryForObject(
                    "SELECT count(*) FROM passaggi WHERE giorno = ? AND affollamento BETWEEN 0 AND 5",
                    Integer.class, data);
            if (utili == null || utili == 0) {
                return 0;
            }
            int scritte = jdbc.update("""
                    WITH t AS (
                        SELECT linea, fermata AS a, affollamento,
                               lag(fermata) OVER w AS da,
                               osservato_il - lag(osservato_il) OVER w AS intervallo,
                               extract(hour FROM osservato_il AT TIME ZONE 'Europe/Rome')::smallint AS ora
                        FROM passaggi
                        WHERE giorno = ?
                        WINDOW w AS (PARTITION BY corsa ORDER BY osservato_il)
                    ), provenienza AS (
                        -- Da quale fermata la linea arriva di solito ad `a`: la conta
                        -- tutti i passaggi, anche quelli senza affollamento.
                        SELECT DISTINCT ON (linea, a) linea, a, da
                        FROM (
                            SELECT linea, a, da, count(*) AS n
                            FROM t
                            WHERE da IS NOT NULL AND da <> a AND intervallo < make_interval(mins => ?)
                            GROUP BY linea, a, da
                        ) x
                        ORDER BY linea, a, n DESC, da
                    )
                    INSERT INTO affollamento_mensile (mese, tipo, linea, da, a, ora, passaggi, in_piedi)
                    SELECT ?, ?, t.linea, p.da, t.a, t.ora, count(*), count(*) FILTER (WHERE t.affollamento >= ?)
                    FROM t JOIN provenienza p ON p.linea = t.linea AND p.a = t.a
                    WHERE t.affollamento BETWEEN 0 AND 5
                    GROUP BY t.linea, p.da, t.a, t.ora
                    ON CONFLICT (mese, tipo, linea, da, a, ora) DO UPDATE SET
                        passaggi = affollamento_mensile.passaggi + EXCLUDED.passaggi,
                        in_piedi = affollamento_mensile.in_piedi + EXCLUDED.in_piedi
                    """,
                    data, INTERVALLO_MASSIMO_MIN, Date.valueOf(giorno.withDayOfMonth(1)), ServiceDayType.di(giorno),
                    IN_PIEDI_DA);
            jdbc.update("UPDATE affollamento_giorni SET passaggi = ? WHERE giorno = ?", utili, data);
            return scritte;
        });
        // Dopo la scrittura, come in SlowSegmentsService: una lettura nel mezzo
        // salverebbe i dati vecchi con la versione nuova.
        versioneAggregato.incrementAndGet();
        return righe == null ? 0 : righe;
    }

    /**
     * Giorni con corse ma non ancora sommati, fino a `finoA` compreso: al primo
     * avvio dopo il rilascio sono tutti quelli dello storico (e costano poco:
     * senza affollamento si segnano e basta), poi i giorni in cui il backend era
     * spento all'ora della manutenzione.
     */
    public List<LocalDate> giorniMancanti(LocalDate finoA) {
        return jdbc.queryForList("""
                SELECT DISTINCT giorno FROM corse_osservate WHERE giorno <= ?
                EXCEPT
                SELECT giorno FROM affollamento_giorni
                ORDER BY 1
                """, Date.class, Date.valueOf(finoA))
                .stream().map(Date::toLocalDate).toList();
    }

    /** Toglie i mesi oltre la conservazione. */
    public int eliminaVecchi(LocalDate oggi) {
        Date limite = Date.valueOf(oggi.withDayOfMonth(1).minusMonths(mesiConservati));
        jdbc.update("DELETE FROM affollamento_giorni WHERE giorno < ?", limite);
        return jdbc.update("DELETE FROM affollamento_mensile WHERE mese < ?", limite);
    }

    private TransactionTemplate transazione() {
        TransactionTemplate t = transazione;
        if (t == null) {
            t = new TransactionTemplate(new DataSourceTransactionManager(Objects.requireNonNull(jdbc.getDataSource())));
            transazione = t;
        }
        return t;
    }

    // ---- lettura ---------------------------------------------------------------

    /**
     * L'affollamento per tratto e ora, per un tipo di giorno (ServiceDayType),
     * sugli ultimi `mesi` mesi compreso quello in corso. Con `linea`, solo i suoi
     * passaggi. Vuoto se lo storico non e' disponibile: database spento, o
     * funzione disattivata.
     */
    public Optional<ApiCrowdingSegmentsDTO> tratti(String tipo, int mesi, String linea) {
        if (!enabled) {
            return Optional.empty();
        }
        YearMonth ultimo = YearMonth.from(PunctualityService.oggi());
        YearMonth primo = ultimo.minusMonths(mesi - 1L);
        Map<String, Risposta> risposte = linea == null ? risposteRete : risposteLinea;
        String chiave = tipo + "|" + primo + "|" + ultimo + "|" + (linea == null ? "" : linea);
        long aggregato = versioneAggregato.get();
        Instant versioneGtfs = gtfs.dataVersion();
        Risposta pronta = risposte.get(chiave);
        if (pronta != null && pronta.aggregato() == aggregato && pronta.gtfs().equals(versioneGtfs)) {
            return Optional.of(pronta.dto());
        }

        Periodo periodo;
        try {
            periodo = leggi(tipo, primo, ultimo, linea);
        } catch (DataAccessException e) {
            log.warn("[Affollamento] lettura fallita: {}", e.toString());
            return Optional.empty();
        }

        List<ApiCrowdingSegmentsDTO.Segment> segmenti = new ArrayList<>();
        for (Tratto t : periodo.tratti()) {
            GtfsIndexService.Stop da = gtfs.stopByIdOrNull(t.da());
            GtfsIndexService.Stop a = gtfs.stopByIdOrNull(t.a());
            if (da == null || a == null || da.lat() == null || a.lat() == null) {
                continue; // fermata non piu' nel GTFS: non si saprebbe dove disegnarla
            }
            List<double[]> strada = strade.strada(da, a);
            segmenti.add(new ApiCrowdingSegmentsDTO.Segment(
                    t.da(), da.name(), t.a(), a.name(),
                    t.linee().stream().sorted(SlowSegmentsService.PER_LINEA).toList(),
                    (int) Math.round(SlowSegmentsService.metri(strada)),
                    strada, t.serie().passaggi(), t.serie().inPiedi()));
        }

        Serie rete = new Serie();
        List<ApiCrowdingSegmentsDTO.Line> linee = new ArrayList<>();
        periodo.linee().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(SlowSegmentsService.PER_LINEA))
                .forEach(voce -> {
                    Serie s = voce.getValue();
                    linee.add(new ApiCrowdingSegmentsDTO.Line(voce.getKey(), s.passaggi(), s.inPiedi()));
                    for (int ora = 0; ora < 24; ora++) {
                        rete.passaggi()[ora] += s.passaggi()[ora];
                        rete.inPiedi()[ora] += s.inPiedi()[ora];
                    }
                });

        ApiCrowdingSegmentsDTO dto = new ApiCrowdingSegmentsDTO(tipo, linea, primo.toString(), ultimo.toString(),
                periodo.giorni(), PASSAGGI_MINIMI,
                new ApiCrowdingSegmentsDTO.Hourly(rete.passaggi(), rete.inPiedi()), linee, segmenti, Instant.now());
        if (risposte.size() >= (linea == null ? RISPOSTE_RETE_IN_MEMORIA : RISPOSTE_LINEA_IN_MEMORIA)) {
            risposte.clear();
        }
        risposte.put(chiave, new Risposta(aggregato, versioneGtfs, dto));
        return Optional.of(dto);
    }

    /**
     * Somma i mesi da `primo` a `ultimo` compresi: i tratti con abbastanza
     * passaggi, dal piu' pieno, e ogni linea ora per ora. La provenienza piu'
     * frequente si sceglie di nuovo sul periodo intero, perche' da un giorno
     * all'altro puo' cambiare.
     */
    Periodo leggi(String tipo, YearMonth primo, YearMonth ultimo, String linea) {
        Date[] mesi = primo.atDay(1).datesUntil(ultimo.plusMonths(1).atDay(1))
                .filter(g -> g.getDayOfMonth() == 1).map(Date::valueOf).toArray(Date[]::new);
        int giorni = (int) jdbc.queryForList(
                        "SELECT giorno FROM affollamento_giorni WHERE giorno >= ? AND giorno < ? AND passaggi > 0",
                        Date.class, Date.valueOf(primo.atDay(1)), Date.valueOf(ultimo.plusMonths(1).atDay(1)))
                .stream().filter(g -> ServiceDayType.di(g.toLocalDate()).equals(tipo)).count();
        if (giorni == 0) {
            return new Periodo(0, List.of(), Map.of());
        }

        Map<String, Tratto> tratti = new LinkedHashMap<>();
        jdbc.query(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    WITH r AS (
                        SELECT linea, da, a, ora, sum(passaggi) AS passaggi, sum(in_piedi) AS in_piedi
                        FROM affollamento_mensile
                        WHERE mese = ANY(?) AND tipo = ? AND (?::text IS NULL OR linea = ?)
                        GROUP BY linea, da, a, ora
                    ), provenienza AS (
                        SELECT DISTINCT ON (linea, a) linea, a, da
                        FROM (SELECT linea, a, da, sum(passaggi) AS n FROM r GROUP BY linea, a, da) x
                        ORDER BY linea, a, n DESC, da
                    )
                    SELECT p.da, r.a, r.ora, sum(r.passaggi) AS passaggi, sum(r.in_piedi) AS in_piedi,
                           array_agg(DISTINCT r.linea) AS linee
                    FROM r JOIN provenienza p ON p.linea = r.linea AND p.a = r.a
                    GROUP BY p.da, r.a, r.ora
                    """);
            ps.setArray(1, con.createArrayOf("date", mesi));
            ps.setString(2, tipo);
            ps.setString(3, linea);
            ps.setString(4, linea);
            return ps;
        }, rs -> {
            String da = rs.getString("da");
            String a = rs.getString("a");
            Tratto t = tratti.computeIfAbsent(da + "|" + a, k -> new Tratto(da, a, new Serie(), new TreeSet<>()));
            int ora = rs.getInt("ora");
            if (ora < 0 || ora > 23) {
                return;
            }
            t.serie().passaggi()[ora] += rs.getInt("passaggi");
            t.serie().inPiedi()[ora] += rs.getInt("in_piedi");
            Array linee = rs.getArray("linee");
            t.linee().addAll(List.of((String[]) linee.getArray()));
        });

        Map<String, Serie> linee = new LinkedHashMap<>();
        jdbc.query(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    SELECT linea, ora, sum(passaggi) AS passaggi, sum(in_piedi) AS in_piedi
                    FROM affollamento_mensile
                    WHERE mese = ANY(?) AND tipo = ? AND (?::text IS NULL OR linea = ?)
                    GROUP BY linea, ora
                    """);
            ps.setArray(1, con.createArrayOf("date", mesi));
            ps.setString(2, tipo);
            ps.setString(3, linea);
            ps.setString(4, linea);
            return ps;
        }, rs -> {
            int ora = rs.getInt("ora");
            if (ora < 0 || ora > 23) {
                return;
            }
            Serie s = linee.computeIfAbsent(rs.getString("linea"), k -> new Serie());
            s.passaggi()[ora] += rs.getInt("passaggi");
            s.inPiedi()[ora] += rs.getInt("in_piedi");
        });

        List<Tratto> ordinati = tratti.values().stream()
                .filter(t -> t.serie().totale() >= passaggiMinimiTratto)
                .sorted(Comparator.comparingInt((Tratto t) -> t.serie().totaleInPiedi()).reversed()
                        .thenComparing(Tratto::da).thenComparing(Tratto::a))
                .toList();
        return new Periodo(giorni, ordinati, linee);
    }
}
