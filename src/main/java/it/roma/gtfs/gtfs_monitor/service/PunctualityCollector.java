package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dalle istantanee del feed TripUpdates a due cose: un passaggio per ogni
 * fermata servita da ogni corsa, e una voce per ogni corsa vista.
 *
 * COSA SI MISURA. Per ogni corsa il feed elenca le fermate che mancano, in
 * ordine; la prima e' quella a cui il mezzo si sta avvicinando, e il suo
 * ritardo e' la differenza fra l'arrivo previsto e l'orario programmato. Si
 * tiene l'ultimo valore visto per quella fermata, e lo si registra quando la
 * prima fermata diventa un'altra: cioe' quando il mezzo ci e' passato. Piu' ci
 * si avvicina, piu' la previsione coincide con l'arrivo vero.
 *
 * E' STATO VERIFICATO, non assunto (settembre 2026): su 45 mezzi fermi entro
 * 60 metri da una fermata, il ritardo del feed e il ritardo osservato — ora del
 * GPS meno orario programmato di quella fermata — differivano in mediana di
 * mezzo minuto, e in 41 casi su 45 di meno di due.
 *
 * UNA FERMATA E' PASSATA QUANDO SPARISCE, non quando smette di essere la prima.
 * Il feed a volte fa tornare indietro la prossima fermata — 38, 39, 38, 39 — e
 * contare ogni cambio registrava la stessa fermata piu' volte, e a volte una
 * fermata a cui il mezzo non era ancora arrivato (settembre 2026: 346 doppioni
 * su 8.103 passaggi, 177 corse su 1.239). Ora la vecchia prima fermata vale
 * solo se non compare piu' fra quelle rimanenti della corsa, e ogni fermata si
 * registra una volta sola.
 *
 * Il feed di Roma si aggiorna ogni 30 secondi circa: le fermate superate in
 * meno tempo non compaiono mai, e si perdono (circa il 7%). Campionare piu'
 * spesso non le recupera.
 *
 * Cosa si scarta:
 * - la prima fermata vista per ogni corsa: e' il capolinea, dove il mezzo
 *   aspetta e l'"anticipo" non vuol dire niente, oppure una fermata raccolta a
 *   meta' dopo un riavvio del backend;
 * - i ritardi oltre un'ora in un senso o nell'altro: e' un mezzo abbinato alla
 *   corsa sbagliata, non un bus che viaggia un'ora avanti.
 *
 * Una corsa sparita dal feed ha finito il percorso: la sua ultima fermata vale,
 * purche' la corsa manchi da almeno un minuto (un feed saltato una volta non
 * chiude niente) e sia stata vista da meno di dieci.
 *
 * L'AFFOLLAMENTO viaggia col passaggio: e' quello che il mezzo dichiarava nel
 * feed delle posizioni mentre si avvicinava alla fermata, cioe' quanto era
 * pieno lungo il tratto che ci arriva. Si tiene l'ultimo valore utile visto
 * durante quell'avvicinamento: un giro in cui il mezzo manca dalle posizioni, o
 * dice "nessun dato", non cancella quello di trenta secondi prima. Alla fermata
 * successiva si riparte da capo. Senza dichiarazione resta null: la meta' circa
 * dei mezzi non lo dice mai (vedi la migrazione V3).
 *
 * Lo chiamano due thread — il campionamento e la scrittura su database — quindi
 * i metodi pubblici sono sincronizzati. Nessuna dipendenza da Spring.
 */
public final class PunctualityCollector {

    /** affollamento: codice OccupancyStatus del GTFS-Realtime, null se il mezzo non lo dichiarava. */
    public record Passaggio(
            String routeId, String corsa, String fermataId, String veicolo,
            double ritardoMin, Instant quando, Instant primaVistaCorsa, Integer affollamento) {}

    /** fermateNuove: passaggi registrati dall'ultima estrazione, da sommare a quelli gia' salvati. */
    public record CorsaOsservata(
            String routeId, String corsa, String veicolo,
            Instant primaVista, Instant ultimaVista, int fermateNuove, Double ultimoRitardoMin) {}

    static final double RITARDO_MASSIMO_MIN = 60;
    static final Duration ASSENZA_PER_CHIUDERE = Duration.ofMinutes(1);
    static final Duration SCADENZA = Duration.ofMinutes(10);

    private static final class Corsa {
        final String routeId;
        final Instant primaVista;
        String veicolo;
        String fermataId;
        Double ritardoMin;
        /** Ultima previsione per fermataId: e' l'ora del passaggio che si registra. */
        Instant fermataVista;
        /** Affollamento dichiarato mentre fermataId era la prossima fermata. */
        Integer affollamento;
        /** Ultimo giro in cui la corsa era nel feed: decide quando chiuderla. */
        Instant vista;
        boolean primaFermata = true;
        int fermateNuove;
        /** Fermate gia' registrate: il feed che torna indietro non le conta due volte. */
        final Set<String> registrate = new HashSet<>();

        Corsa(String routeId, Instant primaVista) {
            this.routeId = routeId;
            this.primaVista = primaVista;
        }
    }

    private final Map<String, Corsa> corse = new HashMap<>();
    private final List<CorsaOsservata> chiuse = new ArrayList<>();

    public synchronized List<Passaggio> osserva(List<TripUpdateDTO> righe, Instant adesso) {
        return osserva(righe, Map.of(), adesso);
    }

    /**
     * @param affollamenti per corsa, il codice OccupancyStatus che il mezzo dichiara
     *                     adesso nel feed delle posizioni; le corse che mancano non
     *                     dichiarano niente
     */
    public synchronized List<Passaggio> osserva(List<TripUpdateDTO> righe, Map<String, Integer> affollamenti,
                                                Instant adesso) {
        // La prima riga di ogni corsa, nell'ordine del feed, e' la prossima fermata;
        // tutte le righe insieme sono le fermate che il mezzo non ha ancora passato.
        Map<String, TripUpdateDTO> prossime = new LinkedHashMap<>();
        Map<String, Set<String>> rimanenti = new HashMap<>();
        for (TripUpdateDTO r : righe) {
            if (r == null || r.getCorsa() == null || r.getFermataId() == null) {
                continue;
            }
            prossime.putIfAbsent(r.getCorsa(), r);
            rimanenti.computeIfAbsent(r.getCorsa(), k -> new HashSet<>()).add(r.getFermataId());
        }

        List<Passaggio> out = new ArrayList<>();
        prossime.forEach((id, r) -> {
            Corsa corsa = corse.computeIfAbsent(id, k -> new Corsa(r.getLinea(), adesso));
            corsa.vista = adesso;
            if (r.getVeicolo() != null) {
                corsa.veicolo = r.getVeicolo();
            }
            Double ritardo = ritardo(r);
            if (ritardo == null) {
                return;
            }
            boolean stessaFermata = r.getFermataId().equals(corsa.fermataId);
            if (corsa.fermataId != null && !stessaFermata
                    && !rimanenti.get(id).contains(corsa.fermataId)) {
                registra(id, corsa, out);
                corsa.primaFermata = false;
            }
            Integer affollamento = affollamenti.get(id);
            if (!stessaFermata || affollamentoUtile(affollamento)
                    || (affollamento != null && !affollamentoUtile(corsa.affollamento))) {
                corsa.affollamento = affollamento;
            }
            corsa.fermataId = r.getFermataId();
            corsa.ritardoMin = ritardo;
            corsa.fermataVista = adesso;
        });

        Iterator<Map.Entry<String, Corsa>> it = corse.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Corsa> voce = it.next();
            if (prossime.containsKey(voce.getKey())) {
                continue;
            }
            Corsa corsa = voce.getValue();
            Duration assente = Duration.between(corsa.vista, adesso);
            if (assente.compareTo(ASSENZA_PER_CHIUDERE) < 0) {
                continue;
            }
            if (assente.compareTo(SCADENZA) < 0) {
                registra(voce.getKey(), corsa, out);
            }
            chiuse.add(osservata(voce.getKey(), corsa));
            it.remove();
        }
        return out;
    }

    /**
     * Le corse da salvare: quelle chiuse dall'ultima chiamata e quelle ancora in
     * corso. Azzera i contatori dei passaggi, che nel database si sommano.
     */
    public synchronized List<CorsaOsservata> estraiCorse() {
        List<CorsaOsservata> out = new ArrayList<>(chiuse);
        chiuse.clear();
        corse.forEach((id, corsa) -> {
            out.add(osservata(id, corsa));
            corsa.fermateNuove = 0;
        });
        return out;
    }

    private static CorsaOsservata osservata(String id, Corsa corsa) {
        return new CorsaOsservata(corsa.routeId, id, corsa.veicolo, corsa.primaVista, corsa.vista,
                corsa.fermateNuove, corsa.ritardoMin);
    }

    private static void registra(String id, Corsa corsa, List<Passaggio> out) {
        if (corsa.primaFermata || corsa.ritardoMin == null) {
            return;
        }
        if (Math.abs(corsa.ritardoMin) > RITARDO_MASSIMO_MIN) {
            return;
        }
        if (!corsa.registrate.add(corsa.fermataId)) {
            return;
        }
        out.add(new Passaggio(corsa.routeId, id, corsa.fermataId, corsa.veicolo, corsa.ritardoMin,
                corsa.fermataVista, corsa.primaVista, corsa.affollamento));
        corsa.fermateNuove++;
    }

    /**
     * Da 0 (vuoto) a 5 (pieno) il codice dice quanto e' pieno il mezzo. Gli altri
     * no: 6 "non fa salire", 7 "nessun dato", 8 "non accessibile".
     */
    public static boolean affollamentoUtile(Integer codice) {
        return codice != null && codice >= 0 && codice <= 5;
    }

    private static Double ritardo(TripUpdateDTO r) {
        return r.getRitardoArrivoMin() != null ? r.getRitardoArrivoMin() : r.getRitardoPartenzaMin();
    }
}
