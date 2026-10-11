package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.JourneyLegStopDTO;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Le fermate di un itinerario portano la palina del GTFS solo quando il planner
 * e il GTFS parlano davvero della stessa fermata.
 */
class JourneyPlannerServiceStopsTest {

    /** Termini e una fermata dall'altra parte della citta', come le ha il GTFS. */
    private static final Map<String, GtfsIndexService.Stop> GTFS = Map.of(
            "05000", palinaGtfs("05000", "TERMINI (MA-MB-FS)", 41.901308f, 12.500433f),
            "70101", palinaGtfs("70101", "PLEBISCITO", 41.896467f, 12.480985f),
            "99999", palinaGtfs("99999", "SENZA COORDINATE", null, null));
    private static final Function<String, GtfsIndexService.Stop> PALINE = GTFS::get;

    private static GtfsIndexService.Stop palinaGtfs(String id, String nome, Float lat, Float lon) {
        return new GtfsIndexService.Stop(id, id, nome, null, lat, lon, null, null, null, null, null, false);
    }

    private static Map<String, Object> chiamata(String gtfsId, String nome, double lat, double lon) {
        Map<String, Object> fermata = new java.util.HashMap<>();
        if (gtfsId != null) {
            fermata.put("gtfsId", gtfsId);
        }
        fermata.put("name", nome);
        fermata.put("lat", lat);
        fermata.put("lon", lon);
        return Map.of("stopLocation", fermata);
    }

    @Test
    void toglieIlFeedEDaLaPalinaDelGtfs() {
        assertEquals("05000", JourneyPlannerService.palina("1:05000", 41.901308, 12.500433, PALINE));
        // Senza prefisso del feed vale lo stesso.
        assertEquals("05000", JourneyPlannerService.palina("05000", 41.901308, 12.500433, PALINE));
    }

    /** Le stazioni dei regionali vengono dal NeTEx di Trenitalia: il GTFS di Roma non le ha. */
    @Test
    void unaFermataCheIlGtfsNonHaRestaSenzaPalina() {
        assertNull(JourneyPlannerService.palina("2:IT:ITF3:StopPlace:S08409", 41.896019, 12.454504, PALINE));
        assertNull(JourneyPlannerService.palina(null, 41.9, 12.5, PALINE));
        assertNull(JourneyPlannerService.palina("  ", 41.9, 12.5, PALINE));
        assertNull(JourneyPlannerService.palina("1:", 41.9, 12.5, PALINE));
    }

    /** Un altro feed puo' usare lo stesso codice per una fermata che sta altrove. */
    @Test
    void loStessoCodiceInUnAltroPostoNonELaStessaPalina() {
        // "05000" a Ostia, a venti chilometri da Termini.
        assertNull(JourneyPlannerService.palina("3:05000", 41.7330, 12.2800, PALINE));
    }

    /** Fra un aggiornamento del GTFS e la ricostruzione del grafo una palina puo' essersi spostata. */
    @Test
    void unaPalinaSpostataDiPocoRestaLei() {
        // 44 metri piu' a nord: lo spostamento piu' grande visto in tre giorni (ottobre 2026).
        assertEquals("05000", JourneyPlannerService.palina("1:05000", 41.901308 + 44 / 111_320.0, 12.500433, PALINE));
        // 140 metri: ancora dentro la soglia. 200: fuori.
        assertEquals("05000", JourneyPlannerService.palina("1:05000", 41.901308 + 140 / 111_320.0, 12.500433, PALINE));
        assertNull(JourneyPlannerService.palina("1:05000", 41.901308 + 200 / 111_320.0, 12.500433, PALINE));
    }

    @Test
    void unaPalinaSenzaCoordinateNonSiPuoConfermare() {
        assertNull(JourneyPlannerService.palina("1:99999", 41.9, 12.5, PALINE));
    }

    @Test
    void leFermateDellaTrattaPortanoOgnunaLaSuaPalina() {
        List<JourneyLegStopDTO> fermate = JourneyPlannerService.extractLegStops(List.of(
                chiamata("1:05000", "TERMINI (MA-MB-FS)", 41.901308, 12.500433),
                // La stessa fermata due volte di seguito (arrivo e ripartenza): una sola.
                chiamata("1:05000", "TERMINI (MA-MB-FS)", 41.901308, 12.500433),
                chiamata("2:IT:ITF3:StopPlace:S08409", "ROMA S.PIETRO", 41.896019, 12.454504),
                chiamata(null, "FERMATA SENZA CODICE", 41.89, 12.48),
                chiamata("1:70101", "PLEBISCITO", 41.896467, 12.480985)), PALINE);

        assertEquals(List.of("TERMINI (MA-MB-FS)", "ROMA S.PIETRO", "FERMATA SENZA CODICE", "PLEBISCITO"),
                fermate.stream().map(JourneyLegStopDTO::stopName).toList());
        assertEquals("05000", fermate.get(0).stopId());
        assertNull(fermate.get(1).stopId());
        assertNull(fermate.get(2).stopId());
        assertEquals("70101", fermate.get(3).stopId());
        // Nome e coordinate restano quelli del planner: servono a disegnare l'itinerario.
        assertEquals(41.896467, fermate.get(3).lat());
    }

    @Test
    void senzaFermateOConDatiRottiNonSiRompe() {
        assertEquals(List.of(), JourneyPlannerService.extractLegStops(null, PALINE));
        assertEquals(List.of(), JourneyPlannerService.extractLegStops(List.of(), PALINE));
        assertEquals(List.of(), JourneyPlannerService.extractLegStops(List.of("non una mappa", Map.of("altro", 1)), PALINE));
    }
}
