package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.JourneyLegDTO;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Il treno iniettato fra due stazioni vicine rispetta la richiesta:
 * "parti alle" sceglie il primo prendibile, "arriva entro" l'ultimo che arriva in tempo.
 */
class JourneyPlannerServiceRailTest {

    /** Regionali Ostiense -> Trastevere ogni 15 minuti, 6 minuti di viaggio. */
    private static final List<JourneyLegDTO> TRENI = List.of(
            treno("2026-10-12T08:00:00Z", "2026-10-12T08:06:00Z"),
            treno("2026-10-12T08:15:00Z", "2026-10-12T08:21:00Z"),
            treno("2026-10-12T08:30:00Z", "2026-10-12T08:36:00Z"),
            treno("2026-10-12T08:45:00Z", "2026-10-12T08:51:00Z"));

    private static JourneyLegDTO treno(String parte, String arriva) {
        return new JourneyLegDTO("RAIL", "FL1", "FL1", null, null, null, null, null,
                true, null, "Ostiense", "Trastevere", parte, arriva, 6, null,
                null, null, null, null, null, List.of());
    }

    private static JourneyPlannerService.TimePreference partenza(String quando) {
        return new JourneyPlannerService.TimePreference("earliestDeparture", Instant.parse(quando));
    }

    private static JourneyPlannerService.TimePreference arrivoEntro(String quando) {
        return new JourneyPlannerService.TimePreference("latestArrival", Instant.parse(quando));
    }

    @Test
    void inPartenzaPrendeIlPrimoTrenoRaggiungibileAPiedi() {
        // Parto alle 08:05 con 8 minuti a piedi: in stazione alle 08:13, prendo quello delle 08:15.
        JourneyLegDTO scelto = JourneyPlannerService.scegliTreno(TRENI, partenza("2026-10-12T08:05:00Z"), 8, 4);
        assertEquals("2026-10-12T08:15:00Z", scelto.startTime());
        assertEquals(List.of("2026-10-12T08:30:00Z", "2026-10-12T08:45:00Z"),
                JourneyPlannerService.altrePartenzeTreno(TRENI, scelto, partenza("2026-10-12T08:05:00Z"), 4));
    }

    @Test
    void inArrivoEntroPrendeLUltimoTrenoCheArrivaInTempo() {
        // Devo essere a destinazione entro le 08:45 con 10 minuti a piedi dalla stazione:
        // il treno delle 08:30 (arriva 08:36, a destinazione 08:46) e' tardi, quello delle 08:15 no.
        // Prima del fix si prendeva il primo treno dopo le 08:45 + cammino: arrivo ben oltre la scadenza.
        JourneyPlannerService.TimePreference entro845 = arrivoEntro("2026-10-12T08:45:00Z");
        JourneyLegDTO scelto = JourneyPlannerService.scegliTreno(TRENI, entro845, 8, 10);
        assertEquals("2026-10-12T08:15:00Z", scelto.startTime());
        // Le alternative sono i treni prima, che arrivano comunque in tempo.
        assertEquals(List.of("2026-10-12T08:00:00Z"),
                JourneyPlannerService.altrePartenzeTreno(TRENI, scelto, entro845, 10));
    }

    @Test
    void inArrivoEntroUnTrenoCheArrivaGiustoInTempoVaBene() {
        // 08:36 + 9 minuti = 08:45 esatte.
        JourneyLegDTO scelto = JourneyPlannerService.scegliTreno(TRENI, arrivoEntro("2026-10-12T08:45:00Z"), 8, 9);
        assertEquals("2026-10-12T08:30:00Z", scelto.startTime());
    }

    @Test
    void senzaTreniUtiliNonSiInietta() {
        assertNull(JourneyPlannerService.scegliTreno(TRENI, arrivoEntro("2026-10-12T08:05:00Z"), 8, 4));
        assertNull(JourneyPlannerService.scegliTreno(TRENI, partenza("2026-10-12T08:40:00Z"), 8, 4));
        assertNull(JourneyPlannerService.scegliTreno(List.of(), partenza("2026-10-12T08:00:00Z"), 0, 0));
    }
}
