package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.NearbyArrivalDTO;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Gli arrivi di una fermata con una linea richiesta: i suoi non restano fuori
 * dal taglio, e l'elenco resta in ordine di tempo.
 */
class NearbyServiceLineArrivalsTest {

    private static NearbyArrivalDTO arrivo(String linea, int traMinuti) {
        return new NearbyArrivalDTO(linea, "CAPOLINEA", linea + "-" + traMinuti, "79790", null, null, null, traMinuti,
                Boolean.FALSE, null, null);
    }

    private static List<String> comeTesto(List<NearbyArrivalDTO> arrivi) {
        return arrivi.stream().map(a -> a.line() + "@" + a.etaMinutes()).toList();
    }

    /** Il caso visto in produzione: a Corso Vittorio/Argentina il 46 veniva dopo sei corse di altre linee. */
    private static List<NearbyArrivalDTO> corsoVittorio() {
        return List.of(arrivo("40", 1), arrivo("64", 2), arrivo("62", 3), arrivo("628", 4), arrivo("81", 5),
                arrivo("64", 6), arrivo("40", 8), arrivo("46", 11), arrivo("62", 13), arrivo("46", 24), arrivo("64", 25));
    }

    @Test
    void senzaLineaSonoIPrimiInOrdineDiTempo() {
        assertEquals(List.of("40@1", "64@2", "62@3", "628@4", "81@5", "64@6"), comeTesto(NearbyService.conLaLinea(corsoVittorio(), null, 6)));
        assertEquals(List.of("40@1", "64@2", "62@3", "628@4", "81@5", "64@6"), comeTesto(NearbyService.conLaLinea(corsoVittorio(), " ", 6)));
    }

    @Test
    void laLineaRichiestaEntraAncheSeVerrebbeDopoIlTaglio() {
        // Sei posti: fino a tre per il 46 (ne ha due), gli altri quattro ai piu' vicini.
        assertEquals(List.of("40@1", "64@2", "62@3", "628@4", "46@11", "46@24"),
                comeTesto(NearbyService.conLaLinea(corsoVittorio(), "46", 6)));
    }

    @Test
    void laLineaRichiestaNonSiPrendeTuttiIPosti() {
        List<NearbyArrivalDTO> arrivi = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            arrivi.add(arrivo("64", i * 3));
        }
        arrivi.add(arrivo("40", 4));
        arrivi.add(arrivo("62", 5));
        arrivi.sort(java.util.Comparator.comparingInt(NearbyArrivalDTO::etaMinutes));

        // Quattro posti: due al 64, poi i piu' vicini nel tempo qualunque linea siano.
        assertEquals(List.of("64@3", "40@4", "62@5", "64@6"), comeTesto(NearbyService.conLaLinea(arrivi, "64", 4)));
    }

    @Test
    void ilNomeDellaLineaNonBadaAMaiuscoleESpazi() {
        List<NearbyArrivalDTO> arrivi = List.of(arrivo("40", 1), arrivo("64", 2), arrivo("n8", 30), arrivo("H", 40));
        assertEquals(List.of("40@1", "n8@30"), comeTesto(NearbyService.conLaLinea(arrivi, " N8 ", 2)));
        assertEquals(List.of("40@1", "H@40"), comeTesto(NearbyService.conLaLinea(arrivi, "h", 2)));
    }

    @Test
    void seLaLineaNonPassaRestanoIPrimi() {
        assertEquals(List.of("40@1", "64@2", "62@3"), comeTesto(NearbyService.conLaLinea(corsoVittorio(), "999", 3)));
    }

    @Test
    void conPochiArriviNonCeNienteDaTagliare() {
        List<NearbyArrivalDTO> pochi = List.of(arrivo("40", 1), arrivo("46", 9));
        assertEquals(List.of("40@1", "46@9"), comeTesto(NearbyService.conLaLinea(pochi, "46", 6)));
        assertEquals(List.of(), comeTesto(NearbyService.conLaLinea(List.of(), "46", 6)));
    }
}
