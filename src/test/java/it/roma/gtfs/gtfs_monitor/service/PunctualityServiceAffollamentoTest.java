package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Dall'istantanea delle posizioni al codice di affollamento per corsa. */
class PunctualityServiceAffollamentoTest {

    private static VehiclePositionDTO mezzo(String corsa, String affollamento) {
        return VehiclePositionDTO.builder().linea("64").corsa(corsa).veicolo("v-" + corsa)
                .occupancyStatus(affollamento).build();
    }

    @Test
    void traduceIlNomeDelFeedNelCodiceDellaSpecifica() {
        Map<String, Integer> out = PunctualityService.affollamentoPerCorsa(List.of(
                mezzo("T1", "MANY_SEATS_AVAILABLE"),
                mezzo("T2", "STANDING_ROOM_ONLY"),
                mezzo("T3", "NO_DATA_AVAILABLE")));
        assertEquals(Map.of("T1", 1, "T2", 3, "T3", 7), out);
    }

    @Test
    void saltaChiNonDichiaraNienteEINomiSconosciuti() {
        Map<String, Integer> out = PunctualityService.affollamentoPerCorsa(Arrays.asList(
                mezzo("T1", null),
                mezzo(null, "STANDING_ROOM_ONLY"),
                mezzo("T2", "UN_VALORE_DI_DOMANI"),
                null));
        assertTrue(out.isEmpty());
        assertTrue(PunctualityService.affollamentoPerCorsa(null).isEmpty());
    }
}
