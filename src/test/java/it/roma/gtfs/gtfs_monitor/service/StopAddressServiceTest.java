package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ReverseGeocodeDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La via delle fermate presa dal file: quando un punto "e'" una fermata e
 * quando no.
 */
class StopAddressServiceTest {

    private static StopAddressService conFile(String csv) {
        return new StopAddressService(new ByteArrayResource(csv.getBytes(StandardCharsets.UTF_8)));
    }

    private static final String DUE_FERMATE = """
            stop_id,lat,lon,street,city
            71882,41.917160,12.455040,Piazzale Clodio,Roma
            70200,41.917523,12.455418,"Via della Giuliana, lato dispari",
            """;

    @Test
    @DisplayName("le coordinate come le serviamo noi, con le cifre in piu' del float, trovano la fermata")
    void coordinateDelFeed() {
        ReverseGeocodeDTO indirizzo = conFile(DUE_FERMATE).find(41.91716003417969, 12.455039978027344).orElseThrow();

        assertThat(indirizzo.label()).isEqualTo("Piazzale Clodio, Roma");
        assertThat(indirizzo.street()).isEqualTo("Piazzale Clodio");
        assertThat(indirizzo.city()).isEqualTo("Roma");
        // Il punto resta quello chiesto: chi chiama lo confronta col suo.
        assertThat(indirizzo.lat()).isEqualTo(41.91716003417969);
    }

    @Test
    @DisplayName("a trenta metri dalla fermata non e' piu' la fermata")
    void lontanoDallaFermata() {
        assertThat(conFile(DUE_FERMATE).find(41.917160 + 0.00027, 12.455040)).isEmpty();
    }

    @Test
    @DisplayName("tra due fermate vicine vale la piu' vicina")
    void laPiuVicina() {
        StopAddressService fermate = conFile("""
                stop_id,lat,lon,street,city
                A,41.900000,12.500000,Via Uno,Roma
                B,41.900100,12.500000,Via Due,Roma
                """);

        assertThat(fermate.find(41.900030, 12.500000).orElseThrow().street()).isEqualTo("Via Uno");
        assertThat(fermate.find(41.900070, 12.500000).orElseThrow().street()).isEqualTo("Via Due");
    }

    @Test
    @DisplayName("senza comune l'indirizzo e' la sola via, anche se ha una virgola nel nome")
    void senzaComune() {
        ReverseGeocodeDTO indirizzo = conFile(DUE_FERMATE).find(41.917523, 12.455418).orElseThrow();

        assertThat(indirizzo.label()).isEqualTo("Via della Giuliana, lato dispari");
        assertThat(indirizzo.city()).isNull();
    }

    @Test
    @DisplayName("senza file non si rompe niente: nessuna fermata, si chiedera' fuori")
    void senzaFile() {
        StopAddressService fermate = new StopAddressService(new ClassPathResource("geocode/non-esiste.csv"));

        assertThat(fermate.size()).isZero();
        assertThat(fermate.find(41.917160, 12.455040)).isEmpty();
    }

    @Test
    @DisplayName("il file che parte col backend si legge e contiene le fermate di Roma")
    void fileVero() {
        StopAddressService fermate = new StopAddressService();

        assertThat(fermate.size()).isGreaterThan(8_000);
        assertThat(fermate.find(41.91716003417969, 12.455039978027344).orElseThrow().label())
                .isEqualTo("Piazzale Clodio, Roma");
    }
}
