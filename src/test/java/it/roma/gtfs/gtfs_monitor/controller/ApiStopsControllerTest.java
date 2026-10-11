package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import it.roma.gtfs.gtfs_monitor.service.NearbyService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Con i dati statici non ancora caricati, elenco e ricerca fermate devono
 * rispondere 503: una lista vuota con 200 finirebbe in cache per 5 minuti.
 */
class ApiStopsControllerTest {

    private final NearbyService nearbyService = mock(NearbyService.class);
    private final GtfsIndexService indexService = mock(GtfsIndexService.class);
    private final ApiStopsController controller = new ApiStopsController(nearbyService, indexService);

    @Test
    @DisplayName("listStops prima del caricamento: 503")
    void listStopsNonPronto() {
        when(indexService.isStaticDataLoaded()).thenReturn(false);

        assertThatThrownBy(() -> controller.listStops(null, null, null, null, null))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(e -> org.assertj.core.api.Assertions.assertThat(
                        ((ResponseStatusException) e).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
        verifyNoInteractions(nearbyService);
    }

    @Test
    @DisplayName("searchStops prima del caricamento: 503")
    void searchStopsNonPronto() {
        when(indexService.isStaticDataLoaded()).thenReturn(false);

        assertThatThrownBy(() -> controller.searchStops("termini", null))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(e -> org.assertj.core.api.Assertions.assertThat(
                        ((ResponseStatusException) e).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
        verifyNoInteractions(nearbyService);
    }
}
