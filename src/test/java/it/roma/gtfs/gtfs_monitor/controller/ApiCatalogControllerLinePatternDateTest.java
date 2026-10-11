package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.service.CrowdingService;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import it.roma.gtfs.gtfs_monitor.service.PunctualityService;
import it.roma.gtfs.gtfs_monitor.service.SlowSegmentsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Il ?date= del percorso di linea ha lo stesso limite di /stop-schedules: ogni
 * data fuori dalla cache costa una scansione completa di stop_times.
 */
class ApiCatalogControllerLinePatternDateTest {

    private GtfsIndexService gtfs;
    private ApiCatalogController controller;
    private final LocalDate oggi = LocalDate.now(ZoneId.of("Europe/Rome"));

    @BeforeEach
    void setUp() {
        gtfs = mock(GtfsIndexService.class);
        when(gtfs.isStaticDataLoaded()).thenReturn(true);
        when(gtfs.routeIdsByPublicLine("64")).thenReturn(Set.of("R1"));
        when(gtfs.linePatterns(anyString(), any())).thenReturn(List.of());
        when(gtfs.modeForLine("64")).thenReturn("bus");
        controller = new ApiCatalogController(gtfs, mock(PunctualityService.class),
                mock(SlowSegmentsService.class), mock(CrowdingService.class));
    }

    @Test
    void dataLontanaRifiutataCon400() {
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> controller.linePattern("64", oggi.plusYears(1)));
        assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        verify(gtfs, never()).linePatterns(anyString(), any());
    }

    @Test
    void dataPassataRifiutataCon400() {
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> controller.linePattern("64", oggi.minusDays(2)));
        assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
    }

    @Test
    void dataNelLimiteOAssenteAccettata() {
        assertEquals("64", controller.linePattern("64", oggi.plusDays(14)).line());
        assertEquals("64", controller.linePattern("64", null).line());
    }
}
