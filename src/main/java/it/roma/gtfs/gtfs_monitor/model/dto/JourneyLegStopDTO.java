package it.roma.gtfs.gtfs_monitor.model.dto;

/**
 * Una fermata lungo una tratta dell'itinerario.
 *
 * @param stopId la palina del GTFS (lo stesso stop_id di /api/v1/stops), quando
 *               la fermata del planner e' una di quelle: chi mostra gli arrivi la
 *               apre per codice, invece di cercarla per nome e posizione. Null
 *               per cio' che il GTFS di Roma non ha (le stazioni dei treni
 *               regionali) e quando il planner e il GTFS non concordano su dove
 *               sta: vedi JourneyPlannerService.palina.
 */
public record JourneyLegStopDTO(
        String stopName,
        Double lat,
        Double lon,
        String stopId
) {
}
