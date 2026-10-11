package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

/**
 * Quanto sono pieni i bus, tratto per tratto e ora per ora, per un tipo di
 * giorno. Vedi CrowdingService.
 *
 * Ogni serie ha 24 valori, uno per ora di Roma: {@code samples} sono i passaggi
 * in cui il mezzo dichiarava quanto era pieno, {@code standing} quelli fra loro
 * con soli posti in piedi (o peggio). La quota e' standing / samples, e la
 * calcola chi legge: cosi' puo' sommare le ore in fasce senza richiedere niente.
 *
 * DUE COSE DA RISPETTARE NEL MOSTRARLI. Un'ora (o una fascia) con meno di
 * {@code minSamples} passaggi non ha un valore: e' "dati insufficienti", non
 * "posti liberi". E i tratti che mancano non sono vuoti: sono quelli delle
 * linee i cui mezzi non dichiarano l'affollamento, circa la meta'.
 *
 * {@code days} sono i giorni di quel tipo, fra {@code fromMonth} e
 * {@code toMonth}, in cui c'erano dati. I tratti sono in ordine di passaggi in
 * piedi, dal piu' pieno.
 */
public record ApiCrowdingSegmentsDTO(
        String dayType,
        /** La linea richiesta, o null per tutte. */
        String line,
        /** Primo e ultimo mese sommati, come 2026-10. */
        String fromMonth,
        String toMonth,
        int days,
        int minSamples,
        /** Tutta la rete (o tutta la linea richiesta), ora per ora. */
        Hourly hours,
        List<Line> lines,
        List<Segment> segments,
        Instant generatedAt
) {

    public record Hourly(int[] samples, int[] standing) {}

    public record Line(String line, int[] samples, int[] standing) {}

    /**
     * Il tratto fra due fermate consecutive; i valori sono quelli dichiarati dai
     * mezzi mentre andavano verso la seconda.
     *
     * @param lines       le linee che hanno dati sul tratto; con una linea
     *                    richiesta, solo lei
     * @param coordinates [lon, lat] lungo la strada; due punti se il tracciato
     *                    non si e' trovato (linea dritta fra le fermate)
     */
    public record Segment(
            String fromStopId,
            String fromStopName,
            String toStopId,
            String toStopName,
            List<String> lines,
            int lengthMeters,
            List<double[]> coordinates,
            int[] samples,
            int[] standing
    ) {}
}
