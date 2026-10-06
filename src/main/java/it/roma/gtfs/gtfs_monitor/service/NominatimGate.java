package it.roma.gtfs.gtfs_monitor.service;

import org.springframework.stereotype.Component;

/**
 * Il freno di Nominatim, uno solo per ricerca e geocodifica inversa: il tetto
 * di una richiesta al secondo vale per il server, non per chi di noi chiede.
 */
@Component
public class NominatimGate extends UpstreamGate {
    public NominatimGate() {
        super("Nominatim", 1_000L);
    }
}
