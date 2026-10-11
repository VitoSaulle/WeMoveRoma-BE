package it.roma.gtfs.gtfs_monitor.service;

import org.springframework.stereotype.Component;

/**
 * Il freno di Photon, uno solo per ricerca e geocodifica inversa: come per
 * Nominatim, il servizio conta le richieste del server, non di chi di noi chiede.
 * Prima solo la geocodifica inversa passava da un freno, e l'autocomplete
 * della ricerca chiamava Photon a ogni tasto.
 */
@Component
public class PhotonGate extends UpstreamGate {
    static final long MIN_INTERVAL_MILLIS = 500L;

    public PhotonGate() {
        super("Photon", MIN_INTERVAL_MILLIS);
    }
}
