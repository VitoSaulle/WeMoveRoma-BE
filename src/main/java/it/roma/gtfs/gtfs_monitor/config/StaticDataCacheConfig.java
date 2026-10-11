package it.roma.gtfs.gtfs_monitor.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.web.filter.ShallowEtagHeaderFilter;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * ETag e Cache-Control sugli endpoint quasi statici.
 *
 * Catalogo linee, elenco fermate e geometrie delle corse cambiano solo quando si
 * aggiorna il GTFS (due volte al giorno), ma venivano riscaricati per intero a
 * ogni avvio di ogni client: l'elenco fermate da solo pesa ~240 KB compressi.
 * Con l'ETag il secondo giro costa un 304 vuoto.
 *
 * Il filtro si applica SOLO a questi percorsi. Fuori restano esclusi:
 * - `/vehicles/stream` (SSE): bufferizzare la risposta romperebbe lo streaming;
 * - realtime (`/vehicles`, `/nearby`, `/alerts`, `/stops/{id}/arrivals`): cambiano
 *   ogni 5 secondi, una cache li renderebbe sbagliati.
 */
@Configuration
public class StaticDataCacheConfig {

    /** Quanto i client possono riusare la risposta senza richiedere nulla. */
    private static final long MAX_AGE_SECONDS = 300;

    private static final List<Pattern> CACHEABLE = List.of(
            Pattern.compile("^/api/v1/catalog/.*$"),
            Pattern.compile("^/api/v1/stops$"),
            Pattern.compile("^/api/v1/stops/search$"),
            Pattern.compile("^/api/v1/trips/[^/]+/shape$"),
            Pattern.compile("^/api/v1/trips/[^/]+/stops$")
    );

    static boolean isCacheable(String path) {
        return path != null && CACHEABLE.stream().anyMatch(p -> p.matcher(path).matches());
    }

    /** Cache pubblica solo per i 2xx ancora modificabili; il resto tiene no-store. */
    static void applyCacheControl(HttpServletResponse response) {
        int status = response.getStatus();
        if (!response.isCommitted() && status >= 200 && status < 300) {
            response.setHeader(HttpHeaders.CACHE_CONTROL, "public, max-age=" + MAX_AGE_SECONDS);
        }
    }

    @Bean
    public ShallowEtagHeaderFilter staticDataEtagFilter() {
        return new ShallowEtagHeaderFilter() {
            @Override
            protected boolean shouldNotFilter(HttpServletRequest request) {
                return !"GET".equalsIgnoreCase(request.getMethod()) || !isCacheable(request.getRequestURI());
            }

            @Override
            protected void doFilterInternal(HttpServletRequest request,
                                            HttpServletResponse response,
                                            FilterChain filterChain) throws ServletException, IOException {
                // Senza Cache-Control il browser rivalida comunque a ogni richiesta:
                // l'ETag risparmia banda, questo risparmia anche il giro di rete.
                //
                // Ma solo sulle risposte 2xx: prima l'header partiva prima di sapere
                // lo stato, e un 503 all'avvio (dati GTFS non ancora caricati), un 404
                // o un 400 restavano in cache pubblica per 5 minuti. Si parte quindi da
                // no-store, che resta se la risposta e' un errore o e' gia' stata
                // inviata (sendError), e si passa a public solo a catena finita su un
                // 2xx: il corpo e' ancora nel buffer del filtro ETag, gli header si
                // possono cambiare.
                response.setHeader(HttpHeaders.CACHE_CONTROL, "no-store");
                FilterChain conCache = (req, res) -> {
                    filterChain.doFilter(req, res);
                    applyCacheControl((HttpServletResponse) res);
                };
                super.doFilterInternal(request, response, conCache);
            }
        };
    }
}
