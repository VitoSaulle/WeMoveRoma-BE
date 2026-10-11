package it.roma.gtfs.gtfs_monitor.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.filter.ShallowEtagHeaderFilter;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Il Cache-Control pubblico deve finire solo sulle risposte 2xx: un 503 all'avvio
 * o un 404 messi in cache resterebbero sbagliati per 5 minuti.
 */
class StaticDataCacheConfigTest {

    private final ShallowEtagHeaderFilter filtro = new StaticDataCacheConfig().staticDataEtagFilter();

    private MockHttpServletResponse esegui(String path, String ifNoneMatch, FilterChain catena) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", path);
        if (ifNoneMatch != null) {
            request.addHeader(HttpHeaders.IF_NONE_MATCH, ifNoneMatch);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        filtro.doFilter(request, response, catena);
        return response;
    }

    private static FilterChain corpo(int stato, String testo) {
        return (req, res) -> {
            HttpServletResponse r = (HttpServletResponse) res;
            r.setStatus(stato);
            r.setContentType("application/json");
            r.getOutputStream().write(testo.getBytes(StandardCharsets.UTF_8));
        };
    }

    @Test
    @DisplayName("200 su un percorso statico: cache pubblica ed ETag")
    void okPubblico() throws Exception {
        MockHttpServletResponse response = esegui("/api/v1/stops", null, corpo(200, "{\"items\":[]}"));

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("public, max-age=300");
        assertThat(response.getHeader(HttpHeaders.ETAG)).isNotBlank();
        assertThat(response.getContentAsString()).isEqualTo("{\"items\":[]}");
    }

    @Test
    @DisplayName("304 sulla rivalidazione: resta la cache pubblica")
    void notModifiedPubblico() throws Exception {
        String etag = esegui("/api/v1/stops", null, corpo(200, "{}")).getHeader(HttpHeaders.ETAG);

        MockHttpServletResponse response = esegui("/api/v1/stops", etag, corpo(200, "{}"));

        assertThat(response.getStatus()).isEqualTo(304);
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("public, max-age=300");
    }

    @Test
    @DisplayName("503 con corpo (ProblemDetail): no-store")
    void erroreConCorpoNoStore() throws Exception {
        MockHttpServletResponse response = esegui("/api/v1/stops", null, corpo(503, "{\"status\":503}"));

        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
    }

    @Test
    @DisplayName("404 via sendError (risposta gia' inviata): no-store")
    void sendErrorNoStore() throws Exception {
        MockHttpServletResponse response = esegui("/api/v1/trips/X/shape", null,
                (req, res) -> ((HttpServletResponse) res).sendError(404));

        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
    }

    @Test
    @DisplayName("400 su catalogo: no-store")
    void badRequestNoStore() throws Exception {
        MockHttpServletResponse response = esegui("/api/v1/catalog/lines", null, corpo(400, "{}"));

        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
    }

    @Test
    @DisplayName("Percorsi realtime: il filtro non tocca il Cache-Control")
    void realtimeEscluso() throws Exception {
        MockHttpServletResponse response = esegui("/api/v1/vehicles", null, corpo(200, "{}"));

        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isNull();
        assertThat(response.getHeader(HttpHeaders.ETAG)).isNull();
    }
}
