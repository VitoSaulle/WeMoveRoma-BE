package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Upstream finto per i test dei feed GTFS-RT: un WebClient vero con una
 * ExchangeFunction che conta le chiamate e risponde con quello che il test
 * ha impostato (un feed, oppure "connessione rifiutata").
 */
final class FeedStub {

    final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<byte[]> body = new AtomicReference<>();

    final WebClient webClient = WebClient.builder()
            .exchangeFunction(request -> {
                calls.incrementAndGet();
                byte[] b = body.get();
                if (b == null) {
                    return Mono.error(new ConnectException("connessione rifiutata"));
                }
                return Mono.just(ClientResponse.create(HttpStatus.OK)
                        .header(HttpHeaders.CONTENT_TYPE, "application/octet-stream")
                        .body(Flux.just(DefaultDataBufferFactory.sharedInstance.wrap(b)))
                        .build());
            })
            .build();

    /** Upstream giu': ogni chiamata fallisce. */
    void down() {
        body.set(null);
    }

    void serve(GtfsRealtime.FeedMessage feed) {
        body.set(feed.toByteArray());
    }

    void serve(byte[] raw) {
        body.set(raw);
    }

    static GtfsRealtime.FeedMessage.Builder feed() {
        return GtfsRealtime.FeedMessage.newBuilder()
                .setHeader(GtfsRealtime.FeedHeader.newBuilder().setGtfsRealtimeVersion("2.0"));
    }

    static GtfsProperties props(long refreshMillis) {
        return new GtfsProperties(null, new GtfsProperties.RealtimeProps(
                "http://feed/tu.pb", "http://feed/vp.pb", "http://feed/sa.pb", refreshMillis));
    }
}
