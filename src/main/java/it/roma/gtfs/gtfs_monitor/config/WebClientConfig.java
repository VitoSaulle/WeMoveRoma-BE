package it.roma.gtfs.gtfs_monitor.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

@Configuration
public class WebClientConfig {

    /*
     * Reactor Netty non manda alcun User-Agent, e per un filtro anti-bot una
     * richiesta senza User-Agent e' il primo indizio di traffico automatico:
     * i 403 sporadici su romamobilita.it partono da li'. Dichiarare chi siamo
     * e' anche la cortesia dovuta a chi pubblica il feed.
     */
    private static final String USER_AGENT = "WeMoveRoma/1.0 (+https://www.wemoveroma.com)";

    @Bean
    @Primary
    public WebClient webClient(WebClient.Builder builder) {
        return builder
                .clientConnector(new ReactorClientHttpConnector(newHttpClient(Duration.ofSeconds(30), 30)))
                .defaultHeader(HttpHeaders.USER_AGENT, USER_AGENT)
                .codecs(c -> c.defaultCodecs().maxInMemorySize(20 * 1024 * 1024)) // 20MB
                .build();
    }

    @Bean("staticGtfsWebClient")
    public WebClient staticGtfsWebClient(WebClient.Builder builder) {
        return builder
                .clientConnector(new ReactorClientHttpConnector(newHttpClient(Duration.ofSeconds(120), 120)))
                .defaultHeader(HttpHeaders.USER_AGENT, USER_AGENT)
                .codecs(c -> c.defaultCodecs().maxInMemorySize(150 * 1024 * 1024)) // 150MB
                .build();
    }

    private static HttpClient newHttpClient(Duration responseTimeout, int ioTimeoutSeconds) {
        return HttpClient.create()
                .followRedirect(true)
                .compress(true)
                .responseTimeout(responseTimeout)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
                .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(ioTimeoutSeconds))
                        .addHandlerLast(new WriteTimeoutHandler(ioTimeoutSeconds)));
    }

    /*
     * Qui c'erano due filtri di retry (5xx -> errore, poi retryWhen con backoff),
     * ma non scattavano mai: i filtri si applicano nell'ordine di registrazione,
     * quindi il retry vedeva la risposta 5xx grezza, non ancora un errore; e gli
     * errori di trasporto arrivano incartati in WebClientRequestException, che
     * non e' ne' IOException ne' PrematureCloseException. Toglierli non cambia
     * nulla a runtime. Renderli efficaci sarebbe peggio: fino a 4 tentativi per
     * richiesta, con .block() a valle, moltiplicherebbero i tempi di attesa dei
     * poll. I tentativi, dove servono, li fanno i servizi (un solo retry).
     */
}
