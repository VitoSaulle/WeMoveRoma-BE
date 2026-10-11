package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Stesso pattern di {@link AlertsDispatchTracker}, ma su file separato per gli
 * scioperi MIT — cosi' i due stream non si interferiscono al restart.
 */
@Slf4j
@org.springframework.stereotype.Component
public class StrikesDispatchTracker {

    @Value("${notifications.strikes.state-file:${user.home}/.wemoveroma/strikes-last-seen.json}")
    private String stateFilePath;

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<String> seen = new HashSet<>();

    @PostConstruct
    void load() {
        Path p = Path.of(stateFilePath);
        if (!Files.isRegularFile(p)) {
            log.info("[StrikesTracker] state file inesistente ({}), parto vuoto.", p);
            return;
        }
        try {
            State s = mapper.readValue(p.toFile(), State.class);
            if (s.seen != null) seen.addAll(s.seen);
            log.info("[StrikesTracker] caricati {} sciopero id da {}", seen.size(), p);
        } catch (IOException e) {
            log.warn("[StrikesTracker] lettura {} fallita ({}); parto vuoto.", p, e.toString());
        }
    }

    /** Come {@link AlertsDispatchTracker#isSent}: controlla senza marcare. */
    public boolean isSent(String id) {
        if (id == null || id.isBlank()) return true;
        lock.lock();
        try {
            return seen.contains(id);
        } finally {
            lock.unlock();
        }
    }

    /** Da chiamare solo dopo un invio riuscito. */
    public void markSent(String id) {
        markAllSent(id == null ? Set.of() : Set.of(id));
    }

    /** Marca piu' id con una sola scrittura su file (warmup senza push). */
    public void markAllSent(Collection<String> ids) {
        lock.lock();
        try {
            boolean changed = false;
            for (String id : ids) {
                if (id != null && !id.isBlank()) changed |= seen.add(id);
            }
            if (changed) persistInternal();
        } finally {
            lock.unlock();
        }
    }

    private void persistInternal() {
        Path p = Path.of(stateFilePath);
        try {
            Path parent = p.getParent();
            if (parent != null && !Files.isDirectory(parent)) Files.createDirectories(parent);
            mapper.writeValue(p.toFile(), new State(seen));
        } catch (IOException e) {
            log.warn("[StrikesTracker] persist {} fallita: {}", p, e.toString());
        }
    }

    @SuppressWarnings("unused")
    static final class State {
        public Set<String> seen;
        public State() { this.seen = new HashSet<>(); }
        State(Set<String> seen) { this.seen = seen; }
    }
}
