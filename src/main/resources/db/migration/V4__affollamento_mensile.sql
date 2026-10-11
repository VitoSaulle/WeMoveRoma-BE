-- Quanto sono pieni i bus, tratto per tratto e ora per ora: la base della mappa
-- dell'affollamento. Lo calcola CrowdingService dai passaggi (colonna
-- affollamento, V3), un giorno alla volta.
--
-- PER MESE E NON PER GIORNO, a differenza di tratti_giornalieri. Divisa per
-- linea, tratto e ora una giornata ha 320 mila celle con poco piu' di due
-- passaggi l'una (misurato sullo storico, ottobre 2026): un aggregato
-- giornaliero peserebbe quasi quanto i passaggi stessi. Le celle pero' si
-- ripetono da un giorno all'altro, perche' l'orario e' lo stesso: sommate sul
-- mese restano quelle, con decine di passaggi ciascuna. Quanto pesa e' in
-- CrowdingService, accanto ai mesi conservati.
--
-- Per questo il tipo di giorno qui si salva, mentre in tratti_giornalieri si
-- ricava dalla data: un festivo aggiunto dopo a ServiceDayType non sposta i
-- passaggi gia' sommati. Finche' i passaggi ci sono (90 giorni) un mese si puo'
-- rifare: vedi CrowdingService.
CREATE TABLE affollamento_mensile (
    mese      date     NOT NULL, -- primo giorno del mese del giorno di servizio
    tipo      text     NOT NULL, -- feriale, sabato, festivo (ServiceDayType)
    linea     text     NOT NULL,
    da        text     NOT NULL, -- fermata da cui la linea arriva di solito ad `a` (stop_id)
    a         text     NOT NULL, -- fermata di arrivo: l'affollamento e' quello dichiarato andando verso di lei
    ora       smallint NOT NULL, -- ora di Roma del passaggio ad `a`, 0-23
    passaggi  integer  NOT NULL, -- solo quelli con un valore utile (codici 0-5)
    in_piedi  integer  NOT NULL, -- di cui "solo posti in piedi" o peggio (codici 3-5)
    PRIMARY KEY (mese, tipo, linea, da, a, ora)
);

-- I giorni gia' sommati in affollamento_mensile. Una somma, al contrario di un
-- ricalcolo, non si puo' ripetere: e' questa riga, scritta nella stessa
-- transazione, a impedire di contare un giorno due volte.
CREATE TABLE affollamento_giorni (
    giorno    date    PRIMARY KEY,
    passaggi  integer NOT NULL -- passaggi con un valore utile: 0 se quel giorno nessun mezzo lo dichiarava
);
