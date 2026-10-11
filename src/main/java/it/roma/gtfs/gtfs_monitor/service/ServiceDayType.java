package it.roma.gtfs.gtfs_monitor.service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.MonthDay;
import java.util.List;
import java.util.Set;

/**
 * Che tipo di giorno e' per il servizio: feriale, sabato o festivo.
 *
 * DA CALENDARIO: domeniche, festivita' nazionali e Pasquetta. Un sabato festivo
 * (25 aprile 2026) e' festivo.
 *
 * Il calendario del GTFS e' stato provato e scartato (settembre 2026): gli
 * identificativi dei servizi seguono convenzioni diverse per operatore, il loro
 * numero varia da cinque a centinaia per giorno e i primi giorni del feed sono
 * coperti solo in parte. Confrontarli classificava un giovedi' come sabato e una
 * domenica come sabato.
 *
 * Il 4 ottobre, San Francesco d'Assisi, e' festa nazionale dal 2026 (legge
 * 151/2025, in vigore dal 1 gennaio 2026): prima era un giorno feriale. Che
 * ATAC faccia davvero l'orario festivo va verificato nel GTFS di quel giorno.
 *
 * Non c'e' il 29 giugno, Santi Pietro e Paolo: non e' verificato che ATAC faccia
 * l'orario festivo. Si puo' controllare nel GTFS quando copre fine giugno — a
 * settembre 2026 i servizi ATAC portano il tipo nel prefisso, 10# feriale, 20#
 * sabato, 30# festivo.
 */
public final class ServiceDayType {

    public static final String FERIALE = "feriale";
    public static final String SABATO = "sabato";
    public static final String FESTIVO = "festivo";
    public static final List<String> TUTTI = List.of(FERIALE, SABATO, FESTIVO);

    private static final Set<MonthDay> FESTIVITA_NAZIONALI = Set.of(
            MonthDay.of(1, 1), MonthDay.of(1, 6), MonthDay.of(4, 25), MonthDay.of(5, 1), MonthDay.of(6, 2),
            MonthDay.of(8, 15), MonthDay.of(11, 1), MonthDay.of(12, 8), MonthDay.of(12, 25), MonthDay.of(12, 26));
    /** San Francesco d'Assisi: festa nazionale dal 2026 (legge 151/2025). */
    private static final MonthDay SAN_FRANCESCO = MonthDay.of(10, 4);
    private static final int SAN_FRANCESCO_DAL = 2026;

    private ServiceDayType() {
    }

    public static String di(LocalDate data) {
        if (data.getDayOfWeek() == DayOfWeek.SUNDAY
                || FESTIVITA_NAZIONALI.contains(MonthDay.from(data))
                || (data.getYear() >= SAN_FRANCESCO_DAL && SAN_FRANCESCO.equals(MonthDay.from(data)))
                || data.equals(pasqua(data.getYear()).plusDays(1))) {
            return FESTIVO;
        }
        return data.getDayOfWeek() == DayOfWeek.SATURDAY ? SABATO : FERIALE;
    }

    /** Domenica di Pasqua nel calendario gregoriano (algoritmo di Meeus/Jones/Butcher). */
    static LocalDate pasqua(int anno) {
        int a = anno % 19;
        int b = anno / 100;
        int c = anno % 100;
        int d = b / 4;
        int e = b % 4;
        int f = (b + 8) / 25;
        int g = (b - f + 1) / 3;
        int h = (19 * a + b - d - g + 15) % 30;
        int i = c / 4;
        int k = c % 4;
        int l = (32 + 2 * e + 2 * i - h - k) % 7;
        int m = (a + 11 * h + 22 * l) / 451;
        int mese = (h + l - 7 * m + 114) / 31;
        int giorno = ((h + l - 7 * m + 114) % 31) + 1;
        return LocalDate.of(anno, mese, giorno);
    }
}
