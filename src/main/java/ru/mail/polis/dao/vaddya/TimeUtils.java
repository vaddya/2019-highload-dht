package ru.mail.polis.dao.vaddya;

final class TimeUtils {
    private static int counter;
    private static long lastMillis;

    private TimeUtils() {
    }

    /**
     * Emulate current time in nanoseconds using System.currentTimeMillis and atomic counter
     */
    static long currentTimeNanos() {
        synchronized (TimeUtils.class) {
            final var millis = System.currentTimeMillis();
            if (lastMillis != millis) {
                lastMillis = millis;
                counter = 0;
            }
            return lastMillis * 1_000_000 + counter++;
        }
    }
}
