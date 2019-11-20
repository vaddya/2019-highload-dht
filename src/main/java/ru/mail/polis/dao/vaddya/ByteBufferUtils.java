package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.List;

public final class ByteBufferUtils {
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private ByteBufferUtils() {
    }

    /**
     * Returns empty buffer.
     *
     * @return empty buffer
     */
    @NotNull
    public static ByteBuffer emptyBuffer() {
        return EMPTY_BUFFER;
    }

    /**
     * Creates a buffer from the specified int value.
     *
     * @param value a int value to put into the buffer
     * @return a buffer with int value
     */
    @NotNull
    public static ByteBuffer fromInt(final int value) {
        final var buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.flip();
    }

    /**
     * Creates a buffer from the specified list of ints.
     *
     * @param list a list of int values
     * @return a buffer with int list
     */
    @NotNull
    public static ByteBuffer fromIntList(@NotNull final List<Integer> list) {
        final var buffer = ByteBuffer.allocate(Integer.BYTES * list.size());
        list.forEach(buffer::putInt);
        return buffer.flip();
    }

    /**
     * Create and fill ByteBuffer from MemTable entry.
     *
     * <p>ByteBuffer will contain:
     * <ul>
     * <li> Size of the key (4 bytes)
     * <li> Key of the entry (N bytes)
     * <li> Timestamp (8 bytes), if negative then it is a tombstone and neither value size nor value itself is present
     * <li> Size of the value (4 bytes)
     * <li> Value of the entry (M bytes)
     * </ul>
     */
    @NotNull
    public static ByteBuffer fromTableEntry(@NotNull final TableEntry entry) {
        final var keySize = entry.getKey().remaining();
        if (entry.hasTombstone()) {
            return ByteBuffer.allocate(Integer.BYTES + keySize + Long.BYTES)
                    .putInt(keySize)
                    .put(entry.getKey().duplicate())
                    .putLong(-entry.ts())
                    .flip();
        }

        final var valueSize = entry.getValue().remaining();
        return ByteBuffer.allocate(Integer.BYTES + keySize + Long.BYTES + Integer.BYTES + valueSize)
                .putInt(keySize)
                .put(entry.getKey().duplicate())
                .putLong(entry.ts())
                .putInt(valueSize)
                .put(entry.getValue().duplicate())
                .flip();
    }
}
