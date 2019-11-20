package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.Iters;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static ru.mail.polis.dao.vaddya.ByteBufferUtils.emptyBuffer;

public interface Table {
    /**
     * Get current size of the table entries in bytes.
     */
    long sizeInBytes();

    /**
     * Get number of entries in the table.
     */
    int count();

    /**
     * Get iterator over the table entries starting from the given key.
     */
    @NotNull
    Iterator<TableEntry> iterator(@NotNull ByteBuffer from);

    /**
     * Get iterator over all values of the table.
     */
    default Iterator<TableEntry> iterator() {
        return iterator(emptyBuffer());
    }

    /**
     * Get range operator over value between {@code from} and {@code to}.
     */
    @NotNull
    default Iterator<TableEntry> range(
            @NotNull ByteBuffer from,
            @Nullable ByteBuffer to) {
        if (to == null) {
            return iterator(from);
        }

        if (from.compareTo(to) > 0) {
            return Iters.empty();
        }

        final var bound = TableEntry.from(to, emptyBuffer(), false, -1);
        return Iters.until(iterator(from), bound);
    }
}
