package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Iterator;

final class SSTable implements Table {
    static final int MAGIC = 0xCAFEFEED;

    private final int entriesCount;
    private final IntBuffer offsets;
    private final ByteBuffer entries;

    SSTable(
            final int entriesCount,
            @NotNull final IntBuffer offsets,
            @NotNull final ByteBuffer entries) {
        this.entriesCount = entriesCount;
        this.entries = entries;
        this.offsets = offsets;
    }

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            private int position = position(from);

            @Override
            public boolean hasNext() {
                return position < entriesCount;
            }

            @Override
            public TableEntry next() {
                return entryAt(position++);
            }
        };
    }

    @Override
    public int currentSize() {
        return entries.limit();
    }

    private int position(@NotNull final ByteBuffer key) {
        var left = 0;
        var right = entriesCount - 1;
        while (left <= right) {
            final var mid = left + (right - left) / 2;
            final var cmp = keyAt(mid).compareTo(key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @NotNull
    private ByteBuffer keyAt(final int position) {
        final var offset = offsets.get(position);
        final var keySize = entries.getInt(offset);
        return entries.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize)
                .slice();
    }

    @NotNull
    private TableEntry entryAt(final int position) {
        var offset = offsets.get(position);

        final var keySize = entries.getInt(offset);
        offset += Integer.BYTES;

        final var key = entries.duplicate()
                .position(offset)
                .limit(offset + keySize)
                .slice();
        offset += keySize;

        final var ts = entries.position(offset).getLong();
        offset += Long.BYTES;
        if (ts < 0) {
            return TableEntry.from(key, null, true, -ts);
        }

        final var valueSize = entries.getInt(offset);
        offset += Integer.BYTES;

        final var value = entries.duplicate()
                .position(offset)
                .limit(offset + valueSize)
                .slice();

        return TableEntry.from(key, value, false, ts);
    }
}
