package ru.mail.polis.dao.vaddya.sstable;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.vaddya.TableEntry;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Iterator;

@ThreadSafe
final class SSTableImpl implements SSTable {
    private final long sizeInBytes;
    private final int entriesCount;
    private final IntBuffer offsets;
    private final ByteBuffer entries;

    SSTableImpl(
            final long sizeInBytes,
            final int entriesCount,
            @NotNull final IntBuffer offsets,
            @NotNull final ByteBuffer entries) {
        this.sizeInBytes = sizeInBytes;
        this.entriesCount = entriesCount;
        this.entries = entries;
        this.offsets = offsets;
    }

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        if (from.compareTo(highest()) > 0) {
            return Iters.empty();
        }
        return new Iterator<>() {
            private int position = from.remaining() == 0 ? 0 : position(from);

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
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int count() {
        return entriesCount;
    }

    @Override
    @NotNull
    public ByteBuffer lowest() {
        return keyAt(0);
    }

    @Override
    @NotNull
    public ByteBuffer highest() {
        return keyAt(entriesCount - 1);
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

    @Override
    public String toString() {
        return "SSTableImpl{size=" + sizeInBytes + ", count=" + count() + ", [" + lowest().get() + ", " + highest().get() + "]}";
    }
}
