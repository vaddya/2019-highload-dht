package ru.mail.polis.dao.vaddya.sstable;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.ByteBufferUtils;
import ru.mail.polis.dao.vaddya.Table;
import ru.mail.polis.dao.vaddya.TableEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public interface SSTable extends Table {
    int MAGIC = 0xCAFEFEED;

    @NotNull
    default ByteBuffer lowest() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    default ByteBuffer highest() {
        throw new UnsupportedOperationException();
    }

    /**
     * Flush table entries to the specified channel.
     *
     * <p>File will contain:
     * <ul>
     * <li> Table entries (mapped to bytes using ByteBufferUtils.fromTableEntry)
     * <li> List of offsets (represented by int value), one for each entry
     * <li> Number of entries (represented by int value)
     * <li> Magic number in the end of the file (int Table.MAGIC)
     * </ul>
     *
     * @param channel channel to write entries to
     * @throws IOException if cannot write data
     */
    static void flushEntries(
            @NotNull final Iterator<TableEntry> entries,
            @NotNull final FileChannel channel) throws IOException {
        final var offsets = new ArrayList<Integer>();
        var offset = 0;
        while (entries.hasNext()) {
            offsets.add(offset);
            final var buffer = ByteBufferUtils.fromTableEntry(entries.next());
            offset += buffer.remaining();
            channel.write(buffer);
        }

        final var offsetsBuffer = ByteBufferUtils.fromIntList(offsets);
        channel.write(offsetsBuffer);

        final var sizeBuffer = ByteBufferUtils.fromInt(offsets.size());
        channel.write(sizeBuffer);

        final var magicBuffer = ByteBufferUtils.fromInt(SSTable.MAGIC);
        channel.write(magicBuffer);
    }

    /**
     * Read table from the specified channel.
     *
     * @param channel channel to read entries from
     * @return a table instance
     * @throws IOException if cannot read data or table format is invalid
     */
    @NotNull
    static SSTable from(@NotNull final FileChannel channel) throws IOException {
        final var size = channel.size();
        if (size < Integer.BYTES * 2) { // magic + count
            throw new IOException("Invalid SSTable format: file is too small: " + size);
        }
        final var mapped = channel.map(READ_ONLY, 0, size).order(BIG_ENDIAN);

        final var magic = mapped.getInt(mapped.limit() - Integer.BYTES);
        if (magic != SSTable.MAGIC) {
            throw new IOException("Invalid SSTable format: magic const is missing");
        }

        final var entriesCount = mapped.getInt(mapped.limit() - Integer.BYTES * 2);
        if (entriesCount <= 0 || mapped.limit() < Integer.BYTES + Integer.BYTES * entriesCount) {
            throw new IOException("Invalid SSTable format: wrong entries count: " + entriesCount);
        }

        final var offsets = mapped.duplicate()
                .position(mapped.limit() - Integer.BYTES * 2 - Integer.BYTES * entriesCount)
                .limit(mapped.limit() - Integer.BYTES)
                .slice()
                .asReadOnlyBuffer()
                .asIntBuffer();
        final var entries = mapped.duplicate()
                .position(0)
                .limit(mapped.limit() - Integer.BYTES * 2 - Integer.BYTES * entriesCount)
                .slice()
                .asReadOnlyBuffer();

        return new SSTableImpl(entriesCount, offsets, entries);
    }
}
