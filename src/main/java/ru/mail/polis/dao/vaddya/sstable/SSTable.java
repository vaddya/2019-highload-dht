package ru.mail.polis.dao.vaddya.sstable;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.ByteBufferUtils;
import ru.mail.polis.dao.vaddya.Table;
import ru.mail.polis.dao.vaddya.TableEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public interface SSTable extends Table {
    /**
     * Get the lowest key in the table.
     */
    @NotNull
    ByteBuffer lowest();

    /**
     * Get the highest key in the table.
     */
    @NotNull
    ByteBuffer highest();

    /**
     * Flush table entries to the specified channel.
     *
     * <p>File will contain:
     * <ul>
     * <li> Table entries (mapped to bytes using ByteBufferUtils.fromTableEntry)
     * <li> List of offsets (represented by int value), one for each entry
     * <li> Number of entries (represented by int value)
     * <li> Magic number in the end of the file (int SSTable.MAGIC)
     * </ul>
     *
     * @param channel channel to write entries to
     * @throws IOException if cannot write data
     */
    static void flush(
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

        final var magicBuffer = ByteBufferUtils.fromInt(SSTableImpl.MAGIC);
        channel.write(magicBuffer);

        channel.force(true);
    }

    /**
     * Read table from the specified channel.
     *
     * @param channel channel to read entries from
     * @return a SSTable instance
     * @throws IOException if cannot read data or table format is invalid
     */
    @NotNull
    static SSTable open(@NotNull final FileChannel channel) throws IOException {
        final var size = channel.size();
        if (size < Integer.BYTES * 2) { // magic + count
            throw new IOException("Invalid SSTable format: file is too small: " + size);
        }
        final var mapped = channel.map(READ_ONLY, 0, size).order(BIG_ENDIAN);

        final var magic = mapped.getInt(mapped.limit() - Integer.BYTES);
        if (magic != SSTableImpl.MAGIC) {
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

        return new SSTableImpl(size, entriesCount, offsets, entries);
    }

    /**
     * Combination of {@code flush} and {@code open} methods.
     *
     * @param iterator  entries iterator
     * @param tempPath  temporary path
     * @param finalPath final path
     * @return a SSTable instance
     * @throws IOException if cannot read/write data or table format is invalid
     */
    @NotNull
    static SSTable flushAndOpen(
            @NotNull final Iterator<TableEntry> iterator,
            @NotNull final Path tempPath,
            @NotNull final Path finalPath) throws IOException {
        try (var channel = FileChannel.open(tempPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            flush(iterator, channel);
        }
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
        try (var channel = FileChannel.open(finalPath, StandardOpenOption.READ)) {
            return open(channel);
        }
    }
}
