package ru.mail.polis.dao.vaddya;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static ru.mail.polis.dao.vaddya.ByteBufferUtils.emptyBuffer;
import static ru.mail.polis.dao.vaddya.TimeUtils.currentTimeNanos;

public final class TableEntry implements Comparable<TableEntry> {
    static final Comparator<TableEntry> COMPARATOR = comparing(TableEntry::getKey)
            .thenComparing(TableEntry::ts, reverseOrder());

    private final ByteBuffer key;
    @Nullable
    private final ByteBuffer value;
    private final boolean hasTombstone;
    private final long ts;

    @NotNull
    static TableEntry upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        return new TableEntry(key, value, false, currentTimeNanos());
    }

    @NotNull
    static TableEntry delete(
            @NotNull final ByteBuffer key) {
        return new TableEntry(key, emptyBuffer(), true, currentTimeNanos());
    }

    @NotNull
    static TableEntry from(
            @NotNull final ByteBuffer key,
            @Nullable final ByteBuffer value,
            final boolean hasTombstone,
            final long ts) {
        return new TableEntry(key, value, hasTombstone, ts);
    }

    private TableEntry(
            @NotNull final ByteBuffer key,
            @Nullable final ByteBuffer value,
            final boolean hasTombstone,
            final long ts) {
        this.key = key;
        this.value = value;
        this.hasTombstone = hasTombstone;
        this.ts = ts;
    }

    /**
     * Get the key.
     */
    @NotNull
    public ByteBuffer getKey() {
        return key;
    }

    /**
     * Get the value.
     *
     * @throws IllegalArgumentException if value is absent
     */
    @NotNull
    public ByteBuffer getValue() {
        if (value == null) {
            throw new IllegalArgumentException("Value is absent");
        }
        return value;
    }

    /**
     * Get the tombstone, if true then the value is absent.
     */
    public boolean hasTombstone() {
        return hasTombstone;
    }

    /**
     * Get the entry timestamp in nanos.
     */
    public long ts() {
        return ts;
    }

    @Override
    public int compareTo(@NotNull final TableEntry o) {
        return COMPARATOR.compare(this, o);
    }
}
