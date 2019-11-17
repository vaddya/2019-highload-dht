package ru.mail.polis.dao.vaddya.sstable.leveled;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.vaddya.ByteBufferUtils;
import ru.mail.polis.dao.vaddya.TableEntry;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;

final class LevelUtils {
    private LevelUtils() {
    }

    /**
     * Special class that encapsulates primary information about SSTable
     * and allows to compare SSTables with each other based on their highest key.
     */
    static final class RangedSSTable implements Comparable<RangedSSTable> {
        final int generation;
        final SSTable ssTable;
        final ByteBuffer highest;

        static RangedSSTable generation(final int generation) {
            return new RangedSSTable(generation, SingleValueTable.LOWEST);
        }

        static RangedSSTable value(@NotNull final ByteBuffer value) {
            return new RangedSSTable(-1, SingleValueTable.wrap(value));
        }

        static RangedSSTable of(
                final int generation,
                @NotNull final SSTable ssTable) {
            return new RangedSSTable(generation, ssTable);
        }

        private RangedSSTable(
                final int generation,
                final SSTable ssTable) {
            this.generation = generation;
            this.ssTable = ssTable;
            this.highest = ssTable.highest();
        }

        @Override
        public int compareTo(@NotNull final RangedSSTable o) {
            if (this == o) {
                return 0;
            }
            if (generation == o.generation) {
                return 0;
            }
            final var highestCompare = highest.compareTo(o.highest);
            return highestCompare != 0 ? highestCompare : Integer.compare(generation, o.generation);
        }

        @Override
        public String toString() {
            return "T" + generation + "={" + ssTable + '}';
        }
    }

    /**
     * Special class that can be used as a bound in the range-queries of {@link NavigableSet<SSTable>}.
     */
    static final class SingleValueTable implements SSTable {
        private static final SingleValueTable LOWEST = new SingleValueTable(ByteBufferUtils.emptyBuffer());

        final ByteBuffer value;

        static SingleValueTable wrap(@NotNull final ByteBuffer value) {
            return new SingleValueTable(value);
        }

        private SingleValueTable(@NotNull final ByteBuffer value) {
            this.value = value;
        }

        @NotNull
        @Override
        public ByteBuffer lowest() {
            return value;
        }

        @NotNull
        @Override
        public ByteBuffer highest() {
            return value;
        }

        @Override
        public long sizeInBytes() {
            throw new IllegalStateException("Should never happen");
        }

        @Override
        public int count() {
            throw new IllegalStateException("Should never happen");
        }

        @NotNull
        @Override
        public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
            throw new IllegalStateException("Should never happen");
        }
    }
}
