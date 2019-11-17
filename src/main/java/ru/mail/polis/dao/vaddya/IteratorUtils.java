package ru.mail.polis.dao.vaddya;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collector;

import static java.util.stream.Collectors.toList;

/**
 * Util method to work with {@code Iterator<TableEntry>}.
 */
public final class IteratorUtils {
    private IteratorUtils() {
    }

    /**
     * Merge sorted iterators and remove entries with equal keys.
     *
     * @param iterators iterators to be merged
     * @return an iterator without equal keys
     */
    @NotNull
    public static Iterator<TableEntry> collapseIterators(@NotNull final Collection<Iterator<TableEntry>> iterators) {
        @SuppressWarnings("UnstableApiUsage") final var merged = Iterators.mergeSorted(iterators, TableEntry.COMPARATOR);
        return Iters.collapseEquals(merged, TableEntry::getKey);
    }

    /**
     * Combination of {@code Collectors.toList()} and {@code IteratorUtils.collapseIterators}
     * to merge stream into single iterator without equal keys.
     *
     * @return a collector that can be used to collect {@code Stream<Iterator<TableEntry>>}.
     */
    @NotNull
    public static Collector<Iterator<TableEntry>, ?, Iterator<TableEntry>> toCollapsedMergedIterator() {
        return Collector.of(
                ArrayList::new,
                List::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                IteratorUtils::collapseIterators);
    }

    /**
     * Leave in the specified iterator only alive entries (i.e. without tombstone}.
     *
     * @param iterator iterator to filter
     * @return a filtered iterator
     */
    @NotNull
    public static Iterator<TableEntry> aliveEntries(@NotNull final Iterator<TableEntry> iterator) {
        return Iterators.filter(iterator, e -> !e.hasTombstone());
    }

    /**
     * Leave in the collection of the iterators only alive entries.
     *
     * @param tables tables to be merged
     * @return a filtered iterator
     */
    @NotNull
    public static Iterator<TableEntry> aliveEntries(@NotNull final Collection<SSTable> tables) {
        final var iterators = tables.stream()
                .map(SSTable::iterator)
                .collect(toList());
        final var iterator = collapseIterators(iterators);
        return aliveEntries(iterator);
    }

    /**
     * Transform each {@link TableEntry} to {@link Record} in the specified iterator.
     *
     * @param iterator iterator to transform
     * @return a transformed iterator
     */
    @NotNull
    public static Iterator<Record> toRecords(@NotNull final Iterator<TableEntry> iterator) {
        return Iterators.transform(iterator, e -> Record.of(e.getKey(), e.getValue()));
    }
}
