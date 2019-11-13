package ru.mail.polis.dao.vaddya;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.vaddya.sstable.SSTable;

import java.util.Collection;
import java.util.Iterator;

import static java.util.stream.Collectors.toList;

public final class IteratorUtils {
    private IteratorUtils() {
    }

    @NotNull
    public static Iterator<TableEntry> collapseIterators(@NotNull final Collection<Iterator<TableEntry>> iterators) {
        @SuppressWarnings("UnstableApiUsage") final var merged = Iterators.mergeSorted(iterators, TableEntry.COMPARATOR);
        return Iters.collapseEquals(merged, TableEntry::getKey);
    }

    @NotNull
    public static Iterator<TableEntry> aliveEntries(@NotNull final Iterator<TableEntry> iterator) {
        return Iterators.filter(iterator, e -> !e.hasTombstone());
    }
    
    @NotNull
    public static Iterator<TableEntry> aliveEntries(@NotNull final Collection<SSTable> tables) {
        final var iterators = tables.stream()
                .map(SSTable::iterator)
                .collect(toList());
        final var iterator = collapseIterators(iterators);
        return aliveEntries(iterator);
    }

    @NotNull
    public static Iterator<Record> toRecords(@NotNull final Iterator<TableEntry> iterator) {
        return Iterators.transform(iterator, e -> Record.of(e.getKey(), e.getValue()));
    }
}
