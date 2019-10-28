package ru.mail.polis.dao.vaddya;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import static java.util.stream.Collectors.toList;

final class IteratorUtils {
    private IteratorUtils() {
    }

    @NotNull
    static Collection<Iterator<TableEntry>> collectIterators(
            @NotNull final Table currentTable,
            @NotNull final Collection<Table> tables,
            @NotNull final ByteBuffer from) {
        final var iterators = tables.stream()
                .map(table -> table.iterator(from))
                .collect(toList());
        iterators.add(currentTable.iterator(from));
        return iterators;
    }

    @SuppressWarnings("UnstableApiUsage")
    @NotNull
    static Iterator<TableEntry> collapseIterators(@NotNull final Collection<Iterator<TableEntry>> iterators) {
        final var merged = Iterators.mergeSorted(iterators, TableEntry.COMPARATOR);
        return Iters.collapseEquals(merged, TableEntry::getKey);
    }
}
