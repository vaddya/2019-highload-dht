package ru.mail.polis.service.vaddya.topology;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

final class BasicTopology<T> implements Topology<T> {
    private final T me;
    private final List<T> nodes;

    BasicTopology(
            @NotNull final Set<T> topology,
            @NotNull final T me) {
        this.me = me;
        this.nodes = topology.stream()
                .sorted()
                .collect(toList());
    }

    @Override
    @NotNull
    public T primaryFor(@NotNull final String key) {
        final var index = nodeIndex(key);
        return nodes.get(index);
    }

    @Override
    @NotNull
    public Set<T> primaryFor(
            @NotNull final String key,
            @NotNull final ReplicationFactor rf) {
        if (rf.from() > nodes.size()) {
            throw new IllegalArgumentException("Number of required nodes is too big!");
        }

        final var index = nodeIndex(key);
        return Stream.iterate(index, i -> (i + 1) % nodes.size())
                .limit(rf.from())
                .map(nodes::get)
                .collect(toSet());
    }

    private int nodeIndex(@NotNull final String key) {
        final int hash = key.hashCode();
        return (hash & Integer.MAX_VALUE) % nodes.size();
    }

    @Override
    public boolean isMe(@NotNull final T node) {
        return me.equals(node);
    }

    @Override
    @NotNull
    public Set<T> all() {
        return new HashSet<>(nodes);
    }

    @Override
    public int size() {
        return nodes.size();
    }
}
