package ru.mail.polis.service.vaddya.topology;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toList;

public class BasicTopology<T> implements Topology<T> {
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
    public T primaryFor(@NotNull final ByteBuffer key) {
        final int hash = key.hashCode();
        final int index = (hash & Integer.MAX_VALUE) % nodes.size();
        return nodes.get(index);
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
}
