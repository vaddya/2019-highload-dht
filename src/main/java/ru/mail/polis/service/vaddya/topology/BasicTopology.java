package ru.mail.polis.service.vaddya.topology;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@ThreadSafe
final class BasicTopology<T> implements Topology<T> {
    private final T me;
    private final List<T> nodes;
    private final StampedLock lock = new StampedLock();

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
        final var stamp = lock.readLock();
        try {
            final var index = nodeIndex(key);
            return nodes.get(index);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    @NotNull
    public Set<T> primaryFor(
            @NotNull final String key,
            @NotNull final ReplicationFactor rf) {
        if (rf.from() > nodes.size()) {
            throw new IllegalArgumentException("Number of required nodes is too big!");
        }

        final var stamp = lock.readLock();
        try {
            final var index = nodeIndex(key);
            return Stream.iterate(index, i -> (i + 1) % nodes.size())
                    .limit(rf.from())
                    .map(nodes::get)
                    .collect(toSet());
        } finally {
            lock.unlockRead(stamp); 
        }
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
        final var stamp = lock.readLock();
        try {
            return new HashSet<>(nodes);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public int size() {
        final var stamp = lock.readLock();
        try {
            return nodes.size();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public void addNode(@NotNull final T node) {
        final var stamp = lock.writeLock();
        try {
            nodes.add(node);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void removeNode(@NotNull final T node) {
        final var stamp = lock.writeLock();
        try {
            nodes.remove(node);
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
