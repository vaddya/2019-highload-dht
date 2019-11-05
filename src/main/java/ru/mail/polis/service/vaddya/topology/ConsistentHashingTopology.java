package ru.mail.polis.service.vaddya.topology;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

final class ConsistentHashingTopology<T> implements Topology<T> {
    private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

    private final T me;
    private final Set<T> nodes;
    private final int vnodeCount;
    private final NavigableMap<Long, VirtualNode<T>> ring = new TreeMap<>();
    private final StampedLock lock = new StampedLock();
    @SuppressWarnings("UnstableApiUsage")
    private final HashFunction hashFunction = Hashing.murmur3_128(42);

    ConsistentHashingTopology(
            @NotNull final Set<T> nodes,
            @NotNull final T me,
            final int vnodeCount) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Topology should not be empty");
        }

        this.me = me;
        this.nodes = new HashSet<>(nodes);
        this.vnodeCount = vnodeCount;
        nodes.forEach(this::addNode);
    }

    @Override
    @NotNull
    public T primaryFor(@NotNull final String key) {
        final var stamp = lock.readLock();
        try {
            final var hash = hash(key);
            final var nodeEntry = ring.ceilingEntry(hash);
            if (nodeEntry == null) {
                return ring.firstEntry().getValue().node();
            }
            return nodeEntry.getValue().node();
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
            throw new IllegalArgumentException("Number of the required nodes is too big!");
        }
        
        final var stamp = lock.readLock();
        try {
            final var hash = hash(key);
            final var result = new HashSet<T>();
            var it = ring.tailMap(hash).values().iterator();
            while (result.size() < rf.from()) {
                if (!it.hasNext()) {
                    it = ring.values().iterator();
                }
                result.add(it.next().node());
            }

            return result;
        } finally {
            lock.unlockRead(stamp);
        }
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
            return nodes;
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
            for (var i = 0; i < vnodeCount; i++) {
                final var vnode = new VirtualNode<>(node, i);
                final var vnodeBytes = vnode.name().getBytes(Charsets.UTF_8);
                final var hash = hashFunction.hashBytes(vnodeBytes).asLong();
                ring.put(hash, vnode);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void removeNode(@NotNull final T node) {
        final var stamp = lock.writeLock();
        try {
            nodes.remove(node);
            ring.entrySet().removeIf(e -> e.getValue().node().equals(node));
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private long hash(@NotNull final String key) {
        return hashFunction.hashString(key, DEFAULT_CHARSET).asLong();
    }

    private static class VirtualNode<T> {
        private final T node;
        private final int index;

        VirtualNode(
                @NotNull final T node,
                final int index) {
            this.node = node;
            this.index = index;
        }

        @NotNull
        String name() {
            return node + "_" + index;
        }

        @NotNull
        T node() {
            return node;
        }
    }
}
