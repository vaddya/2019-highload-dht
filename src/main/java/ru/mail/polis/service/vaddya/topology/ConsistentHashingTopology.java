package ru.mail.polis.service.vaddya.topology;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;

final class ConsistentHashingTopology<T> implements Topology<T> {
    private final T me;
    private final Set<T> nodes;
    private final NavigableMap<Long, VirtualNode<T>> ring = new TreeMap<>();
    @SuppressWarnings("UnstableApiUsage")
    private final HashFunction hashFunction = Hashing.murmur3_128(42);

    ConsistentHashingTopology(
            @NotNull final Set<T> nodes,
            @NotNull final T me,
            final int vNodeCount) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Topology should not be empty");
        }

        this.nodes = nodes;
        this.me = me;
        nodes.forEach(node -> addNode(node, vNodeCount));
    }

    @Override
    @NotNull
    public T primaryFor(@NotNull final ByteBuffer key) {
        final var hash = hashFunction.hashBytes(key.duplicate()).asLong();
        final var nodeEntry = ring.ceilingEntry(hash);
        if (nodeEntry == null) {
            return ring.firstEntry().getValue().node();
        }
        return nodeEntry.getValue().node();
    }

    @NotNull
    @Override
    public Set<T> primaryFor(
            @NotNull final ByteBuffer key,
            @NotNull final ReplicationFactor rf) {
        if (rf.from() > nodes.size()) {
            throw new IllegalArgumentException("Number of required nodes is too big!");
        }

        final var hash = hashFunction.hashBytes(key.duplicate()).asLong();
        final var result = new HashSet<T>();
        var it = ring.tailMap(hash).values().iterator();
        while (result.size() < rf.from()) {
            if (!it.hasNext()) {
                it = ring.values().iterator();
            }
            result.add(it.next().node);
        }

        return result;
    }

    @Override
    public boolean isMe(@NotNull final T node) {
        return me.equals(node);
    }

    @Override
    @NotNull
    public Set<T> all() {
        return nodes;
    }

    @Override
    public int size() {
        return nodes.size();
    }

    private void addNode(
            @NotNull final T node,
            final int vNodeCount) {
        for (var i = 0; i < vNodeCount; i++) {
            final var vnode = new VirtualNode<>(node, i);
            final var vnodeBytes = vnode.name().getBytes(Charsets.UTF_8);
            final var hash = hashFunction.hashBytes(vnodeBytes).asLong();
            ring.put(hash, vnode);
        }
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
