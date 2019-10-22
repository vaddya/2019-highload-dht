package ru.mail.polis.service.vaddya.topology;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;

class ConsistentHashingTopology<T> implements Topology<T> {
    private final T me;
    private final Set<T> topology;
    private final NavigableMap<Long, VirtualNode<T>> ring = new TreeMap<>();
    @SuppressWarnings("UnstableApiUsage")
    private final HashFunction hashFunction = Hashing.murmur3_128(42);

    ConsistentHashingTopology(
            @NotNull final Set<T> topology,
            @NotNull final T me,
            final int vNodeCount) {
        this.topology = topology;
        this.me = me;
        topology.forEach(node -> addNode(node, vNodeCount));
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

    @Override
    public boolean isMe(@NotNull final T node) {
        return me.equals(node);
    }

    @Override
    @NotNull
    public Set<T> all() {
        return topology;
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
