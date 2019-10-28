package ru.mail.polis.service.vaddya.topology;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

/**
 * Calculates the data owner for the key.
 */
public interface Topology<T> {

    /**
     * Creates a basic cluster topology instance that uses modulo
     * operator to determine primary node for the given key.
     *
     * @param topology cluster notes
     * @param me       current node
     * @return a topology instance
     */
    @NotNull
    static <T> Topology<T> basic(
            @NotNull final Set<T> topology,
            @NotNull final T me) {
        return new BasicTopology<>(topology, me);
    }

    /**
     * Creates a cluster topology instance that uses consistent
     * hashing algorithm to determine primary node for a given key.
     *
     * @param topology   cluster nodes
     * @param me         current node
     * @param vNodeCount a number of virtual nodes per one physical node
     * @return a topology instance
     */
    @NotNull
    static <T> Topology<T> consistentHashing(
            @NotNull final Set<T> topology,
            @NotNull final T me,
            final int vNodeCount) {
        return new ConsistentHashingTopology<>(topology, me, vNodeCount);
    }

    /**
     * Determine a cluster node for the given key.
     *
     * @param key key for partition algorithm
     * @return node
     */
    @NotNull
    T primaryFor(@NotNull ByteBuffer key);

    /**
     * Determine replicas for the given key.
     *
     * @param key key for partition algorithm
     * @return nodes
     */
    @NotNull
    Set<T> primaryFor(@NotNull ByteBuffer key, @NotNull ReplicationFactor rf);

    /**
     * Check if the given node is equal to me using {@link Object#equals} method.
     *
     * @param node node to be checked
     * @return {@code true} if node and me are equals and {@code false} otherwise
     */
    boolean isMe(@NotNull T node);

    /**
     * Get all nodes of the cluster.
     *
     * @return all nodes
     */
    @NotNull
    Set<T> all();

    /**
     * Get all nodes of the cluster except me.
     *
     * @return all nodes except me
     */
    @NotNull
    default Set<T> others() {
        return all().stream()
                .filter(t -> !isMe(t))
                .collect(Collectors.toSet());
    }
}
