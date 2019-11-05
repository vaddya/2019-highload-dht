package ru.mail.polis.service.vaddya.topology;

import org.jetbrains.annotations.NotNull;

import java.util.Set;

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
     * @param vnodeCount a number of virtual nodes per one physical node
     * @return a topology instance
     */
    @NotNull
    static <T> Topology<T> consistentHashing(
            @NotNull final Set<T> topology,
            @NotNull final T me,
            final int vnodeCount) {
        return new ConsistentHashingTopology<>(topology, me, vnodeCount);
    }

    /**
     * Determine a cluster node for the given key.
     *
     * @param key key for partition algorithm
     * @return node
     */
    @NotNull
    T primaryFor(@NotNull String key);

    /**
     * Determine replicas for the given key.
     *
     * @param key key for the partition algorithm
     * @param rf  replication factor
     * @return nodes
     */
    @NotNull
    Set<T> primaryFor(@NotNull String key, @NotNull ReplicationFactor rf);

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
     * Get size of the cluster.
     *
     * @return cluster size
     */
    int size();

    /**
     * Add node to the cluster.
     *
     * @param node node to be added
     */
    void addNode(@NotNull T node);

    /**
     * Remove node from the cluster.
     *
     * @param node node to be deleted
     */
    void removeNode(@NotNull T node);
}
