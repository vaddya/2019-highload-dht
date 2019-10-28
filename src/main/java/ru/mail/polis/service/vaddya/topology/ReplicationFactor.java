package ru.mail.polis.service.vaddya.topology;

import org.jetbrains.annotations.NotNull;

public final class ReplicationFactor {
    private final int ack;
    private final int from;

    /**
     * Parse replication factor from string in format "ack/from".
     *
     * @param replicas string to parse
     * @return a replication factor instance
     */
    @NotNull
    public static ReplicationFactor parse(@NotNull final String replicas) {
        final var sepIndex = replicas.indexOf('/');
        if (sepIndex == -1) {
            throw new IllegalArgumentException("Wrong replica");
        }
        final var ack = Integer.parseInt(replicas.substring(0, sepIndex));
        final var from = Integer.parseInt(replicas.substring(sepIndex + 1));
        return new ReplicationFactor(ack, from);
    }

    /**
     * Create replication factor equals to quorum for the number of nodes.
     *
     * @param nodeCount number of nodes in the cluster
     * @return a replication factor instance
     */
    @NotNull
    public static ReplicationFactor quorum(final int nodeCount) {
        final var quorum = nodeCount / 2 + 1;
        return new ReplicationFactor(quorum, nodeCount);
    }

    /**
     * Create replication factor using the number of acknowledgements
     * and the number of nodes in the cluster.
     *
     * @param ackCount  number of acknowledgements
     * @param nodeCount number of nodes
     * @return a replication factor instance
     */
    @NotNull
    public static ReplicationFactor create(
            final int ackCount,
            final int nodeCount) {
        return new ReplicationFactor(ackCount, nodeCount);
    }

    private ReplicationFactor(int ack, int from) {
        if (ack > from || ack <= 0) {
            throw new IllegalArgumentException("Wrong RF");
        }
        this.ack = ack;
        this.from = from;
    }

    public int ack() {
        return ack;
    }

    public int from() {
        return from;
    }

    @Override
    public String toString() {
        return ack + "/" + from;
    }
}
