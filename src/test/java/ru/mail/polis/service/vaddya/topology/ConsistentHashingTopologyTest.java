package ru.mail.polis.service.vaddya.topology;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashingTopologyTest {
    private static final String ME = "http://localhost:8080";
    private static final Set<String> NODES = Set.of(ME, "http://localhost:8081", "http://localhost:8082");
    private static final int VNODE_COUNT = 200;
    private static final int KEYS_COUNT = 10_000_000;

    @Test
    void testConsistency() {
        final Topology<String> topology1 = createTopology();
        final Topology<String> topology2 = createTopology();

        for (long i = 0; i < KEYS_COUNT; i++) {
            final ByteBuffer key = ByteBuffer.wrap(("key" + i).getBytes(Charsets.UTF_8));
            final String node1 = topology1.primaryFor(key.duplicate());
            final String node2 = topology2.primaryFor(key);
            assertEquals(node1, node2);
        }
    }

    @Test
    void testConsistencyWithReplicationFactor() {
        final Topology<String> topology1 = createTopology();
        final Topology<String> topology2 = createTopology();
        final ReplicationFactor rf = ReplicationFactor.create(1, 2);

        for (long i = 0; i < KEYS_COUNT; i++) {
            final ByteBuffer key = ByteBuffer.wrap(("key" + i).getBytes(Charsets.UTF_8));
            final Set<String> nodes1 = topology1.primaryFor(key.duplicate(), rf);
            final Set<String> nodes2 = topology2.primaryFor(key, rf);
            assertEquals(nodes1, nodes2);
        }
    }

    @Test
    void testUniformDistribution() {
        final Topology<String> topology = createTopology();
        final int expectedKeysPerNode = KEYS_COUNT / NODES.size();
        final int keysDelta = (int) (expectedKeysPerNode * 0.1); // ±10%

        final Map<String, Integer> counters = new HashMap<>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = "key" + i;
            final String node = topology.primaryFor(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)));
            counters.compute(node, (n, c) -> c == null ? 1 : c + 1);
        }

        validateCounters(expectedKeysPerNode, keysDelta, counters);
    }

    @Test
    void testUniformDistributionWithReplicationFactor() {
        final Topology<String> topology = createTopology();
        final ReplicationFactor rf = ReplicationFactor.create(1, 2);
        final int expectedKeysPerNode = (KEYS_COUNT / NODES.size()) * rf.from();
        final int keysDelta = (int) (expectedKeysPerNode * 0.1); // ±10%

        final Map<String, Integer> counters = new HashMap<>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = "key" + i;
            final Set<String> nodes = topology.primaryFor(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)), rf);
            nodes.forEach(node -> counters.compute(node, (n, c) -> c == null ? 1 : c + 1));
        }

        validateCounters(expectedKeysPerNode, keysDelta, counters);
    }

    private void validateCounters(int expectedKeysPerNode, int keysDelta, Map<String, Integer> counters) {
        counters.entrySet()
                .stream()
                .peek(e -> System.out.println("Node " + e.getKey() + " = " + e.getValue()))
                .forEach(e -> {
                    final String node = e.getKey();
                    final int counter = e.getValue();
                    final int delta = Math.abs(expectedKeysPerNode - counter);
                    System.out.println(node + ": " + counter);
                    assertTrue(delta < keysDelta, "Node keys counter is out of range on node " + node + ", delta = " + delta);
                });
    }

    private static Topology<String> createTopology() {
        return Topology.consistentHashing(NODES, ME, VNODE_COUNT);
    }
}
