package ru.mail.polis.service.vaddya.topology;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashingTopologyTest {
    private static final String ME = "http://localhost:8080";
    private static final Set<String> NODES = Set.of(ME, "http://localhost:8081", "http://localhost:8082");
    private static final String NEW_NODE = "http://localhost:8083";
    private static final ReplicationFactor RF = ReplicationFactor.create(1, 2);
    private static final int KEY_LENGTH = 16;
    private static final int VNODE_COUNT = 200;
    private static final int KEYS_COUNT = 10_000_000;
    private static final float ACCEPTABLE_DELTA = 0.1f; // ±10%

    @Test
    void testConsistency() {
        final Topology<String> topology1 = createTopology();
        final Topology<String> topology2 = createTopology();

        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = randomKey();
            final String node1 = topology1.primaryFor(key);
            final String node2 = topology2.primaryFor(key);
            assertEquals(node1, node2);
        }
    }

    @Test
    void testConsistencyWithReplicationFactor() {
        final Topology<String> topology1 = createTopology();
        final Topology<String> topology2 = createTopology();

        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = randomKey();
            final Set<String> nodes1 = topology1.primaryFor(key, RF);
            final Set<String> nodes2 = topology2.primaryFor(key, RF);
            assertEquals(nodes1, nodes2);
        }
    }

    @Test
    void testKeysMigration() {
        final Topology<String> topology = createTopology();
        final Topology<String> topologyWithNew = createTopology();
        topologyWithNew.addNode(NEW_NODE);
        final Topology<String> topologyWithoutMe = createTopology();
        topologyWithoutMe.removeNode(ME);

        // only k/N keys need to be remapped when k is the number of keys and N is the number of servers 
        // (more specifically, the maximum of the initial and final number of servers)
        final int expectedKeysMigratedWithNew = KEYS_COUNT / Math.max(topology.size(), topologyWithNew.size());
        final int expectedKeysMigratedWithoutMe = KEYS_COUNT / Math.max(topology.size(), topologyWithoutMe.size());
        final int keysDeltaWithNew = (int) (expectedKeysMigratedWithNew * ACCEPTABLE_DELTA);
        final int keysDeltaWithoutMe = (int) (expectedKeysMigratedWithoutMe * ACCEPTABLE_DELTA);

        int keysMigratedWithNew = 0;
        int keysMigratedWithoutMe = 0;
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = randomKey();
            final String node = topology.primaryFor(key);

            final String nodeWithNew = topologyWithNew.primaryFor(key);
            if (!node.equals(nodeWithNew)) {
                keysMigratedWithNew++;
            }

            final String nodeWithoutMe = topologyWithoutMe.primaryFor(key);
            assert !nodeWithoutMe.equals(ME);
            if (!node.equals(nodeWithoutMe)) {
                keysMigratedWithoutMe++;
            }
        }

        assertTrue(keysMigratedWithNew < expectedKeysMigratedWithNew + keysDeltaWithNew,
                "Too many keys have changed their location after adding node: " + keysMigratedWithNew);

        assertTrue(keysMigratedWithoutMe < expectedKeysMigratedWithoutMe + keysDeltaWithoutMe,
                "Too many keys have changed their location after removing node: " + keysMigratedWithoutMe);
    }

    @Test
    void testUniformDistribution() {
        final Topology<String> topology = createTopology();
        final int expectedKeysPerNode = KEYS_COUNT / NODES.size();
        final int keysDelta = (int) (expectedKeysPerNode * ACCEPTABLE_DELTA);

        final Map<String, Integer> counters = new HashMap<>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = randomKey();
            final String node = topology.primaryFor(key);
            counters.compute(node, (n, c) -> c == null ? 1 : c + 1);
        }

        validateCounters(expectedKeysPerNode, keysDelta, counters);
    }

    @Test
    void testUniformDistributionWithReplicationFactor() {
        final Topology<String> topology = createTopology();
        final int expectedKeysPerNode = (KEYS_COUNT / NODES.size()) * RF.from();
        final int keysDelta = (int) (expectedKeysPerNode * ACCEPTABLE_DELTA);

        final Map<String, Integer> counters = new HashMap<>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = randomKey();
            final Set<String> nodes = topology.primaryFor(key, RF);
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

    private static String randomKey() {
        final byte[] result = new byte[KEY_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return new String(result);
    }
}
