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
    private static final int EXPECTED_KEYS_PER_NODE = KEYS_COUNT / NODES.size();
    private static final int KEYS_DELTA = (int) (EXPECTED_KEYS_PER_NODE * 0.1); // ±10%

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
    void testUniformDistribution() {
        final Topology<String> topology = createTopology();
        final Map<String, Integer> counters = new HashMap<>();
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = "key" + i;
            final String node = topology.primaryFor(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)));
            counters.compute(node, (n, c) -> c == null ? 1 : c + 1);
        }

        counters.entrySet()
                .stream()
                .peek(e -> System.out.println("Node " + e.getKey() + " = " + e.getValue()))
                .forEach(e -> {
                    final String node = e.getKey();
                    final int counter = e.getValue();
                    final int delta = Math.abs(EXPECTED_KEYS_PER_NODE - counter);
                    assertTrue(delta < KEYS_DELTA,"Node keys counter is out of range on node " + node + ", delta = " + delta);
                });
    }

    private static Topology<String> createTopology() {
        return Topology.consistentHashing(NODES, ME, VNODE_COUNT);
    }
}
