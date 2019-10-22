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
    private static final int VNODE_COUNT = 100;
    private static final int KEYS_COUNT = 10_000_000;

    private final Map<String, Integer> counters = new HashMap<>();

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
        for (long i = 0; i < KEYS_COUNT; i++) {
            final String key = "key" + i;
            final String node = topology.primaryFor(ByteBuffer.wrap(key.getBytes(Charsets.UTF_8)));
            counters.compute(node, (n, c) -> c == null ? 1 : c + 1);
        }
        counters.forEach((n, c) -> {
            System.out.println("Node " + n + " = " + c);
            assertTrue(c > 0.3 * KEYS_COUNT);
        });
    }

    private static Topology<String> createTopology() {
        return Topology.consistentHashing(NODES, ME, VNODE_COUNT);
    }
}
