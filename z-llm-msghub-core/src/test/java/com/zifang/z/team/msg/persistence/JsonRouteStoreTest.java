package com.zifang.z.team.msg.persistence;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JsonRouteStore 单元测试
 */
public class JsonRouteStoreTest {

    private String testDir;
    private JsonRouteStore store;

    @BeforeEach
    public void setUp() {
        testDir = System.getProperty("java.io.tmpdir") + File.separator + "msghub-test-" + System.currentTimeMillis();
        store = new JsonRouteStore(testDir);
    }

    @AfterEach
    public void tearDown() {
        deleteDir(new File(testDir));
    }

    @Test
    public void testSaveAndLoadSubscriptions() {
        List<RouteStore.SubscriptionSnapshot> subs = new ArrayList<>();
        subs.add(new RouteStore.SubscriptionSnapshot("AgentMsg", "session-1"));
        subs.add(new RouteStore.SubscriptionSnapshot("AgentMsg", "session-2"));

        store.saveSubscriptions(subs);

        List<RouteStore.SubscriptionSnapshot> loaded = store.loadSubscriptions();
        assertEquals(loaded.size(), 2);
        assertEquals(loaded.get(0).topic, "AgentMsg");
        assertEquals(loaded.get(0).sessionId, "session-1");
        assertEquals(loaded.get(1).topic, "AgentMsg");
        assertEquals(loaded.get(1).sessionId, "session-2");
    }

    @Test
    public void testLoadWhenFileNotExistsReturnsEmpty() {
        List<RouteStore.SubscriptionSnapshot> subs = store.loadSubscriptions();
        assertTrue(subs.isEmpty());
    }

    private void deleteDir(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }
            dir.delete();
        }
    }
}
