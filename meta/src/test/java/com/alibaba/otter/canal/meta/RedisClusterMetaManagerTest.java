package com.alibaba.otter.canal.meta;

import com.alibaba.otter.canal.protocol.position.PositionRange;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

@Ignore
public class RedisClusterMetaManagerTest extends AbstractMetaManagerTest {

    @Test
    public void testSubscribeAll() {
        RedisClusterMetaManager metaManager = new RedisClusterMetaManager();
        metaManager.setJedisPoolConfig(new JedisPoolConfig());
        metaManager.setRedisClusterNodes("192.168.199.194:6391,192.168.199.194:6392,192.168.199.194:6393");
        metaManager.setSoTimeout(2000);
        metaManager.setMaxAttempts(3);
        metaManager.start();
        doSubscribeTest(metaManager);
        metaManager.stop();
    }

    @Test
    public void testBatchAll() {
        RedisClusterMetaManager metaManager = new RedisClusterMetaManager();
        metaManager.start();
        doBatchTest(metaManager);

        metaManager.clearAllBatchs(clientIdentity);
        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        metaManager.stop();
    }

    @Test
    public void testCursorAll() {
        RedisClusterMetaManager metaManager = new RedisClusterMetaManager();
        metaManager.start();
        doCursorTest(metaManager);
        metaManager.stop();
    }
}
