package com.alibaba.otter.canal.meta;

import com.alibaba.otter.canal.protocol.position.PositionRange;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

@Ignore
public class RedisMetaManagerTest extends AbstractMetaManagerTest {

    @Test
    public void testSubscribeAll() {
        RedisMetaManager metaManager = new RedisMetaManager();
        metaManager.setJedisPoolConfig(new JedisPoolConfig());
        metaManager.setRedisHost("192.168.199.194");
        metaManager.setRedisPort(6379);
        metaManager.start();
        doSubscribeTest(metaManager);
        metaManager.stop();
    }

    @Test
    public void testBatchAll() {
        RedisMetaManager metaManager = new RedisMetaManager();
        metaManager.start();
        doBatchTest(metaManager);

        metaManager.clearAllBatchs(clientIdentity);
        Map<Long, PositionRange> ranges = metaManager.listAllBatchs(clientIdentity);
        Assert.assertEquals(0, ranges.size());
        metaManager.stop();
    }

    @Test
    public void testCursorAll() {
        RedisMetaManager metaManager = new RedisMetaManager();
        metaManager.start();
        doCursorTest(metaManager);
        metaManager.stop();
    }
}
