package com.alibaba.otter.canal.meta;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

/**
 * redis 集群版实现
 *
 * @author zhimi
 * @since 2020-04-10
 */
public class RedisClusterMetaManager extends RedisMetaManager {

    private static final String KEY_SEPARATOR2 = ";";

    private JedisCluster jedisCluster;

    private String redisClusterNodes;
    private Integer soTimeout;
    private Integer maxAttempts;

    @Override
    public void start() {
        super.start();
        Set<HostAndPort> nodes = parseHostAndPortNodes(redisClusterNodes);

        if (StringUtils.isBlank(redisPassword)) {
            jedisCluster = new JedisCluster(nodes, redisTimeout, jedisPoolConfig);
        } else {
            jedisCluster = new JedisCluster(nodes, redisTimeout, soTimeout, maxAttempts, redisPassword, jedisPoolConfig);

        }

    }

    private Set<HostAndPort> parseHostAndPortNodes(String redisClusterNodes) {
        String[] nodeArr = redisClusterNodes.split(KEY_SEPARATOR2);
        Set<HostAndPort> hostAndPorts = Sets.newHashSet();
        for (String node : nodeArr) {
            String[] hpArr = node.split(":");
            if (hpArr.length == 2) {
                hostAndPorts.add(new HostAndPort(hpArr[0], Integer.parseInt(hpArr[1])));
            }
        }
        return hostAndPorts;
    }

    @Override
    public void stop() {
        super.stop();
        try {
            jedisCluster.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected  <V> V invokeRedis(Function<JedisCommands, V> function) {
        try  {
            return function.apply(jedisCluster);
        } catch (Throwable t) {
            return null;
        }
    }

    public void setRedisClusterNodes(String redisClusterNodes) {
        this.redisClusterNodes = redisClusterNodes;
    }

    public void setSoTimeout(Integer soTimeout) {
        this.soTimeout = soTimeout;
    }

    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }
}