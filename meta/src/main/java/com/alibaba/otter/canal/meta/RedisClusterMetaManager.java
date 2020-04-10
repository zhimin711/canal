package com.alibaba.otter.canal.meta;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import java.io.IOException;
import java.util.Set;
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

    /**
     * 集群节点
     */
    private String redisClusterNodes;
    /**
     * 返回值的超时时间
     */
    private Integer soTimeout;
    /**
     * 出现异常最大重试次数
     */
    private Integer maxAttempts;

    @Override
    public void start() {
        super.start();
        Set<HostAndPort> nodes = parseHostAndPortNodes(redisClusterNodes);
        jedisCluster = StringUtils.isBlank(redisPassword) ?
                new JedisCluster(nodes, redisTimeout, soTimeout, maxAttempts, jedisPoolConfig) :
                new JedisCluster(nodes, redisTimeout, soTimeout, maxAttempts, redisPassword, jedisPoolConfig);

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
    protected <V> V invokeRedis(Function<JedisCommands, V> function) {
        try {
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