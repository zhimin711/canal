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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

/**
 * redis版实现
 *
 * @author zhimi
 * @since 2020-04-09
 */
public class RedisMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(RedisMetaManager.class);

    protected static final String KEY_SEPARATOR = "_";

    protected static final String KEY_SEPARATOR1 = ":";
    protected static final String KEY_SEPARATOR2 = ",";

    private static final String KEY_ROOT = "otter_canal_meta";

    private static final String KEY_DESTINATIONS = "destination";

    private static final String KEY_META = "meta";
    private static final int META_TIMEOUT = 3600 * 24 * 7;

    private static final String KEY_CURSOR = "cursor";

    private static final String KEY_BATCH = "batch";

    private static final String KEY_MAX = "max";

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd", Locale.CHINA);

    private JedisPoolConfig jedisPoolConfig;

    private String redisHost;

    private Integer redisPort;

    protected Integer redisDatabase = 0;

    protected Integer redisTimeout = 2000;

    protected Integer redisMetaExpire = 0;

    protected String redisPassword;

    private String redisSentinelMaster;

    private String redisSentinelNodes;

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

    private Pool<Jedis> jedisPool;
    private JedisCluster jedisCluster;

    private boolean isCluster = false;

    @Override
    public void start() {
        super.start();
        if (StringUtils.isNotBlank(redisHost)) {
            jedisPool = StringUtils.isBlank(redisPassword) ?
                    new JedisPool(jedisPoolConfig, redisHost, redisPort, redisTimeout) :
                    new JedisPool(jedisPoolConfig, redisHost, redisPort, redisTimeout, redisPassword);

        } else if (StringUtils.isNotBlank(redisSentinelNodes)) {
            Set<String> sentinels = parseNodes(redisSentinelNodes);
            jedisPool = StringUtils.isBlank(redisPassword) ?
                    new JedisSentinelPool(redisSentinelMaster, sentinels, jedisPoolConfig) :
                    new JedisSentinelPool(redisSentinelMaster, sentinels, jedisPoolConfig, redisPassword);
        } else if (StringUtils.isNotBlank(redisClusterNodes)) {
            Set<HostAndPort> nodes = parseHostAndPortNodes(redisClusterNodes);
            jedisCluster = StringUtils.isBlank(redisPassword) ?
                    new JedisCluster(nodes, redisTimeout, soTimeout, maxAttempts, jedisPoolConfig) :
                    new JedisCluster(nodes, redisTimeout, soTimeout, maxAttempts, redisPassword, jedisPoolConfig);
            isCluster = true;
        }

    }

    private Set<String> parseNodes(String redisSentinelNodes) {
        String[] nodeArr = redisSentinelNodes.split(KEY_SEPARATOR2);
        return Sets.newHashSet(nodeArr);
    }

    private Set<HostAndPort> parseHostAndPortNodes(String redisClusterNodes) {
        String[] nodeArr = redisClusterNodes.split(KEY_SEPARATOR2);
        Set<HostAndPort> hostAndPorts = Sets.newHashSet();
        for (String node : nodeArr) {
            String[] hpArr = node.split(KEY_SEPARATOR1);
            if (hpArr.length == 2) {
                hostAndPorts.add(new HostAndPort(hpArr[0], Integer.parseInt(hpArr[1])));
            }
        }
        return hostAndPorts;
    }

    @Override
    public void stop() {
        super.stop();
        if (jedisPool != null) {
            jedisPool.close();
        }
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
            } catch (IOException e) {
                logger.error("## redis cluster close error!", e);
            }
        }
    }

    @Override
    public void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        invokeRedis(jedis -> jedis.hset(getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination()), String.valueOf(clientIdentity.getClientId()), JsonUtils.marshalToString(clientIdentity)));
    }

    @Override
    public boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return invokeRedis(jedis -> jedis.hexists(getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination()), String.valueOf(clientIdentity.getClientId())));
    }

    @Override
    public void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        invokeRedis(jedis -> jedis.hdel(getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination()), String.valueOf(clientIdentity.getClientId())));
    }

    @Override
    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String json = invokeRedis(jedis -> jedis.get(getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), KEY_CURSOR)));
        if (StringUtils.isNotEmpty(json)) {
            return JsonUtils.unmarshalFromString(json, Position.class);
        }
        return null;
    }

    @Override
    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        String dateStr = simpleDateFormat.format(new Date());
        String metaKey = getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), dateStr);
        invokeRedis(jedis -> {
            if (redisMetaExpire != null && redisMetaExpire > 3600) { // 配置大于1小时才缓存Binlog游标
                if (jedis.exists(metaKey)) {
                    jedis.sadd(metaKey, position.toString());
                } else {
                    jedis.sadd(metaKey, position.toString());
                    jedis.expire(metaKey, redisMetaExpire);
                }
            }
            return jedis.set(getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), KEY_CURSOR), JsonUtils.marshalToString(position, SerializerFeature.WriteClassName));
        });
    }

    @Override
    public List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        List<String> jsons = invokeRedis(jedis -> jedis.hvals(getRedisKey(KEY_DESTINATIONS, destination)));
        List<ClientIdentity> results = new ArrayList<>();
        for (String json : jsons) {
            results.add(JsonUtils.unmarshalFromString(json, ClientIdentity.class));
        }
        return results;
    }

    @Override
    public PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Set<String> jsons = invokeRedis(jedis -> jedis.zrange(getKeyOfClientBatch(clientIdentity), 0, 0));
        if (jsons == null || jsons.isEmpty()) {
            return null;
        }
        String json = jsons.iterator().next();
        return JsonUtils.unmarshalFromString(json, PositionRange.class);
    }

    @Override
    public PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Set<String> jsons = invokeRedis(jedis -> jedis.zrange(getKeyOfClientBatch(clientIdentity), -1, -1));
        if (jsons == null || jsons.isEmpty()) {
            return null;
        }
        String json = jsons.iterator().next();
        return JsonUtils.unmarshalFromString(json, PositionRange.class);
    }

    @Override
    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        Long batchId = invokeRedis(jedis -> jedis.incr(getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), KEY_MAX, KEY_BATCH)));
        addBatch(clientIdentity, positionRange, batchId);
        return batchId;
    }

    @Override
    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId) throws CanalMetaManagerException {
        String key = getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), KEY_MAX, KEY_BATCH);
        invokeRedis(jedis -> jedis.zadd(getKeyOfClientBatch(clientIdentity), batchId.doubleValue(), JsonUtils.marshalToString(positionRange, SerializerFeature.WriteClassName)));
        String maxBatchId = invokeRedis(jedis -> jedis.get(key));
        if (Long.parseLong(maxBatchId) < batchId) {
            invokeRedis(jedis -> jedis.set(key, String.valueOf(batchId)));
        }
    }

    @Override
    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        Set<String> jsons = invokeRedis(jedis -> jedis.zrangeByScore(getKeyOfClientBatch(clientIdentity), batchId.doubleValue(), batchId.doubleValue()));
        if (jsons == null || jsons.isEmpty()) {
            return null;
        }
        String json = jsons.iterator().next();
        return JsonUtils.unmarshalFromString(json, PositionRange.class);
    }

    @Override
    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        Set<Tuple> tuples = invokeRedis(jedis -> jedis.zrangeWithScores(getKeyOfClientBatch(clientIdentity), 0, 0));
        if (tuples == null || tuples.isEmpty()) {
            return null;
        }
        Long minBatchId = new Double(tuples.iterator().next().getScore()).longValue();
        if (!minBatchId.equals(batchId)) {
            // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
            throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d", batchId, minBatchId));
        }
        PositionRange positionRange = getBatch(clientIdentity, batchId);
        if (positionRange == null) {
            return null;
        }
        invokeRedis(jedis -> jedis.zremrangeByScore(getKeyOfClientBatch(clientIdentity), batchId, batchId));
        return positionRange;
    }

    @Override
    public Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Map<Long, PositionRange> positionRanges = Maps.newLinkedHashMap();
        Set<Tuple> tuples = invokeRedis(jedis -> jedis.zrangeWithScores(getKeyOfClientBatch(clientIdentity), 0, -1));
        if (tuples == null || tuples.isEmpty()) {
            return positionRanges;
        }
        for (Tuple tuple : tuples) {
            Long batchId = new Double(tuple.getScore()).longValue();
            positionRanges.put(batchId, JsonUtils.unmarshalFromString(tuple.getElement(), PositionRange.class));
        }
        return positionRanges;
    }

    @Override
    public void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        invokeRedis(jedis -> jedis.zremrangeByRank(getKeyOfClientBatch(clientIdentity), 0, -1));
    }

    private String getKeyOfClientBatch(ClientIdentity clientIdentity) {
        return getRedisKey(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), clientIdentity, KEY_BATCH);
    }

    private String getRedisKey(Object... args) {
        StringBuilder stringBuilder = new StringBuilder(KEY_ROOT);
        for (Object obj : args) {
            stringBuilder.append(KEY_SEPARATOR1);
            stringBuilder.append(obj);
        }
        return stringBuilder.toString();
    }

    protected <V> V invokeRedis(Function<JedisCommands, V> function) {
        Jedis jedis = null;
        try {
            if (isCluster) {
                return function.apply(jedisCluster);
            }
            jedis = jedisPool.getResource();
            jedis.select(redisDatabase);
            return function.apply(jedis);
        } catch (Throwable t) {
            logger.error("invokeRedis error!", t);
            return null;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public void setRedisPort(Integer redisPort) {
        this.redisPort = redisPort;
    }

    public void setRedisDatabase(Integer redisDatabase) {
        this.redisDatabase = redisDatabase;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public void setRedisSentinelMaster(String redisSentinelMaster) {
        this.redisSentinelMaster = redisSentinelMaster;
    }

    public void setRedisSentinelNodes(String redisSentinelNodes) {
        this.redisSentinelNodes = redisSentinelNodes;
    }

    public void setRedisTimeout(Integer redisTimeout) {
        this.redisTimeout = redisTimeout;
    }

    public void setRedisMetaExpire(Integer redisMetaExpire) {
        this.redisMetaExpire = redisMetaExpire;
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