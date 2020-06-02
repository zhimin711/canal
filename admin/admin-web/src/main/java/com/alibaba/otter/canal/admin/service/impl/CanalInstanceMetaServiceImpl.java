package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;
import com.alibaba.otter.canal.admin.service.CanalInstanceMetaService;
import com.alibaba.otter.canal.admin.utils.RedisMetaUtils;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPoolConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Service
public class CanalInstanceMetaServiceImpl implements CanalInstanceMetaService {

    @Autowired
    private CanalConfigService canalConfigService;

    private Map<String, StringRedisTemplate> redisTemplateMap = Maps.newHashMap();

    @Override
    public LogPosition instanceMetaPosition(Long id) {
        Properties properties = loadConfig(id);
        if (properties == null) return null;
        boolean isZookeeper = "classpath:spring/default-instance.xml".equals(properties.getProperty("canal.instance.global.spring.xml"));
        boolean isRedis = "classpath:spring/redis-instance.xml".equals(properties.getProperty("canal.instance.global.spring.xml"));

        final ClientIdentity clientIdentity = new ClientIdentity(properties.getProperty("destination.name"), (short) 1001, "");
        if (isRedis) {
            StringRedisTemplate redisTemplate = initRedisTemplate(properties);
            if (redisTemplate == null) return null;
            Object json = redisTemplate.opsForValue().get(RedisMetaUtils.getKeyOfCursor(clientIdentity));
            if (json == null) {
                return null;
            }
            return JsonUtils.unmarshalFromString(json.toString(), LogPosition.class);
        } else if (isZookeeper) {
            ZkClientx zkClientx = initZkClientx(properties);
            return getZkCursor(zkClientx, clientIdentity);
        }
        return null;
    }

    private LogPosition getZkCursor(ZkClientx zkClientx, ClientIdentity clientIdentity) {
        String path = ZookeeperPathUtils.getCursorPath(clientIdentity.getDestination(), clientIdentity.getClientId());

        byte[] data = zkClientx.readData(path, true);
        if (data == null || data.length == 0) {
            return null;
        }

        return JsonUtils.unmarshalFromByte(data, LogPosition.class);
    }


    private ZkClientx initZkClientx(Properties properties) {
        String zkServers = properties.getProperty("canal.zkServers");
        return ZkClientx.getZkClient(zkServers);
    }

    private Long getClientId(StringRedisTemplate redisTemplate, String destinationKey) {
        Set<Object> clientJson = redisTemplate.opsForHash().entries(destinationKey).keySet();
        long clientId = 0;
        for (Object key : clientJson) {
            long lid = Long.parseLong(key.toString());
            if (lid > clientId) {
                clientId = lid;
            }
        }
        return clientId;
    }

    private Properties loadConfig(Long id) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig == null) {
            return null;
        }
        CanalConfig config = canalConfigService.getCanalConfig(canalInstanceConfig.getClusterId(), canalInstanceConfig.getServerId());
        Properties properties = new Properties();
        try {
            properties.load(new ByteArrayInputStream(config.getContent().getBytes(StandardCharsets.UTF_8)));
            properties.put("destination.name", canalInstanceConfig.getName());
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private StringRedisTemplate initRedisTemplate(Properties properties) {
        if (properties == null) return null;
        JedisConnectionFactory factory = initJedisConnectionFactory(properties);
        if (factory == null) {
            return redisTemplateMap.get(properties.getProperty("redis.template.key"));
        }
        factory.afterPropertiesSet();

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(factory);
//        log.info("RedisTemplate实例化成功！");
        redisTemplateMap.put(properties.getProperty("redis.template.key"), stringRedisTemplate);
        return stringRedisTemplate;
    }

    private JedisConnectionFactory initJedisConnectionFactory(Properties properties) {
        String password = properties.getProperty("redis.password", null);
        String hostName = properties.getProperty("redis.host", null);
        String port = properties.getProperty("redis.port", null);
        String timeout = properties.getProperty("redis.timeout", "6000");
        String database = properties.getProperty("redis.database", "0");

        String testOnBorrow = properties.getProperty("redis.testOnBorrow", "true");

        String sentinelMaster = properties.getProperty("redis.sentinel.master", null);
        String sentinelNodes = properties.getProperty("redis.sentinel.nodes", null);

        String clusterNodes = properties.getProperty("redis.cluster.nodes", null);

        JedisClientConfiguration.JedisClientConfigurationBuilder jedisClientConfiguration = JedisClientConfiguration.builder();
        jedisClientConfiguration.connectTimeout(Duration.ofMillis(Integer.parseInt(timeout)));
        jedisClientConfiguration.usePooling();

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //最大连接数
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        //最小空闲连接数
        jedisPoolConfig.setMinIdle(20);
        //当池内没有可用的连接时，最大等待时间
        jedisPoolConfig.setMaxWaitMillis(5000);
        jedisPoolConfig.setTestOnBorrow(Boolean.parseBoolean(testOnBorrow));
        jedisPoolConfig.setTestOnReturn(true);
        //------其他属性根据需要自行添加-------------

        jedisClientConfiguration.usePooling();


        if (StringUtils.isNotBlank(hostName)) {
            String key = hostName + "_" + port;
            properties.put("redis.template.key", key);
            if (redisTemplateMap.get(key) != null) {
                return null;
            }
            RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
            redisStandaloneConfiguration.setHostName(hostName);
            if (StringUtils.isNumeric(port)) redisStandaloneConfiguration.setPort(Integer.parseInt(port));
            //由于我们使用了动态配置库,所以此处省略
            redisStandaloneConfiguration.setDatabase(Integer.parseInt(database));
            redisStandaloneConfiguration.setPassword(RedisPassword.of(password));
            return new JedisConnectionFactory(redisStandaloneConfiguration, jedisClientConfiguration.build());
        } else if (StringUtils.isNotBlank(sentinelNodes)) {
            String key = sentinelNodes + "_" + sentinelMaster;
            properties.put("redis.template.key", key);
            if (redisTemplateMap.get(key) != null) {
                return null;
            }
            String[] arr = sentinelNodes.split(",");
            Set<String> nodes = new HashSet<>(Arrays.asList(arr));
            RedisSentinelConfiguration redisSentinelConfiguration = new RedisSentinelConfiguration(sentinelMaster, nodes);
            redisSentinelConfiguration.setDatabase(Integer.parseInt(database));
            redisSentinelConfiguration.setPassword(password);
            return new JedisConnectionFactory(redisSentinelConfiguration, jedisPoolConfig);
        } else if (StringUtils.isNotBlank(clusterNodes)) {
            properties.put("redis.template.key", clusterNodes);
            if (redisTemplateMap.get(clusterNodes) != null) {
                return null;
            }
            String[] arr = clusterNodes.split(",");
            Set<String> nodes = new HashSet<>(Arrays.asList(arr));
            RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration(nodes);
            redisClusterConfiguration.setPassword(password);
            return new JedisConnectionFactory(redisClusterConfiguration, jedisPoolConfig);
        }
        throw new RuntimeException("未配置Mate Redis缓存");

    }

    @Override
    public Boolean updateInstanceMetaPosition(Long id, LogPosition position) {
        Properties properties = loadConfig(id);

        boolean isZookeeper = "classpath:spring/default-instance.xml".equals(properties.getProperty("canal.instance.global.spring.xml"));
        boolean isRedis = "classpath:spring/redis-instance.xml".equals(properties.getProperty("canal.instance.global.spring.xml"));

        final ClientIdentity clientIdentity = new ClientIdentity(properties.getProperty("destination.name"), (short) 1001, "");
        StringRedisTemplate redisTemplate = initRedisTemplate(properties);
        if (redisTemplate == null) return false;

        redisTemplate.delete(RedisMetaUtils.getKeyOfClientBatch(clientIdentity));
        redisTemplate.delete(RedisMetaUtils.getKeyOfMaxBatch(clientIdentity));
        if (position.getPostion() == null) {
            redisTemplate.delete(RedisMetaUtils.getKeyOfCursor(clientIdentity));
        } else {
            redisTemplate.opsForValue().set(RedisMetaUtils.getKeyOfCursor(clientIdentity), JsonUtils.marshalToString(position, SerializerFeature.WriteClassName));
        }

        return true;
    }


    @Override
    public Boolean resetInstanceMetaBatchId(Long id) {
        Properties properties = loadConfig(id);
        StringRedisTemplate redisTemplate = initRedisTemplate(properties);
        if (redisTemplate == null) return false;
        final ClientIdentity clientIdentity = new ClientIdentity(properties.getProperty("destination.name"), (short) 1001, "");
        redisTemplate.delete(RedisMetaUtils.getKeyOfMaxBatch(clientIdentity));

        return true;
    }


}
