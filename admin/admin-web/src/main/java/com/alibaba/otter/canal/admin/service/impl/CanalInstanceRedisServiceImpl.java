package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;
import com.alibaba.otter.canal.admin.service.CanalInstanceRedisService;
import com.alibaba.otter.canal.common.utils.JsonUtils;
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
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Service
public class CanalInstanceRedisServiceImpl implements CanalInstanceRedisService {

    @Autowired
    private CanalConfigService canalConfigService;

    private Map<String, RedisTemplate<String, Object>> redisTemplateMap = Maps.newHashMap();

    @Override
    public LogPosition instanceMetaPosition(Long id) {
        Properties properties = loadConfig(id);
        RedisTemplate<String, Object> redisTemplate = initRedisTemplate(properties);
        if (redisTemplate == null) return null;
        String destinationKey = "otter_canal_meta:destination:" + properties.getProperty("destination.name");
        Long clientId = getClientId(redisTemplate, destinationKey);
        String cursorKey = destinationKey + ":" + clientId + ":cursor";

        Object json = redisTemplate.opsForValue().get(cursorKey);
        if (json == null) {
            return null;
        }
        return JsonUtils.unmarshalFromString(json.toString(), LogPosition.class);
    }

    private Long getClientId(RedisTemplate<String, Object> redisTemplate, String destinationKey) {
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

    private RedisTemplate<String, Object> initRedisTemplate(Properties properties) {
        if (properties == null) return null;
        JedisConnectionFactory factory = initJedisConnectionFactory(properties);
        if (factory == null) {
            return redisTemplateMap.get(properties.getProperty("redis.template.key"));
        }
        factory.afterPropertiesSet();

        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        // 开启事务
        redisTemplate.setEnableTransactionSupport(true);
        redisTemplate.setConnectionFactory(factory);
        redisTemplate.afterPropertiesSet();
//        log.info("RedisTemplate实例化成功！");
        redisTemplateMap.put(properties.getProperty("redis.template.key"), redisTemplate);
        return redisTemplate;
    }

    private JedisConnectionFactory initJedisConnectionFactory(Properties properties) {
        String password = properties.getProperty("redis.password", null);
        String hostName = properties.getProperty("redis.host", null);
        String port = properties.getProperty("redis.port", null);
        String timeout = properties.getProperty("redis.timeout", "6000");
        String database = properties.getProperty("redis.database", "0");

        String sentinelMaster = properties.getProperty("redis.sentinel.master", null);
        String sentinelNodes = properties.getProperty("redis.sentinel.nodes", null);

        String clusterNodes = properties.getProperty("redis.cluster.nodes", null);

        JedisClientConfiguration.JedisClientConfigurationBuilder jedisClientConfiguration = JedisClientConfiguration.builder();
        jedisClientConfiguration.connectTimeout(Duration.ofMillis(Integer.parseInt(timeout)));
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
            return new JedisConnectionFactory(redisSentinelConfiguration, jedisClientConfiguration.build());
        } else if (StringUtils.isNotBlank(clusterNodes)) {
            properties.put("redis.template.key", clusterNodes);
            if (redisTemplateMap.get(clusterNodes) != null) {
                return null;
            }
            String[] arr = clusterNodes.split(",");
            Set<String> nodes = new HashSet<>(Arrays.asList(arr));
            RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration(nodes);
            redisClusterConfiguration.setPassword(password);
            return new JedisConnectionFactory(redisClusterConfiguration, jedisClientConfiguration.build());
        }
        throw new RuntimeException("未配置Mate Redis缓存");

    }

    @Override
    public Boolean updateInstanceMetaPosition(Long id, LogPosition position) {
        Properties properties = loadConfig(id);
        RedisTemplate<String, Object> redisTemplate = initRedisTemplate(properties);
        if (redisTemplate == null) return false;
        String destinationKey = "otter_canal_meta:destination:" + properties.getProperty("destination.name");
        Long clientId = getClientId(redisTemplate, destinationKey);

        String cursorKey = destinationKey + ":" + clientId + ":cursor";
        if (position.getPostion() == null) {
            redisTemplate.delete(cursorKey);
        } else {
            redisTemplate.opsForValue().set(cursorKey, JsonUtils.marshalToString(position, SerializerFeature.WriteClassName));
        }

        String batchKey = destinationKey + ":" + clientId + ":max:batch";
        redisTemplate.opsForValue().set(batchKey, "0");
        return true;
    }
}
