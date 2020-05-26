package com.alibaba.otter.canal.meta;

import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 基于定时刷新的策略的mixed实现
 *
 * <pre>
 * 优化：
 * 1. 去除batch数据刷新到redis中，切换时batch数据可忽略，重新从头开始获取
 * </pre>
 *
 * @author zhimin 2020-6-11 下午02:41:15
 * @version 1.0.0
 */
public class PeriodMixedRedisMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(PeriodMixedRedisMetaManager.class);
    private ScheduledExecutorService executor;
    private RedisMetaManager redisMetaManager;
    @SuppressWarnings("serial")
    private final Position nullCursor = new Position() {
    };
    private long period = 1000;                                                 // 单位ms
    private Set<ClientIdentity> updateCursorTasks;

    public void start() {
        super.start();
        Assert.notNull(redisMetaManager);
        if (!redisMetaManager.isStart()) {
            redisMetaManager.start();
        }

        executor = Executors.newScheduledThreadPool(1);
        destinations = MigrateMap.makeComputingMap(destination -> redisMetaManager.listAllSubscribeInfo(destination));

        cursors = MigrateMap.makeComputingMap(clientIdentity -> {
            Position position = redisMetaManager.getCursor(clientIdentity);
            if (position == null) {
                return nullCursor; // 返回一个空对象标识，避免出现异常
            } else {
                return position;
            }
        });

        batches = MigrateMap.makeComputingMap(clientIdentity -> {
            // 读取一下zookeeper信息，初始化一次
            MemoryClientIdentityBatch batches = MemoryClientIdentityBatch.create(clientIdentity);
            Map<Long, PositionRange> positionRanges = redisMetaManager.listAllBatchs(clientIdentity);
            for (Map.Entry<Long, PositionRange> entry : positionRanges.entrySet()) {
                batches.addPositionRange(entry.getValue(), entry.getKey()); // 添加记录到指定batchId
            }
            return batches;
        });

        updateCursorTasks = Collections.synchronizedSet(new HashSet<>());

        // 启动定时工作任务
        executor.scheduleAtFixedRate(() -> {
            List<ClientIdentity> tasks = new ArrayList<ClientIdentity>(updateCursorTasks);
            for (ClientIdentity clientIdentity : tasks) {
                try {
                    // 定时将内存中的最新值刷到zookeeper中，多次变更只刷一次
                    redisMetaManager.updateCursor(clientIdentity, getCursor(clientIdentity));
                    updateCursorTasks.remove(clientIdentity);
                } catch (Throwable e) {
                    // ignore
                    logger.error("period update" + clientIdentity.toString() + " curosr failed!", e);
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        super.stop();

        if (redisMetaManager.isStart()) {
            redisMetaManager.stop();
        }

        executor.shutdownNow();
        destinations.clear();
        batches.clear();
    }

    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);

        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> redisMetaManager.subscribe(clientIdentity));
    }

    public void unsubscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.unsubscribe(clientIdentity);

        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> redisMetaManager.unsubscribe(clientIdentity));
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        super.updateCursor(clientIdentity, position);
        updateCursorTasks.add(clientIdentity);// 添加到任务队列中进行触发
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Position position = super.getCursor(clientIdentity);
        if (position == nullCursor) {
            return null;
        } else {
            return position;
        }
    }

    // =============== setter / getter ================

    public void setRedisMetaManager(RedisMetaManager redisMetaManager) {
        this.redisMetaManager = redisMetaManager;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

}
