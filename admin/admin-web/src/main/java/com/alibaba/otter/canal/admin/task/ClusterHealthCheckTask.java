package com.alibaba.otter.canal.admin.task;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.admin.model.*;
import com.alibaba.otter.canal.admin.service.*;
import com.alibaba.otter.canal.common.alarm.AlarmType;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

//@Component
public class ClusterHealthCheckTask implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(ClusterHealthCheckTask.class);

    @Autowired
    private CanalClusterService canalClusterService;
    @Autowired
    private NodeServerService nodeServerService;
    @Autowired
    private CanalInstanceService canalInstanceService;

    @Autowired
    private CanalInstanceMetaService canalInstanceMetaService;
    @Autowired
    PollingAlarmService pollingAlarmService;

    private ScheduledExecutorService executor;
    private long period = 60000;                                               // 单位ms
    private long period2 = 10000;                                               // 单位ms

    private static final String CANAL_HEALTH_LOCK = "/otter/canal/health/lock";

    private Set<CanalInstanceAlarm> alarmTasks;

    @Override
    public void afterPropertiesSet() throws Exception {
        executor = Executors.newScheduledThreadPool(1);
        alarmTasks = Collections.synchronizedSet(new HashSet<>());
        //1 重试策略：初试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        // 启动定时工作任务
        executor.scheduleAtFixedRate(() -> {
                    List<CanalCluster> clusters = canalClusterService.findList(new CanalCluster());
                    clusters.forEach(r -> {
                        //2 通过工厂创建连接
                        CuratorFramework cf = CuratorFrameworkFactory.builder()
                                .connectString(r.getZkHosts())
                                .sessionTimeoutMs(5000)
                                .retryPolicy(retryPolicy)
                                .build();
                        //3 开启连接
                        cf.start();
                        InterProcessMutex lock = new InterProcessMutex(cf, CANAL_HEALTH_LOCK);
//                        ZkClientx client = ZkClientx.getZkClient(r.getZkHosts());
//                        client.exists("/otter/canal/health");

                        try {
                            //可重入
                            lock.acquire();
                            healthCheck(r);
                        } catch (Exception e) {
                            logger.error("health check and auto restart error: " + r.getName(), e);
                        } finally {
                            try {
                                lock.release();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                },
                period,
                period,
                TimeUnit.MILLISECONDS);
/*
        executor.scheduleAtFixedRate(() -> {
                    //todo handle alarm to ready start

                },
                period2,
                period2,
                TimeUnit.MILLISECONDS);*/
    }

    private void healthCheck(CanalCluster cluster) {
        NodeServer p1 = new NodeServer();
        p1.setClusterId(cluster.getId());
        Pager<NodeServer> nodeServerPager = nodeServerService.findList(p1, new Pager<>(1, 10));
        if (nodeServerPager.getCount().equals(0L)) {
            return;
        }
        nodeServerPager.getItems().forEach(e -> {
            List<CanalInstanceConfig> insList = canalInstanceService.findActiveInstanceByServerId(e.getId());
            handleInstance(insList);
        });
    }

    private void handleInstance(List<CanalInstanceConfig> instanceConfigs) {
        if (instanceConfigs.isEmpty()) return;
        Date currentDate = DateUtils.current();
        instanceConfigs.forEach(e -> {
            LogPosition position = canalInstanceMetaService.instanceMetaPosition(e.getId());
            if (position == null) {
                logger.warn("{} not found meta!", e.getName());
                return;
            }
            Date time = DateUtils.parseTimestamp(position.getPostion().getTimestamp());
            if (time == null || DateUtils.addMinutes(time, 5).after(currentDate)) return;
            CanalInstanceAlarm alarm = pollingAlarmService.findLastAlarmByNameAndType(e.getName(), AlarmType.META_TIMESTAMP);
            if (alarm == null) {
                logger.info("{} not found pre alarm!", e.getName());
                savePositionAlarm(e, position);
                return;
            }
            LogPosition prePosition = JsonUtils.unmarshalFromString(alarm.getMessage(), LogPosition.class);
            if (prePosition == null) {
                savePositionAlarm(e, position);
                return;
            }
            Date time2 = DateUtils.parseTimestamp(prePosition.getPostion().getTimestamp());
            if (time2 == null || time.after(time2)) {
                savePositionAlarm(e, position);
                return;
            }
            canalInstanceService.instanceOperation(e.getId(), "stop");
        });
    }

    private void savePositionAlarm(CanalInstanceConfig instanceConfig, LogPosition position) {
        String message = JsonUtils.marshalToString(position, SerializerFeature.WriteClassName);
        CanalInstanceAlarm alarm = new CanalInstanceAlarm();
        alarm.setName(instanceConfig.getName());
        alarm.setType(AlarmType.META_TIMESTAMP.name());
        alarm.setStatus("0");
        alarm.setMessage(message);
//        alarm.setCreatedTime();
        pollingAlarmService.save(alarm);
    }

    public void handleAlarm(CanalInstanceAlarm alarm) {
        pollingAlarmService.batchUpdateFinishStatus(alarm);
    }


}
