package com.alibaba.otter.canal.admin.task;

import com.alibaba.otter.canal.admin.model.CanalInstanceAlarm;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.admin.service.CanalInstanceRedisService;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;
import com.alibaba.otter.canal.admin.service.PollingAlarmService;
import com.alibaba.otter.canal.common.alarm.AlarmType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

//@Component
public class AlarmTask implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(AlarmTask.class);

    @Autowired
    private CanalInstanceService canalInstanceService;

    @Autowired
    private CanalInstanceRedisService canalInstanceRedisService;
    @Autowired
    PollingAlarmService pollingAlarmService;

    private ScheduledExecutorService executor;
    private long period = 10000;                                               // 单位ms

    private Set<CanalInstanceAlarm> alarmTasks;

    @Override
    public void afterPropertiesSet() throws Exception {
        executor = Executors.newScheduledThreadPool(1);
        alarmTasks = Collections.synchronizedSet(new HashSet<>());
        // 启动定时工作任务
        executor.scheduleAtFixedRate(() -> {
                    //todo handle alarm to stop instance
                    List<CanalInstanceAlarm> alarms = pollingAlarmService.autoHandleAlarm(AlarmType.META_FIRSTLY);
//                    logger.info("auto handle alarm size: {}", alarms.size());

                    alarms.forEach(r -> {
                        try {
                            CanalInstanceConfig instanceConfig = canalInstanceService.findByInstanceName(r.getName());
                            Pager<CanalInstanceConfig> page = canalInstanceService.findList(instanceConfig, new Pager<>(1, 10));
                            if (page.getCount() > 0) {
                                page.getItems().forEach(e -> {
                                    if ("0".equals(e.getRunningStatus())) {
                                        //  instanceTasks.add(e);
                                        handleInstance(e);
                                        handleAlarm(r);
                                        logger.info("auto handle alarm to start instance: {}", r.getName());

                                    } else if ("1".equals(e.getRunningStatus())) {
                                        canalInstanceService.instanceOperation(e.getId(), "stop");
                                        logger.info("auto handle alarm to stop instance: {}", r.getName());
                                    }
                                });
                            }
                        } catch (Exception e) {
                            logger.error("handleAlarm error: " + r.getName(), e);
                        }
                    });
                },
                period,
                period,
                TimeUnit.MILLISECONDS);

//        executor.scheduleAtFixedRate(() -> {
//                    //todo handle alarm to ready start
//
//                },
//                period,
//                period,
//                TimeUnit.MILLISECONDS);
    }

    private void handleInstance(CanalInstanceConfig instanceConfig) {
        canalInstanceRedisService.resetInstanceMetaBatchId(instanceConfig.getId());
        canalInstanceService.instanceOperation(instanceConfig.getId(), "start");
    }

    public void addAlarm(CanalInstanceAlarm alarm) {
        alarmTasks.add(alarm);
    }

    public void handleAlarm(CanalInstanceAlarm alarm) {
        pollingAlarmService.batchUpdateFinishStatus(alarm);
    }


}
