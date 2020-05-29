package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.CanalInstanceAlarm;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.common.alarm.AlarmType;

import java.util.List;

public interface PollingAlarmService {

    void save(CanalInstanceAlarm record);

    List<CanalInstanceAlarm> autoHandleAlarm(AlarmType alarmType);

    int batchUpdateFinishStatus(CanalInstanceAlarm alarm);

    Pager<CanalInstanceAlarm> findList(CanalInstanceAlarm canalInstanceConfig, Pager<CanalInstanceAlarm> pager);
}
