package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.otter.canal.admin.model.CanalInstanceAlarm;
import com.alibaba.otter.canal.admin.service.PollingAlarmService;
import com.alibaba.otter.canal.common.alarm.AlarmType;
import io.ebean.Query;
import io.ebean.RawSql;
import io.ebean.RawSqlBuilder;
import io.ebean.UpdateQuery;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PollingAlarmServiceImpl implements PollingAlarmService {

    public void save(CanalInstanceAlarm record) {
        record.save();
    }

    @Override
    public List<CanalInstanceAlarm> autoHandleAlarm(AlarmType alarmType) {
        Query<CanalInstanceAlarm> query = CanalInstanceAlarm.find.query();
//        where type in(:type) and status=:status
        String sql = "select type,name from canal_instance_alarm where type =:type and status=:status group by type,name,status having count(1) > 5";
        RawSql rawSql = RawSqlBuilder.parse(sql)
                .create();
        return query.setRawSql(rawSql).setParameter("status", "0").setParameter("type", alarmType.name()).findList();
    }

    @Override
    public int batchUpdateFinishStatus(CanalInstanceAlarm alarm) {
        UpdateQuery<CanalInstanceAlarm> update = CanalInstanceAlarm.find.update();
        update.where().eq("name", alarm.getName()).eq("type", alarm.getType()).eq("status", "0");
        update.set("status", "1");
        return update.update();
    }
}
