package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.otter.canal.admin.model.CanalInstanceAlarm;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.admin.service.PollingAlarmService;
import com.alibaba.otter.canal.common.alarm.AlarmType;
import io.ebean.Query;
import io.ebean.RawSql;
import io.ebean.RawSqlBuilder;
import io.ebean.UpdateQuery;
import org.apache.commons.lang.StringUtils;
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

    public Pager<CanalInstanceAlarm> findList(CanalInstanceAlarm canalInstanceAlarm, Pager<CanalInstanceAlarm> pager) {
        Query<CanalInstanceAlarm> query = CanalInstanceAlarm.find.query()
                .setDisableLazyLoading(true)
                .select("type, message, name, createdTime");
        if (canalInstanceAlarm != null) {
            if (StringUtils.isNotEmpty(canalInstanceAlarm.getName())) {
                query.where().like("name", "%" + canalInstanceAlarm.getName() + "%");
            }
            if (StringUtils.isNotEmpty(canalInstanceAlarm.getType())) {
                query.where().eq("type", canalInstanceAlarm.getType());
            }
        }

        Query<CanalInstanceAlarm> queryCnt = query.copy();
        int count = queryCnt.findCount();
        pager.setCount((long) count);

        query.setFirstRow(pager.getOffset().intValue()).setMaxRows(pager.getSize()).order().asc("id");
        List<CanalInstanceAlarm> canalInstanceConfigs = query.findList();
        pager.setItems(canalInstanceConfigs);

        if (canalInstanceConfigs.isEmpty()) {
            return pager;
        }

        return pager;
    }
}
