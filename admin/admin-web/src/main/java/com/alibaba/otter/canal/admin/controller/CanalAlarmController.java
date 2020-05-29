package com.alibaba.otter.canal.admin.controller;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.CanalInstanceAlarm;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.admin.service.PollingAlarmService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/{env}/canal")
public class CanalAlarmController {

    @Autowired
    PollingAlarmService pollingAlarmService;

    @GetMapping(value = "/alarms")
    public BaseModel<Pager<CanalInstanceAlarm>> list(CanalInstanceAlarm canalInstanceAlarm,
                                                      Pager<CanalInstanceAlarm> pager, @PathVariable String env) {
        return BaseModel.getInstance(pollingAlarmService.findList(canalInstanceAlarm, pager));
    }


}
