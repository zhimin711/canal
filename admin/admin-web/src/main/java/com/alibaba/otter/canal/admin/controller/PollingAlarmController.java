package com.alibaba.otter.canal.admin.controller;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.CanalInstanceAlarm;
import com.alibaba.otter.canal.admin.service.PollingAlarmService;
import com.alibaba.otter.canal.admin.task.AlarmTask;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.security.NoSuchAlgorithmException;

/**
 * Canal Instance配置管理控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/{env}/alarm")
public class PollingAlarmController {

    private static final byte[] seeds = "canal is best!".getBytes();

    @Value(value = "${canal.adminUser}")
    String user;

    @Value(value = "${canal.adminPasswd}")
    String passwd;

    @Autowired
    PollingAlarmService pollingAlarmService;

    /**
     * 添加单个instance的预警
     */
    @PostMapping(value = "/instance_polling/{destination}")
    public BaseModel<Boolean> instanceAlarmPoll(@RequestHeader String user, @RequestHeader String passwd,
                                                 @PathVariable String env,
                                                 @PathVariable String destination, @RequestBody CanalInstanceAlarm record) {
        if (!auth(user, passwd)) {
            throw new RuntimeException("auth :" + user + " is failed");
        }
        record.setName(destination);
        record.setStatus("0");
        pollingAlarmService.save(record);
        return BaseModel.getInstance(Boolean.TRUE);
    }

    private boolean auth(String user, String passwd) {
        // 如果user/passwd密码为空,则任何用户账户都能登录
        if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
            if (StringUtils.isEmpty(this.passwd)) {
                return true;
            } else if (StringUtils.isEmpty(passwd)) {
                // 如果server密码有配置,客户端密码为空,则拒绝
                return false;
            }

            try {
                // manager这里保存了原始密码，反过来和canal发送过来的进行校验
                byte[] passForClient = SecurityUtil.scramble411(this.passwd.getBytes(), seeds);
                return SecurityUtil.scrambleServerAuth(passForClient, SecurityUtil.hexStr2Bytes(passwd), seeds);
            } catch (NoSuchAlgorithmException e) {
                return false;
            }
        }

        return false;
    }
}
