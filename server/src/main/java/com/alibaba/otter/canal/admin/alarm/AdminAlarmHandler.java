package com.alibaba.otter.canal.admin.alarm;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.alarm.Alarm;
import com.alibaba.otter.canal.common.alarm.AlarmType;
import com.alibaba.otter.canal.common.alarm.LogAlarmHandler;
import com.alibaba.otter.canal.instance.manager.plain.HttpHelper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AdminAlarmHandler extends LogAlarmHandler {

    private static final Logger logger = LoggerFactory.getLogger(AdminAlarmHandler.class);

    private final static Integer REQUEST_TIMEOUT = 5000;

    private HttpHelper httpHelper;

    private String alarmEnable;

    private String managerUrl;
    private String user;
    private String passwd;

    @Override
    public void start() {
        if (StringUtils.isBlank(this.managerUrl) && isEnabled()) {
            return;
        }
        super.start();
        this.httpHelper = new HttpHelper();
        if (!StringUtils.startsWithIgnoreCase(managerUrl, "http")) {
            this.managerUrl = "http://" + managerUrl;
        }
        logger.info("## admin alarm handler init finish! {}", managerUrl);
    }

    @Override
    public void sendAlarm(String destination, String msg) {
        super.sendAlarm(destination, msg);
        if (!isStart()) {
            return;
        }
        Alarm alarm = null;
        if (JSONObject.isValid(msg)) {
            alarm = JSONObject.parseObject(msg, Alarm.class);
        }
        if (alarm == null) {
            alarm = new Alarm(AlarmType.UNKNOWN, msg);
        }
        Map<String, String> heads = new HashMap<>();
        heads.put("user", user);
        heads.put("passwd", passwd);
        String url = managerUrl + "/api/v1/alarm/instance_polling/" + destination;
        String response = httpHelper.post(url, heads, alarm, REQUEST_TIMEOUT);
        ResponseModel<Boolean> resp = JSONObject.parseObject(response, new TypeReference<ResponseModel<Boolean>>() {
        });
        if (!HttpHelper.REST_STATE_OK.equals(resp.code)) {
            throw new CanalException("requestPost for canal alarm error: " + resp.message);
        }
    }

    public boolean isEnabled() {
       return Boolean.parseBoolean(alarmEnable);
    }

    public void setAlarmEnable(String alarmEnable) {
        this.alarmEnable = alarmEnable;
    }

    private static class ResponseModel<T> {

        public Integer code;
        public String message;
        public T data;
    }

    public void setManagerUrl(String managerUrl) {
        this.managerUrl = managerUrl;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }
}
