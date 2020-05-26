package com.alibaba.otter.canal.common.alarm;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * 警告信息
 *
 * @author zhimi
 */
public class Alarm {

    private String name;
    private AlarmType type;
    private String message;

    public Alarm() {
    }

    public Alarm(AlarmType type, String message) {
        this.type = type;
        this.message = message;
    }

    public Alarm(String name, AlarmType type, String message) {
        this.name = name;
        this.type = type;
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AlarmType getType() {
        return type;
    }

    public void setType(AlarmType type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static String buildJson(AlarmType type, Throwable e) {
        String msg = ExceptionUtils.getStackTrace(e);
        return buildJson(type, msg);
    }

    public static String buildJson(AlarmType type, String msg) {
        return JSONObject.toJSONString(new Alarm(type, msg));
    }
}
