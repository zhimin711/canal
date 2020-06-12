package com.alibaba.otter.canal.connector.core.util;

import com.alibaba.otter.canal.common.alarm.AlarmType;

/**
 * MQ 回调类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public interface Callback {

    void commit();

    void rollback();

    void alarm(AlarmType alarmType, String message, Throwable throwable);
}
