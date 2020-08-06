package com.alibaba.otter.canal.common.alarm;

public enum AlarmType {

    META_FIRSTLY, META_TIMESTAMP, META_TIMEOUT, META_TIMEOUT_LOG,
    PROCESS, ACK_ERROR, ROLLBACK_ERROR,
    CLIENT_ID_LASTEST_BATCH, CLIENT_IDENTITY_SHOULD_SUBSCRIBE,
    DESTINATION_SHOULD_START, DESTINATION_CONFIG_NOT_FOUND,
    MQ_PRODUCER, UNKNOWN
}
