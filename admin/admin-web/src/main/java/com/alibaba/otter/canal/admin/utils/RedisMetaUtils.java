package com.alibaba.otter.canal.admin.utils;

import com.alibaba.otter.canal.protocol.ClientIdentity;

public class RedisMetaUtils {

    protected static final String KEY_SEPARATOR1 = ":";
    protected static final String KEY_SEPARATOR2 = ",";

    private static final String KEY_ROOT = "otter_canal_meta";

    private static final String KEY_DESTINATIONS = "destination";

    private static final String KEY_CURSOR = "cursor";

    private static final String KEY_BATCH = "batch";

    private static final String KEY_MAX = "max";

    public static String getKeyOfSubscribe(ClientIdentity clientIdentity) {
        return rootAppend(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), clientIdentity, KEY_BATCH);
    }

    public static String getKeyOfCursor(ClientIdentity clientIdentity) {
        return rootAppend(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), KEY_CURSOR);
    }

    public static String getKeyOfClientBatch(ClientIdentity clientIdentity) {
        return rootAppend(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), clientIdentity, KEY_BATCH);
    }

    public static String getKeyOfMaxBatch(ClientIdentity clientIdentity) {
        return rootAppend(KEY_DESTINATIONS, clientIdentity.getDestination(), clientIdentity.getClientId(), KEY_MAX, KEY_BATCH);
    }

    public static String rootAppend(Object... args) {
        StringBuilder stringBuilder = new StringBuilder(KEY_ROOT);
        for (Object obj : args) {
            stringBuilder.append(KEY_SEPARATOR1);
            stringBuilder.append(obj);
        }
        return stringBuilder.toString();
    }
}
