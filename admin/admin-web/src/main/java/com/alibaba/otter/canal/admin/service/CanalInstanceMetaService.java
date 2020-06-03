package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * Canal实例Meta信息业务层接口
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
public interface CanalInstanceMetaService {

    LogPosition instanceMetaPosition(Long id);

    Boolean updateInstanceMetaPosition(Long id, LogPosition position);

    Boolean resetInstanceMetaBatchId(Long id);
}
