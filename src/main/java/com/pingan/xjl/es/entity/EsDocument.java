package com.pingan.xjl.es.entity;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * @author Aaron
 * @date 2020/5/2 21:56
 */
public interface  EsDocument {

    /**
     * 获取文档 id
     * @return
     */
    @JSONField(serialize = false)
    String getId();

    /**
     * 获取索引
     * @return
     */
    @JSONField(serialize = false)
    String getIndex();

}
