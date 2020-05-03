package com.pingan.xjl.es.entity;

/**
 * @author Aaron
 * @date 2020/5/2 21:56
 */
public interface EsDocument {

    /**
     * 获取文档id
     * @return
     */
    String getId();

    /**
     * 获取索引
     * @return
     */
    String getIndex();

}
