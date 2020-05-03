package com.pingan.xjl.es.api;

import java.io.IOException;

/**
 * 索引 api
 * @author Aaron
 * @date 2020/5/3 0:08
 */
public interface IndexApi {

    /**
     * 创建索引
     * @param name 索引名称
     * @param shards 分片数量
     * @param replicas 副本数量
     * @param mappingJsonStr mapping 设置
     * @return 是否创建成功
     * @throws  IOException
     */
    boolean create(String name, int shards, int replicas, String mappingJsonStr) throws IOException;

    /**
     * 删除索引
     * @param name
     * @return
     */
    boolean delete(String name);

}
