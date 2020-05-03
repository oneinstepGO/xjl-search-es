package com.pingan.xjl.es.api;

import com.pingan.xjl.es.entity.EsDocument;

import java.io.IOException;

/**
 * @author Aaron
 * @date 2020/4/29 22:38
 */
public interface DocumentApi {

    /**
     * 添加数据
     * @param o 文档数据
     * @return
     * @throws IOException
     */
    boolean save(EsDocument o) throws IOException;

    /**
     * 删除文档
     * @param o
     * @return
     * @throws IOException
     */
    boolean delete(EsDocument o) throws IOException;

    /**
     * 判断文档是否存在
     * @param o
     * @return
     */
    boolean exists(EsDocument o) throws IOException;



}
