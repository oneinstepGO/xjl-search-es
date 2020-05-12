package com.pingan.xjl.es.api;

import com.pingan.xjl.es.entity.EsDocument;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;

/**
 * 文档 api
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
     * @param queryBuilder
     * @return
     * @throws IOException
     */
    boolean delete(EsDocument o, QueryBuilder queryBuilder) throws IOException;

    /**
     * 通过ids 批量删除
     * @param o
     * @param ids
     * @return
     * @throws IOException
     */
    boolean deleteByIds(EsDocument o, List<String> ids) throws IOException;

    /**
     * 判断文档是否存在
     * @param o
     * @return
     * @throws IOException
     */
    boolean exists(EsDocument o) throws IOException;

    /**
     * 通过id更新 或者插入新文档
     * 修改单个
     *
     * @param o
     * @return
     * @throws IOException
     */
    boolean upsert(EsDocument o) throws IOException;


    /**
     * 批量修改操作
     * @param docs
     * @return
     * @throws IOException
     */
    boolean bulkUpdate(List<? extends EsDocument> docs) throws IOException;

    /**
     * 批量添加
     * @param docs
     * @return
     * @throws IOException
     */
    boolean bulkInsert(List<? extends EsDocument> docs) throws IOException;


}
