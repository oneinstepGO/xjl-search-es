package com.pingan.xjl.es.api;

import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.EsDocument;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * 搜索接口
 * @author Aaron
 * @date 2020/5/5 18:42
 */
public interface SearchApi {

    /**
     * 分页查询
     * 聚合搜索
     * @param index 索引名称
     * @param sourceBuilder
     * @param clazz 文档对应实体的class
     * @return
     * @throws IOException
     */
    AggregationPage<EsDocument> searchForAggregationPage(String index,Class<? extends EsDocument> clazz,SearchSourceBuilder  sourceBuilder) throws IOException;



}
