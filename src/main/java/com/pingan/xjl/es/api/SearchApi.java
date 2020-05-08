package com.pingan.xjl.es.api;

import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.EsDocument;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

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


    /**
     * 根据ids 批量搜索
     * @param ids
     * @return
     */
    List<EsDocument> queryByIds(String index,List<String> ids,Class<? extends EsDocument> clazz) throws IOException;
}
