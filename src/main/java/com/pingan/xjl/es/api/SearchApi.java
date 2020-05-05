package com.pingan.xjl.es.api;

import com.github.pagehelper.Page;
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
     * @param index
     * @param sourceBuilder
     * @param clazz
     * @return
     * @throws IOException
     */
    Page<EsDocument> searchForPage(String index,Class<? extends EsDocument> clazz,SearchSourceBuilder  sourceBuilder) throws IOException;

}
