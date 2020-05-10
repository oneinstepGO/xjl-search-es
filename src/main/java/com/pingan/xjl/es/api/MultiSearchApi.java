package com.pingan.xjl.es.api;

import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.EsDocument;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *  批量搜索api
 * @author Aaron
 * @date 2020/5/10 22:12
 */
public interface MultiSearchApi {


    /**
     * 批量搜索
     * @param params
     * @return
     * @throws IOException
     */
    List<AggregationPage<EsDocument>> multiSearch(List<Map<String,Object>> params) throws IOException;
}
