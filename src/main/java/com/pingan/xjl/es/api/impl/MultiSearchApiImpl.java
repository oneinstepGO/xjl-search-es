package com.pingan.xjl.es.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.pingan.xjl.es.api.MultiSearchApi;
import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.EsDocument;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.pingan.xjl.es.constant.EsConstants.*;

/**
 * @author Aaron
 * @date 2020/5/10 22:16
 */
@Service
@Slf4j
public class MultiSearchApiImpl implements MultiSearchApi {

    @Resource
    private RestHighLevelClient client;

    @Override
    public List<AggregationPage<EsDocument>> multiSearch(List<Map<String, Object>> params) throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        List<Class<? extends EsDocument>> classList = Lists.newArrayList();
        List<AggregationPage<EsDocument>> multiResults = Lists.newArrayList();
        params.forEach(map -> {
            AggregationPage<EsDocument> page = new AggregationPage<>();
            String index = (String) map.get(INDEX);
            Class<? extends EsDocument> clazz = (Class<? extends EsDocument>) map.get(CLAZZ);
            classList.add(clazz);
            SearchSourceBuilder searchSourceBuilder = (SearchSourceBuilder) map.get(SEARCH_BUILDER);
            page.setPageNum(searchSourceBuilder.from()+1);
            page.setPageSize(searchSourceBuilder.size());
            multiResults.add(page);
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(searchSourceBuilder);
            // 添加搜索请求
            request.add(searchRequest);
        });

        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);

        MultiSearchResponse.Item[] responses = response.getResponses();

        for (int i = 0; i < classList.size(); i++) {
            MultiSearchResponse.Item item = responses[i];
            AggregationPage<EsDocument> page = multiResults.get(i);
            Class<? extends EsDocument> aClass = classList.get(i);
            SearchResponse searchResponse = item.getResponse();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                EsDocument esDocument = JSONObject.parseObject(JSON.toJSONString(sourceAsMap),aClass);
                page.getResults().add(esDocument);
            }
            page.setTotal(hits.getTotalHits().value);
        }

        return multiResults;
    }

}
