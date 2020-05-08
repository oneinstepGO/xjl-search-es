package com.pingan.xjl.es.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingan.xjl.es.api.SearchApi;
import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.EsDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.Asserts;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

/**
 * @author Aaron
 * @date 2020/5/5 20:17
 */
@Service
@Slf4j
public class SearchApiImpl implements SearchApi {

    @Resource
    private RestHighLevelClient client;

    @Override
    public AggregationPage<EsDocument> searchForAggregationPage(String index,Class<? extends EsDocument> clazz,SearchSourceBuilder  sourceBuilder) throws IOException {
        Asserts.check(sourceBuilder.from() != -1,"分页参数【pageNo】未指定！");
        Asserts.check(sourceBuilder.size() != -1,"分页参数【pageSize】未指定！");
        AggregationPage<EsDocument> pages = new AggregationPage<>();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            // 处理高亮
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            for (Map.Entry<String, HighlightField> entry: highlightFields.entrySet()) {
                if (sourceAsMap.containsKey(entry.getKey())) {
                    HighlightField highlight = highlightFields.get(entry.getKey());
                    Text[] fragments = highlight.fragments();
                    String fragmentString = fragments[0].string();
                    sourceAsMap.put(entry.getKey(),fragmentString);
                }
            }

            EsDocument esDocument = JSONObject.parseObject(JSON.toJSONString(sourceAsMap), clazz);
            pages.getResults().add(esDocument);
        }
        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
        pages.setAggregationMap(aggregationMap);

        pages.setTotal(hits.getTotalHits().value);
        pages.setPageNum(sourceBuilder.from());
        pages.setPageSize(sourceBuilder.size());
        log.info("查询出文档为 分页对象为 pages: {} ",JSON.toJSONString(pages));
        return pages;
    }


}
