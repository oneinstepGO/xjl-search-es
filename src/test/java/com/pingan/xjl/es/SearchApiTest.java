package com.pingan.xjl.es;

import com.alibaba.fastjson.JSON;
import com.pingan.xjl.es.api.SearchApi;
import com.pingan.xjl.es.constant.EsConstants;
import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.Book;
import com.pingan.xjl.es.entity.Category;
import com.pingan.xjl.es.entity.EsDocument;
import com.pingan.xjl.es.entity.Publish;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Aaron
 * @date 2020/5/5 23:21
 */
@Slf4j
public class SearchApiTest extends XjlSearchEsApplicationTests{

    @Autowired
    private SearchApi searchApi;

    /**
     * 测试分页搜索和高亮
     * @throws IOException
     */
    @Test
    public void testSearchForPage() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("title");
        highlightTitle.preTags("<span style='color:red'>");
        highlightTitle.postTags("</span>");
        highlightBuilder.field(highlightTitle);
        HighlightBuilder.Field highlightSummary = new HighlightBuilder.Field("summary");
        highlightSummary.preTags("<span style='color:red'>");
        highlightSummary.postTags("</span>");
        highlightBuilder.field(highlightSummary);
        searchSourceBuilder.highlighter(highlightBuilder);


        searchSourceBuilder.sort(SortBuilders.fieldSort("price").order(SortOrder.ASC));
        searchSourceBuilder.from(0).size(3);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("和","title","summary"))
        .highlighter(highlightBuilder);
        AggregationPage<EsDocument> esDocuments = searchApi.searchForAggregationPage(EsConstants.BOOK_INDEX, Book.class,searchSourceBuilder);
        log.info("the total is {}",JSON.toJSONString(esDocuments.getTotal()));
        log.info("the pageNum is {}",JSON.toJSONString(esDocuments.getPageNum()));
        log.info("the pageSize is {}",JSON.toJSONString(esDocuments.getPageSize()));

        log.info("the result is {}",JSON.toJSONString(esDocuments.getResults()));

        Assert.isTrue(esDocuments.getTotal() != 0
                && esDocuments.getPageNum() == 0
                && esDocuments.getPageSize() == 3);
    }

    /**
     * 测试聚合搜索
     */
    @Test
    public void testSearchForAggregation() throws IOException {

        String keyword = "指南";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                // 查询字段
                .fetchSource(new String[]{"booId","title","categoryId","publishId"},null)
                // 分页
                .from(0).size(15)
                // 排序
                .sort(SortBuilders.fieldSort("publishDate").order(SortOrder.DESC))
                // 过滤
                .query(QueryBuilders.multiMatchQuery(keyword,"title","summary","authors"));

        // 聚合
        // 分类聚合
        String categoryAggName = "categoryAgg";
        searchSourceBuilder.aggregation(AggregationBuilders.terms(categoryAggName).field("categoryId"));
        // 出版商聚合
        String publishAggName = "publishAgg";
        searchSourceBuilder.aggregation(AggregationBuilders.terms(publishAggName).field("publishId"));
        AggregationPage<EsDocument> docs = searchApi.searchForAggregationPage(EsConstants.BOOK_INDEX, Book.class, searchSourceBuilder);

        List<EsDocument> books = docs.getResults();
        // 解析聚合结果
        Map<String, Aggregation> aggregations = docs.getAggregationMap();
        //解析分类聚合
        List<EsDocument> categories = handleAgg(EsConstants.CATEGORY_INDEX, (Terms) aggregations.get(categoryAggName),Category.class);
        //解析出版商聚合
        List<EsDocument> publishes = handleAgg(EsConstants.PUBLISH_INDEX , (Terms) aggregations.get(publishAggName), Publish.class);

        log.info("the total is {}",JSON.toJSONString(docs.getTotal()));
        log.info("the pageNum is {}",JSON.toJSONString(docs.getPageNum()));
        log.info("the pageSize is {}",JSON.toJSONString(docs.getPageSize()));

        log.info("the books is {}",JSON.toJSONString(books));
        log.info("the aggs is {}",JSON.toJSONString(aggregations));
        log.info("the categories is {}",JSON.toJSONString(categories));
        log.info("the publishes is {}",JSON.toJSONString(publishes));

        Assert.isTrue(docs.getTotal() != 0
                && docs.getPageNum() == 0
                && docs.getPageSize() == 15
                && !books.isEmpty()
                && !aggregations.isEmpty()
                && !categories.isEmpty()
                && !publishes.isEmpty());


    }

    @Test
    public void testQueryByIds() throws IOException {
        List<EsDocument> esDocuments = searchApi.queryByIds(EsConstants.BOOK_INDEX, Arrays.asList("1001", "1002"), Book.class);
        Assert.isTrue(esDocuments.size() == 2);
        log.info("the result is {}",JSON.toJSONString(esDocuments));
    }

    /**
     * 处理聚合数据
     * @param terms
     * @param clazz
     * @return
     */
    private List<EsDocument> handleAgg(String index, Terms terms,Class<? extends EsDocument> clazz) {
        try {
            //获取id
            List<String> ids = terms.getBuckets()
                    .stream()
                    .map(b -> b.getKeyAsString())
                    .collect(Collectors.toList());
            //根据ID查询分类
            List<EsDocument> results = searchApi.queryByIds(index, ids, clazz);
            return results;
        } catch (Exception e) {
            log.error("查询信息失败", e);
            return null;
        }
    }



}
