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
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
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
        highlightBuilder.requireFieldMatch(false);
        searchSourceBuilder.highlighter(highlightBuilder);


        searchSourceBuilder.sort(SortBuilders.fieldSort("price").order(SortOrder.ASC));
        searchSourceBuilder.from(0).size(3);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("指南","title","summary"))
        .highlighter(highlightBuilder);
        AggregationPage<EsDocument> esDocuments = searchApi.searchForAggregationPage(EsConstants.BOOK_INDEX, Book.class,searchSourceBuilder);
        log.info("the total is {}",JSON.toJSONString(esDocuments.getTotal()));
        log.info("the pageNum is {}",JSON.toJSONString(esDocuments.getPageNum()));
        log.info("the pageSize is {}",JSON.toJSONString(esDocuments.getPageSize()));

        log.info("the result is {}",JSON.toJSONString(esDocuments.getResults()));

        Assert.isTrue(esDocuments.getTotal() != 0
                && esDocuments.getPageNum() == 1
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
                && docs.getPageNum() == 1
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

    /**
     *
     */
    @Test
    public void testSuggestion() throws IOException {
        String keyword = "指南";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Suggestion builders need to be added to the top level SuggestBuilder,
        // which itself can be set on the SearchSourceBuilder.
        SuggestionBuilder termSuggestionBuilder =
                SuggestBuilders.phraseSuggestion("title").text("指南");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_title", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);

        searchSourceBuilder
                // 分页
                .from(0).size(15)
                // 排序
                .sort(SortBuilders.fieldSort("publishDate").order(SortOrder.DESC))
                // 过滤
                .query(QueryBuilders.matchPhraseQuery("title",keyword));
        AggregationPage<EsDocument> page = searchApi.searchForAggregationPage(EsConstants.BOOK_INDEX, Book.class, searchSourceBuilder);

    }

    /**
     * 测试各种 query
     */
    @Test
    public void testQuery() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //term查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "指南");

        //range查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("price").gte(20.50).lt(50.80);

        //prefix 查询
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("summary", "使");

        //wildcard 查询
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("summary", "*法");

        //ids 查询
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds("1001","1002","1003");

        //fuzzy查询
        FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery("summary", "Elasticseerch");

        //boolean查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("summary","扩展"));

        //多字段查询
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("指南", "title", "summary");

        //queryString
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("指南")
                .analyzer("ik_max_word")
                .field("title").field("summary");


        // 测试时替换这行
        searchSourceBuilder.query(multiMatchQueryBuilder)
        .from(0).size(30);
        searchApi.searchForAggregationPage(EsConstants.BOOK_INDEX,Book.class,searchSourceBuilder);



    }








}
