package com.pingan.xjl.es;

import com.alibaba.fastjson.JSON;
import com.pingan.xjl.es.api.SearchApi;
import com.pingan.xjl.es.constant.EsConstants;
import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.Book;
import com.pingan.xjl.es.entity.EsDocument;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.IOException;

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

        String keyword = "技术";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                // 查询字段
                .fetchSource("booId,title,categoryId,publishId",null)
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
        searchApi.searchForAggregationPage(EsConstants.BOOK_INDEX, Book.class,searchSourceBuilder);
    }

}
