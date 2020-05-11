package com.pingan.xjl.es;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pingan.xjl.es.api.MultiSearchApi;
import com.pingan.xjl.es.constant.EsConstants;
import com.pingan.xjl.es.dto.AggregationPage;
import com.pingan.xjl.es.entity.Book;
import com.pingan.xjl.es.entity.EsDocument;
import com.pingan.xjl.es.entity.Publish;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.pingan.xjl.es.constant.EsConstants.CLAZZ;

/**
 * @author Aaron
 * @date 2020/5/11 22:00
 */
@Slf4j
public class MultiSearchApiTest extends XjlSearchEsApplicationTests{

    @Autowired
    private MultiSearchApi multiSearchApi;

    @Test
    public void testMultiSearch() throws IOException {

        List<Map<String, Object>> params = Lists.newArrayList();

        Map<String,Object> param1 = Maps.newHashMap();

        String keyword = "指南";

        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1
                // 查询字段
                .fetchSource(new String[]{"booId","title","categoryId","publishId"},null)
                // 分页
                .from(0).size(15)
                // 排序
                .sort(SortBuilders.fieldSort("publishDate").order(SortOrder.DESC))
                // 过滤
                .query(QueryBuilders.multiMatchQuery(keyword,"title","summary","authors"));

        param1.put(EsConstants.INDEX,EsConstants.BOOK_INDEX);
        param1.put(CLAZZ, Book.class);
        param1.put(EsConstants.SEARCH_BUILDER,searchSourceBuilder1);
        params.add(param1);

        Map<String,Object> param2 = Maps.newHashMap();

        String[] searchWords = new String[]{"中信"};
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2
                // 分页
                .from(0).size(15)
                // 过滤
                .query(QueryBuilders.termsQuery("publisher",searchWords));

        param2.put(EsConstants.INDEX,EsConstants.PUBLISH_INDEX);
        param2.put(CLAZZ, Publish.class);
        param2.put(EsConstants.SEARCH_BUILDER,searchSourceBuilder2);
        params.add(param2);

        List<AggregationPage<EsDocument>> pages = multiSearchApi.multiSearch(params);
        pages.forEach(page -> {
            log.info("the result is : {}", JSON.toJSONString(page.getResults()));
            log.info("the total is : {}", JSON.toJSONString(page.getTotal()));
            log.info("---------------------------------------------------");
        });

        Assert.isTrue(pages.size() == 2);
        Assert.isTrue(!pages.get(0).getResults().isEmpty());
        Assert.isTrue(!pages.get(1).getResults().isEmpty());
    }

}
