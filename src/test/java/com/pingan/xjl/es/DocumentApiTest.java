package com.pingan.xjl.es;

import com.pingan.xjl.es.api.DocumentApi;
import com.pingan.xjl.es.entity.Book;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

/**
 * @author Aaron
 * @date 2020/5/3 2:28
 */
public class DocumentApiTest extends XjlSearchEsApplicationTests{

    @Autowired
    private DocumentApi documentApi;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.CHINA);

    @Test
    public void testSaveOrUpdate() throws IOException, ParseException {
        Book book = Book.builder()
                .bookId("1001")
                .title("Elasticsearch: 权威指南")
                .summary("一个分布式实时搜索和分析引擎")
                .authors(Arrays.asList("克林顿·戈姆利", "扎卡里·童"))
                .numReviews(20)
                .price(25.99)
                .publisher("中信出版社")
                .publishDate(new Date(sdf.parse("2015-02-07").getTime()))
                .build();
        Assert.isTrue(documentApi.saveOrUpdate(book));
    }

    @Test
    public void testDelete() throws IOException {
        Assert.isTrue(documentApi.delete(Book.builder().bookId("1001").build()));
    }
}
