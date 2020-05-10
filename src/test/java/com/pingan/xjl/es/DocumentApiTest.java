package com.pingan.xjl.es;

import com.pingan.xjl.es.api.DocumentApi;
import com.pingan.xjl.es.entity.Book;
import com.pingan.xjl.es.entity.Category;
import com.pingan.xjl.es.entity.EsDocument;
import com.pingan.xjl.es.entity.Publish;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 测试文档 api
 * @author Aaron
 * @date 2020/5/3 2:28
 */
public class DocumentApiTest extends XjlSearchEsApplicationTests{

    @Autowired
    private DocumentApi documentApi;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.CHINA);

    @Test
    public void testSave() throws IOException, ParseException {
        Book book = Book.builder()
                .bookId("1001")
                .title("Elasticsearch: 权威指南")
                .summary("一个分布式实时搜索和分析引擎")
                .authors(Arrays.asList("克林顿·戈姆利", "扎卡里·童"))
                .numReviews(20)
                .price(24.99)
                .categoryId("2001")
                .publishId("3001")
                .publishDate(new Date(sdf.parse("2015-02-07").getTime()))
                .build();
        Assert.isTrue(documentApi.save(book));
    }

    @Test
    public void testSaveCateory() throws IOException{
        Category category = Category.builder()
                .categoryId("2001")
                .categoryName("技术书籍")
                .build();
        Assert.isTrue(documentApi.save(category));

        Category category1 = Category.builder()
                .categoryId("2002")
                .categoryName("小说文学")
                .build();
        Assert.isTrue(documentApi.save(category1));

        Category category2 = Category.builder()
                .categoryId("2003")
                .categoryName("旅游指南")
                .build();
        Assert.isTrue(documentApi.save(category2));
    }


    @Test
    public void testSavePublish() throws IOException {
        Publish publish = Publish.builder()
                .publishId("3001")
                .publisher("中信出版社")
                .build();
        Assert.isTrue(documentApi.save(publish));

        Publish publish1 = Publish.builder()
                .publishId("3002")
                .publisher("机械工程出版社")
                .build();
        Assert.isTrue(documentApi.save(publish1));
    }

    /**
     * 测试修改
     * @throws IOException
     */
    @Test
    public void testUpsert() throws IOException {
        Assert.isTrue(documentApi.upsert(Book.builder().bookId("1001").price(26.90).build()));
    }

    /**
     * 测试使用 upsert 新增
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testUpsert2() throws IOException, ParseException {
        Assert.isTrue(documentApi.upsert(Book.builder()
                .bookId("1002")
                .title("标题文字:如何查询，组织和操作")
                .authors(Arrays.asList("grant ingersoll", "thomas morton", "drew farris"))
                .price(15.23)
                .summary("使用诸如全文搜索,专有名称识别,聚合,标记,信息提取等方法来组织文本和摘要")
                .publishId("3002")
                .categoryId("2001")
                .numReviews(12)
                .publishDate(new Date(sdf.parse("2013-01-24").getTime()))
                .build()));
    }

    /**
     * 测试 deleteByQuery
     * @throws IOException
     */
    @Test
    public void testDeleteByQuery() throws IOException {
        Assert.isTrue(documentApi.delete(Book.builder().build(),QueryBuilders.termQuery("title","指南")));
    }

    /**
     * 测试通过ids 删除
     * @throws IOException
     */
    @Test
    public void testDeleteByIds() throws IOException {
        Assert.isTrue(documentApi.deleteByIds(Book.builder().build(), Arrays.asList("1001","1002","1003","1004")));
    }

    /**
     * 测试批量更新
     * @throws IOException
     */
    @Test
    public void testBulkUpdate() throws IOException {
        Book book1 = Book.builder().bookId("1001").price(8.88).build();
        Book book2 = Book.builder().bookId("1002").price(9.99).build();
        List<EsDocument> bookList = Arrays.asList(book1,book2);
        documentApi.bulkUpdate(bookList);
    }

    /**
     * 测试批量插入
     */
    @Test
    public void testBulkInsert() throws IOException, ParseException {
        Book book = Book.builder()
                .bookId("1001")
                .title("Elasticsearch: 权威指南")
                .summary("一个分布式实时搜索和分析引擎")
                .authors(Arrays.asList("克林顿·戈姆利", "扎卡里·童"))
                .numReviews(20)
                .price(24.99)
                .categoryId("2001")
                .publishId("3001")
                .publishDate(new Date(sdf.parse("2015-02-07").getTime()))
                .build();

        Book book1 = Book.builder()
                .bookId("1002")
                .title("标题文字:如何查询，组织和操作")
                .summary("使用诸如全文搜索,专有名称识别,聚合,标记,信息提取等方法来组织文本和摘要")
                .authors(Arrays.asList("grant ingersoll", "thomas morton", "drew farris"))
                .numReviews(12)
                .price(15.23)
                .categoryId("2001")
                .publishId("3002")
                .publishDate(new Date(sdf.parse("2013-01-24").getTime()))
                .build();

        Book book2 = Book.builder()
                .bookId("1003")
                .title("Elasticsearch 实战")
                .summary("使用Elasticsearch构建可扩展的搜索应用,而无需执行复杂的低级编程或了解高级数据科学算法")
                .authors(Arrays.asList("radu gheorge", "matthew lee hinman", "roy russo"))
                .numReviews(18)
                .price(55.98)
                .categoryId("2001")
                .publishId("3002")
                .publishDate(new Date(sdf.parse("2015-12-03").getTime()))
                .build();

        Book book3 = Book.builder()
                .bookId("1004")
                .title("Solr in Action")
                .summary("使用 Apache Solr实现可扩展的搜索引擎的综合指南")
                .authors(Arrays.asList("trey grainger", "timothy potter"))
                .numReviews(23)
                .price(7.66)
                .categoryId("2002")
                .publishId("3001")
                .publishDate(new Date(sdf.parse("2014-04-05").getTime()))
                .build();

        List<EsDocument> bookList = Arrays.asList(book,book1,book2,book3);
        Assert.isTrue(documentApi.bulkInsert(bookList));
    }




}
