package com.pingan.xjl.es.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

import static com.pingan.xjl.es.constant.EsConstants.BOOK_INDEX;

/**
 * @author Aaron
 * @date 2020/4/29 22:48
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Book implements EsDocument {

    /**
     * id
     */
    private String bookId;

    /**
     * 标题
     */
    private String title;

    /**
     * 作者
     */
    private List<String> authors;

    /**
     * 简介
     */
    private String summary;

    /**
     * 价格
     */
    private Double price;

    /**
     * 书本种类id 用于聚合
     */
    private String categoryId;

    /**
     * 出版日期
     */
    private Date publishDate;

    /**
     * 阅读量
     */
    private Integer numReviews;

    /**
     * 出版社id  用于聚合
     */
    private String publishId;


    @Override
    public String getId() {
        return bookId;
    }

    @Override
    public String getIndex() {
        return BOOK_INDEX;
    }

}
