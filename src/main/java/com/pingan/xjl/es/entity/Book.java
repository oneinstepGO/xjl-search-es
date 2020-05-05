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

    private String bookId;

    private String title;

    private List<String> authors;

    private String summary;

    private Double price;

    private Date publishDate;

    private Integer numReviews;

    private String publisher;

    @Override
    public String getId() {
        return this.bookId;
    }

    @Override
    public String getIndex() {
        return BOOK_INDEX;
    }

}
