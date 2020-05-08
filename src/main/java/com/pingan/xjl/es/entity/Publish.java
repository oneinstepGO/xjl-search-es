package com.pingan.xjl.es.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 出版商
 * @author Aaron
 * @date 2020/5/9 1:09
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Publish {

    /**
     * 出版商id
     */
    private Long publishId;

    /**
     * 出版商名称
     */
    private String publisher;

}
