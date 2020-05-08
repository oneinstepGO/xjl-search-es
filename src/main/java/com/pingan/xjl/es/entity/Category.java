package com.pingan.xjl.es.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Aaron
 * @date 2020/5/9 1:06
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Category {

    /**
     * 种类id
     */
    private Long categoryId;

    /**
     * 种类名称
     */
    private String categoryName;

}
