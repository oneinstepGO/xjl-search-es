package com.pingan.xjl.es.entity;

import com.pingan.xjl.es.constant.EsConstants;
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
public class Category implements EsDocument{

    /**
     * 种类id
     */
    private String categoryId;

    /**
     * 种类名称
     */
    private String categoryName;

    @Override
    public String getId() {
        return categoryId;
    }

    @Override
    public String getIndex() {
        return EsConstants.CATEGORY_INDEX;
    }
}
