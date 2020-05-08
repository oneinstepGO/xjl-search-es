package com.pingan.xjl.es.entity;

import com.pingan.xjl.es.constant.EsConstants;
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
public class Publish implements EsDocument {

    /**
     * 出版商id
     */
    private String publishId;

    /**
     * 出版商名称
     */
    private String publisher;

    @Override
    public String getId() {
        return publishId;
    }

    @Override
    public String getIndex() {
        return EsConstants.PUBLISH_INDEX;
    }
}
