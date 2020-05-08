package com.pingan.xjl.es.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.search.aggregations.Aggregation;

import java.util.List;
import java.util.Map;

/**
 * @author Aaron
 * @date 2020/5/9 1:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AggregationPage<T> {

    private int pageNum;

    private int pageSize;

    private Long total;

    private Map<String, Aggregation> aggregationMap;

    private List<T> results;
}
