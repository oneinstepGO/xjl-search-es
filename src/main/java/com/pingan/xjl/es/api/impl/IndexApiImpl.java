package com.pingan.xjl.es.api.impl;

import com.pingan.xjl.es.api.IndexApi;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * 索引操作
 * @author Aaron
 * @date 2020/5/3 0:10
 */
@Service
@Slf4j
public class IndexApiImpl implements IndexApi {

    @Resource
    private RestHighLevelClient client;


    @Override
    public boolean create(String name, int shards, int replicas, String mappingJsonStr) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(name);
        request.settings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
        );
        request.mapping(mappingJsonStr, XContentType.JSON);
        CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = indexResponse.isAcknowledged();
        boolean shardsAcknowledged = indexResponse.isShardsAcknowledged();
        log.info("创建索引：{}>>>>>>>>>>>>>acknowledged:{},shardsAcknowledged:{}",acknowledged&&shardsAcknowledged ? "成功":"失败",acknowledged,shardsAcknowledged);
        return acknowledged;
    }

    @Override
    public boolean delete(String name) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(name);
            client.indices().delete(request, RequestOptions.DEFAULT);
            log.info("删除索引{}成功。",name);
            return true;
        } catch (Exception e) {
            log.error("删除索引失败，索引:{}不存在或者其他原因。",name);
            log.error(e.getMessage());
            return false;
        }
    }
}
