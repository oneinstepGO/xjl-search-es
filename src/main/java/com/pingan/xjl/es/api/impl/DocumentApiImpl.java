package com.pingan.xjl.es.api.impl;

import com.alibaba.fastjson.JSON;
import com.pingan.xjl.es.api.DocumentApi;
import com.pingan.xjl.es.entity.EsDocument;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @author Aaron
 * @date 2020/4/29 22:39
 */
@Service
@Slf4j
public class DocumentApiImpl implements DocumentApi {

    @Resource
    private RestHighLevelClient client;

    @Override
    public boolean saveOrUpdate(EsDocument o) throws IOException {
        IndexRequest request = new IndexRequest(o.getIndex());
        request.id(o.getId());
        String jsonString = JSON.toJSONString(o);
        request.source(jsonString, XContentType.JSON);
        request.opType(DocWriteRequest.OpType.CREATE);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        String index = indexResponse.getIndex();
        String id = indexResponse.getId();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("创建文档成功！数据：{}",jsonString);
            return true;
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("更新文档成功！新数据：{}",jsonString);
            return true;
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            log.error("部分分片未新增成功！");
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
                log.error("分片新增或者更新文档失败原因是：{}",reason);
            }
        }
        return false;
    }

    @Override
    public boolean delete(EsDocument o) throws IOException {
        DeleteRequest request = new DeleteRequest(
                o.getIndex(),
                o.getId());
        DeleteResponse deleteResponse = client.delete(
                request, RequestOptions.DEFAULT);

        String index = deleteResponse.getIndex();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            log.error("删除索引{}中的id为{}的文档失败！文档不存在",o.getIndex(),o.getId());
            return false;
        }
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            log.error("删除索引{}中的id为{}的文档失败！",o.getIndex(),o.getId());
            return false;
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
                log.error("删除分片文档失败！原因为：{}",reason);
            }
            return false;
        }
        return true;
    }
}
