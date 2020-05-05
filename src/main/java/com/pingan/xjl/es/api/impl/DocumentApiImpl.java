package com.pingan.xjl.es.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.PropertyFilter;
import com.pingan.xjl.es.api.DocumentApi;
import com.pingan.xjl.es.constant.EsConstants;
import com.pingan.xjl.es.entity.EsDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.Asserts;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

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
    public boolean save(EsDocument o) throws IOException {
        checkIdAndIndex(o);

        if (exists(o)) {
            log.error("文档已存在，请调用修改接口！index:{},id:{}",o.getIndex(),o.getId());
            return false;
        }

        IndexRequest request = new IndexRequest(o.getIndex());
        request.id(o.getId());


        String jsonString = checkAndParse(o);



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
    public boolean delete(EsDocument o, QueryBuilder queryBuilder) throws IOException {
        Asserts.notEmpty(o.getIndex(),"index 不能为空");
        DeleteByQueryRequest request =
                new DeleteByQueryRequest(o.getIndex());
        request.setConflicts("proceed");
        request.setQuery(queryBuilder);
        request.setRefresh(true);
        BulkByScrollResponse bulkResponse =
                client.deleteByQuery(request, RequestOptions.DEFAULT);

        TimeValue timeTaken = bulkResponse.getTook();
        boolean timedOut = bulkResponse.isTimedOut();
        long totalDocs = bulkResponse.getTotal();
        long deletedDocs = bulkResponse.getDeleted();
        long batches = bulkResponse.getBatches();
        long noops = bulkResponse.getNoops();
        long versionConflicts = bulkResponse.getVersionConflicts();
        long bulkRetries = bulkResponse.getBulkRetries();
        long searchRetries = bulkResponse.getSearchRetries();
        TimeValue throttledMillis = bulkResponse.getStatus().getThrottled();
        TimeValue throttledUntilMillis =
                bulkResponse.getStatus().getThrottledUntil();
        List<ScrollableHitSource.SearchFailure> searchFailures =
                bulkResponse.getSearchFailures();
        List<BulkItemResponse.Failure> bulkFailures =
                bulkResponse.getBulkFailures();

        if (bulkFailures.isEmpty()) {
            log.info("删除文档成功！ 共删除{}条文档。" ,deletedDocs);
            return true;
        }
        return false;
    }

    @Override
    public boolean exists(EsDocument o) throws IOException {
        checkIdAndIndex(o);
        GetRequest getRequest = new GetRequest(
                o.getIndex(),
                o.getId());
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        return exists;
    }

    @Override
    public boolean upsert(EsDocument o) throws IOException {
        checkIdAndIndex(o);
        UpdateRequest request = new UpdateRequest(
                o.getIndex(),
                o.getId());
        String jsonString = checkAndParse(o);
        request.doc(jsonString, XContentType.JSON);
        request.detectNoop(false);
        request.docAsUpsert(true);
        UpdateResponse updateResponse = client.update(
                request, RequestOptions.DEFAULT);
        String index = updateResponse.getIndex();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("修改文档成功！新数据：{}", JSON.toJSONString(o));
            return true;
        }
        else if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("新增文档成功！新数据：{}", JSON.toJSONString(o));
            return true;
        }
        else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            log.error("要更新的文档不存在！index:{},id:{}",o.getIndex(),o.getId());
        }
        return false;
    }


    private String checkAndParse(EsDocument esDocument) {
        PropertyFilter filter = new PropertyFilter() {
            @Override
            public boolean apply(Object o, String name, Object value) {
                if (EsConstants.ID.equalsIgnoreCase(name) || EsConstants.INDEX.equalsIgnoreCase(name)) {
                    return false;
                }
                return true;
            }
        };
        return JSON.toJSONString(esDocument,filter);
    }

    private void checkIdAndIndex(EsDocument o) {
        Asserts.notEmpty(o.getId(),"id 不能为空");
        Asserts.notEmpty(o.getIndex(),"index 不能为空");
    }
}
