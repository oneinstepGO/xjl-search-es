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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
    public boolean deleteByIds(EsDocument o, List<String> ids) throws IOException {
        return this.delete(o, QueryBuilders.idsQuery().addIds(ids.toArray(new String[0])));
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

    @Override
    public boolean bulkUpdate(List<EsDocument> docs) throws IOException {
        BulkRequest request = new BulkRequest();
        docs.forEach(doc -> {
            String jsonString = JSON.toJSONString(doc);
            request.add(new UpdateRequest(doc.getIndex(),doc.getId()).doc(jsonString,XContentType.JSON));
        });
        // 设置超时2分钟
        request.timeout(TimeValue.timeValueMinutes(2));
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        handleBulkResponse(bulkResponse);
        if (!bulkResponse.hasFailures()) {
            log.info("批量更新全部成功！！！");
            return true;
        }
        return false;
    }

    @Override
    public boolean bulkInsert(List<EsDocument> docs) throws IOException {
        BulkRequest request = new BulkRequest();
        docs.forEach(doc -> {
            checkIdAndIndex(doc);
            String jsonString = JSON.toJSONString(doc);
            request.add(new IndexRequest(doc.getIndex())
                    .id(doc.getId())
                    .source(jsonString,XContentType.JSON).opType(DocWriteRequest.OpType.CREATE));
        });
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.timeout(TimeValue.timeValueMinutes(2));
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        handleBulkResponse(bulkResponse);
        if (!bulkResponse.hasFailures()) {
            log.info("全部数据批量插入成功！！！共插入{}条数据！",JSON.toJSONString(docs.size()));
            return true;
        }
        return false;
    }

    private void handleBulkResponse(BulkResponse bulkResponse) {

        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure =
                        bulkItemResponse.getFailure();
                log.info("批量操作失败,原因是:{}",JSON.toJSONString(failure));
            }

            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    String id = indexResponse.getId();
                    String index = indexResponse.getIndex();
                    if (DocWriteResponse.Result.CREATED == indexResponse.getResult()) {
                        log.info("插入数据index:{},id:{}成功！",index,id);
                    } else {
                        log.info("插入数据index:{},id:{}失败！",index,id);
                    }
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    String id1 = updateResponse.getId();
                    String index1 = updateResponse.getIndex();
                    if (DocWriteResponse.Result.UPDATED == updateResponse.getResult()) {
                        log.info("更新数据index:{},id:{}成功！",index1,id1);
                    } else {
                        log.info("更新数据index:{},id:{}失败！",index1,id1);
                    }
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                    String id2 = deleteResponse.getId();
                    String index2 = deleteResponse.getIndex();
                    if (DocWriteResponse.Result.DELETED == deleteResponse.getResult()) {
                        log.info("删除数据index:{},id:{}成功！",index2,id2);
                    } else {
                        log.info("删除数据index:{},id:{}失败！",index2,id2);
                    }
                    break;
                default:
                    log.info("未进行任何操作！");
                    break;
            }
        }
    }
}
