package org.codelibs.elasticsearch.idxproxy.action;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;

public class ProxyActionFilter extends AbstractComponent implements ActionFilter {

    private static final String INDEX_UUID = "idxproxy";

    private IndexingProxyService indexingProxyService;

    @Inject
    public ProxyActionFilter(final Settings settings) {
        super(settings);
    }

    @Override
    public int order() {
        return 1;
    }

    @SuppressWarnings("unchecked")
    private <Request extends ActionRequest, Response extends ActionResponse> Supplier<Response> getExecutor(final Task task,
            final String action, final Request request) {
        if (BulkAction.NAME.equals(action)) {
            final long startTime = System.nanoTime();
            int count = 0;
            final BulkRequest req = (BulkRequest) request;
            for (final DocWriteRequest<?> subReq : req.requests()) {
                if (indexingProxyService.isTargetIndex(subReq.index())) {
                    count++;
                }
            }
            if (count == 0) {
                return null;
            } else if (count != req.requests().size()) {
                throw new ElasticsearchException("Mixed target requests. ({} != {})", count, req.requests().size());
            }
            return () -> {
                final List<BulkItemResponse> responseList = new ArrayList<>(req.requests().size());
                for (int i = 0; i < req.requests().size(); i++) {
                    final DocWriteRequest<?> dwr = req.requests().get(i);
                    if (dwr instanceof IndexRequest) {
                        final IndexRequest r = (IndexRequest) dwr;
                        final String id = r.id() == null ? INDEX_UUID : r.id();
                        final IndexResponse response =
                                new IndexResponse(new ShardId(new Index(r.index(), INDEX_UUID), 0), r.type(), id, r.version(), true);
                        responseList.add(new BulkItemResponse(i, r.opType(), response));
                    } else if (dwr instanceof UpdateRequest) {
                        final UpdateRequest r = (UpdateRequest) dwr;
                        final String id = r.id() == null ? INDEX_UUID : r.id();
                        final UpdateResponse response = new UpdateResponse(new ShardId(new Index(r.index(), INDEX_UUID), 0), r.type(), id,
                                r.version(), Result.CREATED);
                        responseList.add(new BulkItemResponse(i, r.opType(), response));
                    } else if (dwr instanceof DeleteRequest) {
                        final DeleteRequest r = (DeleteRequest) dwr;
                        final String id = r.id() == null ? INDEX_UUID : r.id();
                        final DeleteResponse response =
                                new DeleteResponse(new ShardId(new Index(r.index(), INDEX_UUID), 0), r.type(), id, r.version(), true);
                        response.setShardInfo(new ReplicationResponse.ShardInfo(1, 1, ReplicationResponse.EMPTY));
                        responseList.add(new BulkItemResponse(i, r.opType(), response));
                    } else {
                        responseList.add(new BulkItemResponse(i, dwr.opType(), new BulkItemResponse.Failure(dwr.index(), dwr.type(),
                                dwr.id(), new ElasticsearchException("Unknown request: " + dwr))));
                    }
                }
                return (Response) new BulkResponse(responseList.toArray(new BulkItemResponse[responseList.size()]),
                        (System.nanoTime() - startTime) / 1000000);
            };
        } else if (DeleteAction.NAME.equals(action)) {
            final DeleteRequest req = (DeleteRequest) request;
            if (!indexingProxyService.isTargetIndex(req.index())) {
                return null;
            }
            return () -> {
                final String id = req.id() == null ? INDEX_UUID : req.id();
                final DeleteResponse res =
                        new DeleteResponse(new ShardId(new Index(req.index(), INDEX_UUID), 0), req.type(), id, req.version(), true);
                res.setShardInfo(new ReplicationResponse.ShardInfo(1, 1, ReplicationResponse.EMPTY));
                return (Response) res;
            };
        } else if (DeleteByQueryAction.NAME.equals(action)) {
            final long startTime = System.nanoTime();
            int count = 0;
            final DeleteByQueryRequest req = (DeleteByQueryRequest) request;
            for (final String index : req.indices()) {
                if (indexingProxyService.isTargetIndex(index)) {
                    count++;
                }
            }
            if (count == 0) {
                return null;
            } else if (count != req.indices().length) {
                throw new ElasticsearchException("Mixed target requests. ({} != {})", count, req.indices().length);
            }
            return () -> {
                return (Response) new BulkByScrollResponse(TimeValue.timeValueNanos(System.nanoTime() - startTime),
                        new BulkByScrollTask.Status(null, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimeValue.ZERO, 0, null, TimeValue.ZERO),
                        Collections.emptyList(), Collections.emptyList(), false);
            };
        } else if (IndexAction.NAME.equals(action)) {
            final IndexRequest req = (IndexRequest) request;
            if (!indexingProxyService.isTargetIndex(req.index())) {
                return null;
            }
            return () -> {
                final String id = req.id() == null ? INDEX_UUID : req.id();
                return (Response) new IndexResponse(new ShardId(new Index(req.index(), INDEX_UUID), 0), req.type(), id, req.version(),
                        true);
            };
        } else if (UpdateAction.NAME.equals(action)) {
            final UpdateRequest req = (UpdateRequest) request;
            if (!indexingProxyService.isTargetIndex(req.index())) {
                return null;
            }
            return () -> {
                final String id = req.id() == null ? INDEX_UUID : req.id();
                return (Response) new UpdateResponse(new ShardId(new Index(req.index(), INDEX_UUID), 0), req.type(), id, req.version(),
                        Result.CREATED);
            };
        } else if (UpdateByQueryAction.NAME.equals(action)) {
            final long startTime = System.nanoTime();
            int count = 0;
            final UpdateByQueryRequest req = (UpdateByQueryRequest) request;
            for (final String index : req.indices()) {
                if (indexingProxyService.isTargetIndex(index)) {
                    count++;
                }
            }
            if (count == 0) {
                return null;
            } else if (count != req.indices().length) {
                throw new ElasticsearchException("Mixed target requests. ({} != {})", count, req.indices().length);
            }
            return () -> {
                return (Response) new BulkByScrollResponse(TimeValue.timeValueNanos(System.nanoTime() - startTime),
                        new BulkByScrollTask.Status(null, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimeValue.ZERO, 0, null, TimeValue.ZERO),
                        Collections.emptyList(), Collections.emptyList(), false);
            };
        }
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(final Task task, final String action,
            final Request request, final ActionListener<Response> listener, final ActionFilterChain<Request, Response> chain) {
        if (logger.isDebugEnabled()) {
            logger.debug("node: " + nodeName() + ", action: " + action + ", request: " + request);
        }
        final Supplier<Response> executor = getExecutor(task, action, request);
        if (executor != null) {
            indexingProxyService.write(request, ActionListener.wrap(res -> {
                listener.onResponse(executor.get());
            }, listener::onFailure));
        } else if (FlushAction.NAME.equals(action)//
                || ForceMergeAction.NAME.equals(action)//
                || RefreshAction.NAME.equals(action)//
                || UpgradeAction.NAME.equals(action)//
        ) {
            indexingProxyService.renew(ActionListener.wrap(res -> {
                chain.proceed(task, action, request, listener);
            }, listener::onFailure));
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    public void setIndexingProxyService(final IndexingProxyService indexingProxyService) {
        this.indexingProxyService = indexingProxyService;
    }
}
