package org.codelibs.elasticsearch.idxproxy.util;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;

public final class RequestUtils {

    public static final short TYPE_BULK = 99;

    public static final short TYPE_UPDATE_BY_QUERY = 5;

    public static final short TYPE_UPDATE = 4;

    public static final short TYPE_INDEX = 3;

    public static final short TYPE_DELETE_BY_QUERY = 2;

    public static final short TYPE_DELETE = 1;

    private RequestUtils() {
        // nothing
    }

    public static <Request extends ActionRequest> short getClassType(final Request request) {
        if (DeleteRequest.class.isInstance(request)) {
            return RequestUtils.TYPE_DELETE;
        } else if (DeleteByQueryRequest.class.isInstance(request)) {
            return RequestUtils.TYPE_DELETE_BY_QUERY;
        } else if (IndexRequest.class.isInstance(request)) {
            return RequestUtils.TYPE_INDEX;
        } else if (UpdateRequest.class.isInstance(request)) {
            return RequestUtils.TYPE_UPDATE;
        } else if (UpdateByQueryRequest.class.isInstance(request)) {
            return RequestUtils.TYPE_UPDATE_BY_QUERY;
        } else if (BulkRequest.class.isInstance(request)) {
            return RequestUtils.TYPE_BULK;
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    public static <Request extends ActionRequest> Request newRequest(final short classType) {
        switch (classType) {
        case RequestUtils.TYPE_DELETE:
            return (Request) new DeleteRequest();
        case RequestUtils.TYPE_DELETE_BY_QUERY:
            return (Request) new DeleteByQueryRequest();
        case RequestUtils.TYPE_INDEX:
            return (Request) new IndexRequest();
        case RequestUtils.TYPE_UPDATE:
            return (Request) new UpdateRequest();
        case RequestUtils.TYPE_UPDATE_BY_QUERY:
            return (Request) new UpdateByQueryRequest();
        case RequestUtils.TYPE_BULK:
            return (Request) new BulkRequest();
        default:
            throw new ElasticsearchException("Unknown request type: " + classType);
        }
    }

    public static IndexRequestBuilder createIndexRequest(final Client client, final StreamInput streamInput, final String index)
            throws IOException {
        final IndexRequestBuilder builder = client.prepareIndex();
        final IndexRequest request = builder.request();
        request.readFrom(streamInput);
        if (index != null) {
            request.index(index);
        }
        return builder;
    }

    public static UpdateByQueryRequestBuilder createUpdateByQueryRequest(final Client client, final StreamInput streamInput,
            final String index) throws IOException {
        final UpdateByQueryRequestBuilder builder = client.prepareExecute(UpdateByQueryAction.INSTANCE);
        final UpdateByQueryRequest request = builder.request();
        request.readFrom(streamInput);
        if (index != null) {
            request.indices(index);
        }
        return builder;
    }

    public static UpdateRequestBuilder createUpdateRequest(final Client client, final StreamInput streamInput, final String index)
            throws IOException {
        final UpdateRequestBuilder builder = client.prepareUpdate();
        final UpdateRequest request = builder.request();
        request.readFrom(streamInput);
        if (index != null) {
            request.index(index);
        }
        return builder;
    }

    public static DeleteByQueryRequestBuilder createDeleteByQueryRequest(final Client client, final StreamInput streamInput,
            final String index) throws IOException {
        final DeleteByQueryRequestBuilder builder = client.prepareExecute(DeleteByQueryAction.INSTANCE);
        final DeleteByQueryRequest request = builder.request();
        request.readFrom(streamInput);
        if (index != null) {
            request.indices(index);
        }
        return builder;
    }

    public static DeleteRequestBuilder createDeleteRequest(final Client client, final StreamInput streamInput, final String index)
            throws IOException {
        final DeleteRequestBuilder builder = client.prepareDelete();
        final DeleteRequest request = builder.request();
        request.readFrom(streamInput);
        if (index != null) {
            request.index(index);
        }
        return builder;
    }

    public static BulkRequestBuilder createBulkRequest(final Client client, final StreamInput streamInput, final String index)
            throws IOException {
        final BulkRequestBuilder builder = client.prepareBulk();
        final BulkRequest request = builder.request();
        request.readFrom(streamInput);
        if (index != null) {
            builder.request().requests().stream().forEach(req -> {
                if (req instanceof DeleteRequest) {
                    ((DeleteRequest) req).index(index);
                } else if (req instanceof DeleteByQueryRequest) {
                    ((DeleteByQueryRequest) req).indices(index);
                } else if (req instanceof IndexRequest) {
                    ((IndexRequest) req).index(index);
                } else if (req instanceof UpdateRequest) {
                    ((UpdateRequest) req).index(index);
                } else if (req instanceof UpdateByQueryRequest) {
                    ((UpdateByQueryRequest) req).indices(index);
                } else {
                    throw new ElasticsearchException("Unsupported request in bulk: " + req);
                }
            });
        }
        return builder;
    }
}
