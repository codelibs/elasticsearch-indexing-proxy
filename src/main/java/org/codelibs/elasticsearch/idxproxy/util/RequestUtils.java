package org.codelibs.elasticsearch.idxproxy.util;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

public class RequestUtils {

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

}
