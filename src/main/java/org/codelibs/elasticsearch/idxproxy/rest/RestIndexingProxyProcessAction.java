package org.codelibs.elasticsearch.idxproxy.rest;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.Map;

import org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin.PluginComponent;
import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class RestIndexingProxyProcessAction extends BaseRestHandler {

    private final IndexingProxyService indexingProxyService;

    public RestIndexingProxyProcessAction(final Settings settings, final RestController controller, final PluginComponent pluginComponent) {
        super(settings);
        indexingProxyService = pluginComponent.getIndexingProxyService();

        controller.registerHandler(RestRequest.Method.GET, "/_idxproxy/process", this);
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_idxproxy/process", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_idxproxy/process", this);
        controller.registerHandler(RestRequest.Method.DELETE, "/{index}/_idxproxy/process", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        switch (request.method()) {
        case GET:
            return prepareGetRequest(request);
        case POST:
            return preparePostRequest(request);
        case DELETE:
            return prepareDeleteRequest(request);
        default:
            break;
        }

        return channel -> sendErrorResponse(channel, new ElasticsearchException("Unknown request: " + request));
    }

    private RestChannelConsumer prepareGetRequest(final RestRequest request) {
        final String index = request.param("index");
        final int from = request.paramAsInt("from", 0);
        final int size = request.paramAsInt("size", 10);
        final boolean pretty = request.paramAsBoolean("pretty", false);
        return channel -> {
            if (index == null || index.trim().length() == 0) {
                indexingProxyService.getIndexerInfos(from, size,
                        wrap(res -> sendResponse(channel, res, pretty), e -> sendErrorResponse(channel, e)));
            } else {
                indexingProxyService.getIndexerInfo(index,
                        wrap(res -> sendResponse(channel, res, pretty), e -> sendErrorResponse(channel, e)));
            }
        };
    }

    private RestChannelConsumer prepareDeleteRequest(final RestRequest request) {
        final String index = request.param("index");
        final boolean pretty = request.paramAsBoolean("pretty", false);
        return channel -> indexingProxyService.stopIndexer(index,
                wrap(res -> sendResponse(channel, res, pretty), e -> sendErrorResponse(channel, e)));
    }

    private RestChannelConsumer preparePostRequest(final RestRequest request) {
        final String index = request.param("index");
        final long position = request.paramAsLong("position", 0);
        final boolean pretty = request.paramAsBoolean("pretty", false);
        return channel -> {
            indexingProxyService.startIndexer(index, position,
                    wrap(res -> sendResponse(channel, res, pretty), e -> sendErrorResponse(channel, e)));
        };
    }

    protected void sendResponse(final RestChannel channel, final Map<String, Object> params, final boolean pretty) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            if (pretty) {
                builder.prettyPrint();
            }
            builder.startObject();
            builder.field("acknowledged", true);
            if (params != null) {
                for (final Map.Entry<String, Object> entry : params.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to create a resposne.", e);
        }
    }

    protected void sendErrorResponse(final RestChannel channel, final Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (final Exception e1) {
            logger.error("Failed to send a failure response.", e1);
        }
    }

}
