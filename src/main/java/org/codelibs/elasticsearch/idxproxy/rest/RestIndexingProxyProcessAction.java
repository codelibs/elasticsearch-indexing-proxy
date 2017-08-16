package org.codelibs.elasticsearch.idxproxy.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.Map;

import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
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

    @Inject
    public RestIndexingProxyProcessAction(final Settings settings, final RestController controller,
            final IndexingProxyService indexingProxyService) {
        super(settings);
        this.indexingProxyService = indexingProxyService;

        controller.registerHandler(RestRequest.Method.GET, "/{index}/_idxproxy/process", this);
        controller.registerHandler(RestRequest.Method.POST, "/{index}/_idxproxy/process", this);
        controller.registerHandler(RestRequest.Method.DELETE, "/{index}/_idxproxy/process", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        switch (request.method()) {
        case GET: {

        }
        case POST: {
            final String index = request.param("index");
            return channel -> {
                //indexingProxyService.startIndexing(index);
                //  sendResponse(channel, params);
            };
        }
        case DELETE: {

        }
        }

        return channel -> {
            sendErrorResponse(channel, new ElasticsearchException("Unknown request: " + request));
        };
    }

    protected void sendResponse(final RestChannel channel, final Map<String, Object> params) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
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
