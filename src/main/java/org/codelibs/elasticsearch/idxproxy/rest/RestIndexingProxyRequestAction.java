package org.codelibs.elasticsearch.idxproxy.rest;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;

import org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin.PluginComponent;
import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

public class RestIndexingProxyRequestAction extends BaseRestHandler {

    private final IndexingProxyService indexingProxyService;

    public RestIndexingProxyRequestAction(final Settings settings, final RestController controller, final PluginComponent pluginComponent) {
        super(settings);
        indexingProxyService = pluginComponent.getIndexingProxyService();

        controller.registerHandler(RestRequest.Method.GET, "/_idxproxy/request", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        switch (request.method()) {
        case GET:
            return prepareGetRequest(request);
        default:
            break;
        }

        return channel -> sendErrorResponse(channel, new ElasticsearchException("Unknown request: " + request));
    }

    private RestChannelConsumer prepareGetRequest(final RestRequest request) {
        final int position = request.paramAsInt("position", -1);
        return channel -> {
            indexingProxyService.dumpRequests(position,
                    wrap(res -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, res)), e -> sendErrorResponse(channel, e)));
        };
    }

    protected void sendErrorResponse(final RestChannel channel, final Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (final Exception e1) {
            logger.error("Failed to send a failure response.", e1);
        }
    }

}
