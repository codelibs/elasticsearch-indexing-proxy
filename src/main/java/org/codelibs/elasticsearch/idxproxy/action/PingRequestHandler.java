package org.codelibs.elasticsearch.idxproxy.action;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;

import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;

public class PingRequestHandler implements TransportRequestHandler<PingRequest> {

    private final IndexingProxyService indexingProxyService;

    public PingRequestHandler(final IndexingProxyService indexingProxyService) {
        this.indexingProxyService = indexingProxyService;
    }

    @Override
    public void messageReceived(final PingRequest request, final TransportChannel channel) throws Exception {
        if (indexingProxyService.isRunning(request.getIndex())) {
            channel.sendResponse(new PingResponse(true, true));
        } else {
            indexingProxyService.startRequestSender(request.getIndex(), 0, wrap(response -> {
                channel.sendResponse(new PingResponse(true, false));
            }, e -> {
                try {
                    channel.sendResponse(e);
                } catch (final IOException e1) {
                    throw new ElasticsearchException("Failed to write a response.", e1);
                }
            }));
        }
    }

}
