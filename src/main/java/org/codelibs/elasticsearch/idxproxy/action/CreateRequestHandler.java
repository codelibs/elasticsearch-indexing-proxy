package org.codelibs.elasticsearch.idxproxy.action;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;

import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;

public class CreateRequestHandler implements TransportRequestHandler<CreateRequest> {

    private final IndexingProxyService indexingProxyService;

    public CreateRequestHandler(final IndexingProxyService indexingProxyService) {
        this.indexingProxyService = indexingProxyService;
    }

    @Override
    public void messageReceived(final CreateRequest request, final TransportChannel channel) throws Exception {
        indexingProxyService.renewOnLocal(wrap(res -> channel.sendResponse(new CreateResponse(true)), e -> {
            try {
                channel.sendResponse(e);
            } catch (final IOException e1) {
                throw new ElasticsearchException("Failed to write a response.", e1);
            }
        }));
    }

}
