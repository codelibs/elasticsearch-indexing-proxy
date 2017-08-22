package org.codelibs.elasticsearch.idxproxy.action;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

public class WriteRequest<Request extends ActionRequest> extends TransportRequest {

    private Request request;

    public WriteRequest(final Request request) {
        this.request = request;
    }

    public WriteRequest() {
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        request.readFrom(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    public Request getRequest() {
        return request;
    }

}
