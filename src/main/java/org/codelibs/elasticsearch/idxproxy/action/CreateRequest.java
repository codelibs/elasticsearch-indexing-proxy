package org.codelibs.elasticsearch.idxproxy.action;

import java.io.IOException;

import org.codelibs.elasticsearch.idxproxy.util.RequestUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

public class CreateRequest<Request extends ActionRequest> extends TransportRequest {

    public CreateRequest() {
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
    }

}
