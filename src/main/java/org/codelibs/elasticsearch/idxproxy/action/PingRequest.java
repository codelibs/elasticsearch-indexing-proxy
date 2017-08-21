package org.codelibs.elasticsearch.idxproxy.action;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

public class PingRequest extends TransportRequest {
    private String index;

    public PingRequest() {
    }

    public PingRequest(final String index) {
        this.index = index;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
    }

    public String getIndex() {
        return index;
    }
}
