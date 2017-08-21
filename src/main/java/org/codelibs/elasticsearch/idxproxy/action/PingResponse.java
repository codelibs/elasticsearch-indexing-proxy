package org.codelibs.elasticsearch.idxproxy.action;

import java.io.IOException;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class PingResponse extends AcknowledgedResponse {
    private boolean found;

    public PingResponse() {
    }

    public PingResponse(final boolean acknowledged, final boolean found) {
        super(acknowledged);
        this.found = found;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
        found = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
        out.writeBoolean(found);
    }

    public boolean isFound() {
        return found;
    }
}
