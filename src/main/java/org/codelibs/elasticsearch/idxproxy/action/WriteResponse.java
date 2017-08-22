package org.codelibs.elasticsearch.idxproxy.action;

import java.io.IOException;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class WriteResponse extends AcknowledgedResponse {

    public WriteResponse() {
    }

    public WriteResponse(final boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        readAcknowledged(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        writeAcknowledged(out);
    }

}
