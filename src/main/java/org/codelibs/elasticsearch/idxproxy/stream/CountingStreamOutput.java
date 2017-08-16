package org.codelibs.elasticsearch.idxproxy.stream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.output.CountingOutputStream;
import org.elasticsearch.common.io.stream.StreamOutput;

public class CountingStreamOutput extends StreamOutput {
    private final CountingOutputStream out;

    public CountingStreamOutput(final OutputStream out) {
        this.out = new CountingOutputStream(new BufferedOutputStream(out));
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        out.write(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        out.write(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    public long getByteCount() {
        return out.getByteCount();
    }
}
