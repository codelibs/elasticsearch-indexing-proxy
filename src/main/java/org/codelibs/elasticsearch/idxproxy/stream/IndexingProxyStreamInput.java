package org.codelibs.elasticsearch.idxproxy.stream;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;

public class IndexingProxyStreamInput extends InputStreamStreamInput {

    private final NamedWriteableRegistry namedWriteableRegistry;

    public IndexingProxyStreamInput(final InputStream is, final NamedWriteableRegistry namedWriteableRegistry) {
        super(is);
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(final Class<C> categoryClass) throws IOException {
        final String name = readString();
        return readNamedWriteable(categoryClass, name);
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(@SuppressWarnings("unused") final Class<C> categoryClass,
            @SuppressWarnings("unused") final String name) throws IOException {
        final Writeable.Reader<? extends C> reader = namedWriteableRegistry.getReader(categoryClass, name);
        final C c = reader.read(this);
        if (c == null) {
            throw new IOException(
                    "Writeable.Reader [" + reader + "] returned null which is not allowed and probably means it screwed up the stream.");
        }
        assert name.equals(c.getWriteableName()) : c + " claims to have a different name [" + c.getWriteableName()
                + "] than it was read from [" + name + "].";
        return c;
    }
}
