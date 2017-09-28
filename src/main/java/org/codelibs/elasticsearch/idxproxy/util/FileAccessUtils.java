package org.codelibs.elasticsearch.idxproxy.util;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class FileAccessUtils {
    private FileAccessUtils() {
        // nothing
    }

    public static boolean existsFile(final Path p) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            return p.toFile().exists();
        });
    }
}
