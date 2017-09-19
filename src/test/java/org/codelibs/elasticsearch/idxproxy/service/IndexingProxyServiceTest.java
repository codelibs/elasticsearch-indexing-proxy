package org.codelibs.elasticsearch.idxproxy.service;

import junit.framework.TestCase;

public class IndexingProxyServiceTest extends TestCase {

    public void test_getNextValue() throws Exception {
        assertEquals(1, IndexingProxyService.getNextValue(-1));
        assertEquals(1, IndexingProxyService.getNextValue(0));
        assertEquals(2, IndexingProxyService.getNextValue(1));
        assertEquals(3, IndexingProxyService.getNextValue(2));
        assertEquals(4, IndexingProxyService.getNextValue(3));
        assertEquals(Long.MAX_VALUE, IndexingProxyService.getNextValue(Long.MAX_VALUE - 1));
        assertEquals(1, IndexingProxyService.getNextValue(Long.MAX_VALUE));
    }
}
