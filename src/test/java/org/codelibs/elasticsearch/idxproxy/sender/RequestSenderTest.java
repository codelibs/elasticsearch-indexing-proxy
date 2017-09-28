package org.codelibs.elasticsearch.idxproxy.sender;

import org.codelibs.elasticsearch.idxproxy.sender.RequestSender;

import junit.framework.TestCase;

public class RequestSenderTest extends TestCase {

    public void test_getNextValue() throws Exception {
        assertEquals(1, RequestSender.getNextValue(-1));
        assertEquals(1, RequestSender.getNextValue(0));
        assertEquals(2, RequestSender.getNextValue(1));
        assertEquals(3, RequestSender.getNextValue(2));
        assertEquals(4, RequestSender.getNextValue(3));
        assertEquals(Long.MAX_VALUE, RequestSender.getNextValue(Long.MAX_VALUE - 1));
        assertEquals(1, RequestSender.getNextValue(Long.MAX_VALUE));
    }
}
