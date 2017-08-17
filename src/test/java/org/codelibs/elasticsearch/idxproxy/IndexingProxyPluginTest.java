package org.codelibs.elasticsearch.idxproxy;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.IOException;
import java.util.Map;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.junit.Before;

import junit.framework.TestCase;

public class IndexingProxyPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private int numOfNode = 3;

    private String clusterName;

    @Before
    public void setUp() throws Exception {
        clusterName = "es-idxproxy-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
                settingsBuilder.put("idxproxy.indexer.interval", "1s");
                settingsBuilder.putArray("idxproxy.target.indices", "sample");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(numOfNode)
                .pluginTypes("org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin"));

        // wait for yellow status
        runner.ensureYellow();

    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_indexing() throws Exception {

        Node node = runner.node();

        final String index = "sample";
        final String type = "data";
        runner.createIndex(index, Settings.builder().build());

        runner.ensureYellow(".idxproxy");

        indexRequest(node, index, type, 1000);
        createRequest(node, index, type, 1001);
        updateRequest(node, index, type, 1001);
        createRequest(node, index, type, 1002);
        updateByQueryRequest(node, index, type, 1002);
        createRequest(node, index, type, 1003);
        deleteRequest(node, index, type, 1003);
        createRequest(node, index, type, 1004);
        // deleteByQueryRequest
        // bulkRequest
    }

    private void deleteRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.delete(node, "/" + index + "/" + type + "/" + id).execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("found").toString());
        }
    }

    private void updateByQueryRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type + "/_update_by_query")
                .header("Content-Type", "application/json").body("{\"script\":{\"inline\":\"ctx._source.msg='test " + (id + 200)
                        + "'\",\"lang\":\"painless\"},\"query\":{\"term\":{\"id\":" + id + "}}}")
                .execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("took"));
        }
    }

    private void updateRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type + "/" + id + "/_update")
                .header("Content-Type", "application/json").body("{\"doc\":{\"msg\":\"test " + (id + 100) + "\"}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("result"));
        }
    }

    private void createRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.put(node, "/" + index + "/" + type + "/" + id).header("Content-Type", "application/json")
                .body("{\"id\":\"\" + id + \"\",\"msg\":\"test \" + id + \"\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }
    }

    private void indexRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type).header("Content-Type", "application/json")
                .body("{\"id\":\"" + id + "\",\"msg\":\"test \"+id+\"\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }
    }

}
