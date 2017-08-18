package org.codelibs.elasticsearch.idxproxy;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class IndexingProxyPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private int numOfNode = 3;

    private String clusterName;

    public void setUp(BiConsumer<Integer, Builder> consumer) throws Exception {
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
                consumer.accept(number, settingsBuilder);
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
        setUp((number, settingsBuilder) -> {
            settingsBuilder.put("idxproxy.indexer.interval", "1s");
            settingsBuilder.putArray("idxproxy.target.indices", "sample");
        });

        Node node = runner.node();

        final String alias = "sample";
        final String index1 = "sample1";
        final String type = "data";
        runner.createIndex(index1, Settings.builder().build());
        runner.updateAlias(alias, new String[] { index1 }, new String[0]);

        runner.ensureYellow(".idxproxy");

        indexRequest(node, alias, type, 1000);
        createRequest(node, alias, type, 1001);
        updateRequest(node, alias, type, 1001);
        createRequest(node, alias, type, 1002);
        updateByQueryRequest(node, alias, type, 1002);
        createRequest(node, alias, type, 1003);
        deleteRequest(node, alias, type, 1003);
        createRequest(node, alias, type, 1004);
        deleteByQueryRequest(node, alias, type, 1004);
        bulkRequest(node, alias, type, 1005);

        try (CurlResponse curlResponse = Curl.post(node, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(0, ((Number) hits.get("total")).longValue());
        }

        runner.refresh();

        try (CurlResponse curlResponse = Curl.post(node, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(0, ((Number) hits.get("total")).longValue());
        }

        try (CurlResponse curlResponse =
                Curl.post(node, "/" + index1 + "/_idxproxy/process").header("Content-Type", "application/json").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            // assertEquals(0, ((Number) ((Map<String, Object>) map.get("hits")).get("total")).longValue());
        }

        /*
        try (CurlResponse curlResponse = Curl.post(node, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}},\"sort\":[{\"id\":{\"order\":\"asc\"}}]}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals(0, ((Number) ((Map<String, Object>) map.get("hits")).get("total")).longValue());
        }

        final String index2 = "sample2";
        runner.createIndex(index2, Settings.builder().build());
        */
    }

    private void bulkRequest(Node node, final String index, final String type, final long id) throws IOException {
        final StringBuilder buf = new StringBuilder();
        long value = id;
        buf.append("{\"create\":{\"_index\":\"" + index + "\",\"_type\":\"" + type + "\",\"_id\":\"" + value + "\"}}\n");
        buf.append("{\"id\":" + value + ",\"msg\":\"test " + value + "\"}\n");
        value++;
        createRequest(node, index, type, value);
        buf.append("{\"update\":{\"_index\":\"" + index + "\",\"_type\":\"" + type + "\",\"_id\":\"" + value + "\"}}\n");
        buf.append("{\"doc\":{\"msg\":\"test " + (value + 100) + "\"}}\n");
        value++;
        createRequest(node, index, type, value);
        buf.append("{\"delete\":{\"_index\":\"" + index + "\",\"_type\":\"" + type + "\",\"_id\":\"" + value + "\"}}\n");

        try (CurlResponse curlResponse =
                Curl.post(node, "/_bulk").header("Content-Type", "application/x-ndjson").body(buf.toString()).execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("took"));
        }
    }

    private void deleteByQueryRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type + "/_delete_by_query")
                .header("Content-Type", "application/json").body("{\"query\":{\"term\":{\"id\":" + id + "}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("took"));
        }
    }

    private void deleteRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse =
                Curl.delete(node, "/" + index + "/" + type + "/" + id).header("Content-Type", "application/json").execute()) {
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
                .param("refresh", "true").body("{\"id\":\"\" + id + \"\",\"msg\":\"test \" + id + \"\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }
    }

    private void indexRequest(Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type).header("Content-Type", "application/json")
                .param("refresh", "true").body("{\"id\":\"" + id + "\",\"msg\":\"test \"+id+\"\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }
    }

}
