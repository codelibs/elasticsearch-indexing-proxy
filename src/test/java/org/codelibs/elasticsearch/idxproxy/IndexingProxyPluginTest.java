package org.codelibs.elasticsearch.idxproxy;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.io.FileUtils;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class IndexingProxyPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private final int numOfNode = 3;

    private String clusterName;

    private File dataDir;

    @Override
    public void setUp() throws Exception {
        dataDir = File.createTempFile("es-idxproxy", "");
        dataDir.delete();
        dataDir.mkdirs();
        System.out.println("idxproxy.data.path: " + dataDir.getAbsolutePath());
    }

    public void setUp(final BiConsumer<Integer, Builder> consumer) throws Exception {
        clusterName = "es-idxproxy-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild((number, settingsBuilder) -> {
            settingsBuilder.put("http.cors.enabled", true);
            settingsBuilder.put("http.cors.allow-origin", "*");
            settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
            consumer.accept(number, settingsBuilder);
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
        dataDir.delete();
        FileUtils.deleteDirectory(dataDir);
    }

    @SuppressWarnings("unchecked")
    public void test_indexing() throws Exception {
        setUp((number, settingsBuilder) -> {
            settingsBuilder.put("idxproxy.data.path", dataDir.getAbsolutePath());
            settingsBuilder.put("idxproxy.indexer.interval", "1s");
            settingsBuilder.putArray("idxproxy.target.indices", "sample");
        });

        final Node node1 = runner.getNode(0);
        final Node node2 = runner.getNode(1);

        final String alias = "sample";
        final String index1 = "sample1";
        final String type = "data";
        runner.createIndex(index1, Settings.builder().build());
        runner.updateAlias(alias, new String[] { index1 }, new String[0]);

        runner.ensureYellow(".idxproxy");

        // send requests to data file
        indexRequest(node1, alias, type, 1000);
        createRequest(node1, alias, type, 1001);
        updateRequest(node1, alias, type, 1001);
        createRequest(node1, alias, type, 1002);
        updateByQueryRequest(node1, alias, type, 1002);
        createRequest(node1, alias, type, 1003);
        deleteRequest(node1, alias, type, 1003);
        createRequest(node1, alias, type, 1004);
        deleteByQueryRequest(node1, alias, type, 1004);
        bulkRequest(node1, alias, type, 1005);

        try (CurlResponse curlResponse = Curl.post(node1, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(0, ((Number) hits.get("total")).longValue());
        }

        // flush data file
        runner.refresh();

        try (CurlResponse curlResponse = Curl.post(node1, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(0, ((Number) hits.get("total")).longValue());
        }

        try (CurlResponse curlResponse =
                Curl.post(node1, "/" + index1 + "/_idxproxy/process").header("Content-Type", "application/json").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertEquals(1, ((Integer) map.get("file_position")).intValue());
        }

        waitForNdocs(node1, index1, type, 5);

        try (CurlResponse curlResponse = Curl.post(node1, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}},\"sort\":[{\"id\":{\"order\":\"asc\"}}]}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(5, ((Number) hits.get("total")).longValue());
            final List<Map<String, Object>> list = (List<Map<String, Object>>) hits.get("hits");
            assertEquals("test 1000", ((Map<String, Object>) list.get(0).get("_source")).get("msg"));
            assertEquals("test 1101", ((Map<String, Object>) list.get(1).get("_source")).get("msg"));
            assertEquals("test 1202", ((Map<String, Object>) list.get(2).get("_source")).get("msg"));
            assertEquals("test 1005", ((Map<String, Object>) list.get(3).get("_source")).get("msg"));
            assertEquals("test 1306", ((Map<String, Object>) list.get(4).get("_source")).get("msg"));
        }

        checkFilePosition(node1, index1, 2);

        // send requests to data file
        indexRequest(node1, alias, type, 2000);
        createRequest(node1, alias, type, 2001);
        updateRequest(node1, alias, type, 2001);
        createRequest(node1, alias, type, 2002);
        updateByQueryRequest(node1, alias, type, 2002);
        createRequest(node1, alias, type, 2003);
        deleteRequest(node1, alias, type, 2003);
        createRequest(node1, alias, type, 2004);
        deleteByQueryRequest(node1, alias, type, 2004);
        bulkRequest(node1, alias, type, 2005);

        try (CurlResponse curlResponse = Curl.post(node1, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(5, ((Number) hits.get("total")).longValue());
        }

        checkFilePosition(node1, index1, 2);

        // flush data file
        runner.refresh();

        waitForNdocs(node1, index1, type, 10);

        try (CurlResponse curlResponse = Curl.post(node1, "/" + index1 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}},\"sort\":[{\"id\":{\"order\":\"asc\"}}]}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(10, ((Number) hits.get("total")).longValue());
            final List<Map<String, Object>> list = (List<Map<String, Object>>) hits.get("hits");
            assertEquals("test 1000", ((Map<String, Object>) list.get(0).get("_source")).get("msg"));
            assertEquals("test 1101", ((Map<String, Object>) list.get(1).get("_source")).get("msg"));
            assertEquals("test 1202", ((Map<String, Object>) list.get(2).get("_source")).get("msg"));
            assertEquals("test 1005", ((Map<String, Object>) list.get(3).get("_source")).get("msg"));
            assertEquals("test 1306", ((Map<String, Object>) list.get(4).get("_source")).get("msg"));
            assertEquals("test 2000", ((Map<String, Object>) list.get(5).get("_source")).get("msg"));
            assertEquals("test 2101", ((Map<String, Object>) list.get(6).get("_source")).get("msg"));
            assertEquals("test 2202", ((Map<String, Object>) list.get(7).get("_source")).get("msg"));
            assertEquals("test 2005", ((Map<String, Object>) list.get(8).get("_source")).get("msg"));
            assertEquals("test 2306", ((Map<String, Object>) list.get(9).get("_source")).get("msg"));
        }

        checkFilePosition(node1, index1, 3);

        final String index2 = "sample2";
        runner.createIndex(index2, Settings.builder().build());
        runner.ensureYellow(index2);

        try (CurlResponse curlResponse =
                Curl.post(node2, "/" + index2 + "/_idxproxy/process").header("Content-Type", "application/json").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(((Boolean) map.get("acknowledged")).booleanValue());
            assertEquals(1, ((Integer) map.get("file_position")).intValue());
        }

        waitForNdocs(node2, index2, type, 10);

        try (CurlResponse curlResponse = Curl.post(node2, "/" + index2 + "/" + type + "/_search").header("Content-Type", "application/json")
                .body("{\"query\":{\"match_all\":{}},\"sort\":[{\"id\":{\"order\":\"asc\"}}]}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
            assertEquals(10, ((Number) hits.get("total")).longValue());
            final List<Map<String, Object>> list = (List<Map<String, Object>>) hits.get("hits");
            assertEquals("test 1000", ((Map<String, Object>) list.get(0).get("_source")).get("msg"));
            assertEquals("test 1101", ((Map<String, Object>) list.get(1).get("_source")).get("msg"));
            assertEquals("test 1202", ((Map<String, Object>) list.get(2).get("_source")).get("msg"));
            assertEquals("test 1005", ((Map<String, Object>) list.get(3).get("_source")).get("msg"));
            assertEquals("test 1306", ((Map<String, Object>) list.get(4).get("_source")).get("msg"));
            assertEquals("test 2000", ((Map<String, Object>) list.get(5).get("_source")).get("msg"));
            assertEquals("test 2101", ((Map<String, Object>) list.get(6).get("_source")).get("msg"));
            assertEquals("test 2202", ((Map<String, Object>) list.get(7).get("_source")).get("msg"));
            assertEquals("test 2005", ((Map<String, Object>) list.get(8).get("_source")).get("msg"));
            assertEquals("test 2306", ((Map<String, Object>) list.get(9).get("_source")).get("msg"));
        }
    }

    private void waitForNdocs(final Node node, final String index1, final String type, final long num) throws Exception {
        for (int i = 0; i < 10; i++) {
            try (CurlResponse curlResponse = Curl.post(node, "/" + index1 + "/" + type + "/_search")
                    .header("Content-Type", "application/json").body("{\"query\":{\"match_all\":{}}}").execute()) {
                final Map<String, Object> map = curlResponse.getContentAsMap();
                @SuppressWarnings("unchecked")
                final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
                if (((Number) hits.get("total")).longValue() == num) {
                    Thread.sleep(3000L); // wait for bulk requests
                    return;
                }
            }
            runner.refresh();
            Thread.sleep(1000L);
        }
        fail(num + " docs are not inserted.");
    }

    private void checkFilePosition(final Node node, final String index, final int position) throws IOException {
        try (CurlResponse curlResponse =
                Curl.get(node, "/.idxproxy/config/" + index).header("Content-Type", "application/json").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            @SuppressWarnings("unchecked")
            final Map<String, Object> source = (Map<String, Object>) map.get("_source");
            assertEquals(position, ((Number) source.get("file_position")).longValue());
        }
    }

    private void bulkRequest(final Node node, final String index, final String type, final long id) throws IOException {
        final StringBuilder buf = new StringBuilder();
        long value = id;
        buf.append("{\"create\":{\"_index\":\"" + index + "\",\"_type\":\"" + type + "\",\"_id\":\"" + value + "\"}}\n");
        buf.append("{\"id\":" + value + ",\"msg\":\"test " + value + "\"}\n");
        value++;
        createRequest(node, index, type, value);
        buf.append("{\"update\":{\"_index\":\"" + index + "\",\"_type\":\"" + type + "\",\"_id\":\"" + value + "\"}}\n");
        buf.append("{\"doc\":{\"msg\":\"test " + (value + 300) + "\"}}\n");
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

    private void deleteByQueryRequest(final Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type + "/_delete_by_query")
                .header("Content-Type", "application/json").body("{\"query\":{\"term\":{\"id\":" + id + "}}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("took"));
        }
    }

    private void deleteRequest(final Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse =
                Curl.delete(node, "/" + index + "/" + type + "/" + id).header("Content-Type", "application/json").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("found").toString());
        }
    }

    private void updateByQueryRequest(final Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type + "/_update_by_query")
                .header("Content-Type", "application/json").body("{\"script\":{\"inline\":\"ctx._source.msg='test " + (id + 200)
                        + "'\",\"lang\":\"painless\"},\"query\":{\"term\":{\"id\":" + id + "}}}")
                .execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("took"));
        }
    }

    private void updateRequest(final Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type + "/" + id + "/_update")
                .header("Content-Type", "application/json").body("{\"doc\":{\"msg\":\"test " + (id + 100) + "\"}}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertTrue(map.containsKey("result"));
        }
    }

    private void createRequest(final Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.put(node, "/" + index + "/" + type + "/" + id).header("Content-Type", "application/json")
                .param("refresh", "true").body("{\"id\":" + id + ",\"msg\":\"test " + id + "\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }
    }

    private void indexRequest(final Node node, final String index, final String type, final long id) throws IOException {
        try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/" + type).header("Content-Type", "application/json")
                .param("refresh", "true").body("{\"id\":" + id + ",\"msg\":\"test " + id + "\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }
    }

}
