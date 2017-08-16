package org.codelibs.elasticsearch.idxproxy;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

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

        // post
        try (CurlResponse curlResponse =
                Curl.post(node, "/" + index + "/" + type).body("{\"id\":\"2000\",\"msg\":\"test 2000\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }

        // put
        try (CurlResponse curlResponse =
                Curl.put(node, "/" + index + "/" + type + "/2001").body("{\"id\":\"2001\",\"msg\":\"test 2001\"}").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("created").toString());
        }

        // delete
        try (CurlResponse curlResponse = Curl.delete(node, "/" + index + "/" + type + "/2001").execute()) {
            final Map<String, Object> map = curlResponse.getContentAsMap();
            assertNotNull(map);
            assertEquals("true", map.get("found").toString());
        }

        //        for (int i = 1; i <= 1000; i++) {
        //            final IndexResponse indexResponse = runner.insert(index, type, String.valueOf(i),
        //                    "{\"id\":\"" + i + "\",\"msg\":\"test " + i + "\",\"counter\":" + i + ",\"category\":" + i % 10 + "}");
        //            assertEquals(Result.CREATED, indexResponse.getResult());
        //        }
    }

}
