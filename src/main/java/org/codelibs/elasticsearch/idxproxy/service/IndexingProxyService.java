package org.codelibs.elasticsearch.idxproxy.service;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.codelibs.elasticsearch.idxproxy.action.ProxyActionFilter;
import org.codelibs.elasticsearch.idxproxy.stream.CountingStreamOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

public class IndexingProxyService extends AbstractLifecycleComponent {

    private static final String FILE_MAPPING_JSON = "idxproxy/file_mapping.json";

    private static final String INDEX_NAME = ".idxproxy";

    private static final String TYPE_NAME = "config";

    private static final String WORKING_EXTENTION = ".tmp";

    private static final String DATA_EXTENTION = ".dat";

    private static final short TYPE_DELETE = 1;

    private static final short TYPE_DELETE_BY_QUERY = 2;

    private static final short TYPE_INDEX = 3;

    private static final short TYPE_UPDATE = 4;

    private static final short TYPE_UPDATE_BY_QUERY = 5;

    private static final short TYPE_BULK = 99;

    public static final Setting<String> SETTING_INXPROXY_DATA_FILE_FORMAT =
            Setting.simpleString("idxproxy.data.file.format", Property.NodeScope);

    public static final Setting<String> SETTING_INXPROXY_DATA_PATH = Setting.simpleString("idxproxy.data.path", Property.NodeScope);

    public static final Setting<List<String>> SETTING_INXPROXY_TARGET_INDICES =
            Setting.listSetting("idxproxy.target.indices", Collections.emptyList(), s -> s.trim(), Property.NodeScope);

    public static final Setting<ByteSizeValue> SETTING_INXPROXY_DATA_FILE_SIZE =
            Setting.memorySizeSetting("idxproxy.data.file_size", new ByteSizeValue(100, ByteSizeUnit.MB), Property.NodeScope);

    public static final Setting<TimeValue> SETTING_INXPROXY_INDEXER_INTERVAL =
            Setting.timeSetting("idxproxy.indexer.interval", TimeValue.timeValueSeconds(30), Property.NodeScope);

    private final Client client;

    private final Path dataPath;

    private volatile CountingStreamOutput streamOutput;

    private volatile String fileId;

    private final Set<String> targetIndexSet;

    private final long dataFileSize;

    private final String dataFileFormat;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final TimeValue indexerInterval;

    @Inject
    public IndexingProxyService(final Settings settings, final Environment env, final Client client, final ClusterService clusterService,
            final ThreadPool threadPool, final ActionFilters filters) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        final String dataPathStr = SETTING_INXPROXY_DATA_PATH.get(settings);
        if (dataPathStr == null || dataPathStr.length() == 0) {
            dataPath = env.dataFiles()[0];
        } else {
            dataPath = Paths.get(dataPathStr);
        }

        final String dataFileFormatStr = SETTING_INXPROXY_DATA_FILE_FORMAT.get(settings);
        if (dataFileFormatStr == null || dataFileFormatStr.length() == 0) {
            dataFileFormat = "%s-%019d";
        } else {
            dataFileFormat = dataFileFormatStr;
        }

        dataFileSize = SETTING_INXPROXY_DATA_FILE_SIZE.get(settings).getBytes();
        targetIndexSet = SETTING_INXPROXY_TARGET_INDICES.get(settings).stream().collect(Collectors.toSet());
        indexerInterval = SETTING_INXPROXY_INDEXER_INTERVAL.get(settings);

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof ProxyActionFilter) {
                ((ProxyActionFilter) filter).setIndexingProxyService(this);
            }
        }
    }

    @Override
    protected void doStart() {
        if (!targetIndexSet.isEmpty()) {
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void afterStart() {
                    client.admin().cluster().prepareHealth(INDEX_NAME).setWaitForYellowStatus()
                            .execute(new ActionListener<ClusterHealthResponse>() {

                                @Override
                                public void onResponse(final ClusterHealthResponse response) {
                                    if (response.isTimedOut()) {
                                        logger.warn("Cluster service was timeouted.");
                                    }
                                    checkIfIndexExists(wrap(res -> {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("Created .idxproxy index.");
                                        }
                                    }, e -> {
                                        logger.error("Failed to create .idxproxy.", e);
                                    }));
                                }

                                @Override
                                public void onFailure(final Exception e) {
                                    logger.error("Failed to create .idxproxy.", e);
                                }
                            });
                }
            });
        }
    }

    private void checkIfIndexExists(final ActionListener<ActionResponse> listener) {
        client.admin().indices().prepareExists(INDEX_NAME).execute(new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(INDEX_NAME + " exists.");
                    }
                    listener.onResponse(response);
                } else {
                    createIndex(listener);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                if (e instanceof IndexNotFoundException) {
                    createIndex(listener);
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    private void createIndex(final ActionListener<ActionResponse> listener) {
        try (final Reader in = new InputStreamReader(IndexingProxyService.class.getClassLoader().getResourceAsStream(FILE_MAPPING_JSON),
                StandardCharsets.UTF_8)) {
            final String source = Streams.copyToString(in);
            final XContentBuilder settingsBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject("index")//
                    .field("number_of_replicas", 0)//
                    .endObject()//
                    .endObject();
            client.admin().indices().prepareCreate(INDEX_NAME).setSettings(settingsBuilder)
                    .addMapping(TYPE_NAME, source, XContentFactory.xContentType(source)).execute(new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(final CreateIndexResponse response) {
                            waitForIndex(listener);
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            listener.onFailure(e);
                        }
                    });
        } catch (final IOException e) {
            listener.onFailure(e);
        }
    }

    private void waitForIndex(final ActionListener<ActionResponse> listener) {
        client.admin().cluster().prepareHealth(INDEX_NAME).setWaitForYellowStatus().execute(new ActionListener<ClusterHealthResponse>() {
            @Override
            public void onResponse(final ClusterHealthResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        if (streamOutput != null) {
            closeStreamOutput();
        }
    }

    private <Response extends ActionResponse> void createStreamOutput(final ActionListener<Response> listener) {
        final String oldFileId = fileId;
        client.prepareIndex(INDEX_NAME, TYPE_NAME, "file_id").setSource(Collections.emptyMap()).execute(wrap(res -> {
            synchronized (this) {
                if (oldFileId == null || oldFileId.equals(fileId)) {
                    if (streamOutput != null) {
                        closeStreamOutput();
                    }

                    fileId = String.format(dataFileFormat, nodeName(), res.getVersion());
                    final Path outputPath = dataPath.resolve(fileId + WORKING_EXTENTION);
                    try {
                        streamOutput = new CountingStreamOutput(Files.newOutputStream(outputPath));
                        logger.info("Opening " + outputPath.toAbsolutePath());
                    } catch (final IOException e) {
                        throw new ElasticsearchException("Could not open " + outputPath, e);
                    }
                }
            }

            listener.onResponse(null);
        }, listener::onFailure));
    }

    private void closeStreamOutput() {
        try {
            streamOutput.flush();
            streamOutput.close();

            final Path source = dataPath.resolve(fileId + WORKING_EXTENTION);
            final Path target = dataPath.resolve(fileId + DATA_EXTENTION);
            final Path outputPath = Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            logger.info("Closed " + outputPath.toAbsolutePath());
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to close streamOutput.", e);
        }
    }

    public <Response extends ActionResponse> void renew(final ActionListener<Response> listener) {
        createStreamOutput(listener);
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void write(final Request request,
            final ActionListener<Response> listener) {
        final ActionListener<Response> next = wrap(res -> {
            final short classType = getClassType(request);
            if (classType > 0) {
                synchronized (this) {
                    streamOutput.writeShort(classType);
                    request.writeTo(streamOutput);
                }
            } else {
                throw new ElasticsearchException("Unknown request: " + request);
            }
            listener.onResponse(res);
        }, listener::onFailure);

        if (streamOutput == null || streamOutput.getByteCount() > dataFileSize) {
            createStreamOutput(next);
        } else {
            next.onResponse(null);
        }
    }

    private <Request extends ActionRequest> short getClassType(final Request request) {
        if (DeleteRequest.class.isInstance(request)) {
            return TYPE_DELETE;
        } else if (DeleteByQueryAction.class.isInstance(request)) {
            return TYPE_DELETE_BY_QUERY;
        } else if (IndexRequest.class.isInstance(request)) {
            return TYPE_INDEX;
        } else if (UpdateRequest.class.isInstance(request)) {
            return TYPE_UPDATE;
        } else if (UpdateByQueryRequest.class.isInstance(request)) {
            return TYPE_UPDATE_BY_QUERY;
        } else if (BulkRequest.class.isInstance(request)) {
            return TYPE_BULK;
        }
        return 0;
    }

    public boolean isTargetIndex(final String index) {
        return targetIndexSet.contains(index);
    }

    public void startIndexing(final String index, final long filePosition, final ActionListener<?> listener) {
        client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get("node_name");
                if (Strings.isBlank(workingNodeName)) {
                    listener.onFailure(new ElasticsearchException("Indexer is working in " + workingNodeName));
                } else {
                    final Number pos = (Number) source.get("file_position");
                    launchIndexer(index, filePosition > 0 ? filePosition : (pos == null ? 1 : pos.longValue()), res.getVersion(), listener);
                }
            } else {
                launchIndexer(index, filePosition > 0 ? filePosition : 1, 0, listener);
            }
        }, listener::onFailure));
    }

    private void launchIndexer(final String index, final long filePosition, final long version, final ActionListener<?> listener) {
        final Map<String, Object> source = new HashMap<>();
        source.put("node_name", nodeName());
        source.put("file_position", filePosition);
        source.put("@timestamp", new Date());
        final IndexRequestBuilder builder = client.prepareIndex(INDEX_NAME, TYPE_NAME, index).setSource(source);
        if (version > 0) {
            builder.setVersion(version);
        }
        builder.execute(wrap(res -> {
            if (res.getResult() == Result.CREATED || res.getResult() == Result.UPDATED) {
                threadPool.schedule(TimeValue.ZERO, Names.GENERIC, new Indexer());
                listener.onResponse(null);
            } else {
                listener.onFailure(new ElasticsearchException("Failed to update .idxproxy index: " + res));
            }
        }, listener::onFailure));
    }

    class Indexer implements Runnable {

        private String index;

        @Override
        public void run() {
            client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
                if (res.isExists()) {
                    final Map<String, Object> source = res.getSourceAsMap();
                    final String workingNodeName = (String) source.get("node_name");
                    if (!nodeName().equals(workingNodeName)) {
                        logger.info("Stopped Indexer({}) because of working in [{}].", index, workingNodeName);
                    } else {
                        final Number pos = (Number) source.get("file_position");
                        if (pos == null) {
                            logger.error("Stopped Indexer({}). No file_position.", index);
                        } else {
                            process(pos.longValue());
                        }
                    }
                } else {
                    logger.info("Stopped Indexer({}).", index);
                }
            }, e -> {
                logger.error("Indexer(" + index + ") is failed. ", e);
            }));
        }

        private void process(final long filePosition) {
            final Path path = dataPath.resolve(String.format(dataFileFormat, nodeName(), filePosition) + DATA_EXTENTION);
            if (path.toFile().exists()) {
                logger.info("Indexing from {}", path.toAbsolutePath());
                try (StreamInput streamInput = new InputStreamStreamInput(Files.newInputStream(path))) {
                    final List<ActionRequest> requestList = loadRequests(streamInput);
                    // TODO
                } catch (final IOException e) {
                    logger.error("Failed to read " + path.toAbsolutePath(), e);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} does not exist.", path.toAbsolutePath());
                }
                threadPool.schedule(indexerInterval, Names.GENERIC, this);
            }
        }

        private List<ActionRequest> loadRequests(final StreamInput streamInput) throws IOException {
            final List<ActionRequest> list = new ArrayList<>();
            while (streamInput.available() > 0) {
                final short classType = streamInput.readShort();
                switch (classType) {
                case TYPE_DELETE:
                    // TODO
                    break;
                default:
                    break;
                }
            }
            return list;
        }
    }
}
