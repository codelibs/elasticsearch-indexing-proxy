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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

public class IndexingProxyService extends AbstractLifecycleComponent {

    private static final String TIMESTAMP = "@timestamp";

    private static final String FILE_POSITION = "file_position";

    private static final String NODE_NAME = "node_name";

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

    public static final Setting<Integer> SETTING_INXPROXY_INDEXER_RETRY_COUNT =
            Setting.intSetting("idxproxy.indexer.retry_count", 10, Property.NodeScope);

    public static final Setting<Boolean> SETTING_INXPROXY_INDEXER_SKIP_ERROR_FILE =
            Setting.boolSetting("idxproxy.indexer.skip.error_file", true, Property.NodeScope);

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

    private final int indexerRetryCount;

    private final boolean indexerSkipErrorFile;

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
        indexerRetryCount = SETTING_INXPROXY_INDEXER_RETRY_COUNT.get(settings);
        indexerSkipErrorFile = SETTING_INXPROXY_INDEXER_SKIP_ERROR_FILE.get(settings);

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

    public void startIndexer(final String index, final long filePosition, final ActionListener<Map<String, Object>> listener) {
        client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get(NODE_NAME);
                if (Strings.isBlank(workingNodeName)) {
                    listener.onFailure(new ElasticsearchException("Indexer is working in " + workingNodeName));
                } else {
                    final Number pos = (Number) source.get(FILE_POSITION);
                    final long value = pos == null ? 1 : pos.longValue();
                    launchIndexer(index, filePosition > 0 ? filePosition : value, res.getVersion(), listener);
                }
            } else {
                launchIndexer(index, filePosition > 0 ? filePosition : 1, 0, listener);
            }
        }, listener::onFailure));
    }

    private void launchIndexer(final String index, final long filePosition, final long version,
            final ActionListener<Map<String, Object>> listener) {
        final Map<String, Object> source = new HashMap<>();
        source.put(NODE_NAME, nodeName());
        source.put(FILE_POSITION, filePosition);
        source.put(TIMESTAMP, new Date());
        final IndexRequestBuilder builder =
                client.prepareIndex(INDEX_NAME, TYPE_NAME, index).setSource(source).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        if (version > 0) {
            builder.setVersion(version);
        } else {
            builder.setCreate(true);
        }
        builder.execute(wrap(res -> {
            if (res.getResult() == Result.CREATED || res.getResult() == Result.UPDATED) {
                threadPool.schedule(TimeValue.ZERO, Names.GENERIC, new Indexer());
                listener.onResponse(source);
            } else {
                listener.onFailure(new ElasticsearchException("Failed to update .idxproxy index: " + res));
            }
        }, listener::onFailure));
    }

    public void stopIndexer(final String index, final ActionListener<Map<String, Object>> listener) {
        client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
            final Map<String, Object> params = new HashMap<>();
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get(NODE_NAME);
                params.put("node", nodeName());
                params.put("found", true);
                if (Strings.isBlank(workingNodeName)) {
                    params.put("stop", false);
                    params.put("working", "");
                } else if (nodeName().equals(workingNodeName)) {
                    params.put("working", workingNodeName);
                    // stop
                    final long version = res.getVersion();
                    final Map<String, Object> newSource = new HashMap<>();
                    newSource.put(NODE_NAME, "");
                    newSource.put(TIMESTAMP, new Date());
                    client.prepareUpdate(INDEX_NAME, TYPE_NAME, index).setVersion(version).setDoc(newSource)
                            .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute(wrap(res2 -> {
                                params.put("stop", true);
                                listener.onResponse(params);
                            }, listener::onFailure));
                } else {
                    params.put("stop", false);
                    params.put("working", workingNodeName);
                }
            } else {
                params.put("found", false);
                params.put("stop", false);
                params.put("working", "");
            }
            if (params.containsKey("stop")) {
                listener.onResponse(params);
            }
        }, listener::onFailure));
    }

    public void getIndexerInfos(final int from, final int size, final ActionListener<Map<String, Object>> listener) {
        client.prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setFrom(from).setSize(size).execute(wrap(res -> {
            final Map<String, Object> params = new HashMap<>();
            params.put("took_in_millis", res.getTookInMillis());
            params.put("indexers", Arrays.stream(res.getHits().getHits()).map(hit -> {
                return hit.getSource();
            }).toArray(n -> new Map[n]));
            listener.onResponse(params);
        }, listener::onFailure));
    }

    public void getIndexerInfo(final String index, final ActionListener<Map<String, Object>> listener) {
        client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
            final Map<String, Object> params = new HashMap<>();
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                params.putAll(source);
                params.put("found", true);
            } else {
                params.put("found", false);
            }
            listener.onResponse(params);
        }, listener::onFailure));
    }

    class Indexer implements Runnable {

        private String index;

        private Path path;

        private long filePosition;

        private long version;

        private volatile int errorCount = 0;

        @Override
        public void run() {
            client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
                if (res.isExists()) {
                    final Map<String, Object> source = res.getSourceAsMap();
                    final String workingNodeName = (String) source.get(NODE_NAME);
                    if (!nodeName().equals(workingNodeName)) {
                        logger.info("Stopped Indexer({}) because of working in [{}].", index, workingNodeName);
                        // end
                    } else {
                        final Number pos = (Number) source.get(FILE_POSITION);
                        if (pos == null) {
                            logger.error("Stopped Indexer({}). No file_position.", index);
                            // end: system error
                        } else {
                            filePosition = pos.longValue();
                            version = res.getVersion();
                            process(filePosition);
                            // continue
                        }
                    }
                } else {
                    logger.info("Stopped Indexer({}).", index);
                    // end
                }
            }, e -> {
                retryWithError("Indexer data is not found.", e);
                // retry
            }));
        }

        private void retryWithError(final String message, final Exception e) {
            errorCount++;
            if (errorCount > indexerRetryCount) {
                logger.error("Indexer(" + index + ")@" + errorCount + ": Failed to process " + path.toAbsolutePath(), e);
                if (indexerSkipErrorFile) {
                    processNext();
                }
            } else {
                logger.warn("Indexer(" + index + ")@" + errorCount + ": " + message, e);
                threadPool.schedule(indexerInterval, Names.GENERIC, this);
            }
        }

        private void process(final long filePosition) {
            path = dataPath.resolve(String.format(dataFileFormat, nodeName(), filePosition) + DATA_EXTENTION);
            if (path.toFile().exists()) {
                logger.info("[{}][{}] Indexing from {}", filePosition, version, path.toAbsolutePath());
                try {
                    processRequests(new InputStreamStreamInput(Files.newInputStream(path)));
                    // continue
                } catch (final IOException e) {
                    retryWithError("Failed to access " + path.toAbsolutePath(), e);
                    // retry
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} does not exist.", path.toAbsolutePath());
                }
                threadPool.schedule(indexerInterval, Names.GENERIC, this);
                // retry
            }
        }

        private void processRequests(final StreamInput streamInput) {
            try {
                if (streamInput.available() > 0) {
                    final short classType = streamInput.readShort();
                    switch (classType) {
                    case TYPE_DELETE:
                        processDeleteRequest(streamInput);
                        break;
                    case TYPE_DELETE_BY_QUERY:
                        processDeleteByQueryRequest(streamInput);
                        break;
                    case TYPE_INDEX:
                        processIndexRequest(streamInput);
                        break;
                    case TYPE_UPDATE:
                        processUpdateRequest(streamInput);
                        break;
                    case TYPE_UPDATE_BY_QUERY:
                        processUpdateByQueryRequest(streamInput);
                        break;
                    case TYPE_BULK:
                        processBulkRequest(streamInput);
                        break;
                    default:
                        throw new ElasticsearchException("Unknown request type: " + classType);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    logger.info("Finished to process {}", path.toAbsolutePath());

                    processNext();
                }
            } catch (final Exception e) {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to access streamInput.", e);
                // retry
            }
        }

        private void processNext() {
            final Map<String, Object> source = new HashMap<>();
            source.put(FILE_POSITION, filePosition + 1);
            source.put(TIMESTAMP, new Date());
            client.prepareUpdate(INDEX_NAME, TYPE_NAME, index).setVersion(version).setDoc(source).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                    .execute(wrap(res -> {
                        errorCount = 0;
                        threadPool.schedule(TimeValue.ZERO, Names.GENERIC, this);
                        // retry: success
                    }, e -> {
                        retryWithError("Failed to access streamInput.", e);
                        // retry
                    }));
        }

        private void processBulkRequest(final StreamInput streamInput) throws IOException {
            final BulkRequestBuilder builder = client.prepareBulk();
            final BulkRequest request = builder.request();
            request.readFrom(streamInput);
            builder.request().requests().stream().forEach(req -> {
                if (req instanceof DeleteRequest) {
                    ((DeleteRequest) req).index(index);
                } else if (req instanceof DeleteByQueryRequest) {
                    ((DeleteByQueryRequest) req).indices(index);
                } else if (req instanceof IndexRequest) {
                    ((IndexRequest) req).index(index);
                } else if (req instanceof UpdateRequest) {
                    ((UpdateRequest) req).index(index);
                } else if (req instanceof UpdateByQueryRequest) {
                    ((UpdateByQueryRequest) req).indices(index);
                } else {
                    throw new ElasticsearchException("Unsupported request in bulk: " + req);
                }
            });
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to process (" + request + ")", e);
                // retry
            }));
        }

        private void processUpdateByQueryRequest(final StreamInput streamInput) throws IOException {
            final UpdateByQueryRequestBuilder builder = client.prepareExecute(UpdateByQueryAction.INSTANCE);
            final UpdateByQueryRequest request = builder.request();
            request.readFrom(streamInput);
            request.indices(index);
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to update (" + request + ")", e);
                // retry
            }));
        }

        private void processUpdateRequest(final StreamInput streamInput) throws IOException {
            final UpdateRequestBuilder builder = client.prepareUpdate();
            final UpdateRequest request = builder.request();
            request.readFrom(streamInput);
            request.index(index);
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to update (" + request + ")", e);
                // retry
            }));
        }

        private void processIndexRequest(final StreamInput streamInput) throws IOException {
            final IndexRequestBuilder builder = client.prepareIndex();
            final IndexRequest request = builder.request();
            request.readFrom(streamInput);
            request.index(index);
            builder.execute(wrap(res -> {
                processRequests(streamInput);
            }, e -> {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to index (" + request + ")", e);
                // retry
            }));
        }

        private void processDeleteByQueryRequest(final StreamInput streamInput) throws IOException {
            final DeleteByQueryRequestBuilder builder = client.prepareExecute(DeleteByQueryAction.INSTANCE);
            final DeleteByQueryRequest request = builder.request();
            request.readFrom(streamInput);
            request.indices(index);
            builder.execute(wrap(res -> {
                processRequests(streamInput);
            }, e -> {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to delete (" + request + ")", e);
                // retry
            }));
        }

        private void processDeleteRequest(final StreamInput streamInput) throws IOException {
            final DeleteRequestBuilder builder = client.prepareDelete();
            final DeleteRequest request = builder.request();
            request.readFrom(streamInput);
            request.index(index);
            builder.execute(wrap(res -> {
                processRequests(streamInput);
            }, e -> {
                IOUtils.closeQuietly(streamInput);
                retryWithError("Failed to delete (" + request + ")", e);
                // retry
            }));
        }
    }
}
