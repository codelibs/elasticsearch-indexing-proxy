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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin.PluginComponent;
import org.codelibs.elasticsearch.idxproxy.action.PingRequest;
import org.codelibs.elasticsearch.idxproxy.action.PingRequestHandler;
import org.codelibs.elasticsearch.idxproxy.action.PingResponse;
import org.codelibs.elasticsearch.idxproxy.action.ProxyActionFilter;
import org.codelibs.elasticsearch.idxproxy.action.WriteRequest;
import org.codelibs.elasticsearch.idxproxy.action.WriteRequestHandler;
import org.codelibs.elasticsearch.idxproxy.action.WriteResponse;
import org.codelibs.elasticsearch.idxproxy.stream.IndexingProxyStreamInput;
import org.codelibs.elasticsearch.idxproxy.stream.IndexingProxyStreamOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

public class IndexingProxyService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private static final String FILE_ID = "file_id";

    public static final String ACTION_IDXPROXY_PING = "internal:indices/idxproxy/ping";

    public static final String ACTION_IDXPROXY_WRITE = "internal:indices/idxproxy/write";

    private static final String DOC_TYPE = "doc_type";

    private static final String TIMESTAMP = "@timestamp";

    private static final String FILE_POSITION = "file_position";

    private static final String NODE_NAME = "node_name";

    private static final String FILE_MAPPING_JSON = "idxproxy/mapping.json";

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

    public static final Setting<TimeValue> SETTING_INXPROXY_SENDER_INTERVAL =
            Setting.timeSetting("idxproxy.sender.interval", TimeValue.timeValueSeconds(30), Property.NodeScope);

    public static final Setting<Integer> SETTING_INXPROXY_SENDER_RETRY_COUNT =
            Setting.intSetting("idxproxy.sender.retry_count", 10, Property.NodeScope);

    public static final Setting<Integer> SETTING_INXPROXY_SENDER_REQUEST_RETRY_COUNT =
            Setting.intSetting("idxproxy.sender.request.retry_count", 3, Property.NodeScope);

    public static final Setting<Boolean> SETTING_INXPROXY_SENDER_SKIP_ERROR_FILE =
            Setting.boolSetting("idxproxy.sender.skip.error_file", true, Property.NodeScope);

    public static final Setting<TimeValue> SETTING_INXPROXY_SENDER_ALIVE_TIME =
            Setting.timeSetting("idxproxy.sender.alive_time", TimeValue.timeValueMinutes(10), Property.NodeScope);

    public static final Setting<TimeValue> SETTING_INXPROXY_MONITOR_INTERVAL =
            Setting.timeSetting("idxproxy.monitor.interval", TimeValue.timeValueMinutes(1), Property.NodeScope);

    public static final Setting<Integer> SETTING_INXPROXY_WRITER_RETRY_COUNT =
            Setting.intSetting("idxproxy.writer.retry_count", 10, Property.NodeScope);

    public static final Setting<List<String>> SETTING_INXPROXY_SENDER_NODES =
            Setting.listSetting("idxproxy.sender_nodes", Collections.emptyList(), s -> s.trim(), Property.NodeScope);

    public static final Setting<List<String>> SETTING_INXPROXY_WRITE_NODES =
            Setting.listSetting("idxproxy.writer_nodes", Collections.emptyList(), s -> s.trim(), Property.NodeScope);

    public static final Setting<Boolean> SETTING_INXPROXY_FLUSH_PER_DOC =
            Setting.boolSetting("idxproxy.flush_per_doc", true, Property.NodeScope);

    private final TransportService transportService;

    private final Client client;

    private final ClusterService clusterService;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final ThreadPool threadPool;

    private final Path dataPath;

    private volatile IndexingProxyStreamOutput streamOutput;

    private volatile String fileId;

    private final Set<String> targetIndexSet;

    private final long dataFileSize;

    private final String dataFileFormat;

    private final TimeValue senderInterval;

    private final int senderRetryCount;

    private final boolean senderSkipErrorFile;

    private final int senderRequestRetryCount;

    private final boolean flushPerDoc;

    private final TimeValue senderAliveTime;

    private final TimeValue monitorInterval;

    private final List<String> senderNodes;

    private final List<String> writerNodes;

    private final int writerRetryCount;

    private final Map<String, DocSender> docSenderMap = new ConcurrentHashMap<>();

    private final AtomicBoolean isMasterNode = new AtomicBoolean(false);

    @Inject
    public IndexingProxyService(final Settings settings, final Environment env, final Client client, final ClusterService clusterService,
            final TransportService transportService, final NamedWriteableRegistry namedWriteableRegistry, final ThreadPool threadPool,
            final ActionFilters filters, final PluginComponent pluginComponent) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.threadPool = threadPool;

        final String dataPathStr = SETTING_INXPROXY_DATA_PATH.get(settings);
        if (dataPathStr == null || dataPathStr.length() == 0) {
            dataPath = env.dataFiles()[0];
        } else {
            dataPath = Paths.get(dataPathStr);
        }

        final String dataFileFormatStr = SETTING_INXPROXY_DATA_FILE_FORMAT.get(settings);
        if (dataFileFormatStr == null || dataFileFormatStr.length() == 0) {
            dataFileFormat = "%019d";
        } else {
            dataFileFormat = dataFileFormatStr;
        }

        dataFileSize = SETTING_INXPROXY_DATA_FILE_SIZE.get(settings).getBytes();
        targetIndexSet = SETTING_INXPROXY_TARGET_INDICES.get(settings).stream().collect(Collectors.toSet());
        senderInterval = SETTING_INXPROXY_SENDER_INTERVAL.get(settings);
        senderRetryCount = SETTING_INXPROXY_SENDER_RETRY_COUNT.get(settings);
        senderRequestRetryCount = SETTING_INXPROXY_SENDER_REQUEST_RETRY_COUNT.get(settings);
        senderSkipErrorFile = SETTING_INXPROXY_SENDER_SKIP_ERROR_FILE.get(settings);
        flushPerDoc = SETTING_INXPROXY_FLUSH_PER_DOC.get(settings);
        senderAliveTime = SETTING_INXPROXY_SENDER_ALIVE_TIME.get(settings);
        monitorInterval = SETTING_INXPROXY_MONITOR_INTERVAL.get(settings);
        senderNodes = SETTING_INXPROXY_SENDER_NODES.get(settings);
        writerNodes = SETTING_INXPROXY_WRITE_NODES.get(settings);
        writerRetryCount = SETTING_INXPROXY_WRITER_RETRY_COUNT.get(settings);

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof ProxyActionFilter) {
                ((ProxyActionFilter) filter).setIndexingProxyService(this);
            }
        }

        clusterService.addLocalNodeMasterListener(this);

        transportService.registerRequestHandler(ACTION_IDXPROXY_PING, PingRequest::new, ThreadPool.Names.GENERIC,
                new PingRequestHandler(this));
        transportService.registerRequestHandler(ACTION_IDXPROXY_WRITE, WriteRequest::new, ThreadPool.Names.GENERIC,
                new WriteRequestHandler<>(this));

        pluginComponent.setIndexingProxyService(this);
    }

    @Override
    public void onMaster() {
        isMasterNode.set(true);
        threadPool.schedule(monitorInterval, Names.GENERIC, new Monitor());
    }

    @Override
    public void offMaster() {
        isMasterNode.set(false);
    }

    class Monitor implements Runnable {

        private String getOtherNode(final String nodeName, final Map<String, DiscoveryNode> nodeMap) {
            final List<String> list = new ArrayList<>();
            for (final String name : senderNodes) {
                if (nodeMap.containsKey(name)) {
                    list.add(name);
                }
            }
            list.remove(nodeName);
            if (list.isEmpty()) {
                return "";
            } else if (list.size() == 1) {
                return list.get(0);
            }
            Collections.shuffle(list);
            return list.get(0);
        }

        @Override
        public void run() {
            if (!isMasterNode.get()) {
                logger.info("Stopped Monitor in " + nodeName());
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Processing Monitor in " + nodeName());
            }

            final Map<String, DiscoveryNode> nodeMap = new HashMap<>();
            clusterService.state().nodes().getNodes().valuesIt().forEachRemaining(node -> nodeMap.put(node.getName(), node));

            try {
                client.prepareSearch(INDEX_NAME).setTypes(TYPE_NAME).setQuery(QueryBuilders.termQuery(DOC_TYPE, "index")).setSize(1000)
                        .execute(wrap(response -> {
                            Arrays.stream(response.getHits().getHits()).forEach(hit -> {
                                final String index = hit.getId();
                                final Map<String, Object> source = hit.getSource();
                                final String nodeName = (String) source.get(NODE_NAME);
                                final DiscoveryNode node = nodeMap.get(nodeName);
                                if (node == null) {
                                    final String otherNode = getOtherNode(nodeName, nodeMap);
                                    updateDocSenderInfo(index, otherNode, 0, wrap(res -> {
                                        if (otherNode.length() == 0) {
                                            logger.info("Remove " + nodeName + " from DocSender(" + index + ")");
                                        } else {
                                            logger.info("Replace " + nodeName + " with " + otherNode + " for DocSender(" + index + ")");
                                        }
                                    }, e -> {
                                        logger.warn("Failed to remove " + nodeName + " from DocSender(" + index + ")", e);
                                    }));
                                } else {
                                    transportService.sendRequest(node, ACTION_IDXPROXY_PING, new PingRequest(index),
                                            new TransportResponseHandler<PingResponse>() {

                                                @Override
                                                public PingResponse newInstance() {
                                                    return new PingResponse();
                                                }

                                                @Override
                                                public void handleResponse(final PingResponse response) {
                                                    if (response.isAcknowledged() && !response.isFound()) {
                                                        logger.info("Started DocSender(" + index + ") in " + nodeName);
                                                    } else if (logger.isDebugEnabled()) {
                                                        logger.debug("DocSender(" + index + ") is working in " + nodeName);
                                                    }
                                                }

                                                @Override
                                                public void handleException(final TransportException e) {
                                                    logger.warn("Failed to start DocSender(" + index + ") in " + nodeName, e);
                                                    final String otherNode = getOtherNode(nodeName, nodeMap);
                                                    updateDocSenderInfo(index, otherNode, 0, wrap(res -> {
                                                        if (otherNode.length() == 0) {
                                                            logger.info("Remove " + nodeName + " from DocSender(" + index + ")");
                                                        } else {
                                                            logger.info("Replace " + nodeName + " with " + otherNode + " for DocSender("
                                                                    + index + ")");
                                                        }
                                                    }, e1 -> {
                                                        logger.warn("Failed to remove " + nodeName + " from DocSender(" + index + ")", e1);
                                                    }));
                                                }

                                                @Override
                                                public String executor() {
                                                    return ThreadPool.Names.GENERIC;
                                                }
                                            });
                                }
                            });
                            threadPool.schedule(monitorInterval, Names.GENERIC, this);
                        }, e -> {
                            if (e instanceof IndexNotFoundException) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug(INDEX_NAME + " is not found.", e);
                                }
                            } else {
                                logger.warn("Failed to process Monitor(" + nodeName() + ")", e);
                            }
                            threadPool.schedule(monitorInterval, Names.GENERIC, this);
                        }));
            } catch (final IndexNotFoundException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(INDEX_NAME + " is not found.", e);
                }
                threadPool.schedule(monitorInterval, Names.GENERIC, this);
            } catch (final Exception e) {
                logger.warn("Failed to process Monitor(" + nodeName() + ")", e);
                threadPool.schedule(monitorInterval, Names.GENERIC, this);
            }
        }
    }

    @Override
    public String executorName() {
        return Names.GENERIC;
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
                                    }, e -> logger.error("Failed to create .idxproxy.", e)));
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
        client.admin().indices().prepareExists(INDEX_NAME).execute(wrap(response -> {
            if (response.isExists()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(INDEX_NAME + " exists.");
                }
                listener.onResponse(response);
            } else {
                createIndex(listener);
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                createIndex(listener);
            } else {
                listener.onFailure(e);
            }
        }));
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
                    .addMapping(TYPE_NAME, source, XContentFactory.xContentType(source))
                    .execute(wrap(response -> waitForIndex(listener), listener::onFailure));
        } catch (final IOException e) {
            listener.onFailure(e);
        }
    }

    private void waitForIndex(final ActionListener<ActionResponse> listener) {
        client.admin().cluster().prepareHealth(INDEX_NAME).setWaitForYellowStatus()
                .execute(wrap(listener::onResponse, listener::onFailure));
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

    private <Response extends ActionResponse> void createStreamOutput(final ActionListener<Response> listener, final boolean create) {
        final String oldFileId = fileId;
        final Map<String, Object> source = new HashMap<>();
        source.put(DOC_TYPE, FILE_ID);
        source.put(NODE_NAME, nodeName());
        client.prepareIndex(INDEX_NAME, TYPE_NAME, FILE_ID).setCreate(create).setSource(source).execute(wrap(res -> {
            synchronized (this) {
                if (oldFileId == null || oldFileId.equals(fileId)) {
                    if (streamOutput != null) {
                        closeStreamOutput();
                    }

                    fileId = String.format(dataFileFormat, res.getVersion());
                    final Path outputPath = dataPath.resolve(fileId + WORKING_EXTENTION);
                    streamOutput = AccessController.doPrivileged((PrivilegedAction<IndexingProxyStreamOutput>) () -> {
                        try {
                            return new IndexingProxyStreamOutput(Files.newOutputStream(outputPath));
                        } catch (final IOException e) {
                            throw new ElasticsearchException("Could not open " + outputPath, e);
                        }
                    });
                    logger.info("Opening " + outputPath.toAbsolutePath());
                }
            }

            listener.onResponse(null);
        }, listener::onFailure));
    }

    private void closeStreamOutput() {
        if (logger.isDebugEnabled()) {
            logger.debug("[" + fileId + "] Closing streamOutput.");
        }
        try {
            streamOutput.flush();
            streamOutput.close();
        } catch (final IOException e) {
            throw new ElasticsearchException("Failed to close streamOutput.", e);
        }

        final Path source = dataPath.resolve(fileId + WORKING_EXTENTION);
        final Path target = dataPath.resolve(fileId + DATA_EXTENTION);
        final Path outputPath = AccessController.doPrivileged((PrivilegedAction<Path>) () -> {
            try {
                return Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (final IOException e) {
                throw new ElasticsearchException("Failed to move " + source.toAbsolutePath() + " to " + target.toAbsolutePath(), e);
            }
        });
        logger.info("Closed " + outputPath.toAbsolutePath());
    }

    public <Response extends ActionResponse> void renew(final ActionListener<Response> listener) {
        if (streamOutput != null && streamOutput.getByteCount() == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("No requests in file. Skipped renew action.");
            }
            listener.onResponse(null);
        } else {
            createStreamOutput(listener, false);
        }
    }

    private void randomWait() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(1000L));
        } catch (final InterruptedException e) {
            // ignore
        }
    }

    public <Request extends ActionRequest, Response extends ActionResponse> void write(final Request request,
            final ActionListener<Response> listener) {
        write(request, listener, 0);
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void write(final Request request,
            final ActionListener<Response> listener, final int tryCount) {
        if (logger.isDebugEnabled()) {
            logger.debug("Writing request " + request.toString());
        }

        client.prepareGet(INDEX_NAME, TYPE_NAME, FILE_ID).execute(wrap(res -> {
            if (!res.isExists()) {
                createStreamOutput(wrap(response -> {
                    write(request, listener, tryCount + 1);
                }, listener::onFailure), true);
            } else {
                final Map<String, Object> source = res.getSourceAsMap();
                final String nodeName = (String) source.get(NODE_NAME);
                if (nodeName().equals(nodeName)) {
                    writeOnLocal(request, listener);
                } else {
                    writeOnRemote(nodeName, res.getVersion(), request, listener, tryCount);
                }
            }
        }, e -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to get file_id.", e);
            }
            if (tryCount >= writerRetryCount) {
                listener.onFailure(e);
            } else {
                randomWait();
                write(request, listener, tryCount + 1);
            }
        }));
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void writeOnRemote(final String nodeName, final long version,
            final Request request, final ActionListener<Response> listener, final int tryCount) {
        final List<DiscoveryNode> nodeList = new ArrayList<>();
        clusterService.state().nodes().getNodes().valuesIt().forEachRemaining(node -> {
            if (writerNodes.isEmpty() || writerNodes.contains(node.getName())) {
                nodeList.add(node);
            }
        });

        int pos = -1;
        for (int i = 0; i < nodeList.size(); i++) {
            if (nodeList.get(i).getName().equals(nodeName)) {
                pos = i;
                break;
            }
        }
        if (pos == -1) {
            throw new ElasticsearchException("Writer nodes are not found.");
        }

        final int nodeIdx = pos;
        transportService.sendRequest(nodeList.get(nodeIdx), ACTION_IDXPROXY_WRITE, new WriteRequest<>(request),
                new TransportResponseHandler<WriteResponse>() {

                    @Override
                    public WriteResponse newInstance() {
                        return new WriteResponse();
                    }

                    @Override
                    public void handleResponse(final WriteResponse response) {
                        if (response.isAcknowledged()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Stored request in " + nodeName);
                            }
                            listener.onResponse(null);
                        } else {
                            throw new ElasticsearchException("Failed to store request: " + request);
                        }
                    }

                    @Override
                    public void handleException(final TransportException e) {
                        if (tryCount >= writerRetryCount) {
                            listener.onFailure(e);
                        } else {
                            final DiscoveryNode nextNode = nodeList.get((nodeIdx + 1) % nodeList.size());
                            if (nextNode.getName().equals(nodeName)) {
                                listener.onFailure(e);
                            } else {
                                final Map<String, Object> source = new HashMap<>();
                                source.put(NODE_NAME, nextNode.getName());
                                source.put(TIMESTAMP, new Date());
                                client.prepareUpdate(INDEX_NAME, TYPE_NAME, FILE_ID).setVersion(version).setDoc(source)
                                        .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute(wrap(res -> {
                                            write(request, listener, tryCount + 1);
                                        }, ex -> {
                                            if (logger.isDebugEnabled()) {
                                                logger.debug("Failed to update file_id.", ex);
                                            }
                                            write(request, listener, tryCount + 1);
                                        }));
                            }
                        }
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                });

    }

    public <Request extends ActionRequest, Response extends ActionResponse> void writeOnLocal(final Request request,
            final ActionListener<Response> listener) {
        final ActionListener<Response> next = wrap(res -> {
            final short classType = getClassType(request);
            if (classType > 0) {
                synchronized (this) {
                    streamOutput.writeShort(classType);
                    request.writeTo(streamOutput);
                    if (flushPerDoc) {
                        streamOutput.flush();
                    }
                }
            } else {
                throw new ElasticsearchException("Unknown request: " + request);
            }
            listener.onResponse(res);
        }, listener::onFailure);

        if (streamOutput == null || streamOutput.getByteCount() > dataFileSize) {
            createStreamOutput(next, false);
        } else {
            next.onResponse(null);
        }
    }

    private <Request extends ActionRequest> short getClassType(final Request request) {
        if (DeleteRequest.class.isInstance(request)) {
            return TYPE_DELETE;
        } else if (DeleteByQueryRequest.class.isInstance(request)) {
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
        if (logger.isDebugEnabled()) {
            logger.debug("Unknown request: " + request);
        }
        return 0;
    }

    public boolean isTargetIndex(final String index) {
        return targetIndexSet.contains(index);
    }

    public void startDocSender(final String index, final long filePosition, final ActionListener<Map<String, Object>> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting DocSender(" + index + ")");
        }
        client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get(NODE_NAME);
                if (Strings.isBlank(workingNodeName)) {
                    listener.onFailure(new ElasticsearchException("DocSender is working in " + workingNodeName));
                } else {
                    final Number pos = (Number) source.get(FILE_POSITION);
                    final long value = pos == null ? 1 : pos.longValue();
                    launchDocSender(index, filePosition > 0 ? filePosition : value, res.getVersion(), listener);
                }
            } else {
                launchDocSender(index, filePosition > 0 ? filePosition : 1, 0, listener);
            }
        }, listener::onFailure));
    }

    private void launchDocSender(final String index, final long filePosition, final long version,
            final ActionListener<Map<String, Object>> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Launching DocSender(" + index + ")");
        }
        final Map<String, Object> source = new HashMap<>();
        source.put(NODE_NAME, nodeName());
        source.put(FILE_POSITION, filePosition);
        source.put(TIMESTAMP, new Date());
        source.put(DOC_TYPE, "index");
        final IndexRequestBuilder builder =
                client.prepareIndex(INDEX_NAME, TYPE_NAME, index).setSource(source).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        if (version > 0) {
            builder.setVersion(version);
        } else {
            builder.setCreate(true);
        }
        builder.execute(wrap(res -> {
            if (res.getResult() == Result.CREATED || res.getResult() == Result.UPDATED) {
                final DocSender sender = new DocSender(index);
                final DocSender oldSender = docSenderMap.putIfAbsent(index, sender);
                if (oldSender != null) {
                    oldSender.terminate();
                }
                threadPool.schedule(TimeValue.ZERO, Names.GENERIC, sender);
                listener.onResponse(source);
            } else {
                listener.onFailure(new ElasticsearchException("Failed to update .idxproxy index: " + res));
            }
        }, listener::onFailure));
    }

    public void stopDocSender(final String index, final ActionListener<Map<String, Object>> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Stopping DocSender(" + index + ")");
        }
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
                    updateDocSenderInfo(index, "", version, wrap(res2 -> {
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

    private void updateDocSenderInfo(final String index, final String node, final long version,
            final ActionListener<UpdateResponse> listener) {
        final Map<String, Object> newSource = new HashMap<>();
        newSource.put(NODE_NAME, node);
        newSource.put(TIMESTAMP, new Date());
        final UpdateRequestBuilder builder = client.prepareUpdate(INDEX_NAME, TYPE_NAME, index);
        if (version > 0) {
            builder.setVersion(version);
        }
        builder.setDoc(newSource).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute(listener);
    }

    public void getDocSenderInfos(final int from, final int size, final ActionListener<Map<String, Object>> listener) {
        client.prepareSearch(INDEX_NAME).setQuery(QueryBuilders.termQuery(DOC_TYPE, "index")).setFrom(from).setSize(size)
                .execute(wrap(res -> {
                    final Map<String, Object> params = new HashMap<>();
                    params.put("took_in_millis", res.getTookInMillis());
                    params.put("senders", Arrays.stream(res.getHits().getHits()).map(hit -> {
                        final Map<String, Object> source = new HashMap<>();
                        source.putAll(hit.getSource());
                        final DocSender docSender = docSenderMap.get(hit.getId());
                        source.put("running", docSender != null && docSender.isRunning());
                        return source;
                    }).toArray(n -> new Map[n]));
                    listener.onResponse(params);
                }, listener::onFailure));
    }

    public void getDocSenderInfo(final String index, final ActionListener<Map<String, Object>> listener) {
        client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
            final Map<String, Object> params = new HashMap<>();
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                params.putAll(source);
                params.put("found", true);
                final DocSender docSender = docSenderMap.get(res.getId());
                params.put("running", docSender != null && docSender.isRunning());
            } else {
                params.put("found", false);
            }
            listener.onResponse(params);
        }, listener::onFailure));
    }

    public boolean isRunning(final String index) {
        final DocSender docSender = docSenderMap.get(index);
        return docSender != null && docSender.isRunning();
    }

    class DocSender implements Runnable {

        private final String index;

        private Path path;

        private long filePosition;

        private long version;

        private volatile int errorCount = 0;

        private volatile int requestErrorCount = 0;

        private volatile long heartbeat = System.currentTimeMillis();

        private volatile boolean terminated = false;

        public DocSender(final String index) {
            this.index = index;
        }

        public void terminate() {
            if (logger.isDebugEnabled()) {
                logger.debug("Terminating DocIndexer(" + index + ")");
            }
            terminated = true;
        }

        public boolean isRunning() {
            return System.currentTimeMillis() - heartbeat < senderAliveTime.getMillis();
        }

        @Override
        public void run() {
            heartbeat = System.currentTimeMillis();
            if (terminated) {
                logger.warn("Terminate DocIndexer(" + index + ")");
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Running DocSender(" + index + ")");
            }
            client.prepareGet(INDEX_NAME, TYPE_NAME, index).execute(wrap(res -> {
                if (res.isExists()) {
                    final Map<String, Object> source = res.getSourceAsMap();
                    final String workingNodeName = (String) source.get(NODE_NAME);
                    if (!nodeName().equals(workingNodeName)) {
                        logger.info("Stopped DocSender({}) because of working in [{}].", index, workingNodeName);
                        docSenderMap.computeIfPresent(index, (k, v) -> v == this ? null : v);
                        // end
                    } else {
                        final Number pos = (Number) source.get(FILE_POSITION);
                        if (pos == null) {
                            logger.error("Stopped DocSender({}). No file_position.", index);
                            docSenderMap.computeIfPresent(index, (k, v) -> v == this ? null : v);
                            // end: system error
                        } else {
                            filePosition = pos.longValue();
                            version = res.getVersion();
                            process(filePosition);
                            // continue
                        }
                    }
                } else {
                    logger.info("Stopped DocSender({}).", index);
                    docSenderMap.computeIfPresent(index, (k, v) -> v == this ? null : v);
                    // end
                }
            }, e -> {
                retryWithError("DocSender data is not found.", e);
                // retry
            }));
        }

        private void retryWithError(final String message, final Exception e) {
            errorCount++;
            if (errorCount > senderRetryCount) {
                logger.error("DocSender(" + index + ")@" + errorCount + ": Failed to process " + path.toAbsolutePath(), e);
                if (senderSkipErrorFile) {
                    processNext();
                }
            } else {
                logger.warn("DocSender(" + index + ")@" + errorCount + ": " + message, e);
                threadPool.schedule(senderInterval, Names.GENERIC, this);
            }
        }

        private void process(final long filePosition) {
            if (logger.isDebugEnabled()) {
                logger.debug("DocSender(" + index + ") processes " + filePosition);
            }
            path = dataPath.resolve(String.format(dataFileFormat, filePosition) + DATA_EXTENTION);
            if (existsFile(path)) {
                logger.info("[{}][{}] Indexing from {}", filePosition, version, path.toAbsolutePath());
                try {
                    processRequests(AccessController.doPrivileged((PrivilegedAction<IndexingProxyStreamInput>) () -> {
                        try {
                            return new IndexingProxyStreamInput(Files.newInputStream(path), namedWriteableRegistry);
                        } catch (final IOException e) {
                            throw new ElasticsearchException("Failed to read " + path.toAbsolutePath(), e);
                        }
                    }));
                    // continue
                } catch (final Exception e) {
                    retryWithError("Failed to access " + path.toAbsolutePath(), e);
                    // retry
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} does not exist.", path.toAbsolutePath());
                }
                threadPool.schedule(senderInterval, Names.GENERIC, this);
                // retry
            }
        }

        private void processRequests(final StreamInput streamInput) {
            heartbeat = System.currentTimeMillis();
            if (terminated) {
                IOUtils.closeQuietly(streamInput);
                logger.warn("Terminate DocIndexer(" + index + ")");
                return;
            }
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("DocSender(" + index + ") is processing requests.");
                }
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
            if (logger.isDebugEnabled()) {
                logger.debug("DocSender(" + index + ") moves next files.");
            }
            final Map<String, Object> source = new HashMap<>();
            source.put(FILE_POSITION, filePosition + 1);
            source.put(TIMESTAMP, new Date());
            client.prepareUpdate(INDEX_NAME, TYPE_NAME, index).setVersion(version).setDoc(source).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                    .execute(wrap(res -> {
                        errorCount = 0;
                        requestErrorCount = 0;
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
            executeBulkRequest(streamInput, builder);
        }

        private void executeBulkRequest(final StreamInput streamInput, final BulkRequestBuilder builder) {
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                if (senderRequestRetryCount >= 0) {
                    if (requestErrorCount > senderRequestRetryCount) {
                        logger.error("[" + requestErrorCount + "] Failed to process (" + builder.request() + ")", e);
                        requestErrorCount = 0;
                        processRequests(streamInput);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[" + requestErrorCount + "] Failed to process (" + builder.request() + ")", e);
                        }
                        requestErrorCount++;
                        executeBulkRequest(streamInput, builder);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    retryWithError("Failed to process (" + builder.request() + ")", e);
                    // retry
                }
            }));
        }

        private void processUpdateByQueryRequest(final StreamInput streamInput) throws IOException {
            final UpdateByQueryRequestBuilder builder = client.prepareExecute(UpdateByQueryAction.INSTANCE);
            final UpdateByQueryRequest request = builder.request();
            request.readFrom(streamInput);
            request.indices(index);
            executeUpdateByQueryRequest(streamInput, builder);
        }

        private void executeUpdateByQueryRequest(final StreamInput streamInput, final UpdateByQueryRequestBuilder builder) {
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                if (senderRequestRetryCount >= 0) {
                    if (requestErrorCount > senderRequestRetryCount) {
                        logger.error("[" + requestErrorCount + "] Failed to update (" + builder.request() + ")", e);
                        requestErrorCount = 0;
                        processRequests(streamInput);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[" + requestErrorCount + "] Failed to update (" + builder.request() + ")", e);
                        }
                        requestErrorCount++;
                        executeUpdateByQueryRequest(streamInput, builder);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    retryWithError("Failed to update (" + builder.request() + ")", e);
                    // retry
                }
            }));
        }

        private void processUpdateRequest(final StreamInput streamInput) throws IOException {
            final UpdateRequestBuilder builder = client.prepareUpdate();
            final UpdateRequest request = builder.request();
            request.readFrom(streamInput);
            request.index(index);
            executeUpdateRequest(streamInput, builder);
        }

        private void executeUpdateRequest(final StreamInput streamInput, final UpdateRequestBuilder builder) {
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                if (senderRequestRetryCount >= 0) {
                    if (requestErrorCount > senderRequestRetryCount) {
                        logger.error("[" + requestErrorCount + "] Failed to update (" + builder.request() + ")", e);
                        requestErrorCount = 0;
                        processRequests(streamInput);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[" + requestErrorCount + "] Failed to update (" + builder.request() + ")", e);
                        }
                        requestErrorCount++;
                        executeUpdateRequest(streamInput, builder);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    retryWithError("Failed to update (" + builder.request() + ")", e);
                    // retry
                }
            }));
        }

        private void processIndexRequest(final StreamInput streamInput) throws IOException {
            final IndexRequestBuilder builder = client.prepareIndex();
            final IndexRequest request = builder.request();
            request.readFrom(streamInput);
            request.index(index);
            executeIndexRequest(streamInput, builder);
        }

        private void executeIndexRequest(final StreamInput streamInput, final IndexRequestBuilder builder) {
            builder.execute(wrap(res -> {
                processRequests(streamInput);
                // continue
            }, e -> {
                if (senderRequestRetryCount >= 0) {
                    if (requestErrorCount > senderRequestRetryCount) {
                        logger.error("[" + requestErrorCount + "] Failed to index (" + builder.request() + ")", e);
                        requestErrorCount = 0;
                        processRequests(streamInput);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[" + requestErrorCount + "] Failed to index (" + builder.request() + ")", e);
                        }
                        requestErrorCount++;
                        executeIndexRequest(streamInput, builder);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    retryWithError("Failed to index (" + builder.request() + ")", e);
                    // retry
                }
            }));
        }

        private void processDeleteByQueryRequest(final StreamInput streamInput) throws IOException {
            final DeleteByQueryRequestBuilder builder = client.prepareExecute(DeleteByQueryAction.INSTANCE);
            final DeleteByQueryRequest request = builder.request();
            request.readFrom(streamInput);
            request.indices(index);
            executeDeleteByQueryRequest(streamInput, builder);
        }

        private void executeDeleteByQueryRequest(final StreamInput streamInput, final DeleteByQueryRequestBuilder builder) {
            builder.execute(wrap(res -> {
                processRequests(streamInput);
            }, e -> {
                if (senderRequestRetryCount >= 0) {
                    if (requestErrorCount > senderRequestRetryCount) {
                        logger.error("[" + requestErrorCount + "] Failed to delete (" + builder.request() + ")", e);
                        requestErrorCount = 0;
                        processRequests(streamInput);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[" + requestErrorCount + "] Failed to delete (" + builder.request() + ")", e);
                        }
                        requestErrorCount++;
                        executeDeleteByQueryRequest(streamInput, builder);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    retryWithError("Failed to delete (" + builder.request() + ")", e);
                    // retry
                }
            }));
        }

        private void processDeleteRequest(final StreamInput streamInput) throws IOException {
            final DeleteRequestBuilder builder = client.prepareDelete();
            final DeleteRequest request = builder.request();
            request.readFrom(streamInput);
            request.index(index);
            executeDeleteRequest(streamInput, builder);
        }

        private void executeDeleteRequest(final StreamInput streamInput, final DeleteRequestBuilder builder) {
            builder.execute(wrap(res -> {
                requestErrorCount = 0;
                processRequests(streamInput);
            }, e -> {
                if (senderRequestRetryCount >= 0) {
                    if (requestErrorCount > senderRequestRetryCount) {
                        logger.error("[" + requestErrorCount + "] Failed to delete (" + builder.request() + ")", e);
                        requestErrorCount = 0;
                        processRequests(streamInput);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[" + requestErrorCount + "] Failed to delete (" + builder.request() + ")", e);
                        }
                        requestErrorCount++;
                        executeDeleteRequest(streamInput, builder);
                    }
                } else {
                    IOUtils.closeQuietly(streamInput);
                    retryWithError("Failed to delete (" + builder.request() + ")", e);
                    // retry
                }
            }));
        }

        private boolean existsFile(final Path p) {
            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                return p.toFile().exists();
            });
        }
    }
}
