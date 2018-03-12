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
import java.nio.file.attribute.FileTime;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin;
import org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin.PluginComponent;
import org.codelibs.elasticsearch.idxproxy.action.CreateRequest;
import org.codelibs.elasticsearch.idxproxy.action.CreateRequestHandler;
import org.codelibs.elasticsearch.idxproxy.action.CreateResponse;
import org.codelibs.elasticsearch.idxproxy.action.PingRequest;
import org.codelibs.elasticsearch.idxproxy.action.PingRequestHandler;
import org.codelibs.elasticsearch.idxproxy.action.PingResponse;
import org.codelibs.elasticsearch.idxproxy.action.ProxyActionFilter;
import org.codelibs.elasticsearch.idxproxy.action.WriteRequest;
import org.codelibs.elasticsearch.idxproxy.action.WriteRequestHandler;
import org.codelibs.elasticsearch.idxproxy.action.WriteResponse;
import org.codelibs.elasticsearch.idxproxy.sender.RequestSender;
import org.codelibs.elasticsearch.idxproxy.stream.IndexingProxyStreamInput;
import org.codelibs.elasticsearch.idxproxy.stream.IndexingProxyStreamOutput;
import org.codelibs.elasticsearch.idxproxy.util.FileAccessUtils;
import org.codelibs.elasticsearch.idxproxy.util.RequestUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

public class IndexingProxyService extends AbstractLifecycleComponent implements LocalNodeMasterListener {

    private static final String FILE_ID = "file_id";

    private static final String DOC_TYPE = "doc_type";

    private static final String FILE_MAPPING_JSON = "idxproxy/mapping.json";

    private static final String WORKING_EXTENTION = ".tmp";

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

    private final boolean flushPerDoc;

    private final TimeValue monitorInterval;

    private final List<String> senderNodes;

    private final List<String> writerNodes;

    private final int writerRetryCount;

    private final int numberOfReplicas;

    private final int numberOfShards;

    private final Map<String, RequestSender> docSenderMap = new ConcurrentHashMap<>();

    private final AtomicBoolean isMasterNode = new AtomicBoolean(false);

    private final Set<String> renewActions;

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

        final String dataPathStr = IndexingProxyPlugin.SETTING_INXPROXY_DATA_PATH.get(settings);
        if (dataPathStr == null || dataPathStr.length() == 0) {
            dataPath = env.dataFiles()[0];
        } else {
            dataPath = Paths.get(dataPathStr);
        }

        final String dataFileFormatStr = IndexingProxyPlugin.SETTING_INXPROXY_DATA_FILE_FORMAT.get(settings);
        if (dataFileFormatStr == null || dataFileFormatStr.length() == 0) {
            dataFileFormat = "%019d";
        } else {
            dataFileFormat = dataFileFormatStr;
        }

        dataFileSize = IndexingProxyPlugin.SETTING_INXPROXY_DATA_FILE_SIZE.get(settings).getBytes();
        targetIndexSet = IndexingProxyPlugin.SETTING_INXPROXY_TARGET_INDICES.get(settings).stream().collect(Collectors.toSet());
        flushPerDoc = IndexingProxyPlugin.SETTING_INXPROXY_FLUSH_PER_DOC.get(settings);
        monitorInterval = IndexingProxyPlugin.SETTING_INXPROXY_MONITOR_INTERVAL.get(settings);
        senderNodes = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_NODES.get(settings);
        writerNodes = IndexingProxyPlugin.SETTING_INXPROXY_WRITE_NODES.get(settings);
        writerRetryCount = IndexingProxyPlugin.SETTING_INXPROXY_WRITER_RETRY_COUNT.get(settings);
        numberOfReplicas = IndexingProxyPlugin.SETTING_INXPROXY_NUMBER_OF_REPLICAS.get(settings);
        numberOfShards = IndexingProxyPlugin.SETTING_INXPROXY_NUMBER_OF_SHARDS.get(settings);
        renewActions = IndexingProxyPlugin.SETTING_INXPROXY_RENEW_ACTIONS.get(settings).stream().collect(Collectors.toSet());

        for (final ActionFilter filter : filters.filters()) {
            if (filter instanceof ProxyActionFilter) {
                ((ProxyActionFilter) filter).setIndexingProxyService(this);
            }
        }

        clusterService.addLocalNodeMasterListener(this);

        transportService.registerRequestHandler(IndexingProxyPlugin.ACTION_IDXPROXY_CREATE, CreateRequest::new, ThreadPool.Names.GENERIC,
                new CreateRequestHandler(this));
        transportService.registerRequestHandler(IndexingProxyPlugin.ACTION_IDXPROXY_PING, PingRequest::new, ThreadPool.Names.GENERIC,
                new PingRequestHandler(this));
        transportService.registerRequestHandler(IndexingProxyPlugin.ACTION_IDXPROXY_WRITE, WriteRequest::new, ThreadPool.Names.GENERIC,
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
                client.prepareSearch(IndexingProxyPlugin.INDEX_NAME).setTypes(IndexingProxyPlugin.TYPE_NAME).setQuery(QueryBuilders.termQuery(DOC_TYPE, "index")).setSize(1000)
                        .execute(wrap(response -> {
                            checkSender(nodeMap, Arrays.asList(response.getHits().getHits()).iterator());
                        }, e -> {
                            if (e instanceof IndexNotFoundException) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug(IndexingProxyPlugin.INDEX_NAME + " is not found.", e);
                                }
                            } else {
                                logger.warn("Monitor(" + nodeName() + ") could not access " + IndexingProxyPlugin.INDEX_NAME, e);
                            }
                            threadPool.schedule(monitorInterval, Names.GENERIC, this);
                        }));
            } catch (final IndexNotFoundException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(IndexingProxyPlugin.INDEX_NAME + " is not found.", e);
                }
                threadPool.schedule(monitorInterval, Names.GENERIC, this);
            } catch (final Exception e) {
                logger.warn("Failed to process Monitor(" + nodeName() + ")", e);
                threadPool.schedule(monitorInterval, Names.GENERIC, this);
            }
        }

        private void checkSender(final Map<String, DiscoveryNode> nodeMap, final Iterator<SearchHit> hitIter) {
            if (!hitIter.hasNext()) {
                threadPool.schedule(monitorInterval, Names.GENERIC, this);
                return;
            }
            final SearchHit hit = hitIter.next();
            final String index = hit.getId();
            final Map<String, Object> source = hit.getSource();
            final String nodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
            if (Strings.isBlank(nodeName)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("RequestSender(" + index + ") is stopped");
                }
                checkSender(nodeMap, hitIter);
                return;
            }
            final DiscoveryNode node = nodeMap.get(nodeName);
            if (node == null) {
                final String otherNode = getOtherNode(nodeName, nodeMap);
                updateRequestSenderInfo(index, otherNode, 0, wrap(res -> {
                    if (otherNode.length() == 0) {
                        logger.info("Remove " + nodeName + " from RequestSender(" + index + ")");
                    } else {
                        logger.info("Replace " + nodeName + " with " + otherNode + " for RequestSender(" + index + ")");
                    }
                    checkSender(nodeMap, hitIter);
                }, e -> {
                    logger.warn("Failed to remove " + nodeName + " from RequestSender(" + index + ")", e);
                    checkSender(nodeMap, hitIter);
                }));
            } else {
                transportService.sendRequest(node, IndexingProxyPlugin.ACTION_IDXPROXY_PING, new PingRequest(index),
                        new TransportResponseHandler<PingResponse>() {

                            @Override
                            public PingResponse newInstance() {
                                return new PingResponse();
                            }

                            @Override
                            public void handleResponse(final PingResponse response) {
                                if (response.isAcknowledged() && !response.isFound()) {
                                    logger.info("Started RequestSender(" + index + ") in " + nodeName);
                                } else if (logger.isDebugEnabled()) {
                                    logger.debug("RequestSender(" + index + ") is working in " + nodeName);
                                }
                                checkSender(nodeMap, hitIter);
                            }

                            @Override
                            public void handleException(final TransportException e) {
                                logger.warn("Failed to start RequestSender(" + index + ") in " + nodeName, e);
                                final String otherNode = getOtherNode(nodeName, nodeMap);
                                updateRequestSenderInfo(index, otherNode, 0, wrap(res -> {
                                    if (otherNode.length() == 0) {
                                        logger.info("Remove " + nodeName + " from RequestSender(" + index + ")");
                                    } else {
                                        logger.info("Replace " + nodeName + " with " + otherNode + " for RequestSender(" + index + ")");
                                    }
                                    checkSender(nodeMap, hitIter);
                                }, e1 -> {
                                    logger.warn("Failed to remove " + nodeName + " from RequestSender(" + index + ")", e1);
                                    checkSender(nodeMap, hitIter);
                                }));
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.GENERIC;
                            }
                        });
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
                    client.admin().cluster().prepareHealth(IndexingProxyPlugin.INDEX_NAME).setWaitForYellowStatus()
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
        client.admin().indices().prepareExists(IndexingProxyPlugin.INDEX_NAME).execute(wrap(response -> {
            if (response.isExists()) {
                if (logger.isDebugEnabled()) {
                    logger.debug(IndexingProxyPlugin.INDEX_NAME + " exists.");
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
                    .field("number_of_shards", numberOfShards)//
                    .field("number_of_replicas", numberOfReplicas)//
                    .endObject()//
                    .endObject();
            client.admin().indices().prepareCreate(IndexingProxyPlugin.INDEX_NAME).setSettings(settingsBuilder)
                    .addMapping(IndexingProxyPlugin.TYPE_NAME, source, XContentFactory.xContentType(source))
                    .execute(wrap(response -> waitForIndex(listener), listener::onFailure));
        } catch (final IOException e) {
            listener.onFailure(e);
        }
    }

    private void waitForIndex(final ActionListener<ActionResponse> listener) {
        client.admin().cluster().prepareHealth(IndexingProxyPlugin.INDEX_NAME).setWaitForYellowStatus()
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

    private <Response extends ActionResponse> void createStreamOutput(final ActionListener<Response> listener) {
        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, FILE_ID).setRefresh(true).execute(wrap(res -> {
            if (!res.isExists()) {
                createStreamOutput(listener, 0);
            } else {
                final Map<String, Object> source = res.getSourceAsMap();
                final String nodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
                if (nodeName().equals(nodeName)) {
                    createStreamOutput(listener, res.getVersion());
                } else {
                    listener.onResponse(null);
                }
            }
        }, listener::onFailure));
    }

    private <Response extends ActionResponse> void createStreamOutput(final ActionListener<Response> listener, final long version) {
        final String oldFileId = fileId;
        final Map<String, Object> source = new HashMap<>();
        source.put(DOC_TYPE, FILE_ID);
        source.put(IndexingProxyPlugin.NODE_NAME, nodeName());
        source.put(IndexingProxyPlugin.TIMESTAMP, new Date());
        final IndexRequestBuilder builder = client.prepareIndex(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, FILE_ID);
        if (version > 0) {
            builder.setVersion(version);
        } else {
            builder.setCreate(true);
        }
        builder.setSource(source).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute(wrap(res -> {
            synchronized (this) {
                if (oldFileId == null || oldFileId.equals(fileId)) {
                    if (streamOutput != null) {
                        closeStreamOutput();
                    }

                    fileId = String.format(dataFileFormat, res.getVersion());
                    final Path outputPath = dataPath.resolve(fileId + WORKING_EXTENTION);
                    if (FileAccessUtils.existsFile(outputPath)) {
                        finalizeDataFile();
                        createStreamOutput(listener, res.getVersion());
                        return;
                    }
                    streamOutput = AccessController.doPrivileged((PrivilegedAction<IndexingProxyStreamOutput>) () -> {
                        try {
                            return new IndexingProxyStreamOutput(Files.newOutputStream(outputPath));
                        } catch (final IOException e) {
                            throw new ElasticsearchException("Could not open " + outputPath, e);
                        }
                    });
                    logger.info("[Writer] Opening　 " + outputPath.toAbsolutePath());
                }
            }

            listener.onResponse(null);
        }, listener::onFailure));
    }

    private String getLastModifiedTime(final long version, final String ext) {
        final String id = String.format(dataFileFormat, version);
        final Path outputPath = dataPath.resolve(id + ext);
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            if (Files.exists(outputPath)) {
                try {
                    final FileTime time = Files.getLastModifiedTime(outputPath);
                    return time.toString();
                } catch (IOException e) {
                    return "";
                }
            }
            return null;
        });
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
        finalizeDataFile();
    }

    private void finalizeDataFile() {
        final Path source = dataPath.resolve(fileId + WORKING_EXTENTION);
        final Path target = dataPath.resolve(fileId + IndexingProxyPlugin.DATA_EXTENTION);
        final Path outputPath = AccessController.doPrivileged((PrivilegedAction<Path>) () -> {
            try {
                return Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (final IOException e) {
                throw new ElasticsearchException("Failed to move " + source.toAbsolutePath() + " to " + target.toAbsolutePath(), e);
            }
        });
        logger.info("[Writer] Finalized " + outputPath.toAbsolutePath());
    }

    public <Response extends ActionResponse> void renew(final ActionListener<Response> listener) {
        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, FILE_ID).setRefresh(true).execute(wrap(res -> {
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String nodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
                if (nodeName().equals(nodeName)) {
                    renewOnLocal(listener);
                } else {
                    renewOnRemote(nodeName, listener);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No file_id. Skipped renew action.");
                }
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void renewOnRemote(final String nodeName,
            final ActionListener<Response> listener) {
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
            listener.onFailure(new ElasticsearchException("Writer nodes are not found for renew."));
        }

        final int nodeIdx = pos;
        transportService.sendRequest(nodeList.get(nodeIdx), IndexingProxyPlugin.ACTION_IDXPROXY_CREATE, new CreateRequest(),
                new TransportResponseHandler<CreateResponse>() {

                    @Override
                    public CreateResponse newInstance() {
                        return new CreateResponse();
                    }

                    @Override
                    public void handleResponse(final CreateResponse response) {
                        if (response.isAcknowledged()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Update file_id in " + nodeName);
                            }
                            listener.onResponse(null);
                        } else {
                            throw new ElasticsearchException("Failed to update file_id in " + nodeName);
                        }
                    }

                    @Override
                    public void handleException(final TransportException e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                });

    }

    public <Response extends ActionResponse> void renewOnLocal(final ActionListener<Response> listener) {
        if (streamOutput == null || (streamOutput != null && streamOutput.getByteCount() == 0)) {
            if (logger.isDebugEnabled()) {
                logger.debug("No requests in file. Skipped renew action.");
            }
            listener.onResponse(null);
        } else {
            createStreamOutput(listener);
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
            logger.debug("Writing request " + request);
        }

        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, FILE_ID).setRefresh(true).execute(wrap(res -> {
            if (!res.isExists()) {
                createStreamOutput(wrap(response -> {
                    write(request, listener, tryCount + 1);
                }, listener::onFailure), 0);
            } else {
                final Map<String, Object> source = res.getSourceAsMap();
                final String nodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
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
            if (tryCount >= writerRetryCount) {
                listener.onFailure(new ElasticsearchException("Writer nodes are not found for writing."));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No available write node.");
                }
                Collections.shuffle(nodeList);
                final DiscoveryNode nextNode = nodeList.get(0);
                final Map<String, Object> source = new HashMap<>();
                source.put(IndexingProxyPlugin.NODE_NAME, nextNode.getName());
                source.put(IndexingProxyPlugin.TIMESTAMP, new Date());
                client.prepareUpdate(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, FILE_ID).setVersion(version).setDoc(source)
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

        final int nodeIdx = pos;
        transportService.sendRequest(nodeList.get(nodeIdx), IndexingProxyPlugin.ACTION_IDXPROXY_WRITE, new WriteRequest<>(request),
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
                            throw new ElasticsearchException("Failed to store request: " + RequestUtils.getClassType(request));
                        }
                    }

                    @Override
                    public void handleException(final TransportException e) {
                        if (tryCount >= writerRetryCount) {
                            listener.onFailure(e);
                        } else {
                            final DiscoveryNode nextNode = nodeList.get((nodeIdx + 1) % nodeList.size());
                            if (nextNode.getName().equals(nodeName)) {
                                if (tryCount >= writerRetryCount) {
                                    listener.onFailure(e);
                                } else {
                                    randomWait();
                                    write(request, listener, tryCount + 1);
                                }
                            } else {
                                final Map<String, Object> source = new HashMap<>();
                                source.put(IndexingProxyPlugin.NODE_NAME, nextNode.getName());
                                source.put(IndexingProxyPlugin.TIMESTAMP, new Date());
                                client.prepareUpdate(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, FILE_ID).setVersion(version).setDoc(source)
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
            final short classType = RequestUtils.getClassType(request);
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
            createStreamOutput(next);
        } else {
            next.onResponse(null);
        }
    }
 

    public boolean isTargetIndex(final String index) {
        return targetIndexSet.contains(index);
    }

    public void startRequestSender(final String index, final long filePosition, final ActionListener<Map<String, Object>> listener) {
        if (!senderNodes.isEmpty() && !senderNodes.contains(nodeName())) {
            listener.onFailure(new ElasticsearchException(nodeName() + " is not a Sender node."));
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Starting RequestSender(" + index + ")");
        }
        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index).setRefresh(true).execute(wrap(res -> {
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
                if (!Strings.isBlank(workingNodeName) && !nodeName().equals(workingNodeName)) {
                    listener.onFailure(new ElasticsearchException("RequestSender is working in " + workingNodeName));
                } else {
                    final Number pos = (Number) source.get(IndexingProxyPlugin.FILE_POSITION);
                    final long value = pos == null ? 1 : pos.longValue();
                    launchRequestSender(index, filePosition > 0 ? filePosition : value, res.getVersion(), listener);
                }
            } else {
                launchRequestSender(index, filePosition > 0 ? filePosition : 1, 0, listener);
            }
        }, listener::onFailure));
    }

    private void launchRequestSender(final String index, final long filePosition, final long version,
            final ActionListener<Map<String, Object>> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Launching RequestSender(" + index + ")");
        }
        final Map<String, Object> source = new HashMap<>();
        source.put(IndexingProxyPlugin.NODE_NAME, nodeName());
        source.put(IndexingProxyPlugin.FILE_POSITION, filePosition);
        source.put(IndexingProxyPlugin.TIMESTAMP, new Date());
        source.put(DOC_TYPE, "index");
        final IndexRequestBuilder builder =
                client.prepareIndex(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index).setSource(source).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        if (version > 0) {
            builder.setVersion(version);
        } else {
            builder.setCreate(true);
        }
        builder.execute(wrap(res -> {
            if (res.getResult() == Result.CREATED || res.getResult() == Result.UPDATED) {
                final RequestSender sender = new RequestSender(settings, client, threadPool, namedWriteableRegistry, nodeName(), dataPath,
                        index, dataFileFormat, docSenderMap, logger);
                final RequestSender oldSender = docSenderMap.put(index, sender);
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

    public void stopRequestSender(final String index, final ActionListener<Map<String, Object>> listener) {
        if (logger.isDebugEnabled()) {
            logger.debug("Stopping RequestSender(" + index + ")");
        }
        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index).setRefresh(true).execute(wrap(res -> {
            final Map<String, Object> params = new HashMap<>();
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
                params.put("node", nodeName());
                params.put("found", true);
                if (Strings.isBlank(workingNodeName)) {
                    params.put("stop", false);
                    params.put("working", "");
                } else if (nodeName().equals(workingNodeName)) {
                    params.put("working", workingNodeName);
                    // stop
                    final long version = res.getVersion();
                    updateRequestSenderInfo(index, "", version, wrap(res2 -> {
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

    private void updateRequestSenderInfo(final String index, final String node, final long version,
            final ActionListener<UpdateResponse> listener) {
        final Map<String, Object> newSource = new HashMap<>();
        newSource.put(IndexingProxyPlugin.NODE_NAME, node);
        newSource.put(IndexingProxyPlugin.TIMESTAMP, new Date());
        final UpdateRequestBuilder builder = client.prepareUpdate(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index);
        if (version > 0) {
            builder.setVersion(version);
        }
        try {
            builder.setDoc(newSource).setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute(listener);
        } catch (final Exception e) {
            listener.onFailure(e);
        }
    }

    public void getRequestSenderInfos(final int from, final int size, final ActionListener<Map<String, Object>> listener) {
        client.prepareSearch(IndexingProxyPlugin.INDEX_NAME).setQuery(QueryBuilders.termQuery(DOC_TYPE, "index")).setFrom(from).setSize(size)
                .execute(wrap(res -> {
                    final Map<String, Object> params = new HashMap<>();
                    params.put("took_in_millis", res.getTookInMillis());
                    params.put("senders", Arrays.stream(res.getHits().getHits()).map(hit -> {
                        final Map<String, Object> source = new HashMap<>();
                        source.putAll(hit.getSource());
                        final RequestSender docSender = docSenderMap.get(hit.getId());
                        source.put("index", hit.getId());
                        source.put("running", docSender != null && docSender.isRunning());
                        if (docSender != null) {
                            source.put("heartbeat", docSender.getHeartbeat());
                        }
                        final Number pos = (Number) source.get(IndexingProxyPlugin.FILE_POSITION);
                        if (pos != null) {
                            final String t = getLastModifiedTime(pos.longValue() - 1, IndexingProxyPlugin.DATA_EXTENTION);
                            if (t != null) {
                                source.put(IndexingProxyPlugin.FILE_TIMESTAMP, t);
                            }
                        }
                        return source;
                    }).toArray(n -> new Map[n]));
                    if (fileId != null) {
                        final String t = getLastModifiedTime(Long.parseLong(fileId), WORKING_EXTENTION);
                        if (t != null) {
                            params.put("writer", Collections.singletonMap(IndexingProxyPlugin.FILE_TIMESTAMP, t));
                        }
                    }
                    listener.onResponse(params);
                }, listener::onFailure));
    }

    public void getRequestSenderInfo(final String index, final ActionListener<Map<String, Object>> listener) {
        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index).setRefresh(true).execute(wrap(res -> {
            final Map<String, Object> params = new HashMap<>();
            if (res.isExists()) {
                params.put("found", true);
                final Map<String, Object> sender = new HashMap<>();
                params.put("sender", sender);
                final Map<String, Object> source = res.getSourceAsMap();
                sender.putAll(source);
                final RequestSender docSender = docSenderMap.get(res.getId());
                sender.put("running", docSender != null && docSender.isRunning());
                if (docSender != null) {
                    source.put("heartbeat", docSender.getHeartbeat());
                }
                final Number pos = (Number) sender.get(IndexingProxyPlugin.FILE_POSITION);
                if (pos != null) {
                    final String t = getLastModifiedTime(pos.longValue() - 1, IndexingProxyPlugin.DATA_EXTENTION);
                    if (t != null) {
                        sender.put(IndexingProxyPlugin.FILE_TIMESTAMP, t);
                    }
                }
            } else {
                params.put("found", false);
            }
            if (fileId != null) {
                final String t = getLastModifiedTime(Long.parseLong(fileId), WORKING_EXTENTION);
                if (t != null) {
                    params.put("writer", Collections.singletonMap(IndexingProxyPlugin.FILE_TIMESTAMP, t));
                }
            }
            listener.onResponse(params);
        }, listener::onFailure));
    }

    public void dumpRequests(final int filePosition, ActionListener<String> listener) {
        final Path path = dataPath.resolve(String.format(dataFileFormat, filePosition) + IndexingProxyPlugin.DATA_EXTENTION);
        if (FileAccessUtils.existsFile(path)) {
            try (IndexingProxyStreamInput streamInput = AccessController.doPrivileged((PrivilegedAction<IndexingProxyStreamInput>) () -> {
                try {
                    return new IndexingProxyStreamInput(Files.newInputStream(path), namedWriteableRegistry);
                } catch (final IOException e) {
                    throw new ElasticsearchException("Failed to read " + path.toAbsolutePath(), e);
                }
            })) {
                final StringBuilder buf = new StringBuilder(10000);
                while (streamInput.available() > 0) {
                    final short classType = streamInput.readShort();
                    switch (classType) {
                    case RequestUtils.TYPE_DELETE:
                        DeleteRequest deleteRequest = RequestUtils.createDeleteRequest(client, streamInput, null).request();
                        buf.append(deleteRequest.toString());
                        break;
                    case RequestUtils.TYPE_DELETE_BY_QUERY:
                        DeleteByQueryRequest deleteByQueryRequest =
                                RequestUtils.createDeleteByQueryRequest(client, streamInput, null).request();
                        buf.append(deleteByQueryRequest.toString());
                        buf.append(' ');
                        buf.append(deleteByQueryRequest.getSearchRequest().toString().replace("\n", ""));
                        break;
                    case RequestUtils.TYPE_INDEX:
                        IndexRequest indexRequest = RequestUtils.createIndexRequest(client, streamInput, null).request();
                        buf.append(indexRequest.toString());
                        break;
                    case RequestUtils.TYPE_UPDATE:
                        UpdateRequest updateRequest = RequestUtils.createUpdateRequest(client, streamInput, null).request();
                        buf.append("update {[").append(updateRequest.index()).append("][").append(updateRequest.type()).append("][")
                                .append(updateRequest.id()).append("] source[")
                                .append(updateRequest.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).string())
                                .append("]}");
                        break;
                    case RequestUtils.TYPE_UPDATE_BY_QUERY:
                        UpdateByQueryRequest updateByQueryRequest =
                                RequestUtils.createUpdateByQueryRequest(client, streamInput, null).request();
                        buf.append(updateByQueryRequest.toString());
                        buf.append(' ');
                        buf.append(updateByQueryRequest.getSearchRequest().toString().replace("\n", ""));
                        break;
                    case RequestUtils.TYPE_BULK:
                        BulkRequest bulkRequest = RequestUtils.createBulkRequest(client, streamInput, null).request();
                        buf.append("bulk [");
                        buf.append(bulkRequest.requests().stream().map(req -> {
                            if (req instanceof UpdateRequest) {
                                UpdateRequest upreq = (UpdateRequest) req;
                                try {
                                    return "update {[" + upreq.index() + "][" + upreq.type() + "][" + upreq.id() + "] source["
                                            + upreq.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).string() + "]}";
                                } catch (IOException e) {
                                    return e.getMessage();
                                }
                            } else {
                                return req.toString();
                            }
                        }).collect(Collectors.joining(",")));
                        buf.append("]");
                        break;
                    default:
                        listener.onFailure(new ElasticsearchException("Unknown request type: " + classType));
                    }
                    buf.append('\n');
                }
                listener.onResponse(buf.toString());
            } catch (IOException e) {
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new ElasticsearchException("The data file does not exist: " + dataPath));
        }
    }

    public boolean isRunning(final String index) {
        final RequestSender docSender = docSenderMap.get(index);
        return docSender != null && docSender.isRunning();
    }

    public boolean isRenewAction(String action) {
        return renewActions.contains(action);
    }
 }
