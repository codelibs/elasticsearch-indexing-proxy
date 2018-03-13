package org.codelibs.elasticsearch.idxproxy.sender;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.idxproxy.IndexingProxyPlugin;
import org.codelibs.elasticsearch.idxproxy.stream.IndexingProxyStreamInput;
import org.codelibs.elasticsearch.idxproxy.stream.IndexingProxyStreamOutput;
import org.codelibs.elasticsearch.idxproxy.util.FileAccessUtils;
import org.codelibs.elasticsearch.idxproxy.util.RequestUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

public class RequestSender implements Runnable {

    private static final String ERROR_EXTENTION = ".err";

    private final String index;

    private Path path;

    private long filePosition;

    private long version;

    private volatile int errorCount = 0;

    private volatile int requestErrorCount = 0;

    private volatile long heartbeat = System.currentTimeMillis();

    private volatile boolean terminated = false;

    private volatile long requestPosition = 0;

    private final Client client;

    private final ThreadPool threadPool;

    private final Logger logger;

    private final Map<String, RequestSender> docSenderMap;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final TimeValue senderAliveTime;

    private final boolean senderSkipErrorFile;

    private final TimeValue senderInterval;

    private final int senderRetryCount;

    private final int senderLookupFiles;

    private final int senderRequestRetryCount;

    private final Path dataPath;

    private final String nodeName;

    private final String dataFileFormat;

    public RequestSender(Settings settings, Client client, ThreadPool threadPool, NamedWriteableRegistry namedWriteableRegistry,
            final String nodeName, Path dataPath, String index, String dataFileFormat, Map<String, RequestSender> docSenderMap,
            Logger logger) {
        this.client = client;
        this.threadPool = threadPool;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.nodeName = nodeName;
        this.index = index;
        this.dataPath = dataPath;
        this.dataFileFormat = dataFileFormat;
        this.docSenderMap = docSenderMap;
        this.logger = logger;

        senderInterval = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_INTERVAL.get(settings);
        senderRetryCount = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_RETRY_COUNT.get(settings);
        senderRequestRetryCount = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_REQUEST_RETRY_COUNT.get(settings);
        senderSkipErrorFile = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_SKIP_ERROR_FILE.get(settings);
        senderAliveTime = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_ALIVE_TIME.get(settings);
        senderLookupFiles = IndexingProxyPlugin.SETTING_INXPROXY_SENDER_LOOKUP_FILES.get(settings);
    }

    public Date getHeartbeat() {
        return new Date(heartbeat);
    }

    public void terminate() {
        if (logger.isDebugEnabled()) {
            logger.debug("Terminating DocIndexer(" + index + ")");
        }
        terminated = true;
    }

    public boolean isRunning() {
        return !terminated && System.currentTimeMillis() - heartbeat < senderAliveTime.getMillis();
    }

    @Override
    public void run() {
        heartbeat = System.currentTimeMillis();
        if (terminated) {
            logger.warn("[Sender][" + index + "] Terminate DocIndexer");
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Running RequestSender(" + index + ")");
        }
        client.prepareGet(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index).setRefresh(true).execute(wrap(res -> {
            if (res.isExists()) {
                final Map<String, Object> source = res.getSourceAsMap();
                final String workingNodeName = (String) source.get(IndexingProxyPlugin.NODE_NAME);
                if (!nodeName.equals(workingNodeName)) {
                    logger.info("[Sender][{}] Stopped RequestSender because of working in [{}].", index, workingNodeName);
                    docSenderMap.computeIfPresent(index, (k, v) -> v == this ? null : v);
                    // end
                } else {
                    final Number pos = (Number) source.get(IndexingProxyPlugin.FILE_POSITION);
                    if (pos == null) {
                        logger.error("[Sender][{}] Stopped RequestSender. No file_position.", index);
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
                logger.info("[Sender][{}] Stopped RequestSender.", index);
                docSenderMap.computeIfPresent(index, (k, v) -> v == this ? null : v);
                // end
            }
        }, e -> {
            retryWithError("RequestSender data is not found.", e);
            // retry
        }));
    }

    private void retryWithError(final String message, final Exception e) {
        errorCount++;
        if (errorCount > senderRetryCount) {
            if (senderSkipErrorFile) {
                logger.error("[Sender][" + index + "][" + errorCount + "] Failed to process " + path.toAbsolutePath(), e);
                processNext(getNextValue(filePosition));
            } else {
                logger.error("[Sender][" + index + "][" + errorCount + "] Stopped RequestSender: Failed to process " + path.toAbsolutePath(),
                        e);
            }
        } else {
            logger.warn("[Sender][" + index + "][" + errorCount + "] " + message, e);
            threadPool.schedule(senderInterval, Names.GENERIC, this);
        }
    }

    private void process(final long filePosition) {
        if (logger.isDebugEnabled()) {
            logger.debug("RequestSender(" + index + ") processes " + filePosition);
        }
        path = dataPath.resolve(String.format(dataFileFormat, filePosition) + IndexingProxyPlugin.DATA_EXTENTION);
        if (FileAccessUtils.existsFile(path)) {
            logger.info("[Sender][{}] Indexing: {}", index, path.toAbsolutePath());
            requestPosition = 0;
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
            long next = getNextValue(filePosition);
            for (long i = 0; i < senderLookupFiles; i++) {
                if (FileAccessUtils
                        .existsFile(dataPath.resolve(String.format(dataFileFormat, next) + IndexingProxyPlugin.DATA_EXTENTION))) {
                    logger.warn("[Sender][" + index + "] file_id " + filePosition + " is skipped. Moving to file_id " + next);
                    processNext(next);
                    return;
                    // continue
                }
                next = getNextValue(next);
            }
            threadPool.schedule(senderInterval, Names.GENERIC, this);
            // retry
        }
    }

    private void processRequests(final StreamInput streamInput) {
        heartbeat = System.currentTimeMillis();
        if (terminated) {
            IOUtils.closeQuietly(streamInput);
            logger.warn("[Sender][" + index + "] Terminate DocIndexer.");
            return;
        }
        requestPosition++;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("RequestSender(" + index + ") is processing requests.");
            }
            if (streamInput.available() > 0) {
                final short classType = streamInput.readShort();
                switch (classType) {
                case RequestUtils.TYPE_DELETE:
                    processDeleteRequest(streamInput);
                    break;
                case RequestUtils.TYPE_DELETE_BY_QUERY:
                    processDeleteByQueryRequest(streamInput);
                    break;
                case RequestUtils.TYPE_INDEX:
                    processIndexRequest(streamInput);
                    break;
                case RequestUtils.TYPE_UPDATE:
                    processUpdateRequest(streamInput);
                    break;
                case RequestUtils.TYPE_UPDATE_BY_QUERY:
                    processUpdateByQueryRequest(streamInput);
                    break;
                case RequestUtils.TYPE_BULK:
                    processBulkRequest(streamInput);
                    break;
                default:
                    throw new ElasticsearchException("Unknown request type: " + classType);
                }
            } else {
                IOUtils.closeQuietly(streamInput);
                long fileSize = 0;
                if (FileAccessUtils.existsFile(path)) {
                    fileSize = AccessController.doPrivileged((PrivilegedAction<Long>) () -> {
                        try {
                            return Files.size(path);
                        } catch (final IOException e) {
                            throw new ElasticsearchException("Failed to read " + path.toAbsolutePath(), e);
                        }
                    });
                }
                logger.info("[Sender][{}] Indexed:  {} {} {}", index, path.toAbsolutePath(), requestPosition - 1, fileSize);

                processNext(getNextValue(filePosition));
            }
        } catch (final Exception e) {
            IOUtils.closeQuietly(streamInput);
            retryWithError("Failed to access streamInput.", e);
            // retry
        }
    }

    private void processNext(final long position) {
        if (logger.isDebugEnabled()) {
            logger.debug("RequestSender(" + index + ") moves next files.");
        }
        final Map<String, Object> source = new HashMap<>();
        source.put(IndexingProxyPlugin.FILE_POSITION, position);
        source.put(IndexingProxyPlugin.TIMESTAMP, new Date());
        client.prepareUpdate(IndexingProxyPlugin.INDEX_NAME, IndexingProxyPlugin.TYPE_NAME, index).setVersion(version).setDoc(source)
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL).execute(wrap(res -> {
                    errorCount = 0;
                    requestErrorCount = 0;
                    threadPool.schedule(TimeValue.ZERO, Names.GENERIC, this);
                    // retry: success
                }, e -> {
                    logger.error("[Sender][" + index + "] Failed to update config data.", e);
                    threadPool.schedule(TimeValue.ZERO, Names.GENERIC, this);
                    // retry
                }));
    }

    private void processBulkRequest(final StreamInput streamInput) throws IOException {
        final BulkRequestBuilder builder = RequestUtils.createBulkRequest(client, streamInput, index);
        executeBulkRequest(streamInput, builder);
    }

    private void executeBulkRequest(final StreamInput streamInput, final BulkRequestBuilder builder) {
        builder.execute(wrap(res -> {
            processRequests(streamInput);
            // continue
        }, e -> {
            if (senderRequestRetryCount >= 0) {
                if (requestErrorCount > senderRequestRetryCount) {
                    logger.error("[Sender][" + index + "][" + requestErrorCount + "] Failed to process the bulk request.", e);
                    requestErrorCount = 0;
                    writeError(requestPosition, builder.request(), wrap(r -> processRequests(streamInput), ex -> {
                        logger.warn("Failed to store an error request.", ex);
                        processRequests(streamInput);
                    }));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[Sender][" + index + "][" + requestErrorCount + "] Failed to process the bulk request.", e);
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
        final UpdateByQueryRequestBuilder builder = RequestUtils.createUpdateByQueryRequest(client, streamInput, index);
        executeUpdateByQueryRequest(streamInput, builder);
    }

    private void executeUpdateByQueryRequest(final StreamInput streamInput, final UpdateByQueryRequestBuilder builder) {
        builder.execute(wrap(res -> {
            processRequests(streamInput);
            // continue
        }, e -> {
            if (senderRequestRetryCount >= 0) {
                if (requestErrorCount > senderRequestRetryCount) {
                    logger.error("[Sender][" + index + "][" + requestErrorCount + "] Failed to update requests.", e);
                    requestErrorCount = 0;
                    writeError(requestPosition, builder.request(), wrap(r -> processRequests(streamInput), ex -> {
                        logger.warn("[Sender][" + index + "] Failed to store an error request.", ex);
                        processRequests(streamInput);
                    }));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + requestErrorCount + "] Failed to update requests.", e);
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
        final UpdateRequestBuilder builder = RequestUtils.createUpdateRequest(client, streamInput, index);
        executeUpdateRequest(streamInput, builder);
    }

    private void executeUpdateRequest(final StreamInput streamInput, final UpdateRequestBuilder builder) {
        builder.execute(wrap(res -> {
            processRequests(streamInput);
            // continue
        }, e -> {
            if (senderRequestRetryCount >= 0) {
                if (requestErrorCount > senderRequestRetryCount) {
                    logger.error("[Sender][" + index + "][" + requestErrorCount + "] Failed to update [" + builder.request().index() + "]["
                            + builder.request().type() + "][" + builder.request().id() + "]", e);
                    requestErrorCount = 0;
                    writeError(requestPosition, builder.request(), wrap(r -> processRequests(streamInput), ex -> {
                        logger.warn("[Sender][" + index + "] Failed to store an error request.", ex);
                        processRequests(streamInput);
                    }));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + requestErrorCount + "] Failed to update [" + builder.request().index() + "]["
                                + builder.request().type() + "][" + builder.request().id() + "]", e);
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
        final IndexRequestBuilder builder = RequestUtils.createIndexRequest(client, streamInput, index);
        executeIndexRequest(streamInput, builder);
    }

    private void executeIndexRequest(final StreamInput streamInput, final IndexRequestBuilder builder) {
        builder.execute(wrap(res -> {
            processRequests(streamInput);
            // continue
        }, e -> {
            if (senderRequestRetryCount >= 0) {
                if (requestErrorCount > senderRequestRetryCount) {
                    logger.error("[Sender][" + index + "][" + requestErrorCount + "] Failed to index [" + builder.request().index() + "]["
                            + builder.request().type() + "][" + builder.request().id() + "]", e);
                    requestErrorCount = 0;
                    writeError(requestPosition, builder.request(), wrap(r -> processRequests(streamInput), ex -> {
                        logger.warn("[Sender][" + index + "] Failed to store an error request.", ex);
                        processRequests(streamInput);
                    }));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + requestErrorCount + "] Failed to index [" + builder.request().index() + "]["
                                + builder.request().type() + "][" + builder.request().id() + "]", e);
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
        final DeleteByQueryRequestBuilder builder = RequestUtils.createDeleteByQueryRequest(client, streamInput, index);
        executeDeleteByQueryRequest(streamInput, builder);
    }

    private void executeDeleteByQueryRequest(final StreamInput streamInput, final DeleteByQueryRequestBuilder builder) {
        builder.execute(wrap(res -> {
            processRequests(streamInput);
            // continue
        }, e -> {
            if (senderRequestRetryCount >= 0) {
                if (requestErrorCount > senderRequestRetryCount) {
                    logger.error("[Sender][" + index + "][" + requestErrorCount + "] Failed to delete ["
                            + Arrays.toString(builder.request().indices()) + "]", e);
                    requestErrorCount = 0;
                    writeError(requestPosition, builder.request(), wrap(r -> processRequests(streamInput), ex -> {
                        logger.warn("[Sender][" + index + "] Failed to store an error request.", ex);
                        processRequests(streamInput);
                    }));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + requestErrorCount + "] Failed to delete [" + Arrays.toString(builder.request().indices()) + "]",
                                e);
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
        final DeleteRequestBuilder builder = RequestUtils.createDeleteRequest(client, streamInput, index);
        executeDeleteRequest(streamInput, builder);
    }

    private void executeDeleteRequest(final StreamInput streamInput, final DeleteRequestBuilder builder) {
        builder.execute(wrap(res -> {
            processRequests(streamInput);
            // continue
        }, e -> {
            if (senderRequestRetryCount >= 0) {
                if (requestErrorCount > senderRequestRetryCount) {
                    logger.error("[Sender][" + index + "][" + requestErrorCount + "] Failed to delete [" + builder.request().index() + "]["
                            + builder.request().type() + "][" + builder.request().id() + "]", e);
                    requestErrorCount = 0;
                    writeError(requestPosition, builder.request(), wrap(r -> processRequests(streamInput), ex -> {
                        logger.warn("[Sender][" + index + "] Failed to store an error request.", ex);
                        processRequests(streamInput);
                    }));
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + requestErrorCount + "] Failed to delete [" + builder.request().index() + "]["
                                + builder.request().type() + "][" + builder.request().id() + "]", e);
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

    private <Request extends ActionRequest, Response extends ActionResponse> void writeError(final long position, final Request request,
            final ActionListener<Response> listener) {
        final String fileId = String.format(dataFileFormat, version);
        final Path outputPath = dataPath.resolve(fileId + "_" + position + ERROR_EXTENTION);
        logger.info("Saving " + outputPath.toAbsolutePath());
        final short classType = RequestUtils.getClassType(request);
        if (classType > 0) {
            try (IndexingProxyStreamOutput out = AccessController.doPrivileged((PrivilegedAction<IndexingProxyStreamOutput>) () -> {
                try {
                    return new IndexingProxyStreamOutput(Files.newOutputStream(outputPath));
                } catch (final IOException e) {
                    throw new ElasticsearchException("Could not open " + outputPath, e);
                }
            })) {
                out.writeShort(classType);
                request.writeTo(out);
                out.flush();
            } catch (final Exception e) {
                listener.onFailure(e);
            }
        } else {
            listener.onFailure(new ElasticsearchException("Unknown request: " + request));
        }
    }

    static long getNextValue(final long current) {
        if (current == Long.MAX_VALUE || current < 0) {
            return 1;
        }
        return current + 1;
    }
}
