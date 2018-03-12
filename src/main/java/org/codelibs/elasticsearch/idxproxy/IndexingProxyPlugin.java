package org.codelibs.elasticsearch.idxproxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.idxproxy.action.ProxyActionFilter;
import org.codelibs.elasticsearch.idxproxy.rest.RestIndexingProxyProcessAction;
import org.codelibs.elasticsearch.idxproxy.rest.RestIndexingProxyRequestAction;
import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

public class IndexingProxyPlugin extends Plugin implements ActionPlugin {

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

    public static final Setting<Integer> SETTING_INXPROXY_SENDER_LOOKUP_FILES =
            Setting.intSetting("idxproxy.sender.lookup_files", 1000, Property.NodeScope);

    public static final Setting<TimeValue> SETTING_INXPROXY_MONITOR_INTERVAL =
            Setting.timeSetting("idxproxy.monitor.interval", TimeValue.timeValueSeconds(15), Property.NodeScope);

    public static final Setting<Integer> SETTING_INXPROXY_WRITER_RETRY_COUNT =
            Setting.intSetting("idxproxy.writer.retry_count", 10, Property.NodeScope);

    public static final Setting<List<String>> SETTING_INXPROXY_SENDER_NODES =
            Setting.listSetting("idxproxy.sender_nodes", Collections.emptyList(), s -> s.trim(), Property.NodeScope);

    public static final Setting<List<String>> SETTING_INXPROXY_WRITE_NODES =
            Setting.listSetting("idxproxy.writer_nodes", Collections.emptyList(), s -> s.trim(), Property.NodeScope);

    public static final Setting<Boolean> SETTING_INXPROXY_FLUSH_PER_DOC =
            Setting.boolSetting("idxproxy.flush_per_doc", true, Property.NodeScope);

    public static final Setting<Integer> SETTING_INXPROXY_NUMBER_OF_SHARDS =
            Setting.intSetting("idxproxy.number_of_shards", 1, Property.NodeScope);

    public static final Setting<Integer> SETTING_INXPROXY_NUMBER_OF_REPLICAS =
            Setting.intSetting("idxproxy.number_of_replicas", 1, Property.NodeScope);

    public static final Setting<List<String>> SETTING_INXPROXY_RENEW_ACTIONS = Setting.listSetting("idxproxy.renew_actions",
            Arrays.asList(FlushAction.NAME, ForceMergeAction.NAME, RefreshAction.NAME, UpgradeAction.NAME), s -> {
                if (s.indexOf(':') >= 0) {
                    return s;
                }
                return "indices:admin/" + s;
            }, Property.NodeScope);

    public static final String DATA_EXTENTION = ".dat";

    public static final String TYPE_NAME = "config";

    public static final String INDEX_NAME = ".idxproxy";

    public static final String NODE_NAME = "node_name";

    public static final String FILE_POSITION = "file_position";

    public static final String FILE_TIMESTAMP = "file_timestamp";

    public static final String TIMESTAMP = "@timestamp";

    public static final String ACTION_IDXPROXY_WRITE = "internal:indices/idxproxy/write";

    public static final String ACTION_IDXPROXY_PING = "internal:indices/idxproxy/ping";

    public static final String ACTION_IDXPROXY_CREATE = "internal:indices/idxproxy/create";

    private final PluginComponent pluginComponent = new PluginComponent();

    @Override
    public Collection<Object> createComponents(final Client client, final ClusterService clusterService, final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService, final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry) {
        final Collection<Object> components = new ArrayList<>();
        components.add(pluginComponent);
        return components;
    }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
            final ClusterSettings clusterSettings, final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver, final Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new RestIndexingProxyProcessAction(settings, restController, pluginComponent),
                new RestIndexingProxyRequestAction(settings, restController, pluginComponent));
    }

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        return Arrays.asList(ProxyActionFilter.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(SETTING_INXPROXY_DATA_PATH, //
                SETTING_INXPROXY_DATA_FILE_FORMAT, //
                SETTING_INXPROXY_DATA_FILE_SIZE, //
                SETTING_INXPROXY_SENDER_INTERVAL, //
                SETTING_INXPROXY_SENDER_RETRY_COUNT, //
                SETTING_INXPROXY_SENDER_REQUEST_RETRY_COUNT, //
                SETTING_INXPROXY_SENDER_SKIP_ERROR_FILE, //
                SETTING_INXPROXY_SENDER_LOOKUP_FILES, //
                SETTING_INXPROXY_MONITOR_INTERVAL, //
                SETTING_INXPROXY_WRITER_RETRY_COUNT, //
                SETTING_INXPROXY_SENDER_NODES, //
                SETTING_INXPROXY_WRITE_NODES, //
                SETTING_INXPROXY_FLUSH_PER_DOC, //
                SETTING_INXPROXY_NUMBER_OF_REPLICAS, //
                SETTING_INXPROXY_NUMBER_OF_SHARDS, //
                SETTING_INXPROXY_TARGET_INDICES, //
                SETTING_INXPROXY_RENEW_ACTIONS);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Arrays.asList(IndexingProxyService.class);
    }

    public static class PluginComponent {
        private IndexingProxyService indexingProxyService;

        public IndexingProxyService getIndexingProxyService() {
            return indexingProxyService;
        }

        public void setIndexingProxyService(final IndexingProxyService indexingProxyService) {
            this.indexingProxyService = indexingProxyService;
        }

    }
}
