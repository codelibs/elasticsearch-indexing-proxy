package org.codelibs.elasticsearch.idxproxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.idxproxy.action.ProxyActionFilter;
import org.codelibs.elasticsearch.idxproxy.rest.RestIndexingProxyProcessAction;
import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

public class IndexingProxyPlugin extends Plugin implements ActionPlugin {

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
        return Arrays.asList(new RestIndexingProxyProcessAction(settings, restController, pluginComponent));
    }

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        return Arrays.asList(ProxyActionFilter.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(IndexingProxyService.SETTING_INXPROXY_DATA_PATH, //
                IndexingProxyService.SETTING_INXPROXY_DATA_FILE_FORMAT, //
                IndexingProxyService.SETTING_INXPROXY_DATA_FILE_SIZE, //
                IndexingProxyService.SETTING_INXPROXY_SENDER_INTERVAL, //
                IndexingProxyService.SETTING_INXPROXY_SENDER_RETRY_COUNT, //
                IndexingProxyService.SETTING_INXPROXY_SENDER_REQUEST_RETRY_COUNT, //
                IndexingProxyService.SETTING_INXPROXY_SENDER_SKIP_ERROR_FILE, //
                IndexingProxyService.SETTING_INXPROXY_MONITOR_INTERVAL, //
                IndexingProxyService.SETTING_INXPROXY_FLUSH_PER_DOC, //
                IndexingProxyService.SETTING_INXPROXY_TARGET_INDICES);
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
