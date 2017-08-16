package org.codelibs.elasticsearch.idxproxy;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.idxproxy.action.ProxyActionFilter;
import org.codelibs.elasticsearch.idxproxy.service.IndexingProxyService;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

public class IndexingProxyPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        return Arrays.asList(ProxyActionFilter.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(IndexingProxyService.SETTING_INXPROXY_DATA_PATH, //
                IndexingProxyService.SETTING_INXPROXY_DATA_FILE_FORMAT, //
                IndexingProxyService.SETTING_INXPROXY_DATA_FILE_SIZE, //
                IndexingProxyService.SETTING_INXPROXY_TARGET_INDICES);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Arrays.asList(IndexingProxyService.class);
    }
}
