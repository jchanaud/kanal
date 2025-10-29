package io.kanal.runner.beanfactories;

import io.kanal.runner.engine.converters.ConnectRecordConverter;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;


@Factory
public class ConnectBeanFactory {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ConnectBeanFactory.class);

    @Singleton
    ConnectRecordConverter<SinkRecord> sinkRecordConverter() {
        return new ConnectRecordConverter<>();
    }

    @Context
    Plugins plugins() {
        Plugins p = new Plugins(Map.of(WorkerConfig.PLUGIN_PATH_CONFIG, "libs"));
        p.connectors().forEach(pluginDesc ->
                LOG.info("Connector plugin loaded from {}: {}[{}]", pluginDesc.location(), pluginDesc.className(), pluginDesc.version())
        );
        return p;
    }
}
