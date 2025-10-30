package io.kanal.runner.engine.stages;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.entities.DataPacket;
import io.kanal.runner.engine.entities.SourceStage;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Prototype
public class KafkaConsumerStage extends SourceStage {
    final static Logger LOG = org.slf4j.LoggerFactory.getLogger(KafkaConsumerStage.class);

    KafkaConsumer<byte[], byte[]> consumer;

    StageDefinition stageDefinition;
    KafkaDefaultConfiguration kafkaDefaultConfiguration;
    Map<TopicPartition, Long> initialLSO;
    Map<TopicPartition, Long> lastReadOffsets = new HashMap<>();
    Plugins plugins;
    Converter keyConverter;
    Converter valueConverter;
    HeaderConverter headerConverter;
    boolean allCaughtUp = false;

    public KafkaConsumerStage(@Parameter String name, @Parameter StageDefinition stageDefinition, KafkaDefaultConfiguration kafkaDefaultConfiguration, Plugins plugins) {
        super(name, CacheSupport.SUPPORTED);
        this.stageDefinition = stageDefinition;
        this.kafkaDefaultConfiguration = kafkaDefaultConfiguration;
        this.plugins = plugins;
    }

    @Override
    public void initialize() {
        String schemaInline = """
                {
                   "$schema": "http://json-schema.org/draft-07/schema#",
                   "title": "Generated schema for Root",
                   "type": "object",
                   "properties": {
                     "item": {
                       "type": "number"
                     }
                   },
                   "required": [
                     "item"
                   ]
                 }""";
        ConnectorConfig connConfig = new SinkConnectorConfig(plugins, Map.of(
                ConnectorConfig.NAME_CONFIG, name,
                ConnectorConfig.CONNECTOR_CLASS_CONFIG, "fake",
                ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter",
                ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + ".schemas.enable", "false",
                ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter",
                ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + ".schema.content", schemaInline,
                ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.SimpleHeaderConverter"
        ));

        keyConverter = plugins.newConverter(connConfig, ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, ConnectorConfig.KEY_CONVERTER_VERSION_CONFIG);
        valueConverter = plugins.newConverter(connConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ConnectorConfig.VALUE_CONVERTER_VERSION_CONFIG);
        headerConverter = plugins.newHeaderConverter(connConfig, ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG, ConnectorConfig.HEADER_CONVERTER_VERSION_CONFIG);

    }

    @Override
    public void poll() {

        if (isCacheSource) {
            LOG.info("Starting Kafka consumer poll loop SEEK for stage: " + name);
            var props = kafkaDefaultConfiguration.getConfig();
            props.put("enable.auto.commit", "false");
            consumer = new KafkaConsumer<byte[], byte[]>(props);

            List<TopicPartition> partitions = consumer.partitionsFor(stageDefinition.topic)
                    .stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            // Get the initial LSO that will be used to determine when we are caught up
            initialLSO = consumer.endOffsets(partitions);
            partitions.forEach(tp -> lastReadOffsets.put(tp, -1L));

            consumer.assign(partitions);
            consumer.seekToBeginning(consumer.assignment());
        } else {
            LOG.info("Starting Kafka consumer poll loop with group 'toto' for stage: " + name);
            consumer = new KafkaConsumer<byte[], byte[]>(kafkaDefaultConfiguration.getConfig());
            consumer.subscribe(List.of(stageDefinition.topic));
        }
        ObjectMapper mapper = new ObjectMapper();
        while (true) {

            var records = consumer.poll(Duration.ofSeconds(5));
            LOG.info("Polled " + records.count() + " records for stage: " + name);
            for (var msg : records) {
                try {
                    // Deserialize the record value
                    LOG.info("Record: " + msg.value() + " from topic: " + msg.topic() + " partition: " + msg.partition() + " offset: " + msg.offset());

                    SchemaAndValue keyAndSchema = keyConverter.toConnectData(msg.topic(), msg.headers(), msg.key());

                    SchemaAndValue valueAndSchema = valueConverter.toConnectData(msg.topic(), msg.headers(), msg.value());
                    // JsonConverter

                    Headers headers = convertHeadersFor(msg);


                    Long timestamp = ConnectUtils.checkAndConvertTimestamp(msg.timestamp());
                    SinkRecord origRecord = new SinkRecord(msg.topic(), msg.partition(),
                            keyAndSchema.schema(), keyAndSchema.value(),
                            valueAndSchema.schema(), valueAndSchema.value(),
                            msg.offset(),
                            timestamp,
                            msg.timestampType(),
                            headers);


                    var node = new DataPacket(Map.of("record", origRecord));
                    //com.dashjoin.jsonata.json.Json.
                    // Send to output link
                    emit("output", node);
                } catch (Exception e) {
                    LOG.error("ouch", e);
                    var errorPacket = new DataPacket(Map.of(
                            "error", e.getMessage(),
                            "valueString", msg.value()));
                    if (links.containsKey("error")) {
                        emit("error", errorPacket);
                    } else
                        throw e;
                }
                lastReadOffsets.put(new TopicPartition(msg.topic(), msg.partition()), msg.offset());
            }
            if (isCacheSource && !allCaughtUp)
                maybeAllCaughtUp();

        }
    }

    private Headers convertHeadersFor(ConsumerRecord<byte[], byte[]> record) {
        Headers result = new ConnectHeaders();
        org.apache.kafka.common.header.Headers recordHeaders = record.headers();
        if (recordHeaders != null) {
            String topic = record.topic();
            for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                SchemaAndValue schemaAndValue = headerConverter.toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                result.add(recordHeader.key(), schemaAndValue);
            }
        }
        return result;
    }

    private void maybeAllCaughtUp() {
        // TODO: optimize this mess
        var beginOffsets = consumer.beginningOffsets(initialLSO.keySet());
        long remainingRecords = initialLSO.entrySet()
                .stream()
                .mapToLong(lsoEntry -> {
                            long lastRead = lastReadOffsets.get(lsoEntry.getKey());
                            long begin = beginOffsets.get(lsoEntry.getKey());
                            long target = lsoEntry.getValue() - 1;
                            if (lastRead == -1L) {
                                return lsoEntry.getValue() - begin;
                            }
                            long remaining = target - lastRead;
                            return Math.max(0, remaining);
                        }
                ).sum();

        if (remainingRecords <= 0) {
            allCaughtUp = true;
            LOG.info("KafkaConsumerStage [" + name + "] is all caught up.");
        } else {
            LOG.info("KafkaConsumerStage [" + name + "] total remaining records to catch up: " + remainingRecords);
        }

    }

    @Override
    public boolean allCaughtUp() {
        return allCaughtUp;
    }

    @Override
    public void onData(String port, DataPacket packet) {
        // No-op
    }
}
