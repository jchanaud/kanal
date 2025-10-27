package io.kanal.runner.engine.stages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.entities.DataPacket;
import io.kanal.runner.engine.entities.SourceStage;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Prototype
public class KafkaConsumerStage extends SourceStage {
    final static Logger LOG = org.slf4j.LoggerFactory.getLogger(KafkaConsumerStage.class);

    KafkaConsumer<String, String> consumer;

    StageDefinition stageDefinition;
    KafkaDefaultConfiguration kafkaDefaultConfiguration;
    Map<TopicPartition, Long> initialLSO;
    Map<TopicPartition, Long> lastReadOffsets = new HashMap<>();
    boolean allCaughtUp = false;

    public KafkaConsumerStage(@Parameter String name, @Parameter StageDefinition stageDefinition, KafkaDefaultConfiguration kafkaDefaultConfiguration) {
        super(name, CacheSupport.SUPPORTED);
        this.stageDefinition = stageDefinition;
        this.kafkaDefaultConfiguration = kafkaDefaultConfiguration;
    }

    @Override
    public void initialize() {


    }

    @Override
    public void poll() {

        if (isCacheSource) {
            LOG.info("Starting Kafka consumer poll loop SEEK for stage: " + name);
            var props = kafkaDefaultConfiguration.getConfig();
            props.put("enable.auto.commit", "false");
            consumer = new KafkaConsumer<String, String>(props);

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
            consumer = new KafkaConsumer<String, String>(kafkaDefaultConfiguration.getConfig());
            consumer.subscribe(List.of(stageDefinition.topic));
        }
        ObjectMapper mapper = new ObjectMapper();
        while (true) {

            var records = consumer.poll(Duration.ofSeconds(5));
            LOG.info("Polled " + records.count() + " records for stage: " + name);
            for (var record : records) {
                try {
                    // Deserialize the record value
                    LOG.info("Record: " + record.value() + " from topic: " + record.topic() + " partition: " + record.partition() + " offset: " + record.offset());
                    JsonNode data = mapper.readTree(record.value());
                    var map = new HashMap<String, Object>();
                    map.put("value", data);
                    var node = new DataPacket(map);
                    //com.dashjoin.jsonata.json.Json.
                    // Send to output link
                    emit("output", node);
                } catch (Exception e) {
                    var errorPacket = new DataPacket(Map.of(
                            "error", e.getMessage(),
                            "valueString", record.value()));
                    emit("error", errorPacket);
                }
                lastReadOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            if (!allCaughtUp)
                maybeAllCaughtUp();

        }
    }

    private void maybeAllCaughtUp() {

        long remainingRecords = initialLSO.entrySet()
                .stream()
                .mapToLong(lsoEntry -> {
                            long lastRead = lastReadOffsets.get(lsoEntry.getKey());
                            long target = lsoEntry.getValue() - 1;
                            long remaining = target - lastRead;
                            return Math.max(0, remaining);
                        }
                ).sum();

        if (remainingRecords == 0) {
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
