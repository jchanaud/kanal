package io.kanal.runner.engine.stages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.entities.DataPacket;
import io.kanal.runner.engine.entities.Stage;
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
public class KafkaConsumerStage extends Stage {
    final static Logger LOG = org.slf4j.LoggerFactory.getLogger(KafkaConsumerStage.class);

    KafkaConsumer<String, String> consumer;

    StageDefinition stageDefinition;
    KafkaDefaultConfiguration kafkaDefaultConfiguration;

    public KafkaConsumerStage(@Parameter String name, @Parameter StageDefinition stageDefinition, KafkaDefaultConfiguration kafkaDefaultConfiguration) {
        super(name);
        this.stageDefinition = stageDefinition;
        this.kafkaDefaultConfiguration = kafkaDefaultConfiguration;
    }

    @Override
    public void initialize() {


    }

    public void poll() {

        if (isCacheSource) {
            LOG.info("Starting Kafka consumer poll loop SEEK for stage: " + name);
            var props = kafkaDefaultConfiguration.getConfig();
            props.put("enable.auto.commit", "false");
            consumer = new KafkaConsumer<String, String>(props);
            consumer.assign(List.of(new TopicPartition(stageDefinition.topic, 0)));
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
            }
        }
    }

    @Override
    public void onData(String port, DataPacket packet) {
        // No-op
    }

}
