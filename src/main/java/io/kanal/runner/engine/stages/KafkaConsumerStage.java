package io.kanal.runner.engine.stages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.Stage;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import jakarta.inject.Named;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Prototype
public class KafkaConsumerStage extends Stage {
    final static Logger LOG = org.slf4j.LoggerFactory.getLogger(KafkaConsumerStage.class);

    KafkaConsumer<String, String> consumer;

    StageDefinition stageDefinition;
    KafkaDefaultConfiguration kafkaDefaultConfiguration;

    public KafkaConsumerStage(@Parameter String name, @Parameter StageDefinition stageDefinition, KafkaDefaultConfiguration kafkaDefaultConfiguration){
        super(name);
        this.stageDefinition = stageDefinition;
        this.kafkaDefaultConfiguration = kafkaDefaultConfiguration;
    }

    @Override
    public void initialize() {

        consumer = new KafkaConsumer<String, String>(kafkaDefaultConfiguration.getConfig());
        consumer.subscribe(List.of(stageDefinition.topic));

    }

    public void poll() {
        LOG.info("Starting Kafka consumer poll loop for stage: " + name);
        ObjectMapper mapper = new ObjectMapper();
        while (true){
            var records = consumer.poll(Duration.ofSeconds(5));
            LOG.info("Polled " + records.count() + " records for stage: " + name);
            for (var record : records) {

                try {
                    // Deserialize the record value
                    LOG.info("Record: " + record.value()+ " from topic: " + record.topic() + " partition: " + record.partition() + " offset: " + record.offset());
                    JsonNode data = mapper.readTree(record.value());
                    var node = new DataPacket(Map.of("value", data));
                    // Send to output link
                    emit("output", node);
                } catch(Exception e){
                    if(links.containsKey("error")){
                        var errorPacket = new DataPacket(Map.of(
                                "error", e.getMessage(),
                                "valueString", record.value()));
                        emit("error", errorPacket);
                    } else {
                        // TODO: behavior on unhandled error
                        LOG.error("Error", e);
                    }
                }

            }
        }
    }

    @Override
    public void onData(String port, DataPacket packet) {
        // No-op
    }

}
