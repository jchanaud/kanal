package io.kanal.runner.engine;

import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.stages.KafkaConsumerStage;
import io.kanal.runner.engine.stages.PeekStage;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class StageFactory {

    @Inject
    KafkaDefaultConfiguration kafkaDefaultConfiguration;

    public Stage create(String name, StageDefinition stageDefinition) {

        if(stageDefinition.type.equals("kafka-consumer"))
            return new KafkaConsumerStage(name, stageDefinition, kafkaDefaultConfiguration);
        if(stageDefinition.type.equals("peek"))
            return new PeekStage(name);
        if(stageDefinition.type.equals("transform"))
            return new PeekStage(name); // TODO: Replace with TransformStage
        return null;
    }
}

