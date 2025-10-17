package io.kanal.runner.engine;

import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.stages.KafkaConsumerStage;
import io.kanal.runner.engine.stages.LookupStage;
import io.kanal.runner.engine.stages.PeekStage;
import io.kanal.runner.engine.stages.TransformStage;
import io.micronaut.configuration.kafka.config.KafkaDefaultConfiguration;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.Qualifier;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public class StageFactory {

    @Inject
    ApplicationContext context;

    public Stage create(String name, StageDefinition stageDefinition) {

        if(stageDefinition.type.equals("kafka-consumer"))
            return context.createBean(KafkaConsumerStage.class, /*Qualifiers.byName(name),*/ name, stageDefinition);

        if(stageDefinition.type.equals("peek"))
            return context.createBean(PeekStage.class, name, stageDefinition);
        if(stageDefinition.type.equals("lookup"))
            return context.createBean(LookupStage.class, name, stageDefinition);
        if(stageDefinition.type.equals("transform"))
            return context.createBean(TransformStage.class, name, stageDefinition);
        return null;
    }
}

