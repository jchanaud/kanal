package io.kanal.runner.engine.stages;

import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.entities.DataPacket;
import io.kanal.runner.engine.entities.Stage;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

@Prototype
public class PeekStage extends Stage {
    Logger LOG = LoggerFactory.getLogger(PeekStage.class);
    Level logLevel;
    StageDefinition stageDefinition;
    @Inject
    MeterRegistry meterRegistry;

    public PeekStage(@Parameter String name, @Parameter StageDefinition stageDefinition) {
        super(name);
        this.stageDefinition = stageDefinition;
    }

    @Override
    public void initialize() {
        try {
            LOG.info("Initializing PeekStage [" + name + "] with log level: " + stageDefinition.logLevel);
            logLevel = Level.valueOf(stageDefinition.logLevel.toUpperCase());
        } catch (Exception e) {
            LOG.warn("Error initializing PeekStage [" + name + "], defaulting to INFO: " + e.getMessage());
            logLevel = Level.INFO;
        }
//        Counter.builder("peekstage.ondata.count")
//                .description("Number of times PeekStage is initialized")
//                .
//                .register(meterRegistry)

    }

    @Counted(value = "peekstage.ondata.count", description = "Number of times onData is called in PeekStage")
    @Override
    public void onData(String port, DataPacket dataPacket) {
        LOG.atLevel(logLevel).log("PeekStage [" + name + "] data: " + dataPacket.getData());
        emit("out", dataPacket);
    }
}
