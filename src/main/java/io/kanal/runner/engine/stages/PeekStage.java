package io.kanal.runner.engine.stages;

import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.Stage;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import org.slf4j.Logger;

@Prototype
public class PeekStage extends Stage {
    Logger LOG = org.slf4j.LoggerFactory.getLogger(PeekStage.class);
    public PeekStage(@Parameter String name, @Parameter StageDefinition stageDefinition) {
        super(name);
    }

    @Override
    public void initialize() {
        // No initialization needed for PeekStage
    }

    @Override
    public void onData(String port, DataPacket dataPacket) {
        LOG.info("PeekStage [" + name + "] data: " + dataPacket.getData());
        emit("out", dataPacket);
    }
}
