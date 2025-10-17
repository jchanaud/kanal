package io.kanal.runner.engine.stages;

import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.Stage;
import io.micronaut.context.annotation.Parameter;
import org.slf4j.Logger;

public class LookupStage extends Stage {
    Logger LOG = org.slf4j.LoggerFactory.getLogger(LookupStage.class);

    public LookupStage(@Parameter String name, @Parameter StageDefinition stageDefinition) {
        super(name);
    }

    @Override
    public void initialize() {

    }

    @Override
    public void onData(String port, DataPacket packet) {
        LOG.info("Received data at LookupStage [" + name + "] on port: " + port);

    }
}
