package io.kanal.runner.engine.stages;

import io.kanal.runner.engine.Stage;
import org.slf4j.Logger;

public class PeekStage extends Stage {
    Logger LOG = org.slf4j.LoggerFactory.getLogger(PeekStage.class);
    public PeekStage(String name) {
        super(name);
    }

    @Override
    public void initialize() {
        // No initialization needed for PeekStage
    }

    @Override
    public void onData(DataPacket dataPacket) {
        LOG.info("PeekStage [" + name + "] data: " + dataPacket.getData());
        emit("out", dataPacket);
    }
}
