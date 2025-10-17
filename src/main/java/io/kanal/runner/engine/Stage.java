package io.kanal.runner.engine;

import io.kanal.runner.engine.stages.DataPacket;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class Stage {
    private final Logger LOG = org.slf4j.LoggerFactory.getLogger(getClass());
    public  String name;
    protected Map<String, List<Stage>> links = new HashMap<>();

    public Stage(String name) {
        this.name = name;
    }

    public abstract void initialize();
    public abstract void onData(String port, DataPacket packet);

    public void addLink(String port, List<Stage> stages) {
        links.put(port, stages);
    }

    protected void emit(String port, DataPacket packet) {
        LOG.info("Stage [" + name + "] emitting data on port: " + port);
        if(links.containsKey(port)) {
            for (Stage targetStage : links.get(port)) {
                LOG.info("Stage [" + name + "] sending data to stage: " + targetStage.name);
                targetStage.onData(port, packet);
            }
        } else {
            LOG.warn("Stage [" + name + "] has no links for port: " + port);
            // TODO: handle unconnected mandatory ports (during validation phase, not here)
        }
    }
}
