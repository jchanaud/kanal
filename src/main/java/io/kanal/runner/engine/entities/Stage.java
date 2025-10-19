package io.kanal.runner.engine.entities;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class Stage {
    private final Logger LOG = org.slf4j.LoggerFactory.getLogger(getClass());
    public String name;
    public int priority = 0;
    public boolean isCacheSource = false;
    protected Map<String, List<Link.StagePort>> links = new HashMap<>();

    public Stage(String name) {
        this.name = name;
    }

    public abstract void initialize();

    public abstract void onData(String port, DataPacket packet);

    public void addLink(String port, List<Link.StagePort> stages) {
        links.put(port, stages);
    }

    protected void emit(String port, DataPacket packet) {
        LOG.info("Stage [" + name + "] emitting data on port: " + port);
        if (links.containsKey(port)) {
            for (Link.StagePort targetStage : links.get(port)) {
                LOG.info("Stage [" + name + "] sending data to stage: " + targetStage.stage.name);
                targetStage.stage.onData(targetStage.port, packet);
            }
        } else {
            LOG.warn("Stage [" + name + "] has no links for port: " + port);
            // TODO: handle unconnected mandatory ports (during validation phase, not here)
        }
    }

    public void setCacheSource() {
        this.priority += 1;
        this.isCacheSource = true;
        LOG.info("Stage [" + name + "] set as Cache source & priority of increased to: " + this.priority);
        // Propagate to upstream stages on "input" port
        links.getOrDefault("input", List.of()).forEach(link -> link.stage.setCacheSource());
    }
}
