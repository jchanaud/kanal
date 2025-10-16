package io.kanal.runner.engine;

import io.kanal.runner.engine.stages.DataPacket;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class Stage {
    Logger LOG = org.slf4j.LoggerFactory.getLogger(Stage.class);
    public  String name;
    protected Map<String, List<Stage>> links = new HashMap<>();

    public Stage(String name) {
        this.name = name;
    }

    public abstract void initialize();
    public abstract void onData(DataPacket packet);

    public void addLink(String port, List<Stage> stages) {
        links.put(port, stages);
    }

    protected void emit(String port, DataPacket packet) {
        LOG.info("Stage [" + name + "] emitting data on port: " + port);
        links.get(port).forEach(stage -> stage.onData(packet));
    }
}
