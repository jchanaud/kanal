package io.kanal.runner.engine;

import io.kanal.runner.engine.stages.KafkaConsumerStage;

import java.util.Map;

public class Pipeline {
    private Map<String, Stage> stages;

    private String name;
    private String version;

    public Pipeline(String name, String version, Map<String, Stage> stages) {
          this.stages = stages;
          this.name = name;
          this.version = version;
    }

    public void start() {
        // TODO: Figure out the stages that need to be loaded first
        // TODO: Load the caches KCache? Kwack?
        // TODO: thread model
        stages.values().forEach(Stage::initialize);
        // TODO: naive priority model for caches
        //stages.values().stream().filter(Stage::needsCache).forEach(Stage::incrementCachePriority);
        // TODO: start the stages that need to be started first

        stages.values().stream()
                .filter(s -> s instanceof KafkaConsumerStage)
                .forEach(s -> new Thread(((KafkaConsumerStage)s)::poll).start());
    }

    public void addStage(String key, Stage stage) {
        stages.put(key, stage);
    }
}
