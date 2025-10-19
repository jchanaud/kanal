package io.kanal.runner.engine.entities;

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
        // Start exploration from the Sources and explore until all Destinations reached. (out + errors)
        // Through the links: count the caches reached via (ref) link. More caches = higher priority
        // TODO: Load the caches KCache? Kwack?
        // TODO: thread model
        stages.values().forEach(Stage::initialize);
        // TODO: naive priority model for caches
        //stages.values().stream().filter(Stage::needsCache).forEach(Stage::incrementCachePriority);
        // TODO: start the stages that need to be started first
        // Let them load...
        // Finally start the other Sources


        stages.values().stream()
                .filter(s -> s instanceof KafkaConsumerStage)
                //TODO: extend to other source types
                .sorted((r1, r2) -> Integer.compare(r2.priority, r1.priority))
                // TODO: wait for caches to be ready
                .forEach(s -> new Thread(((KafkaConsumerStage) s)::poll).start());
    }

    public void addStage(String key, Stage stage) {
        stages.put(key, stage);
    }
}
