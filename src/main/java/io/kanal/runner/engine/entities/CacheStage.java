package io.kanal.runner.engine.entities;

import java.util.List;

public abstract class CacheStage extends Stage {

    public CacheStage(String name) {
        super(name);
    }

    public List<SourceStage> getCacheSources() {
        // navigate up until we find the source stages
        return this.links.get("reference").stream().flatMap(stagePort -> getCacheSources(stagePort.stage).stream()).toList();
    }

    private List<SourceStage> getCacheSources(Stage stage) {
        if (stage instanceof SourceStage sourceStage)
            return List.of(sourceStage);
        var inputsOfStage = stage.links.get("input");
        return inputsOfStage.stream().flatMap(sp -> getCacheSources(sp.stage).stream()).toList();
    }
}
