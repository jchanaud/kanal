package io.kanal.runner.engine.entities;

import java.util.List;
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
        // Figure out the stages that need to be loaded first
        List<CacheStage> caches = stages.values().stream()
                .filter(CacheStage.class::isInstance)
                .map(CacheStage.class::cast)
                .toList();

        List<SourceStage> cacheSources = caches
                .stream()
                .map(CacheStage::getCacheSources)
                .flatMap(List::stream)
                .distinct()
                .toList();

        cacheSources.forEach(SourceStage::setCacheSource);

        stages.values().forEach(Stage::initialize);

        // naive priority model for caches
        // start the stages that need to be started first
        cacheSources.forEach(s -> new Thread(s::poll).start());
        // Let them load...
        boolean allCachesLoaded = false;
        while (!allCachesLoaded) {
            allCachesLoaded = caches.stream()
                    .flatMap(c -> c.getCacheSources().stream())
                    .allMatch(SourceStage::allCaughtUp);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // Finally start the other Sources
        stages.values().stream()
                .filter(SourceStage.class::isInstance)
                .map(SourceStage.class::cast)
                .filter(stage -> !stage.isCacheSource())
                .forEach(stage -> new Thread(stage::poll).start());
    }

    public void addStage(String key, Stage stage) {
        stages.put(key, stage);
    }
}
