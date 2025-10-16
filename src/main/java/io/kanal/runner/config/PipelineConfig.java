package io.kanal.runner.config;

import io.micronaut.core.annotation.Introspected;

import java.util.Map;

@Introspected
public class PipelineConfig {
    public String name;
    public String version;
    public Map<String, StageDefinition> stagesDefinitions;

}