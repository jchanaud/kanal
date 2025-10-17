package io.kanal.runner.config;

import io.micronaut.core.annotation.Introspected;

import java.util.Map;

public class PipelineConfig {
    public String name;
    public String version;
    public Map<String, StageDefinition> stages;
    public Map<String, LinkDefinition> links;

}