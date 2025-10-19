package io.kanal.runner.config;

import java.util.Map;

public class PipelineConfig {
    public String name;
    public String version;
    public Map<String, StageDefinition> stages;
    public Map<String, LinkDefinition> links;

}