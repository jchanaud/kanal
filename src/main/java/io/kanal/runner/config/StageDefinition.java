package io.kanal.runner.config;

import io.micronaut.core.annotation.Introspected;

import java.util.Map;

public class StageDefinition {
    public String name;
    public String type;
    public String topic;
    public String table;
    public String mode;
    public String mapping;
    public String key;
    public String lookupFailureBehavior;
}
