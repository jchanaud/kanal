package io.kanal.runner.config;

import io.micronaut.core.annotation.Introspected;

public class LinkDefinition {
    public String name;
    public StagePort source;
    public StagePort target;
    public StagePort reference;

    public static class StagePort {
        public String stage;
        public String port;
    }
}

