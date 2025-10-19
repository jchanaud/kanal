package io.kanal.runner.config;

public class LinkDefinition {
    public String name;
    public StagePortDefinition source;
    public StagePortDefinition target;
    public StagePortDefinition reference;

    public static class StagePortDefinition {
        public String stage;
        public String port;
    }
}

