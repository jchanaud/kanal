package io.kanal.runner.engine.entities;

public class Link {
    public String name;
    public Stage sourceStage;
    public Stage targetStage;

    // TODO: port information?
    // TODO: type system information
    public static class StagePort {
        public Stage stage;
        public String port;

        public static StagePort of(Stage stage, String port) {
            StagePort sp = new StagePort();
            sp.stage = stage;
            sp.port = port;
            return sp;
        }
    }
}
