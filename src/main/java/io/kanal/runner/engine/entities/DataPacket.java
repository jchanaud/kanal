package io.kanal.runner.engine.entities;

import java.util.HashMap;
import java.util.Map;

public class DataPacket {
    private Map<String, Object> data = new HashMap<>();

    public DataPacket(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}
