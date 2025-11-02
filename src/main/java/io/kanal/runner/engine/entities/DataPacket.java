package io.kanal.runner.engine.entities;

import lombok.Getter;
import org.apache.kafka.connect.connector.ConnectRecord;

@Getter
public class DataPacket {
    private final ConnectRecord<?> record;
    private final Runnable onAck;

    public DataPacket(ConnectRecord<?> record, Runnable onAck) {
        this.onAck = onAck;
        this.record = record;
    }

    public void ack() {
        if (onAck != null) {
            onAck.run();
        }
    }
}
