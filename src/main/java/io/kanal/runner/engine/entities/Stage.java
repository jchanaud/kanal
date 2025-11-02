package io.kanal.runner.engine.entities;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Stage {
    private final Logger LOG = org.slf4j.LoggerFactory.getLogger(getClass());
    public String name;
    protected Map<String, List<Link.StagePort>> links = new HashMap<>();

    public Stage(String name) {
        this.name = name;
    }

    public abstract void initialize();

    public abstract void onData(String port, DataPacket packet);

    public void addLink(String port, List<Link.StagePort> stages) {
        links.put(port, stages);
    }

    protected void emit(String port, DataPacket packet) {
        LOG.info("Stage [" + name + "] emitting data on port: " + port);
        if (links.containsKey(port)) {
            // If multiple targets, wrap the ack
            if (links.get(port).size() > 1) {
                WrappedAck wrappedAck = new WrappedAck(packet::ack, links.get(port).size());
                packet = new DataPacket(packet.getRecord(), wrappedAck);
            }
            for (Link.StagePort targetStage : links.get(port)) {
                LOG.info("Stage [" + name + "] sending data to stage: " + targetStage.stage.name);
                targetStage.stage.onData(targetStage.port, packet);
            }
        } else {
            LOG.warn("Stage [" + name + "] has no links for port: " + port);
            // TODO: handle unconnected mandatory ports (during validation phase, not here)
        }
    }

    static class WrappedAck implements Runnable {
        private final Runnable ack;
        private final AtomicInteger count;

        public WrappedAck(Runnable ack, int initialCount) {
            this.ack = ack;
            this.count = new AtomicInteger(initialCount);
        }

        @Override
        public void run() {
            if (count.decrementAndGet() == 0) {
                ack.run();
            }
        }
    }
}
