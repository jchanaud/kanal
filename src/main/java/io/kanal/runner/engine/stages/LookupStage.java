package io.kanal.runner.engine.stages;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.entities.DataPacket;
import io.kanal.runner.engine.entities.Stage;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.dashjoin.jsonata.Jsonata.jsonata;

@Prototype
public class LookupStage extends Stage {
    Logger LOG = org.slf4j.LoggerFactory.getLogger(LookupStage.class);

    private Map<String, Object> referenceData = new HashMap<>();
    private Jsonata jsonataCacheKey;
    private Jsonata jsonataLookupKey;
    private StageDefinition stageDefinition;

    public LookupStage(@Parameter String name, @Parameter StageDefinition stageDefinition) {
        super(name);
        this.stageDefinition = stageDefinition;
    }

    @Override
    public void initialize() {
        LOG.info("Initializing LookupStage [" + name + "]");
        // Navigate upstream and increase the priority of reference data sources
        links.get("reference").forEach(link -> link.stage.setCacheSource());
        jsonataCacheKey = jsonata(stageDefinition.cacheKey);
        jsonataLookupKey = jsonata(stageDefinition.lookupKey);
    }

    @Override
    public void onData(String port, DataPacket packet) {
        LOG.info("Received data at LookupStage [" + name + "] on port: " + port);
        if (port.equals("reference")) {
            // Handle reference data
            LOG.info("LookupStage [" + name + "] received reference data: " + packet.getData());
            // Store reference data for future lookups
            ObjectMapper om = new ObjectMapper();
            Object input = om.convertValue(packet.getData().get("value"), Object.class);
            var key = jsonataCacheKey.evaluate(input);
            referenceData.put(key.toString(), packet.getData().get("value"));
            LOG.info("LookupStage [" + name + "] stored reference data with key: " + key);
        } else if (port.equals("input")) {
            // Handle input data and perform lookup
            //LOG.info("LookupStage [" + name + "] received input data: " + packet.getData());
            // Perform lookup logic here (not implemented)
            LOG.info("LookupStage [" + name + "] performing lookup (not implemente), seen cache of size" + referenceData.size());

            ObjectMapper om = new ObjectMapper();
            JsonNode node = (JsonNode) packet.getData().get("value");
            Object input = om.convertValue(node, Object.class);
            var lookupKey = jsonataLookupKey.evaluate(input);
            LOG.info("LookupStage [" + name + "] performing lookup with key: " + lookupKey);
            Object lookupValue = referenceData.get(lookupKey.toString());
            LOG.info("LookupStage [" + name + "] found lookup value: " + lookupValue);
            var updatedNode = ((ObjectNode) node).set("ref", (JsonNode) lookupValue);
            packet.getData().put("value", updatedNode);
            emit("output", packet);
        } else {
            LOG.warn("LookupStage [" + name + "] received data on unknown port: " + port);
        }
    }
}
