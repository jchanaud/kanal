package io.kanal.runner.engine.stages;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.Stage;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;

import java.util.Map;

import static com.dashjoin.jsonata.Jsonata.jsonata;

@Prototype
public class TransformStage extends Stage {
    StageDefinition stageDefinition;
    Jsonata jsonata;
    public TransformStage(@Parameter String name, @Parameter StageDefinition stageDefinition) {
        super(name);
        this.stageDefinition = stageDefinition;
    }
    @Override
    public void initialize() {
        jsonata = jsonata(stageDefinition.mapping);
    }

    @Override
    public void onData(String port, DataPacket packet) {
        ObjectMapper om = new ObjectMapper();
        Object input = om.convertValue(packet.getData().get("value"), Object.class);
        var result = jsonata.evaluate(input);
        var resJsonNode = om.convertValue(result, JsonNode.class);

        // TODO: don't lose other fields in the data packet
        emit("output", new DataPacket(Map.of("value", resJsonNode)));
    }
}
