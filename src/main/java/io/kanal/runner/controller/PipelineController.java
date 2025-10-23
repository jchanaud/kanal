package io.kanal.runner.controller;

import io.kanal.runner.config.PipelineConfig;
import io.kanal.runner.engine.StageFactory;
import io.kanal.runner.engine.entities.Link;
import io.kanal.runner.engine.entities.Pipeline;
import io.kanal.runner.engine.entities.Stage;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import jakarta.inject.Inject;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller("/pipeline")
public class PipelineController {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PipelineController.class);
    @Inject
    StageFactory stageFactory;

    @Get("/")
    public String start() {
        Pipeline samplePipeline = getSamplePipeline();
        samplePipeline.start();
        return "Typology Controller is up and running!";
    }

    private Pipeline getSamplePipeline() {
        String inputYaml = """
                stages-definitions:
                  kafka-transactions:
                    type: kafka-consumer
                    topic: transactions
                    links:
                      to: in
                  kafka-items
                    type: kafka-consumer
                    topic: items
                    links:
                      to: lk
                  lookup
                    type: lookup
                    links:
                      main: in
                      ref: lk
                      to: out
                    key: in.item_id == lk.id
                    lookup-failure-behavior: continue
                  transform
                    type: jsonata
                    links:
                      from: out
                      to: db
                    mapping: |
                      {
                        "transaction_id": main.transaction_id,
                        "item_id": main.item_id,
                        "total": main.qty * ref.price
                      }
                  postgres:
                    type: postgres
                    links:
                      from: db
                    table: transactions
                    mode: upsert""";

        inputYaml = """
                name: sample-pipeline
                version: "1.0"
                stages:
                  kafka-items:
                    type: kafka-consumer
                    topic: items
                  kafka-lookup:
                    type: kafka-consumer
                    topic: lookup
                  lookup:
                    type: lookup
                    cacheKey: 'id'
                    lookupKey: 'item_id'
                  transform:
                      type: transform
                      mapping: '
                        {
                          "testouille": test * 2,
                          "ref": ref
                        }'
                  peekaboo:
                    type: peek
                links:
                  in:
                    source:
                      stage: kafka-items
                      port: output
                    target:
                      stage: lookup
                      port: input
                  ref:
                    source:
                      stage: kafka-lookup
                      port: output
                    target:
                      stage: lookup
                      port: reference
                  out_lk:
                    source:
                      stage: lookup
                      port: output
                    target:
                      stage: transform
                      port: input
                  out_tf:
                    source:
                      stage: transform
                      port: output
                    target:
                      stage: peekaboo
                      port: input
                """;

        Yaml yaml = new Yaml();
        PipelineConfig config = yaml.loadAs(inputYaml, PipelineConfig.class);
        // TODO: validate config

        // Creates a Map of links to stages to connect them later
        // (in, (from, [peekaboo]))
        // (in, (to, [kafka-items]))
        Map<String, Stage> stages = new HashMap<>();
        for (var entry : config.stages.entrySet()) {
            Stage stage = stageFactory.create(entry.getKey(), entry.getValue());
            stages.put(entry.getKey(), stage);
        }
        // Connect the stages with each other through their links
        config.links.forEach((linkName, linkDef) -> {
            ;
            // TODO: error handling
            var sourceStage = stages.get(linkDef.source.stage);
            var destStage = stages.get(linkDef.target.stage);
            // Add the destination stage to the source stage's link port
            sourceStage.addLink(linkDef.source.port, List.of(Link.StagePort.of(destStage, linkDef.target.port)));
            // Add the source stage to the destination stage's link port
            destStage.addLink(linkDef.target.port, List.of(Link.StagePort.of(sourceStage, linkDef.source.port)));

        });
        // TODO: consistency check of links/stages
        // TODO: error handling
        // TODO: acyclic check
        Pipeline pipeline = new Pipeline(config.name, config.version, stages);
        return pipeline;
    }
}