package io.kanal.runner.controller;

import io.kanal.runner.config.PipelineConfig;
import io.kanal.runner.engine.Pipeline;
import io.kanal.runner.engine.StageFactory;
import io.kanal.runner.engine.Stage;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import jakarta.inject.Inject;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller("/pipeline")
public class PipelineController {

    @Inject
    StageFactory stageFactory;

    @Get("/")
    public String start() {

        Pipeline samplePipeline = getSamplePipeline();
        samplePipeline.start();;
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
  stagesDefinitions:
    kafka-items:
      type: kafka-consumer
      topic: items
      links:
        to: in
    peekaboo:
      type: peek
      links:
        from: in
        # to: out""";

        Yaml yaml = new Yaml();
        PipelineConfig config = yaml.loadAs(inputYaml, PipelineConfig.class);
        // TODO: validate config

        // Creates a Map of links to stages to connect them later
        // (in, (from, [peekaboo]))
        // (in, (to, [kafka-items]))
        Map<String, Stage> stages = new HashMap<>();
        Map<String, Map<String, List<Stage>>> links = new HashMap<>();
        for (var entry : config.stagesDefinitions.entrySet()) {
            Stage stage = stageFactory.create(entry.getKey(), entry.getValue());
            stages.put(entry.getKey(), stage);
            for (var linkEntry : entry.getValue().links.entrySet()) {
                links.putIfAbsent(linkEntry.getValue(), new HashMap<>());
                // TODO: Support multiple stages per link (e.g., fan-out)
                links.get(linkEntry.getValue()).put(linkEntry.getKey(), List.of(stage));
            }
        }
        // Connect the stages with each other through their links

        for(var stage : stages.values()){
            var stageDef = config.stagesDefinitions.get(stage.name);
            stageDef.links.forEach((port, link) -> {
                stage.addLink(port, links.get(link).get(port));
                // FIXME: from and to are the wrong way round
            });
        }
        // TODO: consistency check of links/stages
        // TODO: error handling
        // TODO: acyclic check
        Pipeline pipeline = new Pipeline(config.name, config.version, stages);
        return pipeline;
    }
}