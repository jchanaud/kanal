package io.kanal.runner.controller;

import com.fasterxml.jackson.databind.JsonNode;
import io.kanal.runner.beanfactories.StageFactory;
import io.kanal.runner.config.PipelineConfig;
import io.kanal.runner.engine.Pipeline;
import io.kanal.runner.engine.converters.ConnectRecordConverter;
import io.kanal.runner.engine.entities.Link;
import io.kanal.runner.engine.entities.Stage;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import jakarta.inject.Inject;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller("/pipeline")
public class PipelineController {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PipelineController.class);
    @Inject
    StageFactory stageFactory;

    @Inject
    ConnectRecordConverter<SinkRecord> recordConverter;

    @Inject
    Plugins plugins;

    @Get("/")
    public String start() {

        //jdbcSinkTest();
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
                  branching:
                    type: branch
                    branches:
                      - condition: 'value.testouille > 10'
                        port: high-values
                      - condition: 'value.testouille <= 10'
                        port: low-values
                    default-port: others
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

    public void jdbcSinkTest() {
        var connector = plugins.newConnector("JdbcSinkConnector");
        SinkTask task = (SinkTask) plugins.newTask(connector.taskClass());
        task.initialize(new SinkTaskContext() {
            @Override
            public Map<String, String> configs() {
                return Map.of();
            }

            @Override
            public void offset(Map<TopicPartition, Long> offsets) {

            }

            @Override
            public void offset(TopicPartition tp, long offset) {

            }

            @Override
            public void timeout(long timeoutMs) {

            }

            @Override
            public Set<TopicPartition> assignment() {
                return Set.of();
            }

            @Override
            public void pause(TopicPartition... partitions) {

            }

            @Override
            public void resume(TopicPartition... partitions) {

            }

            @Override
            public void requestCommit() {

            }

            @Override
            public PluginMetrics pluginMetrics() {
                return null;
            }
        });
        var props = Map.of("connection.url", "jdbc:postgresql://localhost:5432/postgres",
                "connection.user", "postgres",
                "connection.password", "example",
                "auto.create", "true");
        task.start(props);
        Schema valueSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.INT32_SCHEMA)
                .field("name", SchemaBuilder.STRING_SCHEMA)
                .field("address", SchemaBuilder.struct()
                        .field("street", SchemaBuilder.STRING_SCHEMA)
                        .field("city", SchemaBuilder.STRING_SCHEMA)
                        .build())
                .build();
        Struct struct = new Struct(valueSchema)
                .put("id", 1)
                .put("name", "test-name")
                .put("address", new Struct(valueSchema.field("address").schema())
                        .put("street", "123 Main St")
                        .put("city", "Anytown"));
        var record = new SinkRecord("test", 0, null, null, valueSchema, struct, 0L);

        JsonNode node = recordConverter.recordToJsonNode(record);
        SinkRecord newRecord = recordConverter.jsonNodeToRecord(record, node);

        task.put(List.of(record));
    }
}