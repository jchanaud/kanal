package io.kanal.runner.engine.stages;

import io.confluent.connect.jdbc.JdbcSinkConnector;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.kanal.runner.config.StageDefinition;
import io.kanal.runner.engine.entities.DataPacket;
import io.kanal.runner.engine.entities.Stage;
import io.micronaut.context.annotation.Parameter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcSinkStage extends Stage {
    StageDefinition stageDefinition;

    private SinkTask task;

    public JdbcSinkStage(@Parameter String name, @Parameter StageDefinition stageDefinition) {
        super(name);
        this.stageDefinition = stageDefinition;
    }

    @Override
    public void initialize() {
        //var p = new Plugins(Map.of(WorkerConfig.PLUGIN_PATH_CONFIG, "C:\\plugins"));
        //var connector = p.newConnector("io.aiven.connect.jdbc.JdbcSinkConnector");
        //var task = p.newTask(connector.taskClass());

        var props = Map.of("connection.url", "jdbc:postgresql://localhost:5432/postgres",
                "connection.user", "postgres",
                "connection.password", "example",
                "auto.create", "true");

        JdbcSinkConnector connector = new JdbcSinkConnector();
        connector.validate(props);
        task = new JdbcSinkTask();
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
        task.start(props);
    }

    @Override
    public void onData(String port, DataPacket packet) {

        Schema valueSchema = SchemaBuilder.struct()
                .field("id", SchemaBuilder.INT32_SCHEMA)
                .field("name", SchemaBuilder.STRING_SCHEMA)
                .build();
        Struct struct = new Struct(valueSchema)
                .put("id", 1)
                .put("name", "test-name");
        var record = new SinkRecord("test", 0, null, null, valueSchema, struct, 0L);
        try {
            task.put(List.of(record));
        } catch (RetriableException e) {
            // Handle retriable exception
        } catch (Exception e) {
            // Handle non-retriable exception
        }
    }
}
