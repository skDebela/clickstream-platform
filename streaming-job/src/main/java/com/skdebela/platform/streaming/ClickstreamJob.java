package com.skdebela.platform.streaming;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StateOperatorProgress;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class ClickstreamJob {
    private static final String KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String CLICKHOUSE_URL = env("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/analytics");
    private static final String CHECKPOINT_DIR = env("CHECKPOINT_DIR", "/tmp/checkpoints/clickstream");

    private static final StructType EVENT_SCHEMA = new StructType()
            .add("eventId", DataTypes.StringType)
            .add("userId", DataTypes.StringType)
            .add("page", DataTypes.StringType)
            .add("type", DataTypes.StringType)
            .add("productId", DataTypes.StringType)
            .add("timestamp", DataTypes.LongType);

    static void main(String[] args) throws Exception {
        var spark = SparkSession.builder()
                .appName("clickstream")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent event) {}

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                var p = event.progress();
                if (p.numInputRows() == 0) return;
                var ops = p.stateOperators();
                long droppedLate = Arrays.stream(ops).mapToLong(StateOperatorProgress::numRowsDroppedByWatermark).sum();
                long stateRows = Arrays.stream(ops).mapToLong(StateOperatorProgress::numRowsTotal).sum();
                System.out.printf("batch %d: input=%d droppedLate=%d stateRows=%d watermark=%s",
                        p.batchId(), p.numInputRows(), droppedLate, stateRows, p.eventTime().get("watermark"));
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {}
        });

        Dataset<Row> events = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", "clickstream")
                .option("startingOffsets", "latest")
                .load()
                .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
                .select("e.*")
                .withColumn("event_time", expr("timestamp_millis(timestamp)"));

        Dataset<Row> stats = events
                .withWatermark("event_time", "6 minutes")
                .groupBy(window(col("event_time"), "5 minutes"), col("page"), col("type"))
                .count()
                .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("page"),
                        col("type"),
                        col("count").alias("event_count"),
                        current_timestamp().alias("updated_at")
                );

        var query = stats.writeStream()
                .queryName("page_stats")
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .option("checkpointLocation", CHECKPOINT_DIR)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) ClickstreamJob::writeToClickHouse)
                .start();

        System.out.println("Streaming. Spark UI: http://localhost:4040");
        query.awaitTermination();
    }

    private static void writeToClickHouse(Dataset<Row> batch, Long batchId) {
        batch.persist();
        try {
            long rows = batch.count();
            if (rows == 0) return;

            batch.write()
                    .format("jdbc")
                    .option("url", CLICKHOUSE_URL)
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                    .option("dbtable", "page_stats")
                    .option("user", "dev")
                    .option("password", "dev")
                    .option("isolationLevel", "NONE") // ClickHouse has no transaction
                    .mode("append")
                    .save();
            System.out.printf("batch %d: wrote %d rows %n", batchId, rows);
        } finally {
            batch.unpersist();
        }
    }


    private static String env(String key, String defaultValue) {
        var value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}