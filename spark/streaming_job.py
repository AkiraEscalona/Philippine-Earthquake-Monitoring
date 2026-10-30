"""Spark Structured Streaming job: Kafka -> validate/enrich -> MongoDB.

Runs inside the `spark` container (see docker-compose.yml).

* Valid events are upserted into `events` using event_id as _id, so replays and
  PHIVOLCS revisions never create duplicates (idempotent, at-least-once safe).
* Invalid events / unparseable messages go to `rejected_events` with a reason.
* Producer heartbeats and Spark batch stats go to `pipeline_status` (_id = producer | spark),
  which the dashboard uses to show data freshness.
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.environ.get("SPARK_KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "philippine-earthquakes")
MONGO_URI = os.environ.get("SPARK_MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "earthquake_monitor")
CHECKPOINT = os.environ.get("SPARK_CHECKPOINT_DIR", "/opt/checkpoints/earthquakes")
TRIGGER = os.environ.get("SPARK_TRIGGER_INTERVAL", "10 seconds")

MESSAGE_SCHEMA = T.StructType([
    T.StructField("kind", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("timestamp_utc", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("depth_km", T.DoubleType()),
    T.StructField("magnitude", T.DoubleType()),
    T.StructField("location", T.StringType()),
    T.StructField("bulletin_url", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("source_last_updated", T.StringType()),
    T.StructField("fetched_at", T.StringType()),
    T.StructField("feed_total", T.LongType()),
    T.StructField("valid", T.LongType()),
    T.StructField("rejected", T.LongType()),
    T.StructField("published", T.LongType()),
    T.StructField("poll_interval_s", T.LongType()),
])


def write_mongo(df, collection):
    # With the default operationType=replace, rows are matched on _id and upserted.
    (df.write.format("mongodb").mode("append")
       .option("connection.uri", MONGO_URI)
       .option("database", MONGO_DB)
       .option("collection", collection)
       .option("operationType", "replace")
       .option("idFieldList", "_id")
       .save())


def magnitude_class(col):  # keep in sync with eqmon/aggregate.py MAG_LABELS
    return (F.when(col < 2, "<2").when(col < 3, "2-3").when(col < 4, "3-4")
             .when(col < 5, "4-5").when(col < 6, "5-6").otherwise(">=6"))


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    spark = batch_df.sparkSession
    msgs = (batch_df
            .select("topic", "partition", "offset", F.col("value").cast("string").alias("raw_value"))
            .withColumn("m", F.from_json("raw_value", MESSAGE_SCHEMA))
            .persist())
    try:
        kind = F.col("m.kind")
        unknown = msgs.filter(kind.isNull() | (~kind.isin("event", "heartbeat")))

        # ---- validation of event messages -------------------------------------------------
        ev = (msgs.filter(kind == "event")
              .select("m.*", "topic", "partition", "offset", "raw_value")
              .withColumn("event_time", F.to_timestamp("timestamp_utc")))
        reason = (
            F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), "missing_event_id")
            .when(F.col("event_time").isNull(), "bad_timestamp")
            .when(F.col("latitude").isNull() | (F.col("latitude") < -90) | (F.col("latitude") > 90), "bad_latitude")
            .when(F.col("longitude").isNull() | (F.col("longitude") < -180) | (F.col("longitude") > 180), "bad_longitude")
            .when(F.col("magnitude").isNull() | (F.col("magnitude") < -2) | (F.col("magnitude") > 10), "bad_magnitude")
            .when(F.col("depth_km").isNotNull() & ((F.col("depth_km") < 0) | (F.col("depth_km") > 800)), "bad_depth")
            .when(F.col("location").isNull() | (F.trim(F.col("location")) == ""), "missing_location")
        )
        ev = ev.withColumn("reject_reason", reason)
        good = ev.filter(F.col("reject_reason").isNull())
        bad = ev.filter(F.col("reject_reason").isNotNull())

        # ---- processing: de-duplicate inside the batch, enrich ----------------------------
        latest = Window.partitionBy("event_id").orderBy(F.col("fetched_at").desc())
        region_raw = F.trim(F.regexp_extract("location", r"\(([^()]*)\)\s*$", 1))
        local = F.from_utc_timestamp("event_time", "Asia/Manila")
        events_out = (
            good.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1")
            .select(
                F.col("event_id").alias("_id"), "event_id", "event_time", "latitude", "longitude",
                "depth_km", "magnitude", magnitude_class(F.col("magnitude")).alias("magnitude_class"),
                F.trim("location").alias("location"),
                F.when(region_raw != "", region_raw).otherwise("Unknown").alias("region"),
                "bulletin_url", "source",
                F.to_timestamp("source_last_updated").alias("source_last_updated"),
                F.to_timestamp("fetched_at").alias("fetched_at"),
                F.date_format(local, "yyyy-MM-dd").alias("event_date_ph"),
                F.hour(local).alias("hour_ph"),
                F.current_timestamp().alias("ingested_at"),
                F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
            )
        ).persist()

        rejected_out = (
            bad.select("topic", "partition", "offset", "raw_value", F.col("reject_reason").alias("reason"))
            .unionByName(unknown.select("topic", "partition", "offset", "raw_value",
                                         F.lit("invalid_json_or_unknown_kind").alias("reason")))
            .select(
                F.sha2(F.concat_ws("|", "topic", "partition", "offset"), 256).alias("_id"),
                "reason", F.substring("raw_value", 1, 2000).alias("raw_value"),
                "topic", "partition", "offset", F.current_timestamp().alias("rejected_at"),
            )
        ).persist()

        n_valid, n_rejected = events_out.count(), rejected_out.count()
        if n_valid:
            write_mongo(events_out, "events")
        if n_rejected:
            write_mongo(rejected_out, "rejected_events")

        # ---- status documents (written after the data so they never run ahead of it) -------
        hb = (msgs.filter(kind == "heartbeat").select("m.*")
              .withColumn("fetched_ts", F.to_timestamp("fetched_at"))
              .orderBy(F.col("fetched_ts").desc()).limit(1))
        producer_doc = hb.select(
            F.lit("producer").alias("_id"), F.col("fetched_ts").alias("fetched_at"),
            F.to_timestamp("source_last_updated").alias("source_last_updated"),
            "feed_total", "valid", "rejected", "published", "poll_interval_s",
            F.current_timestamp().alias("received_at"),
        )
        write_mongo(producer_doc, "pipeline_status")

        spark_doc = (spark.createDataFrame(
            [("spark", int(batch_id), n_valid, n_rejected)],
            "_id string, last_batch_id long, events_upserted long, events_rejected long")
            .withColumn("last_batch_at", F.current_timestamp()))
        write_mongo(spark_doc, "pipeline_status")
        print(f"[batch {batch_id}] upserted={n_valid} rejected={n_rejected}", flush=True)
    finally:
        msgs.unpersist()


def main():
    spark = (SparkSession.builder.appName("philippine-earthquake-stream")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", 20000)
              .load())

    query = (stream.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
