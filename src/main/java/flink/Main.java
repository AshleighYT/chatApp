package flink;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import model.KafkaEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import flink.process.SensitiveWordFilter;
import flink.serdes.KafkaEventDeserializer;
import flink.serdes.KafkaEventSerializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

public class Main {

    public static final String BOOTSTRAP_SERVERS = "kafka0:9094,kafka1:9094,kafka2:9092";
    public static final String SOURCE_TOPIC = "chat-data";
    public static final String SINK_TOPIC = "processed-data";
    public static final String WARNING_TOPIC = "warning-data";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().enableCheckpointing(60000);

        // kafka source
        KafkaSource<KafkaEvent> source = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId("flink-chat-group")
                .setDeserializer(new KafkaEventDeserializer())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("partition.discovery.interval.ms", "60000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();

        // kafka sink
        KafkaSink<KafkaEvent> sink = KafkaSink.<KafkaEvent>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(new KafkaEventSerializer(SINK_TOPIC))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Define watermark strategy for event time processing
        WatermarkStrategy<KafkaEvent> watermarkStrategy = WatermarkStrategy
                .<KafkaEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        // Process stream for filtering and analyzing messages
        DataStream<KafkaEvent> stream = env.fromSource(source, WatermarkStrategy
                        .<KafkaEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()), SOURCE_TOPIC)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // Apply the sensitive word filter
        DataStream<KafkaEvent> filteredStream = stream
                .map(new SensitiveWordFilter());

        // Define Warning logic
        class GenerateWarningProcessFunction extends ProcessWindowFunction<KafkaEvent, String, String, TimeWindow> {
            @Override
            public void process(String key, Context context, Iterable<KafkaEvent> elements, Collector<String> out) {
                long count = StreamSupport.stream(elements.spliterator(), false).count();
                if (count > 3) {
                    String warning = String.format("WARNING: User %s sent more than 3 sensitive messages in 1 minute",
                            key);
                    out.collect(warning);
                }
            }
        }

        SingleOutputStreamOperator<String> warnings = filteredStream
                .filter(KafkaEvent::isSensitive)
                .keyBy(KafkaEvent::getSenderName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new GenerateWarningProcessFunction());

        // kafka sink for warning
        KafkaSink<String> warningSink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(WARNING_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        filteredStream.process(new EventLogger());

        // Put filtered messages and warnings into sinks
        filteredStream.sinkTo(sink).name(SINK_TOPIC);
        warnings.sinkTo(warningSink);

        env.execute("Chat Application with Flink");
    }
}
