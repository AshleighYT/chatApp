package flink;

import model.KafkaEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLogger extends ProcessFunction<KafkaEvent, KafkaEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(EventLogger.class);

    @Override
    public void processElement(KafkaEvent event, Context context, Collector<KafkaEvent> collector) throws Exception {
        // 打印日志
        LOG.info("Event: {}, Timestamp: {}", event, context.timestamp());

        // 向下游发送事件
        collector.collect(event);
    }
}