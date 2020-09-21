package cn.edu.neu.tiger.athena;

import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LazyFlinkLogConsumer<T> extends FlinkLogConsumer<T> {
    private static final Logger logger = LoggerFactory.getLogger(LazyFlinkLogConsumer.class);
    private long seconds;

    public LazyFlinkLogConsumer(LogDeserializationSchema<T> deserializer, Properties configProps, long seconds) {
        super(deserializer, configProps);
        this.seconds = seconds;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info(String.format("Waiting %d seconds for downstream launching...", seconds));
        Thread.sleep(seconds * 1000L);
        logger.info(String.format("Waiting %d seconds finished", seconds));
        super.open(parameters);
    }
}
