package cn.edu.neu.tiger.tikv.data;

import cn.edu.neu.tiger.tools.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WriteToKafka {
  private static final Logger logger = LoggerFactory.getLogger(WriteToKafka.class);

  public static void main(String[] args) {
    if (args.length != 3) {
      logger.error("Missing argument: <sleep millisecond> <#records/sleep> <print frequency>");
      System.exit(1);
    }

    int sleep = Integer.parseInt(args[0]);
    int recordsPerSleep = Integer.parseInt(args[1]);
    int printFreq = Integer.parseInt(args[2]);
    send("userclick", sleep, recordsPerSleep, printFreq);
  }

  private static void send(String topic, long sleep, int recordsPerSleep, int printFreq) {

    Properties properties = new Properties();
    properties.put(Constants.KAFKA_BOOTSTRAP, Constants.KAFKA_BOOTSTRAP_VALUE);
    properties.put(Constants.KAFKA_BATCH_SIZE, 16384);
    properties.put(Constants.KAFKA_BUFFER_MEMORY, 33554432);
    properties.put(Constants.KAFKA_RETRIES, 0);
    properties.put(
        Constants.KAFKA_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(
        Constants.KAFKA_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(Constants.KAFKA_LINGER_MS, 1);
    properties.put(Constants.KAFKA_ACKS, "all");

//    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//    for (int i = 1; i < 3; i++) {
//      for (int j = 1; j < 10; j++) {
//        try {
//          Thread.sleep(5000);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//        System.out.println(j);
//        producer.send(new ProducerRecord<>(topic, String.valueOf(j)));
//      }
//    }
//    producer.close();

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    int cnt = 0;
    while (true) {
      try {
        for (int i = 0; i < recordsPerSleep; i++) {
          producer.send(new ProducerRecord<>(topic, String.valueOf(cnt)));
          cnt++;
          if (cnt % printFreq == 0)
            logger.info(String.format("%d records written", cnt));
        }
        if (sleep != 0)
          Thread.sleep(sleep);
      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }
    producer.close();
  }
}
