package cn.edu.neu.tiger;

import cn.edu.neu.tiger.athena.AthenaRecommender;
import cn.edu.neu.tiger.tikv.mapfunc.prometheus.RichPredictMapByTiKVWithPrometheus;
import cn.edu.neu.tiger.tikv.mapfunc.prometheus.RichRecallMapByTiKVWithPrometheus;
import cn.edu.neu.tiger.tikv.mapfunc.prometheus.RichSinkMapByTiKVWithPrometheus;
import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.Util;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RecPipelineWithoutWrite {

    private static final Logger logger = LoggerFactory.getLogger(RecPipelineWithoutWrite.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        int parallelism = parameter.getInt("parallelism", 1);

        StreamExecutionEnvironment streamEnv = StreamContextEnvironment.getExecutionEnvironment();

        /**
         * 1.kafka消费数据
         */
        DataStream<String> kafkaSourceStream =
                streamEnv.addSource(
                        new FlinkKafkaConsumer011<>(
                                Constants.KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), Util.getKafkaProPerties())
                ).name("FlinkKafkaConsumer");

//		kafkaSourceStream.map((o) -> {
//			System.out.println("kafkaSourceStream " + o);
//			return null;
//		}).disableChaining();

//		DataStream<Tuple2<String, Long>> kafkaSourceStreamWithTime = kafkaSourceStream.flatMap(new kafkaParser());
        DataStream<Tuple2<String, Long>> kafkaSourceStreamWithTime = kafkaSourceStream.map(new KafkaParser())
                .name("KafkaParser");

//		kafkaSourceStreamWithTime.map((o) -> {
//			System.out.println("kafkaSourceStreamWithTime " + o);
//			return null;
//		}).disableChaining();

        /**
         * 2.根据userid召回用户感兴趣的商品，kv（获取用户历史点击）、hdfs（读取i2i表）
         */
        DataStream<Tuple3<String, List<String>, Long>> itemsStream = kafkaSourceStreamWithTime.map(new RichRecallMapByTiKVWithPrometheus())
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, List<String>, Long>>() {
                })).name("Recall")
                .setParallelism(parallelism);

//		itemsStream.map((o) -> {
//			System.out.println("itemsStream " + o);
//			return null;
//		}).disableChaining();

        /**
         * 3.根据召回商品生成预测样本，kv（用户信息，商品信息）
         */
        DataStream<Row> samples = itemsStream.map(new RichPredictMapByTiKVWithPrometheus()).returns(getTypeInfo())
                .name("SampleGenerator")
                .setParallelism(parallelism + 1);

//		samples.map((o) -> {
//			System.out.println("RichPredictMapByTiKVWithPrometheus " + o);
//			return null;
//		}).disableChaining();

        /**
         * 4.进行预测
         */
        AthenaRecommender athenaRec = AthenaRecommender.fromArgs(args);
        DataStream<Row> probability = athenaRec.inference(samples);
        DataStream<Tuple3<String, List<String>, Long>> recommendation = probability.map(Util::getTopKWithPrometheus)
                .name("GetTopK")
                .setParallelism(1);

<<<<<<< HEAD
       /**
        * 5.将推荐结果写入kv系统，以满足演示系统读取需求 kv
        */
       DataStream<Tuple3<String, List<String>, Long>> result = recommendation.map(new RichSinkMapByTiKVWithPrometheus())
               .returns(TypeInformation.of(new TypeHint<Tuple3<String, List<String>, Long>>() {
               }))
               .setParallelism(1);
=======
//        /**
//         * 5.将推荐结果写入kv系统，以满足演示系统读取需求 kv
//         */
//        DataStream<Tuple3<String, List<String>, Long>> result = recommendation.map(new RichSinkMapByTiKVWithPrometheus())
//                .returns(TypeInformation.of(new TypeHint<Tuple3<String, List<String>, Long>>() {
//                }))
//                .setParallelism(1);
>>>>>>> 080bc189714b48509cca9350d9babcff9ce76a82

        recommendation.map((res) -> {
            logger.info(String.format("User[%s] rec: [%s]",
                    res.f0, String.join(", ", res.f1)));
            return null;
        }).disableChaining();

        streamEnv.execute();
    }

    private static RowTypeInfo getTypeInfo() {
        TypeInformation[] types = new TypeInformation[2];
        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.LONG_TYPE_INFO;
        String[] names = {"sample", "timestamp"};
        return new RowTypeInfo(types, names);
    }

    public static class parser implements FlatMapFunction<RawLogGroupList, Tuple2<String, Long>> {
        @Override
        public void flatMap(RawLogGroupList value, Collector<Tuple2<String, Long>> out) {
            List<RawLogGroup> logGroups = value.getRawLogGroups();
            for (RawLogGroup flg : logGroups) {
                List<RawLog> logs = flg.getLogs();
                for (RawLog row : logs) {
                    String id;
                    try {
                        id = row.getContents().get("user_id");
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }
                    out.collect(Tuple2.of(id, System.currentTimeMillis()));
                }
            }
        }
    }

//    public static class kafkaParser implements FlatMapFunction<String, Tuple2<String, Long>> {
//        @Override
//        public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
//            out.collect(Tuple2.of(value, System.currentTimeMillis()));
//        }
//    }

    public static class KafkaParser implements MapFunction<String, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(String value) throws Exception {
            return Tuple2.of(value, System.currentTimeMillis());
        }
    }
}
