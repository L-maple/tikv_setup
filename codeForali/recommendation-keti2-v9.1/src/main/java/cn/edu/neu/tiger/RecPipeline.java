package cn.edu.neu.tiger;

import cn.edu.neu.tiger.athena.LazyFlinkLogConsumer;
import cn.edu.neu.tiger.tikv.RecommendationFor3;
import cn.edu.neu.tiger.tikv.mapfunc.RichPredictMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.RichRecallMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.RichSinkMapByTiKV;
import cn.edu.neu.tiger.tools.Util;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.neu.tiger.athena.AthenaRecommender;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RecPipeline {
    private static final Logger logger = LoggerFactory.getLogger(RecPipeline.class);

    public static void main(String[] args) throws Exception {
        int parallelism = Integer.parseInt(args[args.length - 1]);
        int waitSec = Integer.parseInt(args[args.length - 2]);

        StreamExecutionEnvironment streamEnv = StreamContextEnvironment.getExecutionEnvironment();

        /**
         * 1.sls消费数据
         */
        Properties configProps = Util.getSlsProperties();
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
//        DataStream<RawLogGroupList> slsStream = streamEnv.addSource(
//                new FlinkLogConsumer<>(deserializer, configProps))
//                .setParallelism(2);
        DataStream<RawLogGroupList> slsStream = streamEnv.addSource(
                new LazyFlinkLogConsumer<>(deserializer, configProps, waitSec))
                .setParallelism(2);
        DataStream<String> sourceStream = slsStream.flatMap(new RecommendationFor3.parser())
                .setParallelism(2);

        /**
         * 2.根据userid召回用户感兴趣的商品，kv（获取用户历史点击）、hdfs（读取i2i表）
         */
        DataStream<Tuple2<String, List<String>>> itemsStream = sourceStream.map(new RichRecallMapByTiKV())
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, List<String>>>() {
                }))
                .setParallelism(parallelism);
        /**
         * 3.根据召回商品生成预测样本，kv（用户信息，商品信息）
         */
        DataStream<Row> samples = itemsStream.map(new RichPredictMapByTiKV()).returns(getTypeInfo())
                .setParallelism(parallelism + 1);

        /**
         * 4.进行预测
         */
        AthenaRecommender athenaRec = AthenaRecommender.fromArgs(Arrays.copyOf(args, args.length - 2));
        DataStream<Row> probability = athenaRec.inference(samples);
        DataStream<Tuple2<String, List<String>>> recommendation = probability.map(Util::getTopK)
                .setParallelism(1);

        recommendation.print();

        /**
         * 5.将推荐结果写入kv系统，以满足演示系统读取需求 kv
         */
        DataStream<Tuple2<String, List<String>>> result = recommendation.map(new RichSinkMapByTiKV())
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, List<String>>>() {}))
                .setParallelism(1);

        result.map((res) -> {
            logger.info(String.format("User[%s] rec: [%s]",
                    res.f0, String.join(", ", res.f1)));
            return null;
        }).disableChaining();

        streamEnv.execute();
    }

    private static RowTypeInfo getTypeInfo() {
        TypeInformation[] types = new TypeInformation[1];
        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        String[] names = {"sample"};
        return new RowTypeInfo(types, names);
    }
}
