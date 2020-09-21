package cn.edu.neu.tiger.tikv;

import cn.edu.neu.tiger.tikv.mapfunc.RichPredictMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.RichRecallMapByTiKV;
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

import java.util.List;
import java.util.Properties;


public class RecommendationFor3Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamContextEnvironment.getExecutionEnvironment();
        int paraleelism = streamEnv.getConfig().getParallelism();

        RowTypeInfo typeInfo = getTypeInfo();
        /**
         * 1.sls消费数据
         */
        Properties configProps = Util.getSlsProperties();
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        DataStream<RawLogGroupList> slsStream = streamEnv.addSource(
                new FlinkLogConsumer<>(deserializer, configProps))
                .setParallelism(2);
        DataStream<String> sourceStream = slsStream.flatMap(new RecommendationFor3.parser())
                .setParallelism(2);

        /**
         * 2.根据userid召回用户感兴趣的商品，kv（获取用户历史点击）、hdfs（读取i2i表）
         */
        DataStream<Tuple2<String, List<String>>> itemsStream = sourceStream.map(new RichRecallMapByTiKV())
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, List<String>>>() {
                }))
                .setParallelism(111);
        /**
         * 3.根据召回商品生成预测样本，kv（用户信息，商品信息）
         */
        DataStream<Row> samples = itemsStream.map(new RichPredictMapByTiKV()).returns(typeInfo)
                .setParallelism(112);
        streamEnv.execute();
    }


    private static RowTypeInfo getTypeInfo() {
        TypeInformation[] types = new TypeInformation[1];
        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        String[] names = {"sample"};
        return new RowTypeInfo(types, names);
    }

}
