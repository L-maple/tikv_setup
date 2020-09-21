package cn.edu.neu.tiger.athena;

import cn.edu.neu.tiger.tools.Util;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AthenaTest {
    private static final Logger logger = LoggerFactory.getLogger(AthenaTest.class);

    public static void main(String[] args) throws Exception {
        AthenaRecommender athenaRec = AthenaRecommender.fromArgs(args);
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> input = streamEnv.fromElements(sampleInput(100)).name("Random Input");
        DataStream<Row> probability = athenaRec.inference(input);
        DataStream<Tuple2<String, List<String>>> recommendation = probability.map(Util::getTopK)
                .setParallelism(1);
        recommendation.map((res) -> {
            logger.info(String.format("User[%s] rec: [%s]",
                    res.f0, String.join(", ", res.f1)));
            return null;
        }).disableChaining();
        streamEnv.execute();
    }

    private static Row[] sampleInput(int size) {
        //"{\"feature49\": \"0.009185\", \"feature48\": \"0.022273\", \"feature47\": \"0.016345\", \"feature46\": \"0.011005\", \"feature45\": \"0.013837\", \"feature44\": \"0.010585\", \"feature43\": \"0.009815\", \"feature42\": \"0.013837\", \"feature41\": \"0.010585\", \"feature40\": \"0.009815\", \"feature6\": \"xxxxx OS\", \"feature7\": \"1000500\", \"feature8\": \"6-9y\", \"feature9\": \"614156593\", \"feature39\": \"0.013837\", \"feature38\": \"0.010585\", \"feature37\": \"0.009815\", \"feature36\": \"0.013837\", \"feature35\": \"0.010585\", \"feature34\": \"0.009815\", \"feature33\": \"0.013837\", \"feature32\": \"0.010585\", \"feature31\": \"0.009815\", \"feature30\": \"0.013837\", \"feature2\": \"101\", \"feature3\": \"6\", \"feature4\": \"F\", \"feature29\": \"0.010585\", \"feature5\": \"36-40\", \"feature28\": \"0.009815\", \"feature27\": \"665942300\", \"feature26\": \"110382805\", \"feature25\": \"200\", \"feature1\": \"101\", \"feature24\": \"36-40\", \"feature23\": \"2\", \"feature22\": \"2\", \"feature21\": \"25\", \"feature20\": \"50006804\", \"feature19\": \"1109871836\", \"feature18\": \"0\", \"feature17\": \"0\", \"feature16\": \"0\", \"feature15\": \"0\", \"feature14\": \"0\", \"feature13\": \"0\", \"feature57\": \"554292657200\", \"feature12\": \"xxxxx\", \"feature56\": \"1109871836\", \"feature11\": \"xxxxx\", \"feature55\": \"b\", \"feature10\": \"xxxxx\", \"feature54\": \"0.012232\", \"feature53\": \"0.009948\", \"feature52\": \"0.009594\", \"feature51\": \"0.012894\", \"feature50\": \"0.010041\"}"
        String sample = "{\"feature49\": \"0.009185\", \"feature48\": \"0.022273\", " +
                "\"feature47\": \"0.016345\", \"feature46\": \"0.011005\", \"feature45\": \"0.013837\", " +
                "\"feature44\": \"0.010585\", \"feature43\": \"0.009815\", \"feature42\": \"0.013837\", " +
                "\"feature41\": \"0.010585\", \"feature40\": \"0.009815\", \"feature6\": \"xxxxx OS\", " +
                "\"feature7\": \"1000500\", \"feature8\": \"6-9y\", \"feature9\": \"614156593\", " +
                "\"feature39\": \"0.013837\", \"feature38\": \"0.010585\", \"feature37\": \"0.009815\", " +
                "\"feature36\": \"0.013837\", \"feature35\": \"0.010585\", \"feature34\": \"0.009815\", " +
                "\"feature33\": \"0.013837\", \"feature32\": \"0.010585\", \"feature31\": \"0.009815\", " +
                "\"feature30\": \"0.013837\", \"feature2\": \"101\", \"feature3\": \"6\", \"feature4\": \"F\", " +
                "\"feature29\": \"0.010585\", \"feature5\": \"36-40\", \"feature28\": \"0.009815\", " +
                "\"feature27\": \"665942300\", \"feature26\": \"110382805\", \"feature25\": \"200\", " +
                "\"feature1\": \"101\", \"feature24\": \"36-40\", \"feature23\": \"2\", \"feature22\": \"2\", " +
                "\"feature21\": \"25\", \"feature20\": \"50006804\", \"feature19\": \"1109871836\", " +
                "\"feature18\": \"0\", \"feature17\": \"0\", \"feature16\": \"0\", \"feature15\": \"0\", " +
                "\"feature14\": \"0\", \"feature13\": \"0\", \"feature57\": \"554292657200\", \"feature12\": \"xxxxx\", " +
                "\"feature56\": \"1109871836\", \"feature11\": \"xxxxx\", \"feature55\": \"b\", \"feature10\": \"xxxxx\", " +
                "\"feature54\": \"0.012232\", \"feature53\": \"0.009948\", \"feature52\": \"0.009594\", " +
                "\"feature51\": \"0.012894\", \"feature50\": \"0.010041\"}";

        Row[] instances = new Row[size];
        for (int i = 0; i < size; i++) {
            Object[] fields = new Object[1];
            StringBuilder sb = new StringBuilder();
            sb.append(sample);
            for (int j = 0; j < 99; j++) {
                sb.append(",");
                sb.append(sample);
            }
            fields[0] = sb.toString();
            instances[i] = Row.of(fields);
        }
        return instances;
    }
}
