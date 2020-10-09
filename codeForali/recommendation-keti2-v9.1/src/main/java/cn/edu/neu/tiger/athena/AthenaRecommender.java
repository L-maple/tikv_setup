package cn.edu.neu.tiger.athena;

import com.alibaba.flink.ml.athena.AthenaConfig;
import com.alibaba.flink.ml.athena.AthenaUtil;
import com.alibaba.flink.ml.operator.coding.RowCSVCoding;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.alibaba.flink.ml.util.MLConstants;
//import net.sourceforge.argparse4j.ArgumentParsers;
//import net.sourceforge.argparse4j.inf.ArgumentParser;
//import net.sourceforge.argparse4j.inf.ArgumentParserException;
//import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AthenaRecommender {
    private AthenaConfig athenaConfig;

    public AthenaRecommender(AthenaConfig athenaConfig) {
        this.athenaConfig = athenaConfig;
    }

//    public DataStream<Row> train(DataStream<Row> samples) throws IOException {
//        StreamExecutionEnvironment streamEnv = samples.getExecutionEnvironment();
//        return AthenaUtil.train(streamEnv, samples, athenaConfig, null);
//    }

    public DataStream<Row> inference(DataStream<Row> samples) throws IOException {
        if (athenaConfig.getPsNum() > 0)
            throw new RuntimeException("Inference should not set PS");

        StreamExecutionEnvironment streamEnv = samples.getExecutionEnvironment();
        return AthenaUtil.inference(streamEnv, samples, athenaConfig,
                TypeInformation.of(Row.class));
    }

    public AthenaConfig getAthenaConfig() {
        return athenaConfig;
    }

    public static AthenaRecommender fromArgs(String[] args) {
        ParameterTool parameter = ParameterTool.fromArgs(args);

//        ArgumentParser parser = ArgumentParsers.newFor("Athena").build();
//        addArgs(parser);
//
//        Namespace res = null;
//        try {
//            res = parser.parseArgs(args);
//            System.out.println(res);
//        } catch (ArgumentParserException e) {
//            parser.handleError(e);
//            System.exit(1);
//        }

        String envPath = parameter.get("envPath", null);
        String codePath = parameter.getRequired("codePath");
        String pyFile = parameter.getRequired("pyFile");
        int workerNum = parameter.getInt("workerNum", 1);
        int psNum = parameter.getInt("psNum", 0);
        Map<String, String> prop = new HashMap<>();
        prop.put(MLConstants.USE_DISTRIBUTE_CACHE, "false");
        prop.put(MLConstants.REMOTE_CODE_ZIP_FILE, codePath);
        prop.put(MLConstants.ENCODING_CLASS, RowCSVCoding.class.getCanonicalName());
        prop.put(MLConstants.DECODING_CLASS, RowCSVCoding.class.getCanonicalName());
        prop.put(RowCSVCoding.ENCODE_TYPES, parameter.get("encodeType", DataTypes.STRING.name()));
        prop.put(RowCSVCoding.DECODE_TYPES, parameter.get("decodeType", DataTypes.STRING.name()));
        prop.put(RowCSVCoding.DELIM_CONFIG, parameter.get("csvDelim", "@"));

        AthenaConfig athenaConfig = new AthenaConfig(workerNum, psNum, prop,
                pyFile, "map_func", envPath);

        return new AthenaRecommender(athenaConfig);
    }

//    public static void addArgs(ArgumentParser parser) {
//        parser.addArgument("--env-path").metavar("envPath").dest("envPath")
//                .help("The HDFS path to the virtual env zip file.");
//        parser.addArgument("--code-path").metavar("codePath").dest("codePath")
//                .required(true).help("Python scriptRunner implementation class name");
//        parser.addArgument("--script").metavar("pyFile").dest("pyFile")
//                .required(true).help("The python script to for inference.");
//        parser.addArgument("--worker-num").metavar("workerNum").dest("workerNum")
//                .type(Integer.class).setDefault(1).help("Number of worker");
//        parser.addArgument("--ps-num").metavar("psNum").dest("psNum")
//                .type(Integer.class).setDefault(0).help("Number of parameter server");
//        parser.addArgument("--encode-type").metavar("encodeType").dest("encodeType")
//                .setDefault(DataTypes.STRING.name()).help("Type of encoding");
//        parser.addArgument("--decode-type").metavar("decodeType").dest("decodeType")
//                .setDefault(DataTypes.STRING.name()).help("Type of decoding");
//        parser.addArgument("--csv-delim").metavar("csvDelim").dest("csvDelim")
//                .setDefault("@").help("Delim of coding");
//    }
}
