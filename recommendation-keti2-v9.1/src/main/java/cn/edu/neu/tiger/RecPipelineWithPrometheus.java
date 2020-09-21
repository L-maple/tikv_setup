package cn.edu.neu.tiger;

import cn.edu.neu.tiger.athena.AthenaRecommender;
import cn.edu.neu.tiger.athena.LazyFlinkLogConsumer;
import cn.edu.neu.tiger.tikv.RecommendationFor3;
import cn.edu.neu.tiger.tikv.mapfunc.RichPredictMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.RichRecallMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.RichSinkMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.prometheus.RichPredictMapByTiKVWithPrometheus;
import cn.edu.neu.tiger.tikv.mapfunc.prometheus.RichRecallMapByTiKVWithPrometheus;
import cn.edu.neu.tiger.tikv.mapfunc.prometheus.RichSinkMapByTiKVWithPrometheus;
import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.Util;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class RecPipelineWithPrometheus {
	private static final Logger logger = LoggerFactory.getLogger(RecPipelineWithPrometheus.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamContextEnvironment.getExecutionEnvironment();
		run(args, streamEnv);
		streamEnv.execute();
	}

	public static void run(String[] args, StreamExecutionEnvironment streamEnv) throws Exception {
		ParameterTool parameter = ParameterTool.fromArgs(args);

		/**
		 * 1.kafka消费数据
		 */
		int kafkaParallelism = parameter.getInt("paraKafka", 1);
		if (kafkaParallelism <= 0) return;

		DataStream<String> kafkaSourceStream =
				streamEnv.addSource(
						new FlinkKafkaConsumer011<>(
								Constants.KAFKA_SOURCE_TOPIC, new SimpleStringSchema(), Util.getKafkaProPerties())
				).name("KafkaConsumer")
				.setParallelism(kafkaParallelism);
		DataStream<Tuple2<String, Long>> kafkaSourceStreamWithTime = kafkaSourceStream.map(new KafkaParser())
				.setParallelism(kafkaParallelism)
				.name("KafkaParser");

		/**
		 * 2.根据userid召回用户感兴趣的商品，kv（获取用户历史点击）、hdfs（读取i2i表）
		 */
		int recallParallelism = parameter.getInt("paraRecall", 1);
		if (recallParallelism <= 0) {
			kafkaSourceStreamWithTime.addSink(new SinkFunction<Tuple2<String, Long>>() {
				@Override
				public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
					logger.info(String.format("Source: [%s,%d]", value.f0, value.f1));
				}
			}).setParallelism(kafkaSourceStreamWithTime.getParallelism())
					.name("LoggingSink").disableChaining();
			return;
		}

		DataStream<Tuple3<String, List<String>, Long>> itemsStream = kafkaSourceStreamWithTime.map(new RichRecallMapByTiKVWithPrometheus())
				.returns(TypeInformation.of(new TypeHint<Tuple3<String, List<String>, Long>>() {
				})).name("Recall")
				.setParallelism(recallParallelism);

		/**
		 * 3.根据召回商品生成预测样本，kv（用户信息，商品信息）
		 */
		int genParallelism = parameter.getInt("paraGen", 1);
		if (genParallelism <= 0) {
			itemsStream.addSink(new SinkFunction<Tuple3<String, List<String>, Long>>() {
				@Override
				public void invoke(Tuple3<String, List<String>, Long> value, Context context) throws Exception {
					logger.info(String.format("Item: [%s, [%s], %d]", value.f0,
							String.join(",", value.f1), value.f2));
				}
			}).setParallelism(itemsStream.getParallelism())
					.name("LoggingSink").disableChaining();
			return;
		}

		DataStream<Row> samples = itemsStream.map(new RichPredictMapByTiKVWithPrometheus()).returns(getTypeInfo())
				.name("SampleGenerator")
				.setParallelism(genParallelism);

		/**
		 * 4.进行预测
		 */
		int inferenceParallelism = parameter.getInt("paraInference", 1);
		if (inferenceParallelism <= 0) {
			samples.addSink(new SinkFunction<Row>() {
				@Override
				public void invoke(Row value, Context context) throws Exception {
					logger.info(String.format("Sample: [%s]",
							String.valueOf(value.getField(0))));
				}
			}).setParallelism(samples.getParallelism())
					.name("LoggingSink").disableChaining();
			return;
		}

		AthenaRecommender athenaRec = AthenaRecommender.fromArgs(args);
		athenaRec.getAthenaConfig().setWorkerNum(inferenceParallelism);
		DataStream<Row> probability = athenaRec.inference(samples);
		DataStream<Tuple3<String, List<String>, Long>> recommendation = probability.map(Util::getTopKWithPrometheus)
				.setParallelism(probability.getParallelism())
				.name("GetTopK");

		/**
		 * 5.将推荐结果写入kv系统，以满足演示系统读取需求 kv
		 */
		int sinkParallelism = parameter.getInt("paraSink", 1);
		if (sinkParallelism <= 0) {
			recommendation.addSink(new SinkFunction<Tuple3<String, List<String>, Long>>() {
				@Override
				public void invoke(Tuple3<String, List<String>, Long> value, Context context) throws Exception {
					logger.info(String.format("Recommendation: [%s, [%s], %d]", value.f0,
							String.join(",", value.f1), value.f2));
				}
			}).setParallelism(recommendation.getParallelism())
					.name("LoggingSink").disableChaining();
			return;
		}

		DataStream<Tuple3<String, List<String>, Long>> result = recommendation.map(new RichSinkMapByTiKVWithPrometheus())
				.returns(TypeInformation.of(new TypeHint<Tuple3<String, List<String>, Long>>() {
				})).setParallelism(sinkParallelism)
				.name("WriteSink");

//		result.addSink(new SinkFunction<Tuple3<String, List<String>, Long>>() {
//			@Override
//			public void invoke(Tuple3<String, List<String>, Long> value, Context context) throws Exception {
//				logger.info(String.format("Result: [%s, [%s], %d]", value.f0,
//						String.join(",", value.f1), value.f2));
//			}
//		}).setParallelism(result.getParallelism())
//				.name("LoggingSink").disableChaining();

//		streamEnv.execute();
	}

	private static RowTypeInfo getTypeInfo() {
		TypeInformation[] types = new TypeInformation[2];
		types[0] = BasicTypeInfo.STRING_TYPE_INFO;
		types[1] = BasicTypeInfo.LONG_TYPE_INFO;
		String[] names = {"sample", "timestamp"};
		return new RowTypeInfo(types, names);
	}

//	public static class parser implements FlatMapFunction<RawLogGroupList, Tuple2<String, Long>> {
//		@Override
//		public void flatMap(RawLogGroupList value, Collector<Tuple2<String, Long>> out) {
//			List<RawLogGroup> logGroups = value.getRawLogGroups();
//			for (RawLogGroup flg : logGroups) {
//				List<RawLog> logs = flg.getLogs();
//				for (RawLog row : logs) {
//					String id;
//					try {
//						id = row.getContents().get("user_id");
//					} catch (Exception e) {
//						e.printStackTrace();
//						continue;
//					}
//					out.collect(Tuple2.of(id, System.currentTimeMillis()));
//				}
//			}
//		}
//	}

//	public static class kafkaParser implements FlatMapFunction<String, Tuple2<String, Long>> {
//		@Override
//		public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
//			out.collect(Tuple2.of(value, System.currentTimeMillis()));
//		}
//	}

	public static class KafkaParser implements MapFunction<String, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> map(String value) throws Exception {
			//return Tuple2.of(value, System.currentTimeMillis());
			return Tuple2.of(String.valueOf(Integer.parseInt(value)%3000000), System.currentTimeMillis());
		}
	}


}
