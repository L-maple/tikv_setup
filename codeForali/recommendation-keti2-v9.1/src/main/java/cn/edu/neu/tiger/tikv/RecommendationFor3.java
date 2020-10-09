package cn.edu.neu.tiger.tikv;

import cn.edu.neu.tiger.tikv.mapfunc.RichPredictMapByTiKV;
import cn.edu.neu.tiger.tikv.mapfunc.RichRecallMapByTiKV;
import cn.edu.neu.tiger.tools.Util;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;


public class RecommendationFor3 {

	StreamExecutionEnvironment streamEnv;

	public RecommendationFor3(StreamExecutionEnvironment streamEnv) {
		this.streamEnv = streamEnv;
	}

	public StreamExecutionEnvironment getStreamEnv() {
		return streamEnv;
	}

	public void setStreamEnv(StreamExecutionEnvironment streamEnv) {
		this.streamEnv = streamEnv;
	}

	private static RowTypeInfo getTypeInfo() {
		TypeInformation[] types = new TypeInformation[1];
		types[0] = BasicTypeInfo.STRING_TYPE_INFO;
		String[] names = {"sample"};
		return new RowTypeInfo(types, names);
	}


	public DataStream<Row> getData() throws Exception {
		RowTypeInfo typeInfo = getTypeInfo();

		/**
		 * 1.sls消费数据
		 */
		Properties configProps = Util.getSlsProperties();
		RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
		DataStream<RawLogGroupList> slsStream = streamEnv.addSource(
				new FlinkLogConsumer<>(deserializer, configProps))
				.setParallelism(1);
		DataStream<String> sourceStream = slsStream.flatMap(new parser())
				.setParallelism(1);

		/**
		 * 2.根据userid召回用户感兴趣的商品，kv（获取用户历史点击）、hdfs（读取i2i表）
		 */
		DataStream<Tuple2<String, List<String>>> itemsStream = sourceStream.map(new RichRecallMapByTiKV())
				.returns(TypeInformation.of(new TypeHint<Tuple2<String, List<String>>>() {
				}))
				.setParallelism(5);

		/**
		 * 3.根据召回商品生成预测样本，kv（用户信息，商品信息）
		 */
		DataStream<Row> samples = itemsStream.map(new RichPredictMapByTiKV()).returns(typeInfo)
				.setParallelism(5);

		return samples;
	}


	public static class parser implements FlatMapFunction<RawLogGroupList, String> {
		@Override
		public void flatMap(RawLogGroupList value, Collector<String> out) {
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
					out.collect(id);
				}
			}
		}
	}

}
