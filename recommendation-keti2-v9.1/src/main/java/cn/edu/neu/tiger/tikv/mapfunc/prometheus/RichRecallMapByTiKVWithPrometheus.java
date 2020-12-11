package cn.edu.neu.tiger.tikv.mapfunc.prometheus;

import cn.edu.neu.tiger.tikv.i2i.RecallFromTiKV;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RichRecallMapByTiKVWithPrometheus extends RichMapFunction<Tuple2<String, Long>, Tuple3<String, List<String>, Long>> {

	private static final Logger logger = LoggerFactory.getLogger(RichRecallMapByTiKVWithPrometheus.class);

	private transient Counter recall_data;

	private transient RecallFromTiKV recallFromTiKV;

	private transient Map<String, Map<String, Object>> i2i;

	private Cache<String, Tuple2<String, List<String>>> resultFromTiKVCache;
	private boolean enableCache = false;
	private int maximumSize = 0;
	private int expireAfterWrite = 0;

	public RichRecallMapByTiKVWithPrometheus() {

	}

	public RichRecallMapByTiKVWithPrometheus(int maximumSize, int expireAfterWrite) {
		this.maximumSize = maximumSize;
		this.expireAfterWrite = expireAfterWrite;
		this.enableCache = true;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		logger.info("open function at {}", this.getClass());
		recall_data = getRuntimeContext().getMetricGroup().counter("tikv_recall_data");

		recallFromTiKV = new RecallFromTiKV(); // 创建Tikv服务

		logger.info("begin load i2i data");
		Random random = new Random();
		this.i2i = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			Map<String, Object> i2i_value = new HashMap<>();
			i2i.put(String.valueOf(i), i2i_value);
			for (int j = 0; j < 20; j++) {
				i2i_value.put(String.valueOf(random.nextInt(200)), random.nextInt(20));
			}
		}
//		this.i2i = HdfsUtil.readHDFS("text");
		logger.info("i2i size: {}", this.i2i.size());
		if (enableCache) {
			this.resultFromTiKVCache = CacheBuilder.newBuilder()
					.maximumSize(maximumSize)
					.expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
					.build();
		}
	}

	@Override
	public Tuple3<String, List<String>, Long> map(Tuple2<String, Long> value) throws Exception {
		//long time = System.currentTimeMillis();
		Tuple2<String, List<String>> resultFromTiKV;
		if (enableCache) {
			resultFromTiKV = resultFromTiKVCache.get(value.f0, () -> recallFromTiKV.recall_new(value.f0, this.i2i));
		} else {
			resultFromTiKV = recallFromTiKV.recall_new(value.f0, this.i2i);
		}

		recall_data.inc();
		//logger.info("{} recall cost: {}", value, (System.currentTimeMillis() - time));
		return Tuple3.of(resultFromTiKV.f0, resultFromTiKV.f1, value.f1);
	}

}
