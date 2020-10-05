package cn.edu.neu.tiger.tikv.mapfunc.prometheus;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import java.io.IOException;
import java.util.List;

public class RichSinkMapByTiKVWithPrometheus extends RichMapFunction<Tuple3<String, List<String>, Long>, Tuple3<String, List<String>, Long>> {

	private transient TiKVStorageService tiKVStorageService;


	private transient Counter sink_data;

	private transient Counter sink_delay;

	@Override
	public void open(Configuration parameters) throws Exception {

		tiKVStorageService = new TikvServiceImpl(Constants.PD_ADDRESS);

		sink_data = getRuntimeContext().getMetricGroup().counter("tikv_sink_data");

		sink_delay = getRuntimeContext().getMetricGroup().counter("tikv_sink_delay");

	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Tuple3<String, List<String>, Long> map(Tuple3<String, List<String>, Long> result) throws Exception {
		//List<Map<String, String>> putInfos = new ArrayList<>();
		String json = JSONObject.toJSONString(result.f1);//json中保存了所有的itemid
		String colName = "result";
		//putInfos.add(HBaseUtil.getPutInfo("result", json, "cf")); //分别对应HBase中列名，列值和列族的value

		//result表中的rowkey为userid
		//在result表中，以"result"作为列名，"json"作为列值，按照原来TiKV的存储方式，将这两个元素仍用":"进行拼接
		//因此result表在TiKV中的设计为：
		//key: result,r,userID
		//value:  result:json
		//tiKVStorageService.writeResultToTiKV(Constants.TABLE_RESULT, result.f0, putInfos);
		tiKVStorageService.writeResultToTiKV(Constants.TABLE_RESULT, result.f0, colName, json);
		sink_data.inc();
		sink_delay.inc(System.currentTimeMillis() - result.f2);
		return result;
	}
}
