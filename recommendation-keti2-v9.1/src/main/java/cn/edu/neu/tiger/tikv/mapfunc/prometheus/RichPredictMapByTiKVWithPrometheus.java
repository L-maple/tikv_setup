package cn.edu.neu.tiger.tikv.mapfunc.prometheus;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class RichPredictMapByTiKVWithPrometheus extends RichMapFunction<Tuple3<String, List<String>, Long>, Row> {

    private static final Logger logger = LoggerFactory.getLogger(RichPredictMapByTiKVWithPrometheus.class);

    private transient Counter predict_data;

    private TiKVStorageService tikvStorageService;

    //Map<String, Object> featureTransMap;
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("open function at {}", this.getClass());
        tikvStorageService = new TikvServiceImpl(Constants.PD_ADDRESS);
        predict_data = getRuntimeContext().getMetricGroup().counter("tikv_predict_data");
    }

    @Override
    public Row map(Tuple3<String, List<String>, Long> record) throws Exception {
        Row row = generateSample(record.f0, record.f1);
        Row result = new Row(2);
        result.setField(0,row.getField(0));
        result.setField(1,record.f2);
        predict_data.inc();
        return result;
    }

    /**
     * 生成样本
     *
     * @param userId  用户id
     * @param itemIds 商品ids
     * @return 样本
     * @throws IOException
     */
    public Row generateSample(String userId, List<String> itemIds) throws Exception {
        //Long startTime = System.currentTimeMillis();
        try {
            Row row = tikvStorageService.generateSample(userId, itemIds);
            //logger.info("{} generateSample cost: {}", userId, (System.currentTimeMillis() - startTime));
            return row;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
