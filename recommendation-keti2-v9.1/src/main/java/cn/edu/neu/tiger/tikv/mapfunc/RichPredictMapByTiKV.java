package cn.edu.neu.tiger.tikv.mapfunc;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.HdfsUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class RichPredictMapByTiKV extends RichMapFunction<Tuple2<String, List<String>>, Row> {

    private static final Logger logger = LoggerFactory.getLogger(RichPredictMapByTiKV.class);

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
    public Row map(Tuple2<String, List<String>> record) throws Exception {
        Row row = generateSample(record.f0, record.f1);
        predict_data.inc();
        return row;
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
        Long startTime = System.currentTimeMillis();
        try {
            Row row = tikvStorageService.generateSample(userId, itemIds);
            logger.info("{} generateSample cost: {}", userId, (System.currentTimeMillis() - startTime));
            return row;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
