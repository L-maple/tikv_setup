package cn.edu.neu.tiger.tikv.impl;

import static cn.edu.neu.tiger.tools.TikvUtil.getRow;

import cn.edu.neu.tiger.tikv.mapfunc.RichRecallMapByTiKV;
import cn.edu.neu.tiger.tools.*;
import com.alibaba.fastjson.JSONObject;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tikv.service.StorageService;
import cn.edu.neu.tiger.tikv.impl.HBaseServiceImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import tikv.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Result;

public class TikvServiceImpl implements TiKVStorageService, Serializable {
    private String PD_ADDRESS;
    private TiSession session;
    private RawKVClient client;
    private int RowID; // 自增变量
    private BigInteger ONE = new BigInteger("1");
    private BigInteger scanTimes;
    private BigInteger NoUniqueIndexTimes;
    private BigInteger UniqueIndexTimes;
    private BigInteger UpdateI2iTimes;
    private BigInteger writeTimes;
    private BigInteger writeJSONTimes;
    Map<String, Object> featureTransMap;
    //private static final Logger logger = LoggerFactory.getLogger(TikvServiceImpl.class);

    public TikvServiceImpl(String pd_addr) throws IOException {
        PD_ADDRESS = pd_addr;
        RowID = 1;
        scanTimes = new BigInteger("0");
        NoUniqueIndexTimes = new BigInteger("0");
        UpdateI2iTimes = new BigInteger("0");
        writeTimes = new BigInteger("0");
        writeJSONTimes = new BigInteger("0");
        session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
        client = session.createRawClient();

        featureTransMap = new HashMap<>();
//        featureTransMap.put("item_id", "feature57");
//        featureTransMap.put("item_expo_id", "feature25");
//        featureTransMap.put("item_category", "feature3");
//        featureTransMap.put("item_category_level1", "feature4");
//        featureTransMap.put("item_seller_city", "feature7");
//        featureTransMap.put("item_seller_prov", "feature19");
//        featureTransMap.put("item_purch_level", "feature22");
//        featureTransMap.put("item_gender", "feature23");
//        featureTransMap.put("item_buyer_age", "feature24");
//        featureTransMap.put("item_style_id", "feature20");
//        featureTransMap.put("item_material_id", "feature21");
//        featureTransMap.put("item_pay_class", "feature26");
//        featureTransMap.put("item_brand_id", "feature27");
//        featureTransMap.put("item__i_shop_id_ctr", "feature52");
//        featureTransMap.put("item__i_brand_id_ctr", "feature49");
//        featureTransMap.put("item__i_category_ctr", "feature45");
//        featureTransMap.put("user_id", "feature56");
//        featureTransMap.put("pred_gender", "feature28");
//        featureTransMap.put("pred_age_level", "feature5");
//        featureTransMap.put("pred_career_type", "feature1");
//        featureTransMap.put("pred_education_degree", "feature2");
//        featureTransMap.put("pred_baby_age", "feature8");
//        featureTransMap.put("pred_has_pet", "feature10");
//        featureTransMap.put("pred_has_car", "feature11");
//        featureTransMap.put("pred_life_stage", "feature9");
//        featureTransMap.put("pred_has_house", "feature12");
//        featureTransMap.put("os", "feature6");
//
//        featureTransMap.put("item_virtual_feature1", "feature13");
//        featureTransMap.put("item_virtual_feature2", "feature14");
//        featureTransMap.put("item_virtual_feature3", "feature15");
//        featureTransMap.put("item_virtual_feature4", "feature16");
//        featureTransMap.put("item_virtual_feature5", "feature17");
//        featureTransMap.put("item_virtual_feature6", "feature18");
//        featureTransMap.put("item_virtual_feature7", "feature29");
//        featureTransMap.put("item_virtual_feature8", "feature30");
//        featureTransMap.put("item_virtual_feature9", "feature31");
//        featureTransMap.put("item_virtual_feature10", "feature32");
//        featureTransMap.put("item_virtual_feature11", "feature33");
//        featureTransMap.put("item_virtual_feature12", "feature34");
//        featureTransMap.put("item_virtual_feature13", "feature35");
//        featureTransMap.put("item_virtual_feature14", "feature36");
//        featureTransMap.put("item_virtual_feature15", "feature37");
//        featureTransMap.put("item_virtual_feature16", "feature38");
//        featureTransMap.put("item_virtual_feature17", "feature39");
//        featureTransMap.put("item_virtual_feature18", "feature40");
//        featureTransMap.put("item_virtual_feature19", "feature41");
//        featureTransMap.put("item_virtual_feature20", "feature42");
//        featureTransMap.put("item_virtual_feature21", "feature43");
//        featureTransMap.put("item_virtual_feature22", "feature44");
//        //featureTransMap.put("item_virtual_feature23", "feature45");
//        featureTransMap.put("item_virtual_feature24", "feature46");
//        featureTransMap.put("item_virtual_feature25", "feature47");
//        featureTransMap.put("item_virtual_feature26", "feature48");
//        //featureTransMap.put("item_virtual_feature27", "feature49");
//        featureTransMap.put("item_virtual_feature28", "feature50");
//        featureTransMap.put("item_virtual_feature29", "feature51");
//        //featureTransMap.put("item_virtual_feature30", "feature52");
//        featureTransMap.put("pred_virtual_feature1", "feature53");
//        featureTransMap.put("pred_virtual_feature2", "feature54");
//        featureTransMap.put("pred_virtual_feature3", "feature55");


//        featureTransMap.put("item_id", "feature1");
//        featureTransMap.put("item_expo_id", "feature2");
//        featureTransMap.put("item_category", "feature3");
//        featureTransMap.put("item_category_level1", "feature4");
//        featureTransMap.put("item_seller_city", "feature5");
//        featureTransMap.put("item_seller_prov", "feature6");
//        featureTransMap.put("item_purch_level", "feature7");
//        featureTransMap.put("item_gender", "feature8");
//        featureTransMap.put("item_buyer_age", "feature9");
//        featureTransMap.put("item_style_id", "feature10");
//        featureTransMap.put("item_material_id", "feature11");
//        featureTransMap.put("item_pay_class", "feature12");
//        featureTransMap.put("item_brand_id", "feature13");
//        featureTransMap.put("item__i_shop_id_ctr", "feature14");
//        featureTransMap.put("item__i_brand_id_ctr", "feature15");
//        featureTransMap.put("item__i_category_ctr", "feature16");
//        featureTransMap.put("user_id", "feature17");
//        featureTransMap.put("pred_gender", "feature18");
//        featureTransMap.put("pred_age_level", "feature19");
//        featureTransMap.put("pred_career_type", "feature20");
//        featureTransMap.put("pred_education_degree", "feature21");
//        featureTransMap.put("pred_baby_age", "feature22");
//        featureTransMap.put("pred_has_pet", "feature23");
//        featureTransMap.put("pred_has_car", "feature24");
//        featureTransMap.put("pred_life_stage", "feature25");
//        featureTransMap.put("pred_has_house", "feature26");
//        featureTransMap.put("os", "feature27");
//        featureTransMap.put("item_virtual_feature1", "feature28");
//        featureTransMap.put("item_virtual_feature2", "feature29");
//        featureTransMap.put("item_virtual_feature3", "feature30");
//        featureTransMap.put("item_virtual_feature4", "feature31");
//        featureTransMap.put("item_virtual_feature5", "feature32");
//        featureTransMap.put("item_virtual_feature6", "feature33");
//        featureTransMap.put("item_virtual_feature7", "feature34");
//        featureTransMap.put("item_virtual_feature8", "feature35");
//        featureTransMap.put("item_virtual_feature9", "feature36");
//        featureTransMap.put("item_virtual_feature10", "feature38");
//        featureTransMap.put("item_virtual_feature11", "feature39");
//        featureTransMap.put("item_virtual_feature12", "feature40");
//        featureTransMap.put("item_virtual_feature13", "feature41");
//        featureTransMap.put("item_virtual_feature14", "feature42");
//        featureTransMap.put("item_virtual_feature15", "feature43");
//        featureTransMap.put("item_virtual_feature16", "feature44");
//        featureTransMap.put("item_virtual_feature17", "feature45");
//        featureTransMap.put("item_virtual_feature18", "feature46");
//        featureTransMap.put("item_virtual_feature19", "feature47");
//        featureTransMap.put("item_virtual_feature20", "feature48");
//        featureTransMap.put("item_virtual_feature21", "feature49");
//        featureTransMap.put("item_virtual_feature22", "feature50");
//        featureTransMap.put("item_virtual_feature23", "feature51");
//        featureTransMap.put("item_virtual_feature24", "feature52");
//        featureTransMap.put("item_virtual_feature25", "feature53");
//        featureTransMap.put("item_virtual_feature26", "feature54");
//        featureTransMap.put("item_virtual_feature27", "feature55");
//        featureTransMap.put("item_virtual_feature28", "feature56");
//        featureTransMap.put("item_virtual_feature29", "feature57");
//        featureTransMap.put("item_virtual_feature30", "feature58");
//        featureTransMap.put("pred_virtual_feature1","feature59");
//        featureTransMap.put("pred_virtual_feature2","feature60");
//        featureTransMap.put("pred_virtual_feature3","feature61");
//        featureTransMap.put("pred_virtual_feature4","feature62");
//        featureTransMap.put("pred_virtual_feature5","feature63");
//        featureTransMap.put("pred_virtual_feature6","feature64");
//        featureTransMap.put("pred_virtual_feature7","feature65");
//        featureTransMap.put("pred_virtual_feature8","feature66");
//        featureTransMap.put("pred_virtual_feature9","feature67");
//        featureTransMap.put("pred_virtual_feature10","feature68");
//        featureTransMap.put("pred_virtual_feature11","feature69");
//        featureTransMap.put("pred_virtual_feature12","feature70");
//        featureTransMap.put("pred_virtual_feature13","feature71");
//        featureTransMap.put("pred_virtual_feature14","feature72");
//        featureTransMap.put("pred_virtual_feature15","feature73");
//        featureTransMap.put("pred_virtual_feature16","feature74");
//        featureTransMap.put("pred_virtual_feature17","feature75");
//        featureTransMap.put("pred_virtual_feature18","feature76");
//        featureTransMap.put("pred_virtual_feature19","feature77");
//        featureTransMap.put("pred_virtual_feature20","feature78");
//        featureTransMap.put("pred_virtual_feature21","feature79");
//        featureTransMap.put("pred_virtual_feature22","feature80");
//        featureTransMap.put("pred_virtual_feature23","feature81");
//        featureTransMap.put("pred_virtual_feature24","feature82");
//        featureTransMap.put("pred_virtual_feature25","feature83");
//        featureTransMap.put("pred_virtual_feature26","feature84");
//        featureTransMap.put("pred_virtual_feature27","feature85");
//        featureTransMap.put("pred_virtual_feature28","feature86");
//        featureTransMap.put("pred_virtual_feature29","feature87");
//        featureTransMap.put("pred_virtual_feature30","feature88");
//        featureTransMap.put("pred_virtual_feature31","feature89");
//        featureTransMap.put("pred_virtual_feature32","feature90");
//        featureTransMap.put("pred_virtual_feature33","feature91");
//        featureTransMap.put("click_virtual_feature1","feature92");
//        featureTransMap.put("click_virtual_feature2","feature93");
//        featureTransMap.put("click_virtual_feature3","feature94");
//        featureTransMap.put("click_virtual_feature4","feature95");
//        featureTransMap.put("click_virtual_feature5","feature96");
//        featureTransMap.put("click_virtual_feature6","feature97");
//        featureTransMap.put("click_virtual_feature7","feature98");
//        featureTransMap.put("click_virtual_feature8","feature99");
//        featureTransMap.put("click_virtual_feature9","feature100");
//        featureTransMap.put("click_virtual_feature10","feature101");
//        featureTransMap.put("click_virtual_feature11","feature102");
//        featureTransMap.put("click_virtual_feature12","feature103");
//        featureTransMap.put("click_virtual_feature13","feature104");
//        featureTransMap.put("click_virtual_feature14","feature105");
//        featureTransMap.put("click_virtual_feature15","feature106");
//        featureTransMap.put("click_virtual_feature16","feature107");
//        featureTransMap.put("click_virtual_feature17","feature108");
//        featureTransMap.put("click_virtual_feature18","feature109");
//        featureTransMap.put("click_virtual_feature19","feature110");
//        featureTransMap.put("click_virtual_feature20","feature111");
//        featureTransMap.put("click_virtual_feature21","feature112");
//        featureTransMap.put("click_virtual_feature22","feature113");
//        featureTransMap.put("click_virtual_feature23","feature114");
//        featureTransMap.put("click_virtual_feature24","feature115");
//        featureTransMap.put("click_virtual_feature25","feature116");
//        featureTransMap.put("click_virtual_feature26","feature117");
//        featureTransMap.put("click_virtual_feature27","feature118");
//        featureTransMap.put("click_virtual_feature28","feature119");
//        featureTransMap.put("click_virtual_feature29","feature120");
//        featureTransMap.put("click_virtual_feature30","feature121");
//        featureTransMap.put("click_virtual_feature31","feature122");
//        featureTransMap.put("click_virtual_feature32","feature123");
//        featureTransMap.put("click_virtual_feature33","feature124");
//        featureTransMap.put("click_virtual_feature34","feature125");
//        featureTransMap.put("click_virtual_feature35","feature126");
//        featureTransMap.put("click_virtual_feature36","feature127");
//        featureTransMap.put("click_virtual_feature37","feature128");
//        featureTransMap.put("click_virtual_feature38","feature129");
//        featureTransMap.put("click_virtual_feature39","feature130");
//        featureTransMap.put("click_virtual_feature40","feature131");


        Configuration configuration = HdfsUtil.createConf();
        FSDataInputStream fsDataInputStream = null;
        OutputStream outputStream = new ByteArrayOutputStream();
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Path path = new Path("/user/qiqi.zp/feature.conf");
            System.out.println("read path " + path.toString());
            fsDataInputStream = fileSystem.open(path);
            IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, false);

        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (fsDataInputStream != null) {
                IOUtils.closeStream(fsDataInputStream);
            }
            outputStream.flush();
            String json = outputStream.toString();
            System.out.println("json: " + json.length());
            outputStream.close();
            featureTransMap = JSONObject.parseObject(json);
        }
    }

    // 注： 这里把Result换掉了，一个是因为它出现的地方不多，另一个是最关键的，就是Kvrpcpb.KvPair这种类型
    // 应该是RawKVClient底层在存储KV数据时用到的，所以我们最好直接用这种类型进行处理
    @Override
    public List<Kvrpcpb.KvPair> scanData(String tableName, ArrayList<Map<String, String>> filters, String startRow, String stopRow)
            throws Exception {
        scanTimes.add(ONE);
        if (Constants.INDEX_ON) {
            return _scanData_PlanA(tableName, filters, startRow, stopRow);
        } else return _scanData_PlanB(tableName, filters, startRow, stopRow);
    }

    public List<Kvrpcpb.KvPair> _scanData_PlanA(
            String tableName, ArrayList<Map<String, String>> filters, String startRow, String stopRow) throws Exception {


        // Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(conf, clientBuilder, startKey, endKey);
        List<Kvrpcpb.KvPair> result = new ArrayList<>();
        boolean isInited = false;
        // iterator.forEachRemaining(result::add);

        // If filters don't exist, just scan the whole specified table
        // Although the method "item", "user", "click" table written to DB was different from "i2i",
        // "train_data" table,
        // the key format is the same.
        if (filters.isEmpty()) {
            ByteString tkey_min =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(1)));
            ByteString tkey_max =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s", tableName + ",", "r" + ",", Constants.ROWID_MAX));
            result = client.scan(tkey_min, tkey_max);
        }
        // If filters exist, treat it separately
        else {
            // If there is more than one filter, it can't be handled now, throw an exception
            if (filters.size() != 1) throw new Exception("filter.size bigger than one !!!");
            for (Map<String, String> filterInfo : filters) {
                List<Kvrpcpb.KvPair> tmpResult = new ArrayList<>();
                String filterKey = filterInfo.get("filterKey");
                String filterValue = filterInfo.get("filterValue");

                // if we want to find user by user_id or find item by item_id
                if ((tableName.equals(Constants.TABLE_USER) && filterKey.equals("user_id"))
                        || (tableName.equals(Constants.TABLE_ITEM) && filterKey.equals("item_id"))) {
                    ByteString ikey =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s#%s", tableName + ",", "i" + ",", filterKey + ",", filterValue));
                    ByteString index = client.get(ikey);
                    ByteString targetKey =
                            ByteString.copyFromUtf8(
                                    String.format("%s#%s#%s", tableName + ",", "r" + ",", index.toStringUtf8()));
                    tmpResult = client.scan(targetKey, 1);
                }
                // else scan click by user_id or flag
                else if (tableName.equals(Constants.TABLE_CLICK)) {
                    ByteString ikey_min = ByteString.copyFromUtf8(String.format("%s#%s#%s#%s#%s",
                            tableName + ",",
                            "i" + ",",
                            filterKey + ",",
                            filterValue + ",",
                            String.valueOf(0)));

                    ByteString ikey_max = ByteString.copyFromUtf8(String.format("%s#%s#%s#%s#%s",
                            tableName + ",",
                            "i" + ",",
                            filterKey + ",",
                            filterValue + ",",
                            String.valueOf(Constants.ROWID_MAX)));

                    List<Kvrpcpb.KvPair> indexList = client.scan(ikey_min, ikey_max);
                    for (Kvrpcpb.KvPair index : indexList) {
                        List<Kvrpcpb.KvPair> scanResult =
                                client.scan(
                                        ByteString.copyFromUtf8(
                                                String.format(
                                                        "%s#%s#%s",
                                                        tableName + ",", "r" + ",", index.getValue().toStringUtf8())),
                                        1);
                        tmpResult.addAll(scanResult);
                    }
                } else {
                    throw new Exception(String.format("target table %s don't support scan", tableName));
                }

                // If this is the first filter, tmpResult should be put into result
                // else tmpResult should intersect with result
                if (!isInited) result.addAll(tmpResult);
                else result.retainAll(tmpResult);
            }
        }
        return result;
    }

    public List<Kvrpcpb.KvPair> _scanData_PlanB(
            String tableName, ArrayList<Map<String, String>> filters,String startRow,String stopRow) throws Exception {
        // create db connection
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
        if (client == null) {
            client = session.createRawClient();
        }
        // Iterator<Kvrpcpb.KvPair> iterator = rawScanIterator(conf, clientBuilder, startKey, endKey);
        List<Kvrpcpb.KvPair> result = new ArrayList<>();
        boolean isInited = false;
        // iterator.forEachRemaining(result::add);

        // If filters don't exist, just scan the whole specified table
        // the key format of "click" is different from "user" & "item"
        if (filters.isEmpty()) {
            if (tableName.equals(Constants.TABLE_USER) || tableName.equals(Constants.TABLE_ITEM)) {
                ByteString tkey_min =
                       ByteString.copyFromUtf8(
                                String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(0)));
                ByteString tkey_max =
                        ByteString.copyFromUtf8(
                                String.format("%s#%s#%s", tableName + ",", "r" + ",", Constants.USERID_MAX));
                //        System.out.println(tkey_min.toStringUtf8() + '\t' + tkey_max.toStringUtf8());
                result = client.scan(tkey_min, tkey_max);
            } else {
                ByteString tkey_min =
                        ByteString.copyFromUtf8(
                                String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", 0 + ",", 0));
                ByteString tkey_max =
                        ByteString.copyFromUtf8(
                                String.format(
                                        "%s#%s#%s#%s",
                                        tableName + ",", "r" + ",", Constants.USERID_MAX + ",", Constants.ROWID_MAX));
                result = client.scan(tkey_min, tkey_max);
            }
        }
        // If filters exist, treat it separately
        else {
            // If there is more than one filter, it can't be handled now, throw an exception
            //if (filters.size() != 1) throw new Exception("filter.size bigger than one !!!");

            for (Map<String, String> filterInfo : filters) {
                List<Kvrpcpb.KvPair> tmpResult = new ArrayList<>();
                String filterKey = filterInfo.get("filterKey");
                String filterValue = filterInfo.get("filterValue");

                // if we want to find user by user_id or find item by item_id
                if ((tableName.equals(Constants.TABLE_USER) && filterKey.equals("user_id"))
                        || (tableName.equals(Constants.TABLE_ITEM) && filterKey.equals("item_id"))) {
                    ByteString targetKey =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s", tableName + ",", "r" + ",", filterInfo.get("filterValue")));
                    tmpResult = client.scan(targetKey, 1);
                }
                // else scan click by user_id or flag
                else if (tableName.equals(Constants.TABLE_CLICK)) {
                    if (filterInfo.get("filterKey").equals("user_id")) {
                        ByteString tkey_min =
                                ByteString.copyFromUtf8(
                                        String.format(
                                                "%s#%s#%s#%s",
                                                tableName + ",",
                                                "r" + ",",
                                                filterInfo.get("filterValue") + ",",
                                                String.valueOf(0)));
                        ByteString tkey_max =
                                ByteString.copyFromUtf8(
                                        String.format(
                                                "%s#%s#%s#%s",
                                                tableName + ",",
                                                "r" + ",",
                                                filterInfo.get("filterValue") + ",",
                                                Constants.ROWID_MAX));
                        tmpResult = client.scan(tkey_min, tkey_max);
                    } else if (filterInfo.get("filterKey").equals("flag") || filterInfo.get("filterKey").equals("date")) {
                        ByteString ikey_min =
                                ByteString.copyFromUtf8(
                                        String.format(
                                                "%s#%s#%s#%s#%s",
                                                tableName + ",",
                                                "i" + ",",
                                                filterKey + ",",
                                                filterValue + ",",
                                                String.valueOf(0)));

                        ByteString ikey_max =
                                ByteString.copyFromUtf8(
                                        String.format(
                                                "%s#%s#%s#%s#%s",
                                                tableName + ",",
                                                "i" + ",",
                                                filterKey + ",",
                                                filterValue + ",",
                                                String.valueOf(Constants.ROWID_MAX)));
                        List<Kvrpcpb.KvPair> indexList = client.scan(ikey_min, ikey_max);
                        for (Kvrpcpb.KvPair index : indexList) {
                            List<Kvrpcpb.KvPair> scanResult =
                                    client.scan(
                                            ByteString.copyFromUtf8(
                                                    String.format("%s#%s#%s", tableName + ",", "r" + ",", index.getValue())),
                                            1);
                            tmpResult.addAll(scanResult);
                        }
                    }else
                        throw new Exception(String.format("target table %s don't support scan", tableName));
                }

                // If this is the first filter, tmpResult should be put into result
                // else tmpResult should intersect with result
                if (!isInited) result.addAll(tmpResult);
                else result.retainAll(tmpResult);
            }
        }
        client.close();
        session.close();
        return result;

    }

    // 处理train_data和i2i表的插入，value数据类型用JSON来存，这样在代码中有两个地方存KV数据时比较方便
    @Override
    public int writeDataWithJSON(String tableName, String data) {
        writeJSONTimes.add(ONE);
        // 建立数据库连接
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
        if (client == null) {
            client = session.createRawClient();
        }
        // 创建表数据（根据HBaseServiceImpl里的调用，发现只有train_data表和i2i表用到了writeData方法来创建HTable）
        // 注意，因为tableName含有"_"，所以采用了","来作分隔符
        // tkey由 tablePrefix_rowPrefix_rowID 构成
        ByteString tkey =
                ByteString.copyFromUtf8(
                        String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(RowID)));
        RowID++; // 每插入一行数据RowID自增1
        ByteString tvalue = ByteString.copyFromUtf8(data);
        client.put(tkey, tvalue);
        // 关闭数据库连接
        return RowID;
    }

    @Override
    // 处理item、user和click表的插入
    public int writeData(String tableName, Map<String, String> data) throws Exception {
        writeTimes.add(ONE);
        if (Constants.INDEX_ON) return _writeData_PlanA(tableName, data);
        else return _writeData_PlanB(tableName, data);
    }

    public int _writeData_PlanA(String tableName, Map<String, String> data) {
        // 创建表数据（根据HBaseServiceImpl里的调用，发现只有train_data表和i2i表用到了writeData方法来创建HTable）
        // 注意，因为tableName含有"_"，所以采用了","来作分隔符
        // former version use ";" as separator, now changing to ","
        // because ";" in ascii is bigger than "9", it will confuse key range split
        // if ";" was used as separator, #0:#1 < #10:#1 < #11:#1 < #19:#1 < #1:#1 < #20:#1 < #2:#1
        // tkey由 tablePrefix_rowPrefix_rowID 构成
        ByteString tkey =
                ByteString.copyFromUtf8(
                        String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(RowID)));
        RowID++; // 每插入一行数据RowID自增1

        // tvalue由"col1Name:col1value,col2Name:col2value"组成
        String valueBuilder = "";
        int kv_nums = data.size() - 1;
        int i = 0;
        for (Map.Entry<String, String> entry : data.entrySet()) {
            valueBuilder = valueBuilder + entry.getKey() + ":" + entry.getValue();
            if (i < kv_nums) {
                valueBuilder += ",";
                i++;
            }
        }
        ByteString tvalue = ByteString.copyFromUtf8(valueBuilder);
        client.put(tkey, tvalue);
        return RowID;
    }

    public int _writeData_PlanB(String tableName, Map<String, String> data) throws Exception {

        // for "user" table, the tkey is tablePrefix_rowPrefix_userID
        // for "item" table, the tkey is tablePrefix_rowPrefix_itemID
        // for "click" table, the tkey is tablePrefix_rowPrefix_userID_rowID

        ByteString tkey;
        if (tableName.equals(Constants.TABLE_USER)) {
            tkey =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s", tableName + ",", "r" + ",", data.get("feature1")));
        } else if (tableName.equals(Constants.TABLE_ITEM)) {
            tkey =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s", tableName + ",", "r" + ",", data.get("feature20")));
        } else if (tableName.equals(Constants.TABLE_CLICK)) {
            tkey =
                    ByteString.copyFromUtf8(
                            String.format(
                                    "%s#%s#%s#%s",
                                    tableName + ",", "r" + ",", data.get("feature1") + ",", System.nanoTime()+""));
            //RowID++;
        } else {
            throw new Exception("passing a wrong table to writeData function: " + tableName);
        }
        //RowID++;

        // tvalue由"col1Name:col1value,col2Name:col2value"组成
        String valueBuilder = "";
        int kv_nums = data.size() - 1;
        int i = 0;
        for (Map.Entry<String, String> entry : data.entrySet()) {
            valueBuilder = valueBuilder + entry.getKey() + ":" + entry.getValue();
            if (i < kv_nums) {
                valueBuilder += ",";
                i++;
            }
        }
        //logger.info(" insert data-size : {}",i);
        ByteString tvalue = ByteString.copyFromUtf8(valueBuilder);

        client.put(tkey, tvalue);

        //System.out.println(" tkey :"+tkey+ "    tv:" + tvalue);
        return RowID;
    }

    // 创建unique索引
    @Override
    public boolean createUniqueIndex(
            String tableName, String index_name, String index_value, int rowid) {
        // ikey由tablePrefix_idxPrefix_indexID_indexColumnsValue 构成
        ByteString ikey =
                ByteString.copyFromUtf8(
                        String.format(
                                "%s#%s#%s#%s", tableName + ",", "i" + ",", index_name + ",", index_value));
        // ivalue为RowID
        ByteString ivalue = ByteString.copyFromUtf8(String.valueOf(rowid));
        client.put(ikey, ivalue);
        ByteString return_ivalue = client.get(ikey);
        if (return_ivalue.equals(ivalue)) {
            return true;
        } else return false;
    }

    // 创建非unique索引
    @Override
    public boolean createNoUniqueIndex(
            String tableName, String index_name, String index_value, int rowid, String user_id) {
        NoUniqueIndexTimes.add(ONE);
        if (Constants.INDEX_ON)
            return _createNoUniqueIndex_PlanA(tableName, index_name, index_value, rowid, user_id);
        else return _createNoUniqueIndex_PlanB(tableName, index_name, index_value, rowid, user_id);
    }

    public boolean _createNoUniqueIndex_PlanA(
            String tableName, String index_name, String index_value, int rowid, String user_id) {
        // ikey由tablePrefix_idxPrefix_indexID_ColumnsValue_rowID构成
        ByteString ikey =
                ByteString.copyFromUtf8(
                        String.format(
                                "%s#%s#%s#%s#%s",
                                tableName + ",",
                                "i" + ",",
                                index_name + ",",
                                index_value + ",",
                                String.valueOf(rowid)));
        // ivalue为RowID
        ByteString ivalue = ByteString.copyFromUtf8(String.valueOf(RowID));
        client.put(ikey, ivalue);
        ByteString return_ivalue = client.get(ikey);

        if (return_ivalue.equals(ivalue)) {
            return true;
        } else return false;
    }

    public boolean _createNoUniqueIndex_PlanB(
            String tableName, String index_name, String index_value, int rowid, String user_id) {
        // ikey由tablePrefix_idxPrefix_indexID_ColumnsValue_rowID构成
        ByteString ikey =
                ByteString.copyFromUtf8(
                        String.format(
                                "%s#%s#%s#%s#%s",
                                tableName + ",",
                                "i" + ",",
                                index_name + ",",
                                index_value + ",",
                                String.valueOf(rowid)));
        // ivalue为 user_id:rowID
        ByteString ivalue =
                ByteString.copyFromUtf8(String.format("%s#%s", user_id + ",", String.valueOf(rowid)));
        client.put(ikey, ivalue);
        ByteString return_ivalue = client.get(ikey);
        if (return_ivalue.equals(ivalue)) {
            return true;
        } else return false;
    }

    @Override
    public void updateI2i(
            Object result,
            Map<String, Map<String, Integer>> userRecord,
            Map<String, Map<String, Integer>> i2i) {
        UpdateI2iTimes.add(ONE);
        List<Kvrpcpb.KvPair> list = (List<Kvrpcpb.KvPair>) result;
        try {
            for (Kvrpcpb.KvPair result1 : list) {
                Map<String, String> row = getRow(result1.getValue());
                String value = "";
                for (Map.Entry<String, String> entry : row.entrySet()) {
                    value = entry.getValue() + "\t" + value;
                }
                Util.updateUserRecordMapByTiKV(value, userRecord);
            }
        } catch (Exception ignore) {
            System.out.println("读click数据失败：数据不存在或者hbase读取失败！");
        }
        Util.createI2i(userRecord, i2i);
    }

    // 根据唯一索引取值
    @Override
    public String getDataByUniqueIndexKey(String tableName, String indexKey, String indexValue) {
        // 查找索引找到rowid
        ByteString ikey =
                ByteString.copyFromUtf8(
                        String.format("%s#%s#%s#%s", tableName + ",", "i" + ",", indexKey + ",", indexValue));
        ByteString ivalue = client.get(ikey);
        String rowid = ivalue.toStringUtf8();
        ByteString tkey =
                ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ",", "r" + ",", rowid));
        ByteString tvalue = client.get(tkey);
        String value = tvalue.toStringUtf8();
        return value;
    }

    // 根据非唯一索引取值
    @Override
    public String getDataByNoUniqueIndexKey(
            String tableName, String indexKey, String indexValue, int rowID) {
        // 查找索引找到rowid
        ByteString ikey =
                ByteString.copyFromUtf8(
                        String.format(
                                "%s#%s#%s#%s#%s",
                                tableName + ",",
                                "i" + ",",
                                indexKey + ",",
                                indexValue + ",",
                                String.valueOf(rowID)));
        ByteString ivalue = client.get(ikey);
        if (ivalue == null) return null;
        String rowid = ivalue.toStringUtf8();
        ByteString tkey =
                ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ",", "r" + ",", rowid));
        ByteString tvalue = client.get(tkey);
        String value = tvalue.toStringUtf8();

        return value;
    }

    @Override
    public Map<String, Map<String, Object>> getI2i() throws IOException {
        Map<String, Map<String, Object>> i2i = new HashMap<>();
        long time = System.currentTimeMillis();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String date = df.format(time);
        try {
            // 这里得到的tvalue应该就是i2i的json字符串
            String tvalue = getDataByUniqueIndexKey("i2i", "date", date);
            if (tvalue == null) return null;
            Map<String, Object> i2iJSONMap = JSONObject.parseObject(tvalue);
            for (String key : i2iJSONMap.keySet()) {
                Map<String, Object> itemMap = JSONObject.parseObject(i2iJSONMap.get(key).toString());
                i2i.put(key, itemMap);
            }

        } catch (Exception e) {
            System.out.println("table i2i for date " + date + "is not exist, please update!");
        }
        return i2i;
    }

    @Override
    public Set<String> getItemIds() throws Exception {

        List<Kvrpcpb.KvPair> results = scanData("item", new ArrayList<>(), null, null);
        System.out.println("------------item.size()" + results.size() + "----------------");

        Set<String> ids = new HashSet<>();
        String[] cells;
        String[] value;
        try {
            for (Kvrpcpb.KvPair result : results) {
                cells = result.getValue().toStringUtf8().split(",");
                for (String cell : cells) {
                    if (cell.contains("item_id")) {
                        value = cell.split(":");
                        //System.out.println("value" + value[0]);
                        if ("item_id".equals(value[0].trim())) { // trim方法是去除首位的空字符
                            ids.add(value[1].trim());
                            break;
                        }
                    }
                }
            }
        } catch (Exception ignore) {
        }
        return ids;
    }

    @Override
    public List<Kvrpcpb.KvPair> scanClickByUserID(String tableName, String userId) {

        List<Kvrpcpb.KvPair> results = new ArrayList<>();
        ByteString tkey_min = ByteString.copyFromUtf8(String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", userId + ",", 0));
        ByteString tkey_max = ByteString.copyFromUtf8(String.format("%s#%s#%s#%s", tableName + ",", "r" + ",", userId + ",", Constants.ROWID_MAX));
        //results = client.scan(tkey_min, tkey_max);
        results = client.scan(tkey_min, tkey_max, 10);
        return results;
    }

    @Override
    public Map<String, Integer> getUserClickRecord(String userId) {
        Map<String, Map<String, Integer>> userRecord = new HashMap<>();
        //click表的主键由"userid"+"-"+click(flag)组成
        //String startRow = userId + "-" + "0";
        //String stopRow = userId + "-" + "2";
        //因为flag只有0和1两个值，所以直接用userid作为主键进行查找
        //List<Kvrpcpb.KvPair> results = scanData(Constants.TABLE_CLICK, new ArrayList<>(), startRow, stopRow);
        List<Kvrpcpb.KvPair> results = scanClickByUserID(Constants.TABLE_CLICK, userId);
        //List<Kvrpcpb.KvPair> results = new ArrayList<>();
        for (Kvrpcpb.KvPair result : results) {
            System.out.println("the size of kv is: "+(result.getKey().size()+result.getValue().size())+"B in "+Constants.TABLE_CLICK);
            Map<String, String> row = TikvUtil.getRow(result.getValue());
            String value = row.get("feature1")+"\t"+row.get("feature20")+"\t"+row.get("flag")+"\t"+row.get("date");
            Util.updateUserRecordMapByTiKV(value, userRecord);
        }

        return userRecord.get(userId) == null ? new HashMap<>() : userRecord.get(userId);
    }

    /**
     * 说明：按照key查询数据
     *
     * @param tableName kv表名
     * @param Key
     * @return ByteString value
     * @throws IOException
     */
    @Override
    public Map<String, String> getDataByKey(String tableName, String Key) {
        ByteString rkey = ByteString.copyFromUtf8(String.format(
                "%s#%s#%s", tableName + ",", "r" + ",", Key));
	//Long getUserTime = System.currentTimeMillis();
        ByteString rvalue = client.get(rkey);
	//System.out.println("+++++++++++++++++++get one user cost:"+(System.currentTimeMillis() - getUserTime)+"ms +++++++++++++++++++++");
//        System.out.println("return value: "+rvalue.toStringUtf8()+" from key: "+rkey.toStringUtf8());

        Map<String, String> result = TikvUtil.getRow(rvalue);
        return result;
    }

    /**
     * @param userId  用户id
     * @param itemIds 商品id
     * @return flink.row格式的样本数据
     * @throws IOException
     */
    @Override
    public Row generateSample(String userId, List<String> itemIds) throws IOException {
	//Long getUserTime = System.currentTimeMillis();
        Map<String, String> userCells = getDataByKey(Constants.TABLE_USER, userId);
	//System.out.println("+++++++++++++++++++get one user cost:"+(System.currentTimeMillis() - getUserTime)+"ms +++++++++++++++++++++");
        
	Map<String, String> userInfo = TikvUtil.getRowByCells(userCells, featureTransMap);
        if (userInfo.isEmpty()) {
            userInfo = Util.getUserMap();
            userInfo.put("feature56", userId);
        }

        StringBuilder result = new StringBuilder();
	//Long getAllItemTime = System.currentTimeMillis();
	//System.out.println("-------------------------------the number of itemId is: "+itemIds.size()+"-----------------------");
        for (String itemId : itemIds) {
	    //getDataByKey(Constants.TABLE_USER, userId);
	    //Long getOneItemTime = System.currentTimeMillis();
            Map<String, String> itemCells = getDataByKey(Constants.TABLE_ITEM, itemId);
            //System.out.println("###########get one item cost:"+(System.currentTimeMillis() - getOneItemTime)+"ms ######################itemId is: "+itemId+"######################");
	    
	    Map<String, String> itemInfo = TikvUtil.getRowByCells(itemCells, featureTransMap);
            if (itemInfo.isEmpty()) {
                itemInfo = Util.getItemMap();
            }

            Map<String, String> sample = Util.getSample(userInfo, itemInfo);
            result.append(JSONObject.toJSONString(sample));
            result.append(",");
	    
        }
	//System.out.println("----------------------get all items cost:"+(System.currentTimeMillis() - getAllItemTime)+"ms -----------");
	//result.append(JSONObject.toJSONString(userInfo));
        Row row = new Row(1);
        row.setField(0, result.toString().substring(0, result.length() - 1));
        return row;
    }

    @Override
    public void createTiKVClient() {
        TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
        TiSession session = TiSession.create(conf);
        if (client == null) {
            client = session.createRawClient();
        }
    }

    @Override
    public void delete(Object key) {
        TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
        TiSession session = TiSession.create(conf);
        if (client == null) {
            client = session.createRawClient();
        }
        client.delete((ByteString) key);
    }

    @Override
    public void deleteDataByTableName(String tableName) throws Exception {

        List<Kvrpcpb.KvPair> results = scanData(tableName, new ArrayList<>(), null, null);
        for (Kvrpcpb.KvPair result : results) {
            delete(result.getKey());
        }
    }

    @Override
    public void deleteIndexByTableName(String tableName) {
        TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
        TiSession session = TiSession.create(conf);
        if (client == null) {
            client = session.createRawClient();
        }

        switch (tableName) {
            // for "user" "item", if Constants.INDEX_ON, the index column is user_id/item_id, else index
            // doesn't exist
            case Constants.TABLE_USER:
            case Constants.TABLE_ITEM:
                if (Constants.INDEX_ON) {
                    ByteString ikey_min =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s#%s", tableName + ",", "i" + ",", tableName + "_id" + ",", "0"));
                    // the index key is String.format("%s#%s#%s#%s", tableName + ",", "i" + ",",
                    // indexColumnName + ",", indexColumnValue)
                    ByteString ikey_max =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s#%s",
                                            tableName + ",", "i" + ",", tableName + "_id" + ",", Constants.USERID_MAX));
                    List<Kvrpcpb.KvPair> indexResults = client.scan(ikey_min, ikey_max);
                    for (Kvrpcpb.KvPair indexResult : indexResults) {
                        client.delete(indexResult.getKey());
                    }
                }
                break;
            case Constants.TABLE_CLICK:
                // for "click", if Constants.INDEX_ON, there is a unique index on column user_id and a
                // non-unique index on column flag
                // if Constants.INDEX_ON is false, there is only a non-unique index on column flag.
                // Pay attention, although the value format of non-unique index is different under these two
                // cases, the key format is same.
                if (Constants.INDEX_ON) {
                    ByteString ikey_min =
                            ByteString.copyFromUtf8(
                                    String.format("%s#%s#%s#%s", tableName + ",", "i" + ",", "user_id" + ",", "0"));
                    // the index key is String.format("%s#%s#%s#%s", tableName + ",", "i" + ",",
                    // indexColumnName + ",", indexColumnValue)
                    ByteString ikey_max =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s#%s",
                                            tableName + ",", "i" + ",", "user_id" + ",", Constants.USERID_MAX));
                    List<Kvrpcpb.KvPair> indexResults = client.scan(ikey_min, ikey_max);
                    for (Kvrpcpb.KvPair indexResult : indexResults) {
                        client.delete(indexResult.getKey());
                    }
                }
                ByteString ikey_min =
                        ByteString.copyFromUtf8(
                                String.format(
                                        "%s#%s#%s#%s#%s",
                                        tableName + ",",
                                        "i" + ",",
                                        "flag" + ",",
                                        "0" + ",",
                                        "0")); // the first "0" is flag, the second "0" is the minimize of rowid
                ByteString ikey_max =
                        ByteString.copyFromUtf8(
                                String.format(
                                        "%s#%s#%s#%s#%s",
                                        tableName + ",",
                                        "i" + ",",
                                        "flag" + ",",
                                        "1" + ",",
                                        Constants.ROWID_MAX)); // the "1" is flag
                // delete all data under non-unique index
                List<Kvrpcpb.KvPair> indexResults = client.scan(ikey_min, ikey_max);
                for (Kvrpcpb.KvPair indexResult : indexResults) {
                    client.delete(indexResult.getKey());
                }
                break;
            default:
                System.out.println(
                        String.format(
                                "---------------target table [%s] doesn't support deleteIndex--------------",
                                tableName));
        }
    }

    //把result表写入TiKV中
    @Override
    public void writeResultToTiKV(String tableName, String userId, String colName, String colvalue) {
        //if (client == null) {
        //  client = session.createRawClient();
        //}
        ByteString tkey = ByteString.copyFromUtf8(
                String.format("%s#%s#%s", tableName + ",", "r" + ",", userId));
        ByteString tvalue = ByteString.copyFromUtf8(colName + ":" + colvalue);

        client.put(tkey, tvalue);
        //System.out.println("insert " + tkey.toStringUtf8() + '\t' + tvalue.toStringUtf8() + " into result");
    }

<<<<<<< HEAD
    //将数据从Hbase导入TiKV
    @Override
    public void loadToTiKVFromHBase(String tableName, List<String> features) throws Exception {
	    StorageService storageService = new HBaseServiceImpl(); // hbase服务的接口
        storageService.open();

        List<Result> results = (List<Result>)storageService.scanData(tableName, new ArrayList<>(), null, null);
        // first decide the key format prefix
        ByteString tkey;
        String keyColumn;
        if (tableName.equals(Constants.TABLE_USER)) {
            tkey = ByteString.copyFromUtf8(String.format("%s#%s#", tableName + ",", "r" + ","));
            keyColumn = "user_id";
        } else if (tableName.equals(Constants.TABLE_ITEM)) {
            tkey = ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ",", "r" + ","));
            keyColumn = "item_id";
        } else if (tableName.equals(Constants.TABLE_CLICK)) {
            tkey = ByteString.copyFromUtf8(String.format("%s#%s#", tableName + ",", "r" + ","));
            keyColumn = "user_id";
        } else {
            throw new Exception("passing a wrong table to loadToTiKBFromHBase function: " + tableName);
        }

        try {
            ExecutorService exec = Executors.newFixedThreadPool(100);
            Map<ByteString,ByteString> kvs = new HashMap<>();
            Map<ByteString,ByteString> index_flag = new HashMap<>();
            Map<ByteString,ByteString> index_date = new HashMap<>();
            String flag_name = null;
            String flag_value = null;
            String date_name = null;
            String date_value = null;
            ByteString ikey_flag = null;
            ByteString ivalue_flag = null;
            ByteString ikey_date = null;
            ByteString ivalue_date = null;
            Boolean isClick = false;
            int count = 0;//每计数至1000个KV对，就调用一次线程进行插入
            for (Result result : results) {
                //kvs = new HashMap<>();
                Map<String, String> row = HBaseUtil.getRowByCells(result,((HBaseServiceImpl) storageService).getFeatureTransMap());
                ByteString kv_key;
                if (tableName.equals(Constants.TABLE_USER) || tableName.equals(Constants.TABLE_ITEM))
                    kv_key = tkey.concat(ByteString.copyFromUtf8(row.get(keyColumn)));
                else {
                    isClick = true;
                    kv_key =
                            tkey.concat(
                                    ByteString.copyFromUtf8(
                                            String.format("%s#%s", row.get(keyColumn) + ",", String.valueOf(RowID))));
                    //分别创建基于flag和date的索引
                    // ikey由tablePrefix_idxPrefix_indexID_ColumnsValue_rowID构成
                    flag_name = "flag";
                    flag_value = row.get("flag");
                    date_name = "date";
                    date_value = row.get("date");
                    ikey_flag =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s#%s#%s",
                                            tableName + ",",
                                            "i" + ",",
                                            flag_name + ",",
                                            flag_value + ",",
                                            String.valueOf(RowID)));
                    // ivalue为 user_id,rowID
                    ivalue_flag =
                            ByteString.copyFromUtf8(String.format("%s#%s", row.get(keyColumn) + ",", String.valueOf(RowID)));
                    ikey_date =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s#%s#%s",
                                            tableName + ",",
                                            "i" + ",",
                                            date_name + ",",
                                            date_value + ",",
                                            String.valueOf(RowID)));
                    ivalue_date =
                            ByteString.copyFromUtf8(String.format("%s#%s", row.get(keyColumn) + ",", String.valueOf(RowID)));
                }

                RowID++;
                //value由"colName1:colValue1,colName2:colValue2"的形式构成
                String valueBuilder = "";
                int kv_nums = row.size() - 1;
                int i = 0;
                for (Map.Entry<String, String> entry : row.entrySet()) {
                    valueBuilder = valueBuilder + entry.getKey() + ":" + entry.getValue();
                    if (i < kv_nums) {
                        valueBuilder += ",";
                        i++;
                    }
                }
                ByteString kv_value = ByteString.copyFromUtf8(valueBuilder);

                if (count < 1000){
                    kvs.put(kv_key,kv_value);
                    if (isClick){
                        index_flag.put(ikey_flag,ivalue_flag);
                        index_date.put(ikey_date,ivalue_date);
                    }
                    count++;
                }else{
                        /*调用线程处理tikv的写入操作
                         Thread thread = new Thread(new TiKVPutTask(row,kv_key,client));
                                                 thread.start();
                                                                         */
		     //使用线程池处理tikv的写入操作
                    exec.execute(new TiKVPutTask(kvs,index_flag,index_date));
                    count = 0;
                    kvs.clear();
                    index_date.clear();
                    index_flag.clear();
                }
            }
            exec.shutdown();

        } catch (Exception ignore) {

        }
        storageService.close();
=======
    //函数已改，待验证
    @Override
    public void loadToTiKVFromHBase(String tableName, List<String> features) throws Exception {
>>>>>>> 080bc189714b48509cca9350d9babcff9ce76a82
    }


    //tikv写入线程类：
    class TiKVPutTask implements Runnable {
        Map<ByteString, ByteString> kvs = new HashMap<>();
        Map<ByteString, ByteString> index_flag = new HashMap<>();
        Map<ByteString, ByteString> index_date = new HashMap<>();
<<<<<<< HEAD
 //       RawKVClient client;
 //       TiSession session;
=======
        RawKVClient client;
        TiSession session;
>>>>>>> 080bc189714b48509cca9350d9babcff9ce76a82

        public TiKVPutTask(Map<ByteString, ByteString> kvs, Map<ByteString, ByteString> index_flag, Map<ByteString, ByteString> index_date) {
            //this.kvs = kvs;
            this.kvs.putAll(kvs);
            if (!index_date.isEmpty()) {
                this.index_date.putAll(index_date);
            }
            if (!index_flag.isEmpty()) {
                this.index_flag.putAll(index_flag);
            }
            //this.client = client;
<<<<<<< HEAD
//            this.session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
//            this.client = session.createRawClient();
=======
            this.session = TiSession.create(TiConfiguration.createRawDefault(PD_ADDRESS));
            this.client = session.createRawClient();
>>>>>>> 080bc189714b48509cca9350d9babcff9ce76a82
        }

        @Override
        public void run() {
            for (Map.Entry<ByteString, ByteString> entry : kvs.entrySet()) {
                client.put(entry.getKey(), entry.getValue());
            }
            if (!index_date.isEmpty()) {
                for (Map.Entry<ByteString, ByteString> entry : index_date.entrySet()) {
                    client.put(entry.getKey(), entry.getValue());
                }

            }
            if (!index_flag.isEmpty()) {
                for (Map.Entry<ByteString, ByteString> entry : index_flag.entrySet()) {
                    client.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}

