package cn.edu.neu.tiger.tikv.impl;

import cn.edu.neu.tiger.tikv.service.StorageService;
import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.HBaseUtil;
import cn.edu.neu.tiger.tools.HdfsUtil;
import cn.edu.neu.tiger.tools.Util;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author qiqi
 * kv系统方法类，实现场景对kv系统的增删改查
 */
public class HBaseServiceImpl implements StorageService, Serializable {

    HTablePool tablePool;
    Map<String, Object> featureTransMap;
    HTableInterface tableUser;
    HTableInterface tableItem;
    HTableInterface tableClick;
    HTableInterface tableResult;

    /**
     * open方法，初始化kv连接池，数据特征映射表
     *
     * @throws IOException
     */
    @Override
    public void open() throws IOException {
        tablePool = HBaseUtil.getTablePool();
        tableUser = tablePool.getTable(Constants.TABLE_USER);
        tableItem = tablePool.getTable(Constants.TABLE_ITEM);
        tableClick = tablePool.getTable(Constants.TABLE_CLICK);
        tableResult = tablePool.getTable(Constants.TABLE_RESULT);
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

    public Map<String, Object> getFeatureTransMap() {
        return featureTransMap;
    }

    /**
     * 说明：tikv没有rowkey概念，因此kv同学要做适当修改，满足查询要求，随时讨论
     *
     * @param tableName   kv表名
     * @param filterInfos 过滤条件
     * @param startRow    hbase概念，rowkey起始点位
     * @param stopRow     hbase概念，rowkey结束点位
     * @return 查询结果
     * @throws IOException
     */
    @Override
    public List<Result> scanData(String tableName, ArrayList<Map<String, String>> filterInfos, String startRow, String stopRow) throws IOException {
        HTableInterface table = getTable(tableName);
        Scan scan = new Scan();
        if (!filterInfos.isEmpty()) {
            FilterList filterList = new FilterList();
            for (Map<String, String> filterInfo : filterInfos) {
                SingleColumnValueFilter filter = new SingleColumnValueFilter(
                        Bytes.toBytes(filterInfo.get("family")), Bytes.toBytes(filterInfo.get("filterKey")),
                        CompareFilter.CompareOp.EQUAL, Bytes.toBytes(filterInfo.get("filterValue")));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
            }
            scan.setFilter(filterList);
        }
        if (startRow != null) {
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
        }

        Iterator<Result> results = table.getScanner(scan).iterator();
        List<Result> list = new ArrayList<>();
        int num = 0;
        while (results.hasNext()) {
            list.add(results.next());
            num++;
            if (num > Constants.MAXNUM) {
                break;
            }
        }
        return list;
    }

    /**
     * 说明：按照rowkey查询数据
     *
     * @param tableName kv表名
     * @param rowKey
     * @return
     * @throws IOException
     */
    @Override
    public List<Cell> getDataByRowKey(String tableName, String rowKey) throws IOException {
        HTableInterface table = getTable(tableName);
        try {
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
            List<Cell> cells = new ArrayList<>();
            for (Cell cell : result.list()) {
                cells.add(cell);
            }
            return cells;
        } catch (Exception e) {
            System.out.println(String.format("there is no rowkey %s for table: %s", rowKey, tableName));
            return new ArrayList<>();
        }

    }

    /**
     * @param tableName kv表明
     * @param rowKey
     * @param data      写入数据
     * @throws IOException
     */
    @Override
    public void writeData(String tableName, String rowKey, List<Map<String, String>> data) throws IOException {
        HTableInterface table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map<String, String> kv : data) {
            put.add(Bytes.toBytes(kv.get(Constants.HBASE_FAMILY)),
                    Bytes.toBytes(kv.get(Constants.HBASE_PUTKEY)),
                    Bytes.toBytes(kv.get(Constants.HBASE_PUTVALUE)));
        }
        table.put(put);
    }

    @Override
    public void updateI2i(Object result, Map<String, Map<String, Integer>> userRecord, Map<String, Map<String, Integer>> i2i) {
        List<Result> list = (List<Result>) result;
        try {
            for (Result result1 : list) {
                String value = "";
                for (Cell cell : result1.list()) {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\t" + value;
                }
                Util.updateUserRecordMap(value, userRecord);
            }
        } catch (Exception ignore) {
            System.out.println("读click数据失败：数据不存在或者hbase读取失败！");
        }
        Util.createI2i(userRecord, i2i);
    }

    @Override
    public Map<String, Integer> getUserClickRecord(String userId) throws IOException {
        Map<String, Map<String, Integer>> userRecord = new HashMap<>();
        String startRow = userId + "-" + "0";
        String stopRow = userId + "-" + "2";
        List<Result> results = scanData(Constants.TABLE_CLICK, new ArrayList<>(), startRow, stopRow);
        try {
            for (Result result : results) {
                String value = "";
                for (Cell cell : result.list()) {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\t" + value;
                }
                Util.updateUserRecordMap(value, userRecord);
            }
        } catch (Exception ignore) {
            System.out.println("no history data for user: " + userId);
            return new HashMap<>();
        }
        return userRecord.get(userId) == null ? new HashMap<>() : userRecord.get(userId);
    }

    /**
     * 废弃不用，i2i改从hdfs获取
     *
     * @return
     * @throws IOException
     */
    @Override
    public Map<String, Map<String, Object>> getI2i() throws IOException {
        return null;
    }


    /**
     * @param userId  用户id
     * @param itemIds 商品id
     * @return flink.row格式的样本数据
     * @throws IOException
     */
    @Override
    public org.apache.flink.types.Row generateSample(String userId, List<String> itemIds) throws IOException {

        List<Cell> userCells = getDataByRowKey(Constants.TABLE_USER, userId);
        Map<String, String> userInfo = HBaseUtil.getRowByCells(userCells, featureTransMap);
        if (userInfo.isEmpty()) {
            userInfo = Util.getUserMap();
            userInfo.put("feature56", userId);
        }

        StringBuilder result = new StringBuilder();

        for (String itemId : itemIds) {

            List<Cell> itemCells = getDataByRowKey(Constants.TABLE_ITEM, itemId);
            Map<String, String> itemInfo = HBaseUtil.getRowByCells(itemCells, featureTransMap);
            if (itemInfo.isEmpty()) {
                itemInfo = Util.getItemMap();
            }

            Map<String, String> sample = Util.getSample(userInfo, itemInfo);
            result.append(JSONObject.toJSONString(sample));
            result.append(",");
        }
        org.apache.flink.types.Row row = new Row(1);
        row.setField(0, result.toString().substring(0, result.length() - 1));
        return row;
    }

    /**
     * 说明：调试系统时使用
     *
     * @param tableName kv名
     * @throws IOException
     */
    @Override
    public void deleteAllData(String tableName) throws IOException {

        HTableInterface table = getTable(tableName);

        List<String> rowKeys = getRowKeys(tableName);
        List<Delete> list = new ArrayList<>();
        for (String rowkey : rowKeys) {
            list.add(new Delete(Bytes.toBytes(rowkey)));
        }
        table.delete(list);
    }

    /**
     * @param tableName kv表名
     * @return kv表用所有rowkey
     * @throws IOException
     */
    @Override
    public List<String> getRowKeys(String tableName) throws IOException {

        List<Result> results = scanData(tableName, new ArrayList<>(), null, null);
        List<String> rowKeys = new ArrayList<>();
        for (Result result : results) {
            String rowKey;
            for (KeyValue kv : result.raw()) {
                int rowlength = Bytes.toShort(kv.getBuffer(), kv.getOffset() + KeyValue.ROW_OFFSET);
                rowKey = Bytes.toStringBinary(kv.getBuffer(), kv.getOffset() + KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT, rowlength);
                if (!StringUtils.isBlank(rowKey)) {
                    rowKeys.add(rowKey);
                    break;
                }
            }
        }
        return rowKeys;
    }

    /**
     * 关闭连接池
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        tableUser.close();
        tableItem.close();
        tableClick.close();
        tableResult.close();
        tablePool.close();
    }

    private HTableInterface getTable(String tableName) {
        switch (tableName) {
            case Constants.TABLE_USER:
                return tableUser;
            case Constants.TABLE_ITEM:
                return tableItem;
            case Constants.TABLE_CLICK:
                return tableClick;
            case Constants.TABLE_RESULT:
                return tableResult;
        }
        System.out.println("优化无效" + tableName);
        return tablePool.getTable(tableName);
    }
}
