package cn.edu.neu.tiger.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseUtil {
    public static Configuration getHbaseConf() {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, Constants.ZOOKEEPER_QUORUM_VALUE);
        hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Constants.ZOOKEEPER_CLIENT_PORT_VALUE);
        hbaseConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        hbaseConf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        return hbaseConf;
    }

    public static HTablePool getTablePool() {
        Configuration conf = getHbaseConf();
        HTablePool tablePool = new HTablePool(conf, 100);
        return tablePool;
    }

    public static Map<String, String> getRowByCells(Object cells, Map<String, Object> featureTransMap) {
        List<Cell> cellList = (List<Cell>) cells;
        Map<String, String> row = new HashMap<>();
        for (Cell cell : cellList) {
            String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            String name = key;
            if(featureTransMap.containsKey(key)){
                name = featureTransMap.get(key).toString();
            }
            row.put(name, value);
        }
        return row;
    }

    public static Connection getHbaseConnection() throws IOException {
        Configuration config = getHbaseConf();
        return ConnectionFactory.createConnection(config);
    }

    public static Map<String, String> getRow(Result result) {
        Map<String, String> row = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String name = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            row.put(name, value);
        }
        return row;
    }

    public static Map<String, String> getFilterInfo(String filterKey, String filterValue, String family) {
        Map<String, String> filterInfo = new HashMap<>();
        filterInfo.put("filterKey", filterKey);
        filterInfo.put("filterValue", filterValue);
        filterInfo.put("family", family);
        return filterInfo;
    }

    public static Map<String, String> getPutInfo(String putKey, String putValue, String family) {
        Map<String, String> putInfo = new HashMap<>();
        putInfo.put(Constants.HBASE_PUTKEY, putKey);
        putInfo.put(Constants.HBASE_PUTVALUE, putValue);
        putInfo.put(Constants.HBASE_FAMILY, family);
        return putInfo;
    }

}
