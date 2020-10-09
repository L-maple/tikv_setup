package cn.edu.neu.tiger.tikv.service;

import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author qiqi
 * 存储服务接口
 */
public interface StorageService {
    Object scanData(String tableName, ArrayList<Map<String, String>> filters, String startRow, String stopRow) throws IOException;

    Object getDataByRowKey(String tableName, String rowKey) throws IOException;

    void writeData(String tabelName, String rowKey, List<Map<String, String>> data) throws IOException;

    void updateI2i(Object result, Map<String, Map<String, Integer>> userRecord, Map<String, Map<String, Integer>> i2i);

    Map<String, Integer> getUserClickRecord(String userId) throws IOException;

    Map<String, Map<String, Object>> getI2i() throws IOException;

    Row generateSample(String userId, List<String> itemIds) throws IOException;

    void deleteAllData(String tableName) throws IOException;

    List<String> getRowKeys(String tableName) throws IOException;

    void close() throws IOException;

    void open() throws IOException;

}
