package cn.edu.neu.tiger.tools;

import tikv.com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public class TikvUtil {

    public static Map<String, String> getRow(ByteString tvalue) {
        Map<String, String> row = new HashMap<>();
        String val = tvalue.toStringUtf8();
        /**
         *当kv中不存在该数据时，返回空map
         */
        if (val.isEmpty()) {
            return row;
        }
        String[] col_value = val.split(",");
        String[] cv;
        for (String s : col_value) {
            cv = s.split(":");
            String name = cv[0];
            String value = cv[1];
            row.put(name, value);
        }
        return row;
    }

    public static Map<String, String> getRowByCells(Map<String, String> cells, Map<String, Object> featureTransMap) {
        Map<String, String> row = new HashMap<>();
        for (Object obj : cells.entrySet()) {
            Map.Entry me = (Map.Entry) obj;
            String key = (String) me.getKey();
            String value = (String) me.getValue();
            String name = key;
            if (featureTransMap != null && featureTransMap.containsKey(key)) {
                name = featureTransMap.get(key).toString();
            }
            row.put(name, value);
        }
        return row;
    }

    public static Map<String, String> getFilterInfo(
            String filter_Key, String filter_Value, String family) {
        Map<String, String> filterInfo = new HashMap<>();
        filterInfo.put("filterKey", filter_Key);
        filterInfo.put("filterValue", filter_Value);
        filterInfo.put("family", family);
        return filterInfo;
    }
}
