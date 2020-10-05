package cn.edu.neu.tiger.tools;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.util.*;


public class Util {

    /**
     * 获取sls配置
     *
     * @return
     */
    public static Properties getSlsProperties() {
        Properties configProps = new Properties();
        configProps.put(ConfigConstants.LOG_ENDPOINT, Constants.SLS_ENDPOITN);
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, Constants.SLS_ACCESSKEYID);
        configProps.put(ConfigConstants.LOG_ACCESSKEY, Constants.SLS_ACCESSKEYSECRET);
        configProps.put(ConfigConstants.LOG_PROJECT, Constants.SLS_PROJECT);
        configProps.put(ConfigConstants.LOG_LOGSTORE, Constants.SLS_LOGSTORE);
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR);
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "500");
        configProps.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, "50");
        return configProps;
    }
    /**
     * 获取kafka配置
     * @return
     */
    public static Properties getKafkaProPerties() {
        Properties properties = new Properties();
        properties.setProperty(Constants.KAFKA_BOOTSTRAP, Constants.KAFKA_BOOTSTRAP_VALUE);
        properties.setProperty(
                Constants.ZK_CONNECT,
                Constants.ZOOKEEPER_QUORUM_VALUE + ":" + Constants.ZOOKEEPER_CLIENT_PORT_VALUE);
        properties.setProperty("group.id","test-consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("enable.auto.commit", "true");
//        properties.setProperty(Constants.ZK_CONNECT,Constants.ZOOKEEPER_CLIENT);
        return properties;
    }

    public static void updateUserRecordMapByTiKV(String line, Map<String, Map<String, Integer>> userRecord) {
        if (!StringUtils.isBlank(line)) {

            String[] cells = line.trim().split("\t");
            //注意：这里第1列是itemID的值，第2列是userID的值
            String userId = cells[0];
            String itemId = cells[1];
            String flag = cells[2];

//            System.out.println("userID is: "+userId+'\t'+"itemID is: "+itemId+'\t'+"flag is: "+flag);
            Map<String, Integer> itemMap;

            if (!userRecord.containsKey(userId)) {
                itemMap = new HashMap<>();
                userRecord.put(userId, itemMap);
            } else {
                itemMap = userRecord.get(userId);
            }

            int num = 0;
            if (itemMap.containsKey(itemId)) {
                num = itemMap.get(itemId);
            }

            if (flag.equals("1")) {
                itemMap.put(itemId, num + 2);
            } else {
                itemMap.put(itemId, num - 1);
            }
        }
    }

    public static Map<String, String> getFilterInfo(String filterKey, String filterValue, String family) {
        Map<String, String> filterInfo = new HashMap<>();
        filterInfo.put("filterKey", filterKey);
        filterInfo.put("filterValue", filterValue);
        filterInfo.put("family", family);
        return filterInfo;
    }

    public static void createI2i(Map<String, Map<String, Integer>> userRecord, Map<String, Map<String, Integer>> i2i) {

        for (Map<String, Integer> record : userRecord.values()) {
            List<String> list = new ArrayList<>(record.keySet());
            for (int i = 0; i < list.size(); i++) {
                String key1 = list.get(i);
                for (int j = i; j < list.size(); j++) {

                    int num1 = 1;
                    int num2 = 1;
                    String key2 = list.get(j);

                    Map<String, Integer> temp1 = new HashMap<>();
                    Map<String, Integer> temp2 = new HashMap<>();
                    if (i2i.containsKey(key1)) {
                        temp1 = i2i.get(key1);
                    }
                    if (i2i.containsKey(key2)) {
                        temp2 = i2i.get(key2);
                    }
                    if (temp1.containsKey(key2)) {
                        num1 = temp1.get(key2) + 1;
                    }
                    if (temp2.containsKey(key1)) {
                        num2 = temp1.get(key1) + 1;
                    }
                    temp1.put(key2, num1);
                    temp2.put(key1, num2);
                    i2i.put(key1, temp1);
                    i2i.put(key2, temp2);
                }
            }
        }
    }

    public static Map<String, String> getSample(Map<String, String> userInfo, Map<String, String> itemInfo) {
        Map<String, String> sample = new HashMap<>();
        sample.putAll(userInfo);
        sample.putAll(itemInfo);
        return sample;
    }

    public static Map<String, String> getUserMap() {
        Map<String, String> map = new HashMap<>();
        map.put("feature1", "0");
        map.put("feature2", "0");
        map.put("feature3", "0");
        map.put("feature4", "0");
        map.put("feature5", "0");
        map.put("feature6", "0");
        map.put("feature7", "0");
        map.put("feature8", "0");
        map.put("feature9", "0");
        map.put("feature10", "0");
        map.put("feature11", "0");
        map.put("feature12", "0");
        map.put("feature13", "0");
        map.put("feature14", "0");
        map.put("feature15", "0");
        map.put("feature16", "0");
        map.put("feature17", "0");
        map.put("feature18", "0");
        map.put("feature19", "0");
        return map;
    }

    public static Map<String, String> getItemMap() {
        Map<String, String> map = new HashMap<>();
        map.put("feature57", "0");
        map.put("feature20", "0");
        map.put("feature21", "0");
        map.put("feature22", "0");
        map.put("feature23", "0");
        map.put("feature24", "0");
        map.put("feature25", "0");
        map.put("feature26", "0");
        map.put("feature27", "0");
        map.put("feature28", "0");
        map.put("feature29", "0");
        map.put("feature30", "0");
        map.put("feature31", "0");
        map.put("feature32", "0");
        map.put("feature33", "0");
        map.put("feature34", "0");
        map.put("feature35", "0");
        map.put("feature36", "0");
        map.put("feature37", "0");
        map.put("feature38", "0");
        map.put("feature39", "0");
        map.put("feature40", "0");
        map.put("feature41", "0");
        map.put("feature42", "0");
        map.put("feature43", "0");
        map.put("feature44", "0");
        map.put("feature45", "0");
        map.put("feature46", "0");
        map.put("feature47", "0");
        map.put("feature48", "0");
        map.put("feature49", "0");
        map.put("feature50", "0");
        map.put("feature51", "0");
        map.put("feature52", "0");
        map.put("feature53", "0");
        map.put("feature54", "0");
        map.put("feature55", "0");
        return map;
    }

    public static Tuple2<String, List<String>> getTopK(Row row) {
        Tuple2<String, List<String>> res = new Tuple2<>();
        List<String> topk = new ArrayList<>();
        String valueStr = String.valueOf(row.getField(0)).trim();
        if (!Character.isDigit(valueStr.charAt(valueStr.length() - 1)))
            valueStr = valueStr.substring(0, valueStr.length() - 1);
        String[] keys = valueStr.split(":");
        String userId = keys[0];
        String[] items = keys[1].split("\\|");
        List<Tuple2<String, Double>> tuple2List = new ArrayList<>();
        for (String item : items) {
            if (!StringUtils.isBlank(item)) {
                String[] itemKeys = item.split("-");
                Tuple2<String, Double> tuple = new Tuple2<>();
                tuple.setField(itemKeys[0], 0);
                tuple.setField(Double.parseDouble(itemKeys[1]), 1);
                tuple2List.add(tuple);
            }
        }
        tuple2List.sort(new Comparator<Tuple2<String, Double>>() {
            @Override
            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                double alpha = 0.000001;
                if (o1.f1 - o2.f1 < alpha && o2.f1 - o1.f1 < alpha) {
                    return 0;
                }
                return o1.f1 - o2.f1 > 0 ? -1 : 1;
            }
        });
        for (int i = 0; i < 8; i++) {
            topk.add(tuple2List.get(i).f0);
        }
        res.setField(userId, 0);
        res.setField(topk, 1);
        return res;
    }

    public static Tuple3<String, List<String>, Long> getTopKWithPrometheus(Row row) {
        Tuple3<String, List<String>, Long> res = new Tuple3<>();
        List<String> topk = new ArrayList<>();
        String valueStr = String.valueOf(row.getField(0)).trim();
        if (!Character.isDigit(valueStr.charAt(valueStr.length() - 1)))
            valueStr = valueStr.substring(0, valueStr.length() - 1);
        String[] keys = valueStr.split(":");
        String userId = keys[0];
        String[] items = keys[1].split("\\|");
        long timestamp = Long.parseLong(keys[2]);
        List<Tuple2<String, Double>> tuple2List = new ArrayList<>();
        for (String item : items) {
            if (!StringUtils.isBlank(item)) {
                String[] itemKeys = item.split("-");
                Tuple2<String, Double> tuple = new Tuple2<>();
                tuple.setField(itemKeys[0], 0);
                tuple.setField(Double.parseDouble(itemKeys[1]), 1);
                tuple2List.add(tuple);
            }
        }
        tuple2List.sort(new Comparator<Tuple2<String, Double>>() {
            @Override
            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                double alpha = 0.000001;
                if (o1.f1 - o2.f1 < alpha && o2.f1 - o1.f1 < alpha) {
                    return 0;
                }
                return o1.f1 - o2.f1 > 0 ? -1 : 1;
            }
        });
        for (int i = 0; i < 8; i++) {
            topk.add(tuple2List.get(i).f0);
        }
        res.setField(userId, 0);
        res.setField(topk, 1);
        res.setField(timestamp,2);
        return res;
    }
}
