package cn.edu.neu.tiger.tikv.i2i;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.*;

public class RecallFromTiKV {

    private Map<String, Map<String, Object>> i2i;
    //  String PD_ADDRESS = "127.0.0.1:2379";
    static TiKVStorageService storageService; // 创建Tikv服务

    static{
        try {
            storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Integer> getScore_new(List<String> itemIdes, Map<String, Integer> userClickMap, Map<String, Map<String, Object>> i2i) {
        Map<String, Integer> result = new HashMap<>();
        for (String key : itemIdes) {
            if (i2i.containsKey(key)) {
                Map<String, Object> i2iItem = i2i.get(key);
                int score = compute(userClickMap, i2iItem);
                result.put(key, score);
            } else {
                result.put(key, 0);
            }
        }
        return result;
    }

    private Integer compute(Map<String, Integer> user, Map<String, Object> item) {
        int score = 0;
        for (String key : user.keySet()) {
            if (item.containsKey(key)) {
                score += (user.get(key) * Integer.valueOf(item.get(key).toString()));
            }
        }
        return score;
    }

    /**
     * @param userId，用户id
     * @param i2i，i2i表
     * @return 2元组，分别为用户id，召回商品ids
     * @throws IOException
     */
    public Tuple2<String, List<String>> recall_new(String userId, Map<String, Map<String, Object>> i2i) throws IOException {
        List<String> itemIds = new ArrayList<>();
        if (i2i != null)
            itemIds.addAll(i2i.keySet());
        try {
            Map<String, Integer> userClickMap = storageService.getUserClickRecord(userId);
            //后面的逻辑没有改动
            Map<String, Integer> itemScores = getScore_new(itemIds, userClickMap, i2i);
            List<Map.Entry<String, Integer>> list = new ArrayList<>(itemScores.entrySet());
            list.sort((o1, o2) -> o2.getValue() - o1.getValue());
            int i = 0;

            List<String> recall = new ArrayList<>();
            for (Map.Entry<String, Integer> o1 : list) {
                recall.add(o1.getKey());
                i++;
                if (i == Constants.RECALL_NUM) {
                    break;
                }
            }

            Tuple2<String, List<String>> tuple = new Tuple2<>();
            tuple.setField(userId, 0);
            tuple.setField(recall, 1);
            return tuple;
        } catch (Exception ignore) {
            System.out.println("function:scandata had an exception from getUserClickRecord");
            return null;
        }

    }
}
