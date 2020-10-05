package cn.edu.neu.tiger.tikv.i2i;

import cn.edu.neu.tiger.tools.Util;
import com.alibaba.fastjson.JSONObject;
import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.HdfsUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UpdateI2iByTiKV {
    private static TiKVStorageService tiKVStorageService; // 创建Tikv服务

    static {
        try {
            tiKVStorageService = new TikvServiceImpl(Constants.PD_ADDRESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        Map<String, Map<String, Integer>> userRecord = new HashMap<>();
        Map<String, Map<String, Integer>> i2i = new HashMap<>();

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        long time = System.currentTimeMillis() - 24 * 60 * 60 * 1000;
        String date = df.format(time);

        System.out.println(date.replace("-", ""));

        ArrayList<Map<String, String>> filterInfos = new ArrayList<>();
        filterInfos.add(Util.getFilterInfo("flag", "1", "cf"));
        filterInfos.add(Util.getFilterInfo("date", date.replace("-", ""), "cf"));

        //Object result = storageService.scanData(Constants.TABLE_CLICK, filterInfos, null, null);
        //换成使用tikv来检索数据
        Object result = null;
        try {
            result = tiKVStorageService.scanData(Constants.TABLE_CLICK, filterInfos, null, null);
        } catch (Exception e) {

        }

        tiKVStorageService.updateI2i(result, userRecord, i2i);
        System.out.println("size:" + i2i.size());

        Map<String, Map<String, Integer>> map = new HashMap<>();
        int index = 0;
        for (String key : i2i.keySet()) {
            System.out.println(index + ":" + key);
            map.put(key, i2i.get(key));
            index++;
            if (index == 2000) {
                break;
            }
        }

        time += 24 * 60 * 60 * 1000;
        date = df.format(time);

        String json = JSONObject.toJSONString(map);
        HdfsUtil.writeHDFS(json, date);
        HdfsUtil.readHDFS(date);

    }
}
