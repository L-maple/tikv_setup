package cn.edu.neu.tiger.tikv;

import cn.edu.neu.tiger.tikv.i2i.RecallFromTiKV;
import cn.edu.neu.tiger.tools.HdfsUtil;

import java.io.IOException;
import java.util.Map;

public class ReadTests {
    public static void main(String[] args) throws IOException {

        RecallFromTiKV recallFromTiKV = new RecallFromTiKV();
        Map<String, Map<String, Object>> i2i = HdfsUtil.readHDFS("text");

        while (true) {
            long start = System.currentTimeMillis();
            recallFromTiKV.recall_new("2110932077", i2i);
            long end = System.currentTimeMillis();
            System.out.println("recell cost:" + (end - start));
        }

    }
}
