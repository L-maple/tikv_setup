package cn.edu.neu.tiger.tikv.data;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import tikv.com.google.protobuf.ByteString;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
public class WriteClickRecordToTiKV {
  public static void main(String[] args) throws Exception {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务
<<<<<<< HEAD
    int recordNum = 16043400;//1000万条数据，每个kv对大小为1KB，click表数据量为10G
=======
    int recordNum = 160434;//1000万条数据，每个kv对大小为1KB，click表数据量为10G
>>>>>>> 080bc189714b48509cca9350d9babcff9ce76a82
    Random random = new Random();

    // 循环插入click表的KV数据，同时暂时创建分别关于user_id和flag的非唯一索引
    String tableName = Constants.TABLE_CLICK;
    int rowid;
    boolean isIndexCreated;
    int size = 0;

    ExecutorService exec = Executors.newFixedThreadPool(100);
    Map<String, Map<String, String>> kvs = new HashMap<>();
    int count = 0;//每计数至1000个KV对，就调用一次线程进行插入
    for (int i = 0; i < recordNum; i++) {
      Map<String, String> clickFeature = new HashMap<>();

      long time = System.currentTimeMillis();
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      String date = df.format(time);

      int userId = random.nextInt(4754280);
      int itemId = random.nextInt(768105);
      int flag = random.nextInt(100) < 15 ? 1 : 0;
      clickFeature.put("feature1", String.valueOf(userId));
      clickFeature.put("feature20", String.valueOf(itemId));
      clickFeature.put("flag", String.valueOf(flag));
      clickFeature.put("date", date);
      //将模拟出的新增列添加到value(即clickFeature)中去：
//      clickFeature.put("click_virtual_feature1", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature2", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature3", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature4", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature5", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature6", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature7", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature8", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature9", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature10", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature11", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature12", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature13", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature14", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature15", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature16", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature17", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature18", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature19", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature20", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature21", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature22", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature23", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature24", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature25", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature26", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature27", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature28", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature29", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature30", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature31", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature32", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature33", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature34", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature35", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature36", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature37", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature38", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature39", String.valueOf(random.nextInt(1000)));
//      clickFeature.put("click_virtual_feature40", String.valueOf(random.nextInt(1000)));

      /*if (i == 0) {
        for (Map.Entry<String, String> entry : clickFeature.entrySet()) {
          String mapKey = entry.getKey();
          String mapValue = entry.getValue();
          size += entry.getKey().length() + entry.getValue().length();
        }
        System.out.println("the length of key-value in click table is: " + size);
      }*/
      /*if (count < 100000) {
        kvs.put(tableName, clickFeature);
        count++;
      } else {
        //使用线程池处理tikv的写入操作
        exec.execute(new TiKVPutTask1(kvs));
        count = 0;
        kvs.clear();
      }*/
      // 调用writeData插入表数据
      rowid = storageService.writeData(tableName, clickFeature);
//      if (Constants.INDEX_ON) {
//        // 创建基于"user_id"的非Unique索引
//        String userid_name = "user_id";
//        String userid_value = String.valueOf(i);
//        // 调用createNoUniqueIndex创建非unique索引
//        isIndexCreated =
//            storageService.createNoUniqueIndex(
//                tableName, userid_name, userid_value, rowid, userid_value);
//        if (isIndexCreated) {
//          //          System.out.println("成功创建基于user_id的非Unique索引！！");
//        }
//      }
//      // 创建基于"flag"的非Unique索引
//      String flag_name = "flag";
//      String flag_value = String.valueOf(i);
//      // 调用createNoUniqueIndex创建非unique索引
//      isIndexCreated =
//          storageService.createNoUniqueIndex(
//              tableName, flag_name, flag_value, rowid, String.valueOf(i));
//      if (isIndexCreated) {
//        //        System.out.println("成功创建基于flag的非Unique索引！！");
//      }
      // System.out.println(record);
      //            Thread.sleep(1000);
    }
    /*if (!kvs.isEmpty()) {
      exec.execute(new TiKVPutTask1(kvs));
    }*/
  }
}

class TiKVPutTask1 implements Runnable{
//  TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务;

  Map<String, Map<String, String>> kvs = new HashMap<>();
  Map<ByteString,ByteString> index_flag = new HashMap<>();
  Map<ByteString,ByteString> index_date = new HashMap<>();
  RawKVClient client;
  TiSession session;

  public TiKVPutTask1(Map<String, Map<String, String>> kvs) throws IOException {
    //this.kvs = kvs;

    this.kvs.putAll(kvs);
    //this.client = client;
//        this.session = TiSession.create(TiConfiguration.createRawDefault(Constants.PD_ADDRESS));
//        this.client = session.createRawClient();
  }
  @Override
  public void run(){
    try {
      for (Map.Entry<String, Map<String, String>> entry : kvs.entrySet()) {
        ByteString tkey =
                ByteString.copyFromUtf8(
                        String.format(
                                "%s#%s#%s#%s",
                                entry.getKey() + ",", "r" + ",", entry.getValue().get("feature1") + ",", System.nanoTime()+""));
        // tvalue由"col1Name:col1value,col2Name:col2value"组成
        String valueBuilder = "";
        int kv_nums = entry.getValue().size() - 1;
        int i = 0;
        for (Map.Entry<String, String> entry1 : entry.getValue().entrySet()) {
          valueBuilder = valueBuilder + entry1.getKey() + ":" + entry1.getValue();
          if (i < kv_nums) {
            valueBuilder += ",";
            i++;
          }
        }
        //logger.info(" insert data-size : {}",i);
        ByteString tvalue = ByteString.copyFromUtf8(valueBuilder);

        client.put(tkey, tvalue);
        System.out.println("tkey:"+tkey.toString()+"  tvalue:"+tvalue.toString());
        // 调用writeData插入表数据
        //storageService.writeData(entry.getKey(), entry.getValue());
      }
      System.out.println("insert success!!!!!!!!!!!!!!!");
    }catch (Exception e){
      e.printStackTrace();
      System.out.println(" insert has an exception in TiKV thread!!!");
    }
        this.client.close();
        this.session.close();
  }
}
