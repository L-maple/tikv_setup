package cn.edu.neu.tiger.tikv.data;


import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;

import javax.xml.soap.SAAJResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateUserDataByTiKV {
  public static void main(String[] args) throws Exception {

    List<String> features = new ArrayList<>();
    features.add("feature1");
    features.add("feature2");
    features.add("feature3");
    features.add("feature4");
    features.add("feature5");
    features.add("feature6");
    features.add("feature7");
    features.add("feature8");
    features.add("feature9");
    features.add("feature10");
    features.add("feature11");
    features.add("feature12");
    features.add("feature13");
    features.add("feature14");
    features.add("feature15");
    features.add("feature16");
    features.add("feature17");
    features.add("feature18");
    features.add("feature19");
    //原来已有11个列，value大小应该是198B，现在增加下面34列，使一个value的大小达到1KB
//    features.add("pred_virtual_feature1");
//    features.add("pred_virtual_feature2");
//    features.add("pred_virtual_feature3");
//    features.add("pred_virtual_feature4");
//    features.add("pred_virtual_feature5");
//    features.add("pred_virtual_feature6");
//    features.add("pred_virtual_feature7");
//    features.add("pred_virtual_feature8");
//    features.add("pred_virtual_feature9");
//    features.add("pred_virtual_feature10");
//    features.add("pred_virtual_feature11");
//    features.add("pred_virtual_feature12");
//    features.add("pred_virtual_feature13");
//    features.add("pred_virtual_feature14");
//    features.add("pred_virtual_feature15");
//    features.add("pred_virtual_feature16");
//    features.add("pred_virtual_feature17");
//    features.add("pred_virtual_feature18");
//    features.add("pred_virtual_feature19");
//    features.add("pred_virtual_feature20");
//    features.add("pred_virtual_feature21");
//    features.add("pred_virtual_feature22");
//    features.add("pred_virtual_feature23");
//    features.add("pred_virtual_feature24");
//    features.add("pred_virtual_feature25");
//    features.add("pred_virtual_feature26");
//    features.add("pred_virtual_feature27");
//    features.add("pred_virtual_feature28");
//    features.add("pred_virtual_feature29");
//    features.add("pred_virtual_feature30");
//    features.add("pred_virtual_feature31");
//    features.add("pred_virtual_feature32");
//    features.add("pred_virtual_feature33");
    generate(475428); //10万条数据，每个kv对大小为1KB，user表数据量为10G
  }

  // 创建一个user表，该表是由一个"cf"列族，以及由11个特征作为列来形成的，generate方法会向该表插入50条记录
  public static void generate(int num) throws Exception {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

    Random random = new Random();
    List<String> careers =
        Arrays.asList(
            "R&D personnel",
            "civilian staff",
            "Counter person",
            "professor",
            "Designer",
            "Financial officer",
            "judge",
            "lawyer",
            "Clerk",
            "Guard",
            "editor",
            "Doctors",
            "nurse",
            "engineer",
            "Laboratory staff");
    List<String> educations =
        Arrays.asList(
            "Postgraduate",
            "Undergraduate",
            "University specialties",
            "specialized middle school",
            "Technical",
            "High school",
            "junior high school",
            "primary school",
            "illiteracy");
    // 循环插入user表的KV数据
    String tableName = Constants.TABLE_USER;
    int rowid;
    boolean isIndexCreated;
    int size = 0;
    ExecutorService exec = Executors.newFixedThreadPool(10);
    Map<String, Map<String, String>> kvs = new HashMap<>();
    int count = 0;//每计数至1000个KV对，就调用一次线程进行插入
    for (int i = 0; i < num; i++) {
      Map<String, String> userFeature = new HashMap<>();

      userFeature.put("feature1", String.valueOf(i));
      String career = careers.get(random.nextInt(careers.size()));
      userFeature.put("feature2", career);
      String gender = random.nextInt(1) < 1 ? "male" : "famale";
      userFeature.put("feature3", gender);
      String pet = random.nextInt(10) < 2 ? "yes" : "no";
      userFeature.put("feature4", pet);

      String car = random.nextInt(10) < 7 ? "yes" : "no";
      userFeature.put("feature5", car);

      String life = random.nextInt(2) < 1 ? "married" : "unmarried";
      userFeature.put("feature6", life);

      String house = random.nextInt(8) < 3 ? "yes" : "no";
      userFeature.put("feature7", house);

      String os = random.nextInt(10) < 3 ? "apple" : "android";
      userFeature.put("feature8", os);
      String education = educations.get(random.nextInt(educations.size()));
      userFeature.put("feature9", education);

//      int bage = random.nextInt(Integer.valueOf(userFeature.get("pred_age_level")) + 1);
//      String babyAge = String.valueOf(bage - 3 < 0 ? "-1" : bage);
//      userFeature.put("pred_baby_age", babyAge);

      userFeature.put("feature10",random.nextInt(20) < 7 ? "yes" : "no");
      userFeature.put("feature11",random.nextInt(30) < 15 ? "yes" : "no");
      userFeature.put("feature12",random.nextInt(40) < 30 ? "yes" : "no");
      userFeature.put("feature13", String.valueOf(random.nextInt(100) / 10000.0));
      userFeature.put("feature14", String.valueOf(random.nextInt(200) / 10000.0));
      userFeature.put("feature15", String.valueOf(random.nextInt(300) / 10000.0));
      userFeature.put("feature16", String.valueOf(random.nextInt(400) / 10000.0));
      userFeature.put("feature17", String.valueOf(random.nextInt(500) / 10000.0));
      userFeature.put("feature18", String.valueOf(random.nextInt(80) / 10000.0));
      userFeature.put("feature19", String.valueOf(random.nextInt(70) / 10000.0));



      //将模拟出的新增列添加到value(即userFeature)中去：
//      userFeature.put("pred_virtual_feature1", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature2", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature3", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature4", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature5", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature6", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature7", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature8", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature9", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature10", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature11", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature12", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature13", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature14", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature15", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature16", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature17", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature18", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature19", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature20", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature21", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature22", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature23", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature24", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature25", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature26", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature27", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature28", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature29", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature30", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature31", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature32", String.valueOf(random.nextInt(1000)));
//      userFeature.put("pred_virtual_feature33", String.valueOf(random.nextInt(1000)));
      /*if(i == 0){
        for(Map.Entry<String, String> entry : userFeature.entrySet()){
          String mapKey = entry.getKey();
          String mapValue = entry.getValue();
          size+=entry.getKey().length()+entry.getValue().length();
        }
        System.out.println("the length of key-value in user table is: "+size);
      }*/
      /*if (count < 1000){
        kvs.put(tableName,userFeature);
        count++;
      }else{
        //使用线程池处理tikv的写入操作
        exec.execute(new TiKVPutTask(kvs));
        count = 0;
        kvs.clear();
      }*/
      // 调用writeData插入表数据
      rowid = storageService.writeData(tableName, userFeature);
      // String tableName, String index_name, String index_value,int rowid
//      if (Constants.INDEX_ON) {
//        String index_name = "user_id";
//        String index_value = String.valueOf(i);
//        // 调用createUniqueIndex创建索引
//        isIndexCreated =
//            storageService.createUniqueIndex(tableName, index_name, index_value, rowid);
//        if (isIndexCreated) {
//          System.out.println("user表成功创建索引！！");
//        }
//      }
    }
    /*if(!kvs.isEmpty()){
      storageService.writeData(tableName, userFeature);
      //exec.execute(new TiKVPutTask(kvs));
    }*/
  }
}
