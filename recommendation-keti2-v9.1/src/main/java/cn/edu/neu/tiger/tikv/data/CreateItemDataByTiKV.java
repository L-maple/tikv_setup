package cn.edu.neu.tiger.tikv.data;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateItemDataByTiKV {

  public static void main(String[] args) throws Exception {

    List<String> item_features = new ArrayList<>();
    item_features.add("feature20");
    item_features.add("feature21");
    item_features.add("feature22");
    item_features.add("feature23");
    item_features.add("feature24");
    item_features.add("feature25");
    item_features.add("feature26");
    item_features.add("feature27");
    item_features.add("feature28");
    item_features.add("feature29");
    item_features.add("feature30");
    item_features.add("feature30");
    item_features.add("feature31");
    item_features.add("feature32");
    item_features.add("feature33");
    item_features.add("feature34");
    item_features.add("feature35");
    item_features.add("feature36");
    item_features.add("feature37");
    item_features.add("feature38");
    item_features.add("feature39");
    item_features.add("feature40");
    item_features.add("feature41");
    item_features.add("feature42");
    item_features.add("feature43");
    item_features.add("feature44");
    item_features.add("feature45");
    item_features.add("feature46");
    item_features.add("feature47");
    item_features.add("feature48");
    item_features.add("feature49");
    item_features.add("feature50");
    item_features.add("feature51");
    item_features.add("feature52");
    item_features.add("feature53");
    item_features.add("feature54");
    item_features.add("feature55");
    //原来已有17个列，value大小应该是293B，现在增加下面30列，使一个value的大小达到1KB
//    item_features.add("item_virtual_feature1");
//    item_features.add("item_virtual_feature2");
//    item_features.add("item_virtual_feature3");
//    item_features.add("item_virtual_feature4");
//    item_features.add("item_virtual_feature5");
//    item_features.add("item_virtual_feature6");
//    item_features.add("item_virtual_feature7");
//    item_features.add("item_virtual_feature8");
//    item_features.add("item_virtual_feature9");
//    item_features.add("item_virtual_feature10");
//    item_features.add("item_virtual_feature11");
//    item_features.add("item_virtual_feature12");
//    item_features.add("item_virtual_feature13");
//    item_features.add("item_virtual_feature14");
//    item_features.add("item_virtual_feature15");
//    item_features.add("item_virtual_feature16");
//    item_features.add("item_virtual_feature17");
//    item_features.add("item_virtual_feature18");
//    item_features.add("item_virtual_feature19");
//    item_features.add("item_virtual_feature20");
//    item_features.add("item_virtual_feature21");
//    item_features.add("item_virtual_feature22");
//    item_features.add("item_virtual_feature23");
//    item_features.add("item_virtual_feature24");
//    item_features.add("item_virtual_feature25");
//    item_features.add("item_virtual_feature26");
//    item_features.add("item_virtual_feature27");
//    item_features.add("item_virtual_feature28");
//    item_features.add("item_virtual_feature29");
//    item_features.add("item_virtual_feature30");
    generate(768105); //1000万条数据，每个kv对大小为1KB，item表数据量为10G
  }
  // 创建一个item表，该表是由一个"cf"列族，以及由15个特征作为列来形成的，generate方法会向该表插入200条记录
  public static void generate(int num) throws Exception {
    //    String PD_ADDRESS = "127.0.0.1:2379";
    TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS); // 创建Tikv服务

    Random random = new Random();
    List<String> categories =
        Arrays.asList(
            "Women's clothing",
            "Men's",
            "Women's shoes",
            "makeups",
            "Watch",
            "Mobile phone",
            "Maternal baby",
            "toy",
            "Snack",
            "Tea wine",
            "Fresh",
            "fruit",
            "Home appliance",
            "Furniture",
            "Building materials",
            "car",
            "Accessories",
            "Home textile",
            "medicine",
            "Kitchenware",
            "Storage");

    List<String> cites =
        Arrays.asList(
            "beijing-beijing",
            "shanghai-shanghai",
            "guangzhou-guangdong",
            "shenzheng-shenzheng",
            "chengdu-sichuan",
            "hangzhou-zhejiang",
            "wuhan-hubei",
            "changsha-hunan",
            "kunming-yunnan",
            "xiamen-fujian",
            "zhengzhou-henan",
            "shijiazhuang-hebei");
    // 循环插入item表的KV数据
    String tableName = Constants.TABLE_ITEM;
    int rowid;
    boolean isIndexCreated;
    int size = 0;
    ExecutorService exec = Executors.newFixedThreadPool(10);
    Map<String, Map<String, String>> kvs = new HashMap<>();
    int count = 0;//每计数至1000个KV对，就调用一次线程进行插入
    for (int i = 0; i < num; i++) {
      // value:Map<String,String> data
      Map<String, String> itemFeature = new HashMap<>();

      itemFeature.put("feature20", String.valueOf(i));

      String item_expo_id = String.valueOf(random.nextInt(100));
      itemFeature.put("feature21", item_expo_id);

      String item_category = categories.get(random.nextInt(categories.size()));
      itemFeature.put("feature22", item_category);

      String item_category_level1 = String.valueOf(random.nextInt(4));
      itemFeature.put("feature23", item_category_level1);

      String city = cites.get(random.nextInt(cites.size()));
      itemFeature.put("feature24", city.split("-")[0]);
      //itemFeature.put("item_seller_prov", city.split("-")[1]);

      String item_purch_level = String.valueOf(random.nextInt(5));
      itemFeature.put("feature25", item_purch_level);

//      String item_gender = random.nextInt(2) < 1 ? "male" : "female";
//      itemFeature.put("item_gender", item_gender);

      String item_buyer_age = String.valueOf(random.nextInt(8));
      itemFeature.put("feature26", item_buyer_age);

      String item_style_id = String.valueOf(random.nextInt(20));
      itemFeature.put("feature27", item_style_id);

//      String item_material_id = String.valueOf(random.nextInt(20));
//      itemFeature.put("item_material_id", item_material_id);
//
//      String item_pay_class = String.valueOf(random.nextInt(5));
//      itemFeature.put("item_pay_class", item_pay_class);
//
//      String item_brand_id = String.valueOf(random.nextInt(100));
//      itemFeature.put("item_brand_id", item_brand_id);

      String item__i_shop_id_ctr = String.valueOf(random.nextInt(100) / 10000.0);
      itemFeature.put("feature28", item__i_shop_id_ctr);

      String item__i_brand_id_ctr = String.valueOf(random.nextInt(200) / 10000.0);
      itemFeature.put("feature29", item__i_brand_id_ctr);

      String item__i_category_ctr = String.valueOf(random.nextInt(70) / 10000.0);
      itemFeature.put("feature30", item__i_category_ctr);

      itemFeature.put("feature31", String.valueOf(random.nextInt(100) / 10000.0));
      itemFeature.put("feature32", String.valueOf(random.nextInt(200) / 10000.0));
      itemFeature.put("feature33", String.valueOf(random.nextInt(300) / 10000.0));
      itemFeature.put("feature34", String.valueOf(random.nextInt(400) / 10000.0));
      itemFeature.put("feature35", String.valueOf(random.nextInt(500) / 10000.0));
      itemFeature.put("feature36", String.valueOf(random.nextInt(80) / 10000.0));
      itemFeature.put("feature37", String.valueOf(random.nextInt(70) / 10000.0));
      itemFeature.put("feature38", String.valueOf(random.nextInt(150) / 10000.0));
      itemFeature.put("feature39", String.valueOf(random.nextInt(250) / 10000.0));
      itemFeature.put("feature40", String.valueOf(random.nextInt(350) / 10000.0));
      itemFeature.put("feature41", String.valueOf(random.nextInt(1000) / 10000.0));
      itemFeature.put("feature42", String.valueOf(random.nextInt(10) / 100.0));
      itemFeature.put("feature43", String.valueOf(random.nextInt(35) / 100.0));
      itemFeature.put("feature44", String.valueOf(random.nextInt(40) / 100.0));
      itemFeature.put("feature45", String.valueOf(random.nextInt(50) / 100.0));
      itemFeature.put("feature46", String.valueOf(random.nextInt(60) / 100.0));
      itemFeature.put("feature47", String.valueOf(random.nextInt(70) / 100.0));
      itemFeature.put("feature48", String.valueOf(random.nextInt(150) / 1000.0));
      itemFeature.put("feature49", String.valueOf(random.nextInt(25) / 100.0));
      itemFeature.put("feature50", String.valueOf(random.nextInt(80) / 1000.0));
      itemFeature.put("feature51", String.valueOf(random.nextInt(2000) / 10000.0));
      itemFeature.put("feature52", String.valueOf(random.nextInt(1500) / 10000.0));
      itemFeature.put("feature53", String.valueOf(random.nextInt(2500) / 10000.0));
      itemFeature.put("feature54", String.valueOf(random.nextInt(4000) / 10000.0));
      itemFeature.put("feature55", String.valueOf(random.nextInt(600) / 10000.0));
      //将模拟出的新增列添加到value(即itemFeature)中去：
//      itemFeature.put("item_virtual_feature1", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature2", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature3", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature4", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature5", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature6", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature7", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature8", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature9", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature10", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature11", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature12", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature13", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature14", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature15", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature16", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature17", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature18", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature19", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature20", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature21", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature22", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature23", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature24", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature25", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature26", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature27", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature28", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature29", String.valueOf(random.nextInt(1000)));
//      itemFeature.put("item_virtual_feature30", String.valueOf(random.nextInt(1000)));

/*      if(i == 0){
        for(Map.Entry<String, String> entry : itemFeature.entrySet()){
          String mapKey = entry.getKey();
          String mapValue = entry.getValue();
          size+=entry.getKey().length()+entry.getValue().length();
        }
        System.out.println("the length of key-value in item table is: "+size);
      }*/
      /*System.out.println("i = "+i);
      if (count < 1000){
        kvs.put(tableName,itemFeature);
        count++;
      }else{
        //使用线程池处理tikv的写入操作
        exec.execute(new TiKVPutTask(kvs));
        count = 0;
        kvs.clear();
      }*/

      // 调用writeData插入表数据
      storageService.writeData(tableName, itemFeature);
    }
    /*if(!kvs.isEmpty()){
      exec.execute(new TiKVPutTask(kvs));
    }*/
  }
}
