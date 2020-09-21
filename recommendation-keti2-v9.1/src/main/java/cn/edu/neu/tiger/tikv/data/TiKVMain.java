package cn.edu.neu.tiger.tikv.data;

import cn.edu.neu.tiger.tikv.i2i.RecallFromTiKV;
import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import org.tikv.kvproto.Kvrpcpb;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TiKVMain {
  public static void main(String[] args) throws Exception {
    SimpleDateFormat sdm = new SimpleDateFormat("yyyy年MM月dd日 HH点:mm分:ss:秒");
    if (args[0].equals("itemput")) {
      String time_itemput1 = sdm.format(new Date());
      System.out.println("item表开始插入的时间为：" + time_itemput1);

      CreateItemDataByTiKV.main(null);

      String time_itemput2 = sdm.format(new Date());
      System.out.println("item表插入完成的时间为：" + time_itemput2);
    } else if (args[0].equals("userput")) {
      String time_userput1 = sdm.format(new Date());
      System.out.println("user表开始插入的时间为：" + time_userput1);

      CreateUserDataByTiKV.main(null);

      String time_userput2 = sdm.format(new Date());
      System.out.println("user表插入完成的时间为：" + time_userput2);
    } else if (args[0].equals("clickput")) {
      String time_clickput1 = sdm.format(new Date());
      System.out.println("click表开始插入的时间为：" + time_clickput1);

      WriteClickRecordToTiKV.main(null);

      String time_clickput2 = sdm.format(new Date());
      System.out.println("click表插入完成的时间为：" + time_clickput2);
    } else if (args[0].equals("scan")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      String time_scan_user1 = sdm.format(new Date());
      System.out.println("user表开始全表扫描的时间为：" + time_scan_user1);

      List<Kvrpcpb.KvPair> result =
          (List<Kvrpcpb.KvPair>) storageService.scanData(Constants.TABLE_USER, new ArrayList<>(),null,null);

      String time_scan_user2 = sdm.format(new Date());
      System.out.println("user表扫描完成的时间为：" + time_scan_user2);
      System.out.println(
          "---------------------------------------------user result.size:"
              + result.size()
              + "-------------------------------------");
      for (Kvrpcpb.KvPair user : result) {
        System.out.println(user.getKey().toStringUtf8() + '\t' + user.getValue().toStringUtf8());
      }

      String time_scan_item1 = sdm.format(new Date());
      System.out.println("item表开始全表扫描的时间为：" + time_scan_item1);

      result = (List<Kvrpcpb.KvPair>) storageService.scanData(Constants.TABLE_ITEM, new ArrayList<>(),null,null);

      String time_scan_item2 = sdm.format(new Date());
      System.out.println("item表扫描完成的时间为：" + time_scan_item2);
      System.out.println(
          "---------------------------------------------item result.size:"
              + result.size()
              + "-------------------------------------");
      for (Kvrpcpb.KvPair item : result) {
        System.out.println(item.getKey().toStringUtf8() + '\t' + item.getValue().toStringUtf8());
      }

      String time_scan_click1 = sdm.format(new Date());
      System.out.println("click表开始全表扫描的时间为：" + time_scan_click1);

      result = (List<Kvrpcpb.KvPair>) storageService.scanData(Constants.TABLE_CLICK, new ArrayList<>(),null,null);

      String time_scan_click2 = sdm.format(new Date());
      System.out.println("click表扫描完成的时间为：" + time_scan_click2);
      System.out.println(
          "---------------------------------------------click result.size:"
              + result.size()
              + "-------------------------------------");
      for (Kvrpcpb.KvPair click : result) {
        System.out.println(click.getKey().toStringUtf8() + '\t' + click.getValue().toStringUtf8());
      }
    } else if (args[0].equals("deleteAll")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      storageService.deleteDataByTableName(Constants.TABLE_USER);
      storageService.deleteDataByTableName(Constants.TABLE_ITEM);
      storageService.deleteDataByTableName(Constants.TABLE_CLICK);
      //storageService.deleteIndexByTableName(Constants.TABLE_USER);
      //storageService.deleteIndexByTableName(Constants.TABLE_ITEM);
      //storageService.deleteIndexByTableName(Constants.TABLE_CLICK);
    } else if (args[0].equals("getDataByKey")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      Map<String,String> result = storageService.getDataByKey(Constants.TABLE_USER,"45");
      for (Map.Entry<String, String> entry : result.entrySet()) {
        System.out.println("key-value from getDataByKey is: "+entry.getKey()+'\t'+entry.getValue());
      }
    }else if (args[0].equals("generateSample")) {
      TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
      List<String> itemIds = new ArrayList<>();
      for (int i=1;i <= 20;i++){
        itemIds.add(String.valueOf(i));
      }
      storageService.generateSample("45",itemIds);
    }else if (args[0].equals("RecallFromTiKV")) {
      RecallFromTiKV recallFromTiKV = new RecallFromTiKV();
      recallFromTiKV.recall_new("45",null);
    }
  }
}
