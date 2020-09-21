package cn.edu.neu.tiger.tikv;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import org.tikv.kvproto.Kvrpcpb;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReadClickByUserID {

    public static void main(String[] args) throws Exception {
        //传入userid
        String user_id;
        TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);

        Long time;
        //int i = Integer.parseInt(args[0]);

        int size;
        Random random = new Random();
        ExecutorService exec = Executors.newFixedThreadPool(200);
        //get user
//        while (true){
//            size=0;
//            user_id = String.valueOf(random.nextInt(316952));
//            //调用延迟
//            time = System.currentTimeMillis();
//
//            Map<String, String>  result = storageService.getDataByKey(Constants.TABLE_USER,user_id);
//            System.out.println("外：get user cost :" + String.valueOf(System.currentTimeMillis() - time) +"ms");
//
//            //1个kv对的大小
//            for(Map.Entry<String, String> entry : result.entrySet()){
//                String mapKey = entry.getKey();
//                String mapValue = entry.getValue();
//                System.out.println("key in user table is: "+mapKey);
//                System.out.println("value in user table is: "+mapValue);
//                size+=entry.getKey().length()+entry.getValue().length();
//            }
//            System.out.println("the length of key-value in user table is: "+size);
//            System.out.println("**************************************");
//        }
        //scan click

        while (true) {
            user_id = String.valueOf(99999);
            //user_id = String.valueOf(random.nextInt(3169520));
            //延迟
            time = System.currentTimeMillis();
            List<Kvrpcpb.KvPair> results = storageService.scanClickByUserID(Constants.TABLE_CLICK, user_id);
            System.out.println("外：scan click cost :" + String.valueOf(System.currentTimeMillis() - time) +"ms");

            //kv对的条数
            System.out.println("numbers of kv-pairs is: "+results.size());

            //一个kv对的大小
            for(Kvrpcpb.KvPair re : results){
                System.out.println("key: "+re.getKey()+"    key(toString): "+re.getKey().toString()+"   key(toStringUtf8):" +
                        re.getKey().toStringUtf8()+"    key-length: "+re.getKey().size()+"  key-length(toString): "+
                        re.getKey().toString().length()+"   key-length(toStringUtf8): "+re.getKey().toStringUtf8().length());
                System.out.println("value: "+re.getValue()+"value(toString): "+re.getValue().toString()+"   value(toStringUtf8): "+
                        re.getValue().toStringUtf8()+"    value-length: "+re.getValue().size()+ " value-length(toString):"+
                        re.getValue().toString().length()+"     value-length(toStringUtf8): "+re.getValue().toStringUtf8().length());
                System.out.println("size of kv-paris is: "+(re.getKey().toString().length()+re.getValue().toString().length())+"B");
            }
            //i++;
            System.out.println("**************************************");
            //Thread.sleep(1000);
        }

    }

}
