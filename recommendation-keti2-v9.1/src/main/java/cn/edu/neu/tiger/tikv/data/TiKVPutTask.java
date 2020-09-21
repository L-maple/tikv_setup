package cn.edu.neu.tiger.tikv.data;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
//import com.google.protobuf.ByteString;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import tikv.com.google.protobuf.ByteString;

public class TiKVPutTask implements Runnable{
    static TiKVStorageService storageService; // 创建Tikv服务;

    static {
        try {
            storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<String, Map<String, String>> kvs = new HashMap<>();
    Map<ByteString,ByteString> index_flag = new HashMap<>();
    Map<ByteString,ByteString> index_date = new HashMap<>();
//    RawKVClient client;
//    TiSession session;

    public TiKVPutTask(Map<String, Map<String, String>> kvs) throws IOException {
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
                // 调用writeData插入表数据
                storageService.writeData(entry.getKey(), entry.getValue());
            }
        }catch (Exception e){
            System.out.println(" insert has an exception in TiKV thread!!!");
        }
//        this.client.close();
//        this.session.close();
    }
}
