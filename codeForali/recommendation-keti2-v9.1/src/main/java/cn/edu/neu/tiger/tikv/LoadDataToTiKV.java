package cn.edu.neu.tiger.tikv;

import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import org.tikv.kvproto.Kvrpcpb;

import java.util.List;

public class LoadDataToTiKV {

    public static void main(String[] args) throws Exception {
        TiKVStorageService storageService = new TikvServiceImpl(Constants.PD_ADDRESS);
	    String tableName = args[0];
	    storageService.loadToTiKVFromHBase(tableName,null);	

    }

}
