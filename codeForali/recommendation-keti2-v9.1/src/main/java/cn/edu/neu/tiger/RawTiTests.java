package cn.edu.neu.tiger;

import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.TikvUtil;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import tikv.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 普通kv对的增删查改操作示例
 **/
public class RawTiTests {

    public static void put(int rowid) throws Exception {
	//System.out.println("初始化pd服务");
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(Constants.PD_ADDRESS));
	//System.out.println("TiSession success!!!!!!");
        RawKVClient client = session.createRawClient();
	//System.out.println("RawKVClient create success!!!!!!");
        ByteString tkey = ByteString.copyFromUtf8(
                String.format("%s#%s#%s", "tablename" + ",", "r" + ",", String.valueOf(rowid)));
        ByteString tvalue = ByteString.copyFromUtf8(
                "hello");
        client.put(tkey,tvalue);
	//System.out.println("Put success!!!!!!");
        client.close();
        session.close();
    }
    public static void put() throws Exception {
	TiSession session = TiSession.create(TiConfiguration.createRawDefault(Constants.PD_ADDRESS));
	RawKVClient client = session.createRawClient();
	for(int i=0;i<10000;i++){
		ByteString tkey = ByteString.copyFromUtf8(String.format("%s#%s#%s", "tablename" + ",", "r" + ",", String.valueOf(i)));
        	ByteString tvalue = ByteString.copyFromUtf8("hello");
                client.put(tkey,tvalue);
        }
	client.close();
        session.close();
    }
    public static ByteString get(int rowid) throws Exception {
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(Constants.PD_ADDRESS));
        RawKVClient client = session.createRawClient();
        ByteString tkey = ByteString.copyFromUtf8(
                String.format("%s#%s#%s", "tablename" + ",", "r" + ",", String.valueOf(rowid)));
        ByteString tvalue = client.get(tkey);
        client.close();
        session.close();
        return tvalue;
    }
    public static List<Kvrpcpb.KvPair> scan(String tableName, ArrayList<Map<String, String>> filters) throws Exception {
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(Constants.PD_ADDRESS));
        RawKVClient client = session.createRawClient();
        List<Kvrpcpb.KvPair> result = new ArrayList<>();
        boolean isInited = false;
        if(filters.isEmpty()){
            ByteString tkey_min =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s", tableName + ",", "r" + ",", String.valueOf(0)));
            ByteString tkey_max =
                    ByteString.copyFromUtf8(
                            String.format("%s#%s#%s", tableName + ",", "r" + ",", Constants.ROWID_MAX));
            result = client.scan(tkey_min, tkey_max);
        }else{//遍历所有过滤条件
            for (Map<String, String> filterInfo : filters) {
                List<Kvrpcpb.KvPair> tmpResult = new ArrayList();
                String filterKey = filterInfo.get("filterKey");
                String filterValue = filterInfo.get("filterValue");
                if(filterKey.equals("rowid")){
                    ByteString targetKey =
                            ByteString.copyFromUtf8(
                                    String.format(
                                            "%s#%s#%s", tableName + ",", "r" + ",",filterValue));
                    tmpResult = client.scan(targetKey, 1);
                }
                // If this is the first filter, tmpResult should be put into result
                // else tmpResult should intersect with result
                if (!isInited) result.addAll(tmpResult);
                else result.retainAll(tmpResult);
            }
        }
        client.close();
        session.close();
        return result;
    }
    public static void delete(String tableName,int rowid) {
        TiSession session = TiSession.create(TiConfiguration.createRawDefault(Constants.PD_ADDRESS));
        RawKVClient client = session.createRawClient();
        if (client == null) {
            client = session.createRawClient();
        }
        ByteString rkey = ByteString.copyFromUtf8(String.format(
                "%s#%s#%s", tableName + ",", "r" + ",", rowid));
        ByteString rvalue = client.get(rkey);
        if(rvalue != null && !rvalue.isEmpty()){
            client.delete(ByteString.copyFromUtf8(String.format("%s#%s#%s", tableName + ",", "r" + ",", rowid)));
        }
        rvalue = client.get(rkey);
        if(rvalue == null || rvalue.isEmpty()){
            System.out.println("delete success!!!!");
        }
        client.close();
        session.close();
    }
    public static void main(String[] args) {
        try {
            System.out.println("开始插入10000条kv对");
            /*for(int i=0;i<1000;i++){
                put(i);
            }*/
	    put();
            System.out.println("全表扫描开始");
            List <Kvrpcpb.KvPair> kvPairs = scan("tablename",new ArrayList<>());
            for (Kvrpcpb.KvPair result : kvPairs) {
                ByteString tkey = result.getKey();
                ByteString tvalue = result.getValue();
                System.out.println("key is: "+tkey.toStringUtf8()+" ;value is: "+tvalue.toStringUtf8());
            }
	    /*
            System.out.println("指定rowid进行全表扫描");
            ArrayList<Map<String, String>> userFilterInfos = new ArrayList<>();
            userFilterInfos.add(TikvUtil.getFilterInfo("rowid", "1"));
            List <Kvrpcpb.KvPair> kvPairs2 = scan("tablename",userFilterInfos);
            for (Kvrpcpb.KvPair result : kvPairs2) {
                ByteString tkey = result.getKey();
                ByteString tvalue = result.getValue();
                System.out.println("key is: "+tkey.toStringUtf8()+" ;value is: "+tvalue.toStringUtf8());
            }

            System.out.println("指定rowid通过get进行查询");
            ByteString tvalue = get(1);
            System.out.println("rowid为1时，value值为："+tvalue.toStringUtf8());
            System.out.println("删除userid为1的kv对");
            delete("tablename",1);
            ByteString deleteResult = get(1);
	    */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
