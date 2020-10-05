package cn.edu.neu.tiger.tools;

public class Constants {

	//kafka
	public final static String KAFKA_SOURCE_TOPIC = "userclick";
	public final static String KAFKA_BOOTSTRAP = "bootstrap.servers";
	public final static String KAFKA_BOOTSTRAP_VALUE = "172.17.175.126:9092";
	//	public final static String KAFKA_BOOTSTRAP_VALUE = "11.227.70.150:9092,11.251.155.243:9092,11.251.155.142:9092";
	public final static String KAFKA_BATCH_SIZE = "batch.size";
	public final static String KAFKA_BUFFER_MEMORY = "buffer.memory";
	public final static String KAFKA_KEY_SERIALIZER = "key.serializer";
	public final static String KAFKA_VALUE_SERIALIZER = "value.serializer";
	public final static String KAFKA_RETRIES = "retries";
	public final static String KAFKA_LINGER_MS = "linger.ms";
	public final static String KAFKA_ACKS = "acks";
	public static final String ZK_CONNECT = "zookeeper.connect";
	public final static String ZOOKEEPER_QUORUM_VALUE = "172.17.175.126";//11.227.70.150
	public final static String ZOOKEEPER_CLIENT_PORT_VALUE = "2081";
//	public final static String ZOOKEEPER_CLIENT="172.21.0.15:2181,172.21.0.14:2181,172.21.0.7:2181,172.21.0.4:2181,172.21.0.5:2181,172.21.0.9:2181,172.21.0.10:2181";
	//sls
//	public final static String SLS_ENDPOITN = "http://cn-shanghai-corp.sls.aliyuncs.com";
	public final static String SLS_ENDPOITN = "http://cn-shanghai-intranet.log.aliyun-inc.com";
	public final static String SLS_ACCESSKEYID = "LTAI4FqQPQVAZTh5c7BwCVEh";
	public final static String SLS_ACCESSKEYSECRET = "o2w9MUfLLsnaKWCnyrmzJXgDcxUYZU";
	public final static String SLS_PROJECT = "recommendation";
	public final static String SLS_LOGSTORE = "userclick";
	public final static Integer RECALL_NUM = 100;
	public final static int MAXNUM = 1000000;

	//HBase

	public final static String MODEL_CKPT = "model.ckpt-2";  //模型名称
	public final static String HBASE_FAMILY = "family";
	public final static String HBASE_PUTKEY = "putkey";
	public final static String HBASE_PUTVALUE = "putvalue";
	public final static String TABLE_CLICK = "Leo_click";   //click表
	public final static String TABLE_USER = "Leo_user";     //user表
	public final static String TABLE_ITEM = "Leo_item";     //item表
	public final static String TABLE_I2I = "Leo_i2i";       //i2i表
	public final static String TABLE_RESULT = "Leo_result"; //result表，存放推荐结果
	public final static String DIAMOND_ID = "hbase.diamond.dataid.Leo.hbase";
	public final static String DIAMOND_GROUP = "hbase-diamond-group-name-Leo";
	public final static String trainPy = "predictionR.py";
	public final static String envPath = "hdfs://11.227.70.150:9000/user/qiqi.zp/tfenv.zip";
	public final static String zkConnStr = "hadoop4012.et2:2181,hadoop4134.et2:2181,hadoop4190.et2:2181";
	public final static String codePath = "hdfs://11.227.70.150:9000/user/qiqi.zp/code.zip";
	public final static String CONFIG_ZOOKEEPER_BASE_PATH = "/qiqi.zp";

	//tikv parameters
	public static final String PD_ADDRESS = "tidb-cluster-pd.tidb-cluster.svc:2379";
	public static final String ROWID_MAX =
			"9999999999"; // in ByteString, 0 < 10 < 11 < 19 < 1 < 20 < 20 < 29. so 9 is the biggest
	public static final String USERID_MAX = "9999999999"; // Integer.MAX_VALUE = 2147483647
	public static final boolean INDEX_ON = false;
}
