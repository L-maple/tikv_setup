package cn.edu.neu.tiger;
import cn.edu.neu.tiger.tikv.i2i.RecallFromTiKV;
import cn.edu.neu.tiger.tikv.impl.TikvServiceImpl;
import cn.edu.neu.tiger.tikv.service.TiKVStorageService;
import cn.edu.neu.tiger.tools.Constants;
import cn.edu.neu.tiger.tools.Util;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Arrays;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TikvPipeline{
    private static final Logger logger = LoggerFactory.getLogger(RecPipelineWithPrometheus.class);
    private transient RecallFromTiKV recallFromTiKV;

    private transient Map<String, Map<String, Object>> i2i;
    private TiKVStorageService tikvStorageService;
    private final int threadSize = 1;
    //private final int predictThreadSize = 10;
    private static AtomicLong count = new AtomicLong(0); //统计处理完成的userid个数

    public static void main(String[] args) throws Exception {
	ExecutorService pool = Executors.newFixedThreadPool(10);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Util.getKafkaProPerties());
        consumer.subscribe(Arrays.asList(Constants.KAFKA_SOURCE_TOPIC));
        TikvPipeline tikvPipeline = new TikvPipeline();
        Random random = new Random();
        tikvPipeline.i2i = new HashMap<>();
        //logger.info("begin load i2i data");
        System.out.println("begin load i2i data");
        for (int i = 0;i < 10;i++){
            Map<String, Object> i2i_value = new HashMap<>();
            tikvPipeline.i2i.put(String.valueOf(i),i2i_value);
            for(int j = 0;j<20;j++){
                i2i_value.put(String.valueOf(random.nextInt(200)),random.nextInt(20));
            }
        }
        System.out.println("i2i size:"+tikvPipeline.i2i.size());
        //logger.info("i2i size: {}", tikvPipeline.i2i.size());

        tikvPipeline.recallFromTiKV = new RecallFromTiKV(); // 创建Tikv recall服务
        tikvPipeline.tikvStorageService = new TikvServiceImpl(Constants.PD_ADDRESS);
        //int count=0;
        //Long startPipelineTime = System.currentTimeMillis();
        //Date d = new Date();
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //System.out.println("****************start pipeline at ：" + sdf.format(d)+"**********************");
        //ConsumerRecords<String, String> records;
	Calendar cal =  Calendar.getInstance();
        Date d = cal.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS");
        System.out.println("****************start pipeline at ：" + sdf.format(d)+"**********************");
	while (true) {
            //count = 0;
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            /*count += records.count();
            System.out.println("start processing "+count+" userids");
            Long startTime = System.currentTimeMillis();*/
            for (ConsumerRecord<String, String> record : records){
                //resultFromTiKV = tikvPipeline.recallFromTiKV.recall_new(userid, tikvPipeline.i2i);
                //Row row = tikvPipeline.tikvStorageService.generateSample(resultFromTiKV.f0, resultFromTiKV.f1);
                //count.getAndIncrement();
                pool.execute(new WriteThread(record.value(),tikvPipeline));
		//create 10 threads to execute
                /*WriteThread[] writeThreads = new WriteThread[tikvPipeline.threadSize];
                for (int i = 0; i < tikvPipeline.threadSize; i++) {
                    writeThreads[i] = new WriteThread(record.value(),tikvPipeline);
                    writeThreads[i].start();
                }*/
                /*//Recall
                RecallThread[] recallThreads = new RecallThread[tikvPipeline.recallThreadSize];
                for (int i = 0; i < tikvPipeline.recallThreadSize; i++) {
                    recallThreads[i] = new RecallThread(record.value(),tikvPipeline);
                    recallThreads[i].start();
                }
                //Tuple2<String, List<String>> resultFromTiKV = tikvPipeline.recallFromTiKV.recall_new(record.value(), tikvPipeline.i2i);
                //Predict
                PredictThread[] predictThreads = new PredictThread[tikvPipeline.predictThreadSize];
                for (int i = 0; i < tikvPipeline.predictThreadSize; i++) {
                    predictThreads[i] = new PredictThread(record.value(),tikvPipeline);
                    predictThreads[i].start();
                }
                //Row row = tikvPipeline.tikvStorageService.generateSample(resultFromTiKV.f0, resultFromTiKV.f1);*/
            }
	    cal =  Calendar.getInstance();
            d = cal.getTime();
            System.out.println("****************count is " +count + " at time:"+sdf.format(d)+"**********************");
        }
    }
    public static class WriteThread extends Thread{
        private String userid;
        private TikvPipeline tikvPipeline;
        //private Tuple2<String, List<String>> resultFromTiKV;
	//private Row row;

        public WriteThread(String uid,TikvPipeline tikvPipeline){
            userid = uid;
            this.tikvPipeline = tikvPipeline;
        }

        public void run(){
            try {
                //Long recallTime = System.currentTimeMillis();
                Tuple2<String, List<String>> resultFromTiKV = tikvPipeline.recallFromTiKV.recall_new(userid, tikvPipeline.i2i);
                //System.out.println("recall one time cost:"+(System.currentTimeMillis() - recallTime));
                //Long predictTime = System.currentTimeMillis();
                tikvPipeline.tikvStorageService.generateSample(resultFromTiKV.f0, resultFromTiKV.f1);
                //System.out.println("predict one time cost:"+(System.currentTimeMillis() - predictTime));
                count.getAndIncrement();

            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
/*    public static class RecallThread extends Thread{
        private String userid;
        private Tuple2<String, List<String>> resultFromTiKV;
        private TikvPipeline tikvPipeline;
        public RecallThread(String uid,TikvPipeline tikvPipeline){
            userid = uid;
            this.tikvPipeline = tikvPipeline;
        }

        public void run(){
            try {
                resultFromTiKV = tikvPipeline.recallFromTiKV.recall_new(userid, tikvPipeline.i2i);
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        public Tuple2<String, List<String>> getResultFromTiKV() {
            return resultFromTiKV;
        }
    }

    public static class PredictThread extends Thread{
        private Tuple2<String, List<String>> resultFromTiKV;
        private TikvPipeline tikvPipeline;
        public PredictThread(Tuple2<String, List<String>> resultFromTiKV,TikvPipeline tikvPipeline){
            this.resultFromTiKV = resultFromTiKV;
            this.tikvPipeline = tikvPipeline;
        }

        public void run(){
            try {
                Row row = tikvPipeline.tikvStorageService.generateSample(resultFromTiKV.f0, resultFromTiKV.f1);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }*/
}
