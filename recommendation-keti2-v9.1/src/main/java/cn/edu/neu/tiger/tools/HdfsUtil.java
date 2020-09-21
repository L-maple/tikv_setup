package cn.edu.neu.tiger.tools;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class HdfsUtil {

    public static void writeHDFS(String data, String date) throws IOException {
        Configuration configuration = createConf();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream outputStream = null;
        ByteArrayInputStream stringInputStream = null;

        try {
            Path path = new Path("/user/qiqi.zp/i2i.txt");
            outputStream = fileSystem.create(path);
            stringInputStream = new ByteArrayInputStream(data.getBytes());
            IOUtils.copyBytes(stringInputStream, outputStream, 4096, false);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (stringInputStream != null) {
                IOUtils.closeStream(stringInputStream);
            }
            if (outputStream != null) {
                IOUtils.closeStream(outputStream);
            }
        }
    }

    public static Map<String, Map<String, Object>> readHDFS(String date) throws IOException {
        Configuration configuration = createConf();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fsDataInputStream = null;
        OutputStream outputStream = new ByteArrayOutputStream();
        System.out.println("begin read form hdfs");
        try {
            Path path = new Path("/user/qiqi.zp/i2i.txt");
            System.out.println("read path " + path.toString());
            fsDataInputStream = fileSystem.open(path);
            IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, false);
        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (fsDataInputStream != null) {
                IOUtils.closeStream(fsDataInputStream);
            }
            outputStream.flush();
            String json = outputStream.toString();
            outputStream.close();
            Map<String, Object> map = JSONObject.parseObject(json);
            System.out.println("map size:" + map.size());
            Map<String, Map<String, Object>> i2i = new HashMap<>();
            for (String id : map.keySet()) {
                String value = map.get(id).toString();
                i2i.put(id, JSONObject.parseObject(value));
            }
            return i2i;
        }
    }

    public static Configuration createConf() {
        Configuration configuration = new Configuration();
        String confDir = System.getenv("HADOOP_CONF_DIR");
        System.out.println(confDir);
        configuration.addResource(new Path(confDir + "/core-site.xml"));
        configuration.addResource(new Path(confDir + "/hdfs-site.xml"));
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return configuration;
    }
}
