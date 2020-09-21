package cn.edu.neu.tiger.tikv;

import cn.edu.neu.tiger.tools.Constants;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import tikv.com.google.protobuf.ByteString;

public class ByteStringTest {
    public static void main(String[] args){
        ByteString str1 = ByteString.copyFromUtf8(
                String.format("%s#%s#%s", Constants.TABLE_USER + ",", "r" + ",", "100"));
        System.out.println("the size of ByteString "+str1+" is: "+str1.size());
        System.out.println("the size of "+str1.toString()+" is: "+str1.toString().length());
    }
}
