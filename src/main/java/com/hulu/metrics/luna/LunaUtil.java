package com.hulu.metrics.luna;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class LunaUtil {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://elshadoopnn001.prod.hulu.com:8020");
        FileSystem fs = FileSystem.get(conf);
        if (args.length > 0) {
            String localFile = args[0];
            System.out.println(localFile);
        }
    }
}
