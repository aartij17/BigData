/**
 * Created by kai on 15/8/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HdfsWriter extends Configured implements Tool{
    public static final String FS_PARAM_NAME = "fs.defaultFS";

    public int run(String[] args) throws Exception {

        URI uri= URI.create("hdfs://localhost:9000/"+args[0]);
        Path outputPath = new Path(uri);
        Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        FileSystem fs =FileSystem.get(uri,conf);
        OutputStream os = fs.create(outputPath);
        String content="Hello";

        InputStream is = new BufferedInputStream(new ByteArrayInputStream(content.getBytes("UTF-8")));
        IOUtils.copyBytes(is, os, conf);
        System.out.println("Written");
        return 0;
    }

    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsWriter(), args);
        System.exit(returnCode);
    }
}
