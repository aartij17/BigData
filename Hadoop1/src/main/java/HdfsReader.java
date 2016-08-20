/**
 * Created by kai on 15/8/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Class to read from an existing file.
 * Usage : [HDFSpath_of_file_to_read]
 */
public class HdfsReader extends Configured implements Tool{
    private static final String FS_PARAM_NAME = "fs.defaultFS";
    private static final String BASE_URI="hdfs://localhost:9000/";
    public int run(String[] args) throws Exception {
        if(args.length!=1)
        {
            System.out.println("Usage: [HDFSpath_of_file_to_read]");
        }
        //Setup URI of the file path (base + path given)
        URI uri= URI.create(BASE_URI+args[0]);
        Path outputPath = new Path(uri);
        Configuration conf = getConf();
        //Get a handle of the hadoop file system
        FileSystem fs =FileSystem.get(uri,conf);
        //Perform safety check to see if the path indeed is valid
        //If left out, it would produce exceptions for invalid paths
        if(fs.exists(outputPath))
        {
            System.out.println("Contents of the file " + args[0] +" : ");
            //Open a FSDataInputStream to read the contents
            FSDataInputStream dataInputStream=fs.open(outputPath);
            byte[] writeBuf = new byte[1024];
            int len;
            //Can use ReadUTF to read entire contents or can read some bytes at a time
            //Will still call read internally
            while ((len = dataInputStream.read(writeBuf)) != -1){
                System.out.println(new String(writeBuf, 0, len, "UTF-8"));
            }
            //Finish what we started
            dataInputStream.close();
            fs.close();
            return 0;
        }
        else
        {
            //Path entered is invalid
            System.out.println( args[0] +" : Path does not exist");
            fs.close();
            return 1;
        }

    }

    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new HdfsReader(), args);
        System.exit(returnCode);
    }
}



