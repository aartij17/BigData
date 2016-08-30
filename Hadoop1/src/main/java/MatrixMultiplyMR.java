import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;


/**
 * Usage:
 * Pass 3 args. Path of matrix A in HDFS , Path of Matrix B in HDFS , Path of matrix C in HDFS
 * Set the three variables in main to indicate order of matrix
 * A , B are inputs
 * Their format is :
 * row,col,value    (Value is a double)
 * Can skip giving cell if value is 0
 Input matrix for page Rank(A) :
 0,2,1
 0,3,0.5
 1,0,0.333333333
 2,0,0.333333333
 2,1,0.5
 2,3,0.5
 3,0,0.333333333
 3,1,0.5
 */

public class MatrixMultiplyMR {
    private static final String A_ROWS = "Arows";       // Number of rows in A. Set in main
    private static final String B_COLS = "Bcols";       //Number of columns in B. Set in main
    private static final String DIM = "dim";            //Common Dimension (colsA=rowsB). Set in main
    private static final String KEY_DELIM = ":";        //Reducer keys are delimited by this (row:col)
    private static final String INPUT_DELIM = ",";      //Delimiter for input file (a,0,0,1)
    private static final String A_MATRIX_NAME = "a";
    private static final String B_MATRIX_NAME = "b";
    private static final String BASE_URI = "hdfs://localhost:9000/";

    public static class AMapper
            extends Mapper<Object, Text, Text, Text> {
        Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            String content=A_MATRIX_NAME+INPUT_DELIM+line[0]+INPUT_DELIM+line[1]+INPUT_DELIM+line[2];
            value.set(content);
            int row = Integer.parseInt(line[0].trim());
            int dim = Integer.parseInt(context.getConfiguration().get(B_COLS));
            for (int i = 0; i < dim; i++) {
                word.set(row + KEY_DELIM + i);
                context.write(word, value);
            }
        }
    }

    public static class BMapper
            extends Mapper<Object, Text, Text, Text> {
        Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split(INPUT_DELIM);
            String content=B_MATRIX_NAME+INPUT_DELIM+line[0]+INPUT_DELIM+line[1]+INPUT_DELIM+line[2];
            value.set(content);
            int col = Integer.parseInt(line[1].trim());
            int dim = Integer.parseInt(context.getConfiguration().get(A_ROWS));
            for (int i = 0; i < dim; i++) {
                word.set(i + KEY_DELIM + col);
                context.write(word, value);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int dim = Integer.parseInt(context.getConfiguration().get(DIM));
            double[] as = new double[dim];
            double[] bs = new double[dim];
            for (Text value : values) {
                String[] c = value.toString().split(INPUT_DELIM);
                if (c[0].trim().equals(A_MATRIX_NAME)) {
                    as[Integer.parseInt(c[2].trim())] = Double.parseDouble(c[3].trim());
                } else if (c[0].trim().equals(B_MATRIX_NAME))
                    bs[Integer.parseInt(c[1].trim())] = Double.parseDouble(c[3].trim());
            }
            double total = 0;
            for (int i = 0; i < as.length; i++) {
                total += as[i] * bs[i];
            }
            String[] split = key.toString().split(KEY_DELIM);
            result.set(split[0] + INPUT_DELIM + split[1] + INPUT_DELIM + total);
            System.out.println(result.toString());
            context.write(NullWritable.get(), result);
        }
    }


    public static void main(String[] args) throws Exception {

        String aPath=BASE_URI+args[0];
        String bPath=BASE_URI+args[1];
        String cPath=BASE_URI+args[2];
        int aRows=4;
        int bCols=4;
        int common=4;
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        System.exit(multiplyMatrices(aPath, bPath, cPath, aRows, bCols, common)?0:1);

    }

    public static boolean multiplyMatrices(String inputPath, String inputPath2, String resultPath, int aRows, int bCols, int common) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(A_ROWS, "" + aRows);
        conf.set(B_COLS, "" + bCols);
        conf.set(DIM, "" + common);
        Job job = Job.getInstance(conf, "MatrixMultiplyMR");
        job.setJarByClass(MatrixMultiplyMR.class);
        URI in_uri1 = URI.create(inputPath);
        URI in_uri2 = URI.create(inputPath2);
        MultipleInputs.addInputPath(job, new Path(in_uri1), TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(job, new Path(in_uri2), TextInputFormat.class, BMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        String output = BASE_URI + "files/temp_"+job.getJobName();
        URI out_uri = URI.create(output);
        Path outPath = new Path(out_uri);
        FileSystem fs = FileSystem.get(out_uri, conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        if (job.waitForCompletion(true)) {
            Path dstFile = new Path(resultPath);
            if(fs.exists(dstFile)) {
                fs.delete(dstFile,true);
            }
            boolean success=FileUtil.copyMerge(fs,outPath,fs, dstFile,false,conf,null);
            fs.delete(outPath, true);
            return success;
        }else {
            System.err.println("Error. Please check logs");
            fs.delete(outPath, true);
            return false;
        }
    }

}