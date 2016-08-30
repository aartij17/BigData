import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
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
/*
0,0,0.3870961562458898
1,0,0.12903204502584195
2,0,0.2903230188639708
3,0,0.19354877224489042
2.8184374138429114E-6

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
    private static final double THRESHOLD = 0.00001;


    public static class AMapper
            extends Mapper<Object, Text, Text, Text> {
        Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] line = value.toString().split(INPUT_DELIM);
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
        int bCols=1;
        int common=4;
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        System.exit(computeConvergence(aPath, bPath, cPath, aRows, bCols, common)?0:1);

    }

    public static class PositiveMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String writeKey = split[0] + KEY_DELIM + split[1];
            double writeValue = Double.parseDouble(split[2]);
            context.write(new Text(writeKey),new DoubleWritable(writeValue));
        }
    }
    public static class NegativeMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String writeKey = split[0] + KEY_DELIM + split[1];
            double writeValue = -1* Double.parseDouble(split[2]);
            context.write(new Text(writeKey),new DoubleWritable(writeValue));
        }
    }

    public static class DifferenceReducer
            extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum=0;
            for(DoubleWritable value:values)
            {
                sum+=value.get();
            }
            context.write(NullWritable.get(),new DoubleWritable(sum));
        }
    }

    public static boolean computeConvergence(String inputPath, String inputPath2, String resultPath,int aRows,int bCols,int common) throws InterruptedException, IOException, ClassNotFoundException {
        boolean status = multiplyMatrices(inputPath, inputPath2, resultPath, aRows, bCols, common);
        if(!status)
            return false;
        double difference = computeDifference(inputPath2,resultPath);
        System.out.println(difference);
        if(Math.abs(difference)<=THRESHOLD)
        {
             return true;
        }
        Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(URI.create(resultPath),conf);

        FileUtil.copy(fs,new Path(resultPath),fs,new Path(inputPath2),false,true,conf);
        return computeConvergence(inputPath,inputPath2,resultPath,aRows,bCols,common);
    }

    private static double computeDifference(String inputPath2, String resultPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf, "MatrixSubtractMR");
        job.setJarByClass(MatrixMultiplyMR.class);
        URI in_uri1 = URI.create(inputPath2);
        URI in_uri2 = URI.create(resultPath);
        MultipleInputs.addInputPath(job, new Path(in_uri1), TextInputFormat.class, PositiveMapper.class);
        MultipleInputs.addInputPath(job, new Path(in_uri2), TextInputFormat.class, NegativeMapper.class);
        job.setReducerClass(DifferenceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        String output = BASE_URI + "files/temp_"+job.getJobName();
        URI out_uri = URI.create(output);
        Path outPath = new Path(out_uri);
        FileSystem fs = FileSystem.get(out_uri, conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        if (job.waitForCompletion(true)) {
            return addElements(outPath);
        }else {
            System.err.println("Error. Please check logs");
            fs.delete(outPath, true);
            return 999;
        }
    }
    public static class AddElementMapper
            extends Mapper<Object, Text, NullWritable, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            double writeValue=Math.abs(Double.parseDouble(value.toString()));
            context.write(NullWritable.get(),new DoubleWritable(writeValue));
        }
    }
    public static class AddElementReducer
            extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
        public void reduce(NullWritable key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum=0;
            for(DoubleWritable value:values)
            {
                sum+=value.get();
            }
            context.write(NullWritable.get(),new DoubleWritable(sum));
        }
    }

    private static double addElements(Path inPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf, "AddElementsMR");
        job.setJarByClass(MatrixMultiplyMR.class);
        job.setMapperClass(AddElementMapper.class);
        job.setCombinerClass(AddElementReducer.class);
        job.setReducerClass(AddElementReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, inPath);
        String output = BASE_URI + "files/temp_"+job.getJobName();
        URI out_uri = URI.create(output);
        Path outPath = new Path(out_uri);
        FileSystem fs = FileSystem.get(out_uri, conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        if (job.waitForCompletion(true)) {
            String filename = BASE_URI + "files/temp_" + job.getJobName() + "/part-r-00000";
            if(fs.exists(new Path(filename)))
            {
                FSDataInputStream dataInputStream=fs.open(new Path(filename));
                byte[] writeBuf = new byte[1024];
                dataInputStream.read(writeBuf);
                double result=Double.parseDouble(new String(writeBuf));
                return result;
            }
        }
        return 999;
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