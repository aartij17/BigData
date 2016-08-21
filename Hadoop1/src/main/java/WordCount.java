import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class WordCount {
    static String cont = "";
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(!cont.equals(context.toString())) {
                cont = context.toString();
                System.out.println(cont);
            }
            StringTokenizer itr = new StringTokenizer(value.toString().replace("\n",""));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    static int count=0;
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                count+=1;
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static class IntSumCombiner
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    private static final String BASE_URI="hdfs://localhost:9000/";
    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        URI in_uri= URI.create(BASE_URI+args[0]);
        URI out_uri= URI.create(BASE_URI+args[1]);
        FileInputFormat.addInputPath(job, new Path(in_uri));
        Path outPath=new Path(out_uri);
        FileSystem fs=FileSystem.get(out_uri,conf);
        if(fs.exists(outPath))
        {
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);
        job.waitForCompletion(true);
        System.out.println(count);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}