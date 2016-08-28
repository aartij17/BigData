import java.io.IOException;
import java.net.URI;

import java.text.ParseException;

import java.util.Arrays;
import java.util.Date;


import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class PageRankMR {

    static double[] curr,prev;
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, DoubleWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String[] split = value.toString().split(" ");
            double sum=0;
            for(int i=1; i<split.length;i++)
            {
                sum += Double.parseDouble(split[i])*prev[i-1];
            }

            context.write(new IntWritable(Integer.parseInt(split[0])),new DoubleWritable(sum));
        }
    }


    public static class IntSumReducer
            extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {

        public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            curr[key.get()] = values.iterator().next().get();
            System.out.println(curr);
            //System.out.println(Arrays.toString(curr));
            context.write(key,new DoubleWritable(curr[key.get()]));
        }
    }

    static Job multiply() throws IOException {
        String args[]={"data/matrices","data/outputs"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankMR");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
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
        return job;
    }
    static boolean copy= false;
    static void computeConvergence() throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("-----");
        System.out.println(Arrays.toString(curr));
        System.out.println(Arrays.toString(prev));
        double convergence = 0;
        for(int i=0;i<prev.length;i++)
            convergence += Math.abs(prev[i]-curr[i]);

        if(convergence<Math.pow(10,-12))
        {
            System.out.println(Arrays.toString(curr));
        }
        else
        {
            if(copy)
                for(int i=0;i<prev.length;i++)
                    prev[i]=curr[i];
            copy=true;
            Job job=multiply();
            job.waitForCompletion(true);
            computeConvergence();
        }

    }
    private static final String BASE_URI="hdfs://localhost:9000/";
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        curr = new double[4];
        prev = new double[4];
        prev[0]/*=prev[1]=prev[2]=prev[3]*/=1.0;
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        computeConvergence();
    }
}
