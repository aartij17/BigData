import java.io.IOException;
import java.net.URI;

import java.text.ParseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;


import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.math3.util.Pair;
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

public class PollutionMR {
    static String cont = "";
    static Integer ozone=0;
    static Integer pm=1;
    static Integer cm=2;
    static Integer sd=3;
    static Integer nd=4;
    static Integer TIMESTAMP=7;
    static Integer lat=5;
    static Integer lon=6;
    //static LocalDate sstart=LocalDate.parse("2014-08-01T00:00:00Z",DateTimeFormatter.ISO_INSTANT);//(2014, Month.AUGUST, 1);
    //static LocalDate end=LocalDate.parse("2014-08-31T23:59:59Z",DateTimeFormatter.ISO_INSTANT);
    static String[] pattern=new String[]{"yyyy-MM-dd HH:mm:ss"};
    static Date start;
    static Date end;
    static ArrayList<Pair<Integer,Integer>> results=new ArrayList();

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            try {
                if(values[TIMESTAMP].equals("timestamp"))
                    return;
                Date inputDate = DateUtils.parseDate(values[TIMESTAMP],pattern);
                if(inputDate.after(start) && inputDate.before(end))
                {
                    int pollution = getPollution(values);
                    String filename=context.getInputSplit().toString();
                    String reportId=filename.split(".csv")[0].split("pollutionData")[1];
                    context.write(new IntWritable(Integer.parseInt(reportId)),new IntWritable(pollution));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

            /*if(date.after())
            values[vehicleCount];*/

        }

        private int getPollution(String[] values) {
            int sum=0;
            for(int i=ozone+1;i<=nd;i++)
            {
                sum+=Integer.parseInt(values[i]);
            }
            return sum/Integer.parseInt(values[ozone]);
        }
    }

    static int count=0;
    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable(-1);
        private  IntWritable resultKey= new IntWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                //count+=1;
                sum += val.get();
            }

            Pair<Integer,Integer> record=new Pair<Integer, Integer>(key.get(),sum);
            results.add(record);
        }
    }
    public static class IntSumCombiner
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                //count+=1;
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }

    }
    private static final String BASE_URI="hdfs://localhost:9000/";
    public static void main(String[] args) throws Exception {
        start=DateUtils.parseDate("2014-08-01 00:00:00",pattern);
        end=DateUtils.parseDate("2014-08-31 23:59:59",pattern);

        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pollutionMR");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
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
        Collections.sort(results, new Comparator<Pair<Integer,
                        Integer>>() {
            public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                return o2.getSecond()-o1.getSecond();
            }

        });
        for(Pair record : results)
        {
            System.out.println(record.getFirst()+"__"+record.getSecond());
        }
    }
}