import java.io.IOException;
import java.net.URI;

import java.text.ParseException;

import java.util.Date;


import org.apache.commons.lang.time.DateUtils;
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

public class TrafficMR {
    static String cont = "";
    static Integer status=0;
    static Integer avgMeasuredTime=1;
    static Integer avgSpeed=2;
    static Integer extID=3;
    static Integer medianMeasuredTime=4;
    static Integer TIMESTAMP=5;
    static Integer vehicleCount=6;
    static Integer _id=7;
    static Integer REPORT_ID=8;
    //static LocalDate sstart=LocalDate.parse("2014-08-01T00:00:00Z",DateTimeFormatter.ISO_INSTANT);//(2014, Month.AUGUST, 1);
    //static LocalDate end=LocalDate.parse("2014-08-31T23:59:59Z",DateTimeFormatter.ISO_INSTANT);
    static String[] pattern=new String[]{"yyyy-MM-dd-HH:mm:ss"};
    static Date start;
    static Date end;
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            /*if(!cont.equals((context.getInputSplit()).toString())) {
                //cont = context.toString();
                cont = ( context.getInputSplit()).toString();
                System.out.println(cont);
            }*/
            String[] values = value.toString().split(",");
            try {
                if(values[TIMESTAMP].equals("TIMESTAMP"))
                    return;

                Date inputDate = DateUtils.parseDate(values[TIMESTAMP].replace("T","-"),pattern);
                if(inputDate.after(start) && inputDate.before(end))
                {
                    context.write(new IntWritable(Integer.parseInt(values[REPORT_ID])),new IntWritable(Integer.parseInt(values[vehicleCount])));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

            /*if(date.after())
            values[vehicleCount];*/

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
            if(sum>result.get()) {
                result.set(sum);
                resultKey.set(key.get());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(resultKey, result);
            System.out.println(resultKey+"__"+result);
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
        start=DateUtils.parseDate("2014-08-01T00:00:00".replace("T","-"),pattern);
        end=DateUtils.parseDate("2014-08-31T23:59:59".replace("T","-"),pattern);
        Date date=new Date();
        long startTime=date.getTime();
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(Level.OFF);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "trafficMR");
        job.setJarByClass(TrafficMR.class);
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
        date=new Date();
        long endTime=date.getTime();
        System.out.println("Time taken : "+(endTime-startTime));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}