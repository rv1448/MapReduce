package com.AirlineData;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SelectColumns extends Configured implements Tool {


    public static class SelectMapper extends Mapper<LongWritable, Text, Text,NullWritable>{

       public static boolean isHeader(Text a){
           return a.toString().split(",")[0].equalsIgnoreCase("year");
       }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           if(!isHeader(value)) {
               context.write(value,NullWritable.get());
           }
        }
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2){
            return -1;
        }

        Job job = Job.getInstance(getConf(),"SelectColumns");
        job.setJarByClass(getClass());

        job.setMapperClass(SelectMapper.class);
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        int a = job.waitForCompletion(true) ? 0 : 1 ;

        return a;
    }

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new SelectColumns(),args);
        System.exit(exitcode);
    }

}
