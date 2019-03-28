package com.AirlineData;
import com.AirlineData.Utils.AirlineDataParse;
import org.apache.hadoop.conf.Configuration;
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
import java.text.DecimalFormat;

public class AggregationMRJob extends Configured implements Tool {
    public static final IntWritable RECORD=new IntWritable(0);
    public static final IntWritable ARRIVAL_DELAY=new IntWritable(1);
    public static final IntWritable ARRIVAL_ON_TIME=new IntWritable(2);
    public static final IntWritable DEPARTURE_DELAY=new IntWritable(3);
    public static final IntWritable DEPARTURE_ON_TIME=new IntWritable(4);
    public static final IntWritable IS_CANCELLED=new IntWritable(5);
    public static final IntWritable IS_DIVERTED=new IntWritable(6);

    public static class AggregationMRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{



        StringBuilder string = new StringBuilder();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (AirlineDataParse.isheader(value.toString())) {
            return;
            }

            AirlineDataParse.Assignvalues(value.toString());
            String Month = AirlineDataParse.getMonth();
            int arrivaldelay = AirlineDataParse.parseMinutes(AirlineDataParse.getArrDelay(),0);
            int departuredelay = AirlineDataParse.parseMinutes(AirlineDataParse.getDepDelay(),0);
            boolean iscancelled =  AirlineDataParse.parseBoolean(AirlineDataParse.getCancelled(),false);
            boolean isdiverted = AirlineDataParse.parseBoolean(AirlineDataParse.getDiverted(),false);

        context.write(new Text(Month), RECORD);

       if(arrivaldelay > 0){
           context.write(new Text(Month), ARRIVAL_DELAY);
       }
       else{
           context.write(new Text(Month), ARRIVAL_ON_TIME);
       }
       if(departuredelay > 0){
           context.write(new Text(Month), DEPARTURE_DELAY);
       }
       else {
           context.write(new Text(Month), DEPARTURE_ON_TIME);
       }
       if(iscancelled){
           context.write(new Text(Month), IS_CANCELLED);
       }
       if(isdiverted){
       context.write(new Text(Month), IS_DIVERTED);
       }


       }
    }


    public static class AggregationMRReducer extends Reducer<Text,IntWritable,NullWritable,Text>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double totalRecords = 0;
        double arrivalOnTime = 0;
        double arrivalDelays = 0;
        double departureOnTime = 0;
        double departureDelays = 0;
        double cancellations = 0;
        double diversions = 0;


            for(IntWritable v: values){
            if(v.equals(RECORD)){
                totalRecords++;
            }
            if(v.equals(ARRIVAL_ON_TIME)){
                arrivalOnTime++;
            }
            if(v.equals(ARRIVAL_DELAY)){
                arrivalDelays++;
            }
            if(v.equals(DEPARTURE_ON_TIME)){
                departureOnTime++;
            }
            if(v.equals(DEPARTURE_DELAY)){
                departureDelays++;
            }
            if(v.equals(IS_CANCELLED)){
                cancellations++;
            }
            if(v.equals(IS_DIVERTED)){
                diversions++;
            }
        }
        DecimalFormat df = new DecimalFormat( "0.0000" );
        //Prepare and produce output
        StringBuilder output = new StringBuilder(key.toString());
   output.append(",").append(totalRecords);
   output.append(",").append(df.format(arrivalOnTime/totalRecords));
   output.append(",").append(df.format(arrivalDelays/totalRecords));
   output.append(",").append(df.format(departureOnTime/totalRecords));
   output.append(",").append(df.format(departureDelays/totalRecords));
   output.append(",").append(df.format(cancellations/totalRecords));
   output.append(",").append(df.format(diversions/totalRecords));
      context.write(NullWritable.get(), new Text(output.toString()));
    }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length!= 2){
            System.out.println("Insufficient Arguements Supplied");
            System.exit(-1);
        }

        Job job = new Job(new Configuration());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(AggregationMRMapper.class);
        job.setReducerClass(AggregationMRReducer.class);
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new AggregationMRJob(),args);
        System.exit(exitcode);
    }
}
