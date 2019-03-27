package com.AirlineData;

import com.AirlineData.Utils.AirlineDataParse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WhereclauseMR extends Configured implements Tool {

    public static class Whereclausemapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private int delayminutes;

        @Override
        public void setup(Context context) {
            this.delayminutes = context.getConfiguration().getInt("map.where.delay", 1);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int iArrdel = 0;
            int IDepdel = 0;

            StringBuilder string = new StringBuilder();
            if (!AirlineDataParse.isheader(value.toString())) {
                AirlineDataParse.Assignvalues(value.toString());
                String sArrdel = AirlineDataParse.getArrDelay();
                String sDepdel = AirlineDataParse.getDepDelay();
                iArrdel = AirlineDataParse.parseMinutes(sArrdel, 0);
                IDepdel = AirlineDataParse.parseMinutes(sDepdel, 0);

                string.append(AirlineDataParse.stringconcatenate());

                if (iArrdel > this.delayminutes && IDepdel > this.delayminutes) {
                    string.append(",B");
                    context.write(NullWritable.get(), new Text(string.toString()));
                } else if (iArrdel > this.delayminutes) {
                    string.append(",A");
                    context.write(NullWritable.get(), new Text(string.toString()));
                } else if (IDepdel > this.delayminutes) {
                    string.append(",D");
                    context.write(NullWritable.get(), new Text(string.toString()));
                }

            }


//            if (AirlineDataParse.isheader(value.toString())) {
//                return;
//            }
//
//            AirlineDataParse.Assignvalues(value.toString());
//            String sArrdel = AirlineDataParse.getArrDelay();
//            String sDepdel = AirlineDataParse.getDepDelay();
//            iArrdel = AirlineDataParse.parseMinutes(sArrdel, 0);
//            IDepdel = AirlineDataParse.parseMinutes(sDepdel, 0);
//
//            string.append(AirlineDataParse.stringconcatenate());
//
//            if (iArrdel > this.delayminutes && IDepdel > this.delayminutes) {
//                string.append(",B");
//                context.write(NullWritable.get(), new Text(string.toString()));
//            } else if (iArrdel > this.delayminutes) {
//                string.append(",A");
//                context.write(NullWritable.get(), new Text(string.toString()));
//            } else if (IDepdel > this.delayminutes) {
//                string.append(",D");
//                context.write(NullWritable.get(), new Text(string.toString()));
//            }



        }

    }


    public int run(String[] args) throws Exception {
        if(args.length!= 4){
            System.out.println("Insufficient Arguements Supplied");
            System.exit(-1);
        }

        Job job = new Job(new Configuration());
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(Whereclausemapper.class);
        Path input = new Path(args[2]);
        Path output = new Path(args[3]);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new WhereclauseMR(),args);
        System.exit(exitcode);
    }
}