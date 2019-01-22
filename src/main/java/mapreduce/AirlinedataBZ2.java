package mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AirlinedataBZ2 extends Configured implements Tool {


    public static class Airlinedatamapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            Text val = new Text();
//            String f = new String();
            String h;
            h = value.toString();

            Fileparser f =  new Fileparser(h);
            val.set(f.getYear());

            context.write( new Text(val),new IntWritable(1) );

        }


    }



    public int run(String[] args)  throws  IOException, InterruptedException,ClassNotFoundException {
        if (args.length != 2) {
            System.err.printf("Invalid parameters supplied");
            return -1;
        }



        Configuration conf = new Configuration();
        Job job = new Job(conf,"Just the Mapper");

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        return job.waitForCompletion(true)?0 : 1;


    }



}
