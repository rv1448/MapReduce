package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class AirlinedataBZ2 extends Configured implements Tool {


    public static class Airlinedatamapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            Text val = new Text();
//            String f = new String();
            String h;
            h = value.toString();

//            try {
//                Fileparser f = new Fileparser(h);
//
//                System.out.print(f.printrow());
//                String s = new String(f.printrow());
//                System.out.println(s);
//
//                context.write( new Text(s),new IntWritable(1) );
//            }
//            catch (Exception e){
//                e.printStackTrace();
//            }

          try {
                Fileparser1 f = new Fileparser1();

                String[] k = f.splitrow(h);
                if (f.isheader(k) != true) {
                    StringBuilder m = f.commaseparated(k);
                    context.write(new Text(m.toString()), new IntWritable(1));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public int run(String[] allArgs) throws IOException, InterruptedException, ClassNotFoundException {
        if (allArgs.length != 2) {
            System.err.printf("Invalid parameters supplied");
            return -1;
        }


        Configuration conf = new Configuration();
        Job job = new Job(conf, "Just the Mapper");
        job.setJarByClass(AirlinedataBZ2.class);
        job.setMapperClass(Airlinedatamapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);

        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return job.waitForCompletion(true) ? 0 : 1;


    }


    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new AirlinedataBZ2(), args);
        System.exit(exitcode);
    }
}
