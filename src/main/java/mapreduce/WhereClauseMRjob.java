///////////////////////////////////////////////////////////////////////////
// Filter data dynamically with the parameters supplied to the job MR JOB
// If there is delay at origin append O if the delay at destination then Append D, If there is delay
//-at both Origin and destination then append B

// Note: Every Mapper has a setup to initialize the task level resources for the mapper
//- and Capture the parameters details for Mapper and reducer examples

// Parameters:
// Airlinedata Airlineout –D map.where.delay=10
///////////////////////////////////////////////////////////////////////////


package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WhereClauseMRjob extends Configured implements Tool {

    private static int delayminute = 0;

    public void setup(Mapper.Context context) throws IOException{
        delayminute = context.getConfiguration().getInt("map.where.delay",1);
    }

    public static class Airlinedatamapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String h;
            h = value.toString();


            try {
                String[] k = AirlineUtilParse.splitrow(h);
                if (AirlineUtilParse.isheader(k) != true) {
                    String stringdepdel = AirlineUtilParse.getDepDelay();
                    String stringarrdel = AirlineUtilParse.getArrDelay();

                    int depdel = AirlineUtilParse.parseMinutes(stringdepdel,0);
                    int arrdel = AirlineUtilParse.parseMinutes(stringarrdel,0);


                    StringBuilder m = AirlineUtilParse.commaseparated(k);

                    if(depdel >= delayminute && arrdel >= delayminute){
                        m.append(',');
                        m.append("B");
                        context.write(new Text(m.toString()), new IntWritable(1));
                    }

                    else if (depdel >= delayminute){
                        m.append(',');
                        m.append("D");
                        context.write(new Text(m.toString()), new IntWritable(1));
                    }


                    else if (arrdel >= delayminute){
                        m.append(',');
                        m.append("O");
                        context.write(new Text(m.toString()), new IntWritable(1));
                    }


                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public int run(String[] allArgs) throws IOException, InterruptedException, ClassNotFoundException {
        if (allArgs.length != 4) {
            System.err.printf("Invalid parameters supplied");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Just the Mapper");
        job.setJarByClass(WhereClauseMRjob.class);
        job.setMapperClass(Airlinedatamapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

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
        int exitcode = ToolRunner.run(new WhereClauseMRjob(), args);
        System.exit(exitcode);
    }
}
