package mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class Mapreducetoolrunner extends Configured implements Tool  {

        // Generic option parser
        // Tool
        // Tool Runner
        // Generic option parser

        // implementation of mapper
        public static class Mappertool extends Mapper<LongWritable, Text,Text, IntWritable>

        {
        @Override
            public void map(LongWritable key, Text value, Context context) throws  IOException, InterruptedException{

                String a =  value.toString();
                StringTokenizer b = new StringTokenizer(a," ");
                while(b.hasMoreTokens()){
                    Text d = new Text(b.nextToken());
                    context.write(d,new IntWritable(1));
                }
            }
        }

        // implementation of reducer
        public static class Reducertool extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override

            public void reduce(Text z, Iterable<IntWritable> t, Context context) throws  IOException, InterruptedException{
                int sum = 0;
                Iterator<IntWritable> k = t.iterator();
                while(k.hasNext()){
                    sum +=k.next().get();
                    context.write(z,new IntWritable(sum));
                }
            }
        }


        public int run(String[] args) throws  IOException, InterruptedException,ClassNotFoundException{
           if(args.length != 2){
               System.err.println("In valid Command");
               System.err.println("Usage: <input path> <output path>");
               return -1;
           }
            Configuration conf = new Configuration();
            Job job = new Job(conf,"Wordcount with Tool interface");
            job.setJarByClass(Mapreducetoolrunner.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(Mappertool.class);
            job.setReducerClass(Reducertool.class);
            Path inputpath = new Path(args[0]);
            Path outputpath = new Path(args[1]);
            FileOutputFormat.setOutputPath(job, outputpath);
            FileInputFormat.addInputPath(job, inputpath);
            return job.waitForCompletion(true) ? 0 : 1;
        }

        public static void main(String[] args) throws Exception{
            int exitcode = ToolRunner.run(new Mapreducetoolrunner(), args);
            System.exit(exitcode);
        }



    }


