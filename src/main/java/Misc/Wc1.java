package Misc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class Wc1 extends Configured implements Tool {

    public static class mappper1 extends Mapper<LongWritable, Text,Text, IntWritable>{
        Text word = new Text();
        String[] arr;
        @Override
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
//            for (String s : arr = value.toString().split(" ")) {
//                word.set(s);
//                context.write(new Text(s),new IntWritable(1));
//            }

            StringTokenizer stringtoken = new StringTokenizer(value.toString()," ");
            while (stringtoken.hasMoreTokens()){
               String k =  stringtoken.nextToken().trim().toLowerCase();
                context.write(new Text(k), new IntWritable(1));
            }

        }
    }

    public static class reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        public void reduce(final Text m, final Iterable<IntWritable> n, Context context) throws IOException, InterruptedException {
            IntWritable result = new IntWritable(0);
            int sum = 0;
            for(IntWritable i: n){
               sum += i.get();
            }
           result.set(sum);
            context.write(m, result);
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 2){
            return -1;
        }

        Job job = Job.getInstance(getConf(),"word");
        job.setJarByClass(getClass());

        job.setMapperClass(mappper1.class);
//        job.setReducerClass(reducer1.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        job.setNumReduceTasks(1);
        int a = job.waitForCompletion(true) ? 0 : 1 ;

        return a;
    }


    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new Wc1(),args);
        System.exit(exitcode);
    }

}
