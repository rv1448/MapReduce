package Misc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class Inverted_index extends Configured implements Tool
{

    public static class Invertedmapper extends Mapper<LongWritable, Text, Text,Text>{

        @Override
        public void map( LongWritable key, Text value, Context context){

        }


    }

    public static class Invertedreducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text Key, Iterable<Text> value, Context context){

        }

    }


    public   int run(String[] args) throws Exception {
        if(args.length != 2){
            System.out.print("The Input doesn't provide the input and output path");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Inverted Mapreduce");

        job.setJobName("Inverted");
        job.setJarByClass(Inverted_index.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);


        return  job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int Exitcode = ToolRunner.run(new Inverted_index(), args);
        System.exit(Exitcode);
    }
}
