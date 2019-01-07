package Misc;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class Inverted_index extends Configured implements Tool
{

    public static class Invertedmapper extends Mapper<LongWritable, Text, Text,Text>{

        private Text word = new Text();
        private Text filename = new Text();

        @Override
        public void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit currentSplit = (FileSplit)context.getInputSplit();
            String filenamestr = currentSplit.getPath().getName();
            filename = new Text(filenamestr);

            String line = value.toString();

            StringTokenizer stringsplit = new StringTokenizer(line);

            while(stringsplit.hasMoreTokens()){
                word.set(stringsplit.nextToken());
                context.write(word,filename);
            }


        }


    }

    public static class Invertedreducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text Key, Iterable<Text> value, Context context) throws IOException, InterruptedException{

            StringBuilder stringbuilder = new StringBuilder();

            for (Text val: value){

                stringbuilder.append(val.toString());

                if(value.iterator().hasNext()){
                    stringbuilder.append("|");
                }

            }

            context.write(Key, new Text(stringbuilder.toString()));
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
//        job.setInputFormatClass(TextFormatter.class);

        job.setMapperClass(Invertedmapper.class);
        job.setReducerClass(Invertedreducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
