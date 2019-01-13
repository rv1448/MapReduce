package mapreduce;

// ** this is to caliculate total number of posts by each user

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PostsbyUser extends Configured implements Tool {

    public static class DataMapper extends  Mapper<LongWritable, Text,Text, IntWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           IntWritable count = new IntWritable(1);
           Text word = new Text();
           String a = value.toString();

             String[] b = a.split(",");

//             for(String x : b){
//                 System.out.println(b[1]);
//             }

            if(b.length == 0){
                System.out.print(b);
            }

            if(b.length != 0 ){
//                word = b[1];

//                try{
//                    word.set(b[2]);
//                    System.out.println( word+"True rows");
//                }
//                 catch (ArrayIndexOutOfBoundsException e){
//                    System.out.println(word+"False rows");
//                 }
////                word = "abc";
                word.set(b[1]);
                context.write(new Text(word), count);
            }

        }
    }


    public static class DataReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
        @Override
        public void reduce(Text a, Iterable<IntWritable> b, Context context) throws IOException, InterruptedException{
            int sum = 0 ;
            for(IntWritable x : b){
                sum += x.get();

            }
            context.write(a,new IntWritable(sum));
        }

    }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 2){
            System.err.println("In valid Command");
            System.err.println("Usage: <input path> <output path>");
            return -1;
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf,"Wordcount with Tool interface");
        job.setJarByClass(PostsbyUser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        Path inputpath = new Path(args[0]);
        Path outputpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputpath);
        FileInputFormat.addInputPath(job, inputpath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new PostsbyUser(), args);
        System.exit(exitcode);
    }
}
