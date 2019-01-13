package mapreduce;

// ** this is to caliculate total number of posts by each user

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class PostsbyUser extends Configured implements Tool {

    public static class DataMapper extends  Mapper<LongWritable, Text,Text, IntWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           IntWritable count = new IntWritable(1);

           String[] a = value.toString().split(",");
           Text word = new Text(a[1]);
           context.write(word, count);
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

    public int run(String[] args){
        return  -1;
    }

}
