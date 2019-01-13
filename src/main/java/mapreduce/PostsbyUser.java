package mapreduce;

// ** this is to caliculate total number of posts by each user

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class PostsbyUser extends Configured implements Tool {

    public static class DataMapper extends  Mapper<LongWritable, Text,Text, IntWritable>{

    @Override
    public void map(IntWritable key, Text value, Context context){

    }


    }


    public static class DataReducer extends Reducer<Text, IntWritable,Text, IntWritable>{


    }




    public int run(String[] args){
        return  -1;
    }

}
