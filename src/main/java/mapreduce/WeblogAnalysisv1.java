package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class WeblogAnalysisv1 extends Configured implements Tool {


     public class WeblogAnalysismapper extends Mapper<LongWritable, Text,Text,Text>{

         @Override
         public  void map(LongWritable key, Text value, Context context ){


         }
     }


     public class WeblogAnalysisreducer extends Reducer<Text,Text, Text,Text>{
         @Override
         public void reduce(Text a, Iterable<Text> b, Context context){

         }

     }

    public int run(String[] args) throws IOException{
         Configuration conf = new Configuration();
         Job job = new Job(conf, "Weblog analyzer");

         job.setJarByClass(WeblogAnalysisv1.class);


         return -1;
    }
}
