package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class AirlinedataBZ2 extends Configured implements Tool {


    public static class Airlinedatamapper extends Mapper<LongWritable, Text, Text,Text>{

        @Override
        public void map(LongWritable key, Text value, Context context){



        }


    }



    public int run(String[] args){

        return -1;
    }



}
