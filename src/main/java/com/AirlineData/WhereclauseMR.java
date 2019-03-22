package com.AirlineData;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class WhereclauseMR extends Configured implements Tool {

     class Whereclausemapper extends Mapper<LongWritable, Text, NullWritable,Text>{



    }


    class Whereclausereducer extends Reducer<NullWritable,Text,NullWritable,Text> {

    }






    public int run(String[] strings) throws Exception {
        return 0;
    }
}
