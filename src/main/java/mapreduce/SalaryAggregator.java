package mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import sun.plugin2.jvm.CircularByteBuffer;

public class SalaryAggregator {

public static class Salarymapper extends Mapper<LongWritable, Text,Text, IntWritable>{
    @Override
    public void map(LongWritable Key, Text value, Context context){



    }


}

public static class SalaryReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    public void reduce(Text Key, Iterable<IntWritable> value, Context context){  // Forgetting Iterable



    }

}



}
