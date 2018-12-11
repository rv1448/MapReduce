package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class MapreduceToolrunner extends Configured implements Tool  {

        // Generic option parser
        // Tool
        // Tool Runner
        // Generic option parser

        // implementation of mapper
        public class mapper extends Mapper<IntWritable, Text,Text, IntWritable>

        {
        @Override
            public void map(IntWritable key, Text value, Context context) throws  IOException, InterruptedException{

                String a =  value.toString();
                StringTokenizer b = new StringTokenizer(a," ");
                while(b.hasMoreTokens()){
                    Text d = new Text(b.nextToken());
                    context.write(d,new IntWritable(1));
                }
            }
        }

        // implementation of reducer
        public class reducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override

            public void reduce(Text z, Iterable<IntWritable> t, Context context) throws  IOException, InterruptedException{
                int sum = 0;
                Iterator<IntWritable> k = t.iterator();
                while(k.hasNext()){
                    sum +=k.next().get();
                    context.write(z,new IntWritable(sum));
                }
            }
        }

        @Override



    }


