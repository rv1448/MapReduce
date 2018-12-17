package mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public  class SalaryAggregator extends Configured implements Tool {

public  static class Salarymapper extends Mapper<LongWritable, Text,Text, IntWritable>{
     public IntWritable salary;
     public String creditcardtype;

    public  String[] k;
    public Text cardtype;


    @Override
    public void map(LongWritable Key, Text value, Context context) throws  IOException, InterruptedException{
        k = value.toString().split("|");
        for(String z: k){
            System.out.print(z);
        }

        creditcardtype = k[3];
        salary.set(Integer.parseInt(k[2]));
        //int salary = Integer.parseInt(k[2]);
        cardtype.set(k[3]);
      context.write(cardtype , salary);


    }


}

public static class SalaryReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws  IOException, InterruptedException
    {  // Forgetting Iterable
    int sum =0;

     for( IntWritable a : value){
         sum += a.get();
         context.write(key,new IntWritable(sum));
     }




    }

}


    public int run(String[] args) throws IOException,InterruptedException, ClassNotFoundException {
    if(args.length!= 2){
        System.err.println("please pass all the parameters needed");

        return -1;
    }

    Configuration conf = new Configuration();
    Job job = new Job(conf,"Salary Aggregator");

    job.setJarByClass(SalaryAggregator.class);


    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Salarymapper.class);
    job.setReducerClass(SalaryReducer.class);

    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    FileInputFormat.addInputPath(job,input);
    FileOutputFormat.setOutputPath(job,output);

    return job.waitForCompletion(true)? 0:1;



}

public static   void main(String[] args) throws Exception{
    int exitcode = ToolRunner.run(new SalaryAggregator(),args);

    System.exit(exitcode);
}
}
