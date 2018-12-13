package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import java.util.regex.Matcher;
import java.io.IOException;

public class WeblogAnalysisv1 extends Configured implements Tool {
    public static String APACHE_ACCESS_LOGS_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+) (.+?) \"([^\"]+|(.+?))\"";

    public static Pattern pattern = Pattern.compile(APACHE_ACCESS_LOGS_PATTERN);

    private static final IntWritable one = new IntWritable(1);

     public class WeblogAnalysismapper extends Mapper<LongWritable, Text,Text,IntWritable>{

            private Text url = new Text();


         public void map(Object key, Text value, Context context)
                 throws IOException, InterruptedException {
             Matcher matcher = pattern.matcher(value.toString());
             if (matcher.matches()) {
                 // Group 6 as we want only Page URL
                 url.set(matcher.group(6));
                 System.out.println(url.toString());
                 context.write(this.url, new IntWritable(1));
             }

         }
     }

    public class WeblogAnalysisreducer extends Reducer<Text,IntWritable, Text,Text>{

         @Override
         public void reduce(Text a, Iterable<IntWritable> b, Context context){

         }

     }

    public int run(String[] args) throws IOException, InterruptedException,ClassNotFoundException{
         if(args.length!=2){
             System.err.print("pass two parameters seperated by space");
             return -1;
         }

         Configuration conf = new Configuration();
         Job job = new Job(conf, "Weblog analyzer");

         job.setJarByClass(WeblogAnalysisv1.class);
         job.setMapperClass(WeblogAnalysismapper.class);
         job.setReducerClass(WeblogAnalysisreducer.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true)?0:1;
    }


    public static void main(String[] args) throws  Exception{
         int exitcode = ToolRunner.run(new WeblogAnalysisv1(),args);
         System.exit(exitcode);
    }
}
