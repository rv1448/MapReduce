package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;



//We will then import all the libraries org.apache.hadoop Libraries

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;

public class WordCountGenericParser {

    public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

        }

        Map<String, String> parsed = MRDPUtils.transforXmlToMap(value.toString());+
        String txt = parsed.get("Text");

        if(txt ==null)

        {
            // Skip this row
            return;
        }

        txt =StringEscapeUtils.unescapeHtml(txt.LowerCase());
        txt =txt.replaceAll("'",""); // remove single quotes (e.g., can't)
        txt =txt.replaceAll("[^a-zA-Z]"," "); // replace the rest with a space
        StringTokenizer itr = new StringTokenizer(txt);
        while(itr.hasMoreTokens())

        {
            word.set(itr.nextToken());
            context.write(word, one);

        }
    }

    public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); // Brings the job configuration from the XML files
        String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherargs.length!=2){
            System.err.println("Usage:: CommentWordCount <in> <out> ");
            System.exit(2);

        }

        Job job = new Job(conf,"Stackoverflow Comment Word Count");
        job.setJarByClass(WordCountGenericParser.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(otherargs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

}