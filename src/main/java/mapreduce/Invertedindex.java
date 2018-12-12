package mapreduce;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class Invertedindex extends Configured implements Tool {

public static class Invertedindexmapper extends Mapper<LongWritable, Text,Text,Text>{
    private Text filename = new Text();
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws  IOException, InterruptedException{
        FileSplit currentSplit = (FileSplit) context.getInputSplit();
        String filenamestr = currentSplit.getPath().getName();

        String line = value.toString();

        StringTokenizer e = new StringTokenizer(line," ");
        while (e.hasMoreTokens()){
            word.set(e.nextToken());
            context.write(word, new Text(filenamestr));
        }
    }
}


public static class Invertedindexreducer extends Reducer<Text,Text, Text, Text>{

    @Override
    public void reduce(Text c, Iterable<Text> p, Context context) throws IOException, InterruptedException{
        StringBuilder h = new StringBuilder();

        Iterator<Text> q = p.iterator();

        for(Text value : p){
            h.append(value.toString());
            if(p.iterator().hasNext()){
                h.append("|");
            }
            context.write(c,new Text(h.toString()));
        }
    }
}



    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    if(args.length != 2){
        System.err.println("Invalid arguments supplied");
        System.err.println("Please re-supply the correct arguments");
        return  -1;
    }

    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

    Job job = new Job(conf,"Inverted index");
    job.getConfiguration().set("mapreduce.output.textoutputformat.seperator","|");
    job.setJarByClass(Invertedindex.class);

    job.setMapperClass(Invertedindexmapper.class);
    job.setReducerClass(Invertedindexreducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    Path inputpath = new Path(args[0]);
    Path outputpath = new Path(args[1]);
    FileInputFormat.addInputPath(job,inputpath);
    FileOutputFormat.setOutputPath(job,outputpath);
        //FileInputFormat.setInputDirRecursive();
        return job.waitForCompletion(true)? 0: 1;
    }
    public static void main(String[] args) throws  Exception{
    int exitcode = ToolRunner.run(new Invertedindex(), args); // remember this
        System.exit(exitcode);
    }
}
