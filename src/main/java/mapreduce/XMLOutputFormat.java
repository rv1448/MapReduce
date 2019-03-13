package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class XMLOutputFormat extends FileOutputFormat<LongWritable, Text> {
    protected static class XMLRecordWriter extends RecordWriter<LongWritable,Text>{

        private DataOutputStream out;
        public XMLRecordWriter(DataOutputStream out) throws IOException {
        this.out = out;
        out.writeBytes("<recs>\n");
        }

        private void writeTag(String tag,String value) throws IOException {
            out.writeBytes("<"+tag+">"+value+"</"+tag+">");
        }

        @Override
        public void write(LongWritable key, Text value) throws IOException {
                out.writeBytes("<rec>");
                this.writeTag("key",Long.toString(key.get()));
                 String[] contents = value.toString().split(",");
                 AirlineUtilParse.commaseparated(contents);
                    String year = AirlineUtilParse.getYear();
                    this.writeTag("year", year);
                    out.writeBytes("</rec>\n");
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException {
            try{
                out.writeBytes("</recs>\n");

            }finally {
                out.close();
            }

        }
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) {
//        return new XMLRecordWriter();
        return null;
    }
}
