package Misc;

/*
Possible implemations include
    Displaying the file twice
    FSDatainputStream
    Append data to the existing file
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import java.io.InputStream;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;

public class Filesystemcat {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copy(in, System.out);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }
}