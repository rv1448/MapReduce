package com.AirlineData.Utils;

import org.apache.hadoop.io.Text;

import java.io.*;

public class AirlineDataParse {

    public static boolean isheader(Text record){
        return record.toString().split(",")[0].equalsIgnoreCase("Year");
    }



    public static void main(String[] args) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("1990_test.csv");
        InputStreamReader x = new InputStreamReader(is,"UTF-8");

        BufferedReader y = new BufferedReader(x);

        for(String p; (p = y.readLine()) != null;){
            System.out.println(p);
        }

//        while(y.readLine() != "0"){
//        System.out.println(y.readLine());
//        }

//        InputStream i = new InputStream.read
//        i = new InputStreamReader(new FileReader(""));
//        File file = new File();
//        BufferedReader i = new BufferedReader(FileReader())
    }
}
