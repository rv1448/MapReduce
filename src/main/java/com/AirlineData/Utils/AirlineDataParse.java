package com.AirlineData.Utils;

import org.apache.hadoop.io.Text;

import java.io.*;

public class AirlineDataParse {

    public static boolean isheader(Text record){
        return record.toString().split(",")[0].equalsIgnoreCase("Year");
    }

    public static boolean isheader(String record){
        return record.toString().split(",")[0].equalsIgnoreCase("Year");
    }




    public static void main(String[] args) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("1990_test.csv");
        InputStreamReader x = new InputStreamReader(is,"UTF-8");

        BufferedReader y = new BufferedReader(x);


        for(String p; (p = y.readLine()) != null;)
        {
            System.out.println(p);
            if(!isheader(p)){
            //isheader(p);

                try{

                }

                catch (Exception e){
                    System.out.println("this cannot be printed out");
                }

            }

    }}
}
