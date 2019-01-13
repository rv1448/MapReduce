package Misc;

import java.io.*;

public class File_reader {

    public static void main(String[] args) throws IOException {
        InputStream is = new FileInputStream("/Users/RahulReddy/Documents/Hadoop/Input/QueryResults.csv");
        BufferedReader a = new BufferedReader(new InputStreamReader(is));
        String word;

        String line = a.readLine();
        String line2 = a.readLine();



        while (a.readLine() != null){
            String[] b = line2.split(",");
            word = b[0];
            System.out.println(word);
        }


    }


}
