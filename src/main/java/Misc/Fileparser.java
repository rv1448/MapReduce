package Misc;

public class Fileparser {


     Fileparser(String row)
     {

        String[] a = row.split(",");

        while (isheader(a[0]) != true)
        {

        }
     }

    public boolean isheader(String b ) {
        if (b == "YEAR") {
            return true;
        }
        else {
            return false;
        }
    }



}
