package mapreduce;


public  class Fileparser {
   static String Year, DayofMonth,DayOfWeek,DepTime,b;
    static StringBuilder comma = new StringBuilder("");


   Fileparser(String row)
     {


        String[] a = row.split(",");

        if (isheader(a[0]) != true)
     {
//         Year  =  a[0];
//         DayofMonth = a[1];
         //break;
         commaseparated(a);
     }


     }

    public static boolean isheader(String b ) {
         return b.equals("Year");

    }

    public static String getYear(){
         return Year;
    }

    public static void commaseparated( String[] arr){

        Year = arr[0];
        DayofMonth = arr[1];
        DayOfWeek = arr[2];
        DepTime =arr[3];


        comma.append(Year).append(",");
        comma.append(DayofMonth).append(",");
        comma.append(DayOfWeek).append(",");
        comma.append(DepTime);

    }


    public static String printrow(){

        return comma.toString();

    }



}
