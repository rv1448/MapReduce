package mapreduce;


public class Fileparser {
String Year;

     Fileparser(String row)
     {
//         String
//         DayofMonth,
//         DayOfWeek,
//         DepTime,
//         CRSDepTime,
//         ArrTime,
//         CRSArrTime,
//         UniqueCarrier,
//         FlightNum,
//         TailNum,
//         ActualElapsedTime,
//         CRSElapsedTime,
//         AirTime,
//         ArrDelay,
//         DepDelay,
//         Origin,
//         Dest,
//         Distance,
//         TaxiIn,
//         TaxiOut,
//         Cancelled,
//         CancellationCode,
//         Diverted,
//         CarrierDelay,
//         WeatherDelay,
//         NASDelay,
//         SecurityDelay,
//         LateAircraftDelay;
        String[] a = row.split(",");

        if (!isheader(a[0]))
        {
            Year  =  a[0];
            //break;
        }
     }

    public boolean isheader(String b ) {
         return b.equals("Year");

    }

    public String getYear(){
         return this.Year;
    }



}
