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

        while (isheader(a[0]) != true)
        {
            Year  =  a[0];
            break;
        }
     }

    public boolean isheader(String b ) {
        if (b.equals("Year")) {
            return true;
        }
        else {
            return false;
        }
    }

    public String getYear(){
         return this.Year;
    }



}
