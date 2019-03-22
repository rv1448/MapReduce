package com.AirlineData.Utils;

import org.apache.hadoop.io.Text;

import java.io.*;

public class AirlineDataParse {

    private static String Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier;
    private static String FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin;
    private static String Dest, Distance, TaxiIn, TaxiOut, Cancelled;
    private static String CancellationCode, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay;


    public static String getYear() {
        return Year;
    }

    public static String getMonth() {
        return Month;
    }

    public static String getDayofMonth() {
        return DayofMonth;
    }

    public static String getDayOfWeek() {
        return DayOfWeek;
    }

    public static String getDepTime() {
        return DepTime;
    }

    public static String getCRSDepTime() {
        return CRSDepTime;
    }

    public static String getArrTime() {
        return ArrTime;
    }

    public static String getCRSArrTime() {
        return CRSArrTime;
    }

    public static String getUniqueCarrier() {
        return UniqueCarrier;
    }

    public static String getFlightNum() {
        return FlightNum;
    }

    public static String getTailNum() {
        return TailNum;
    }

    public static String getActualElapsedTime() {
        return ActualElapsedTime;
    }

    public static String getCRSElapsedTime() {
        return CRSElapsedTime;
    }

    public static String getAirTime() {
        return AirTime;
    }

    public static String getArrDelay() {
        return ArrDelay;
    }

    public static String getDepDelay() {
        return DepDelay;
    }

    public static String getOrigin() {
        return Origin;
    }

    public static String getDest() {
        return Dest;
    }

    public static String getDistance() {
        return Distance;
    }

    public static String getTaxiIn() {
        return TaxiIn;
    }

    public static String getTaxiOut() {
        return TaxiOut;
    }

    public static String getCancelled() {
        return Cancelled;
    }

    public static String getCancellationCode() {
        return CancellationCode;
    }

    public static String getDiverted() {
        return Diverted;
    }

    public static String getCarrierDelay() {
        return CarrierDelay;
    }

    public static String getWeatherDelay() {
        return WeatherDelay;
    }

    public static String getNASDelay() {
        return NASDelay;
    }

    public static String getSecurityDelay() {
        return SecurityDelay;
    }

    public static String getLateAircraftDelay() {
        return LateAircraftDelay;
    }


    public static boolean isheader(Text record){
        return record.toString().split(",")[0].equalsIgnoreCase("Year");
    }

    public static boolean isheader(String record){
        return record.split(",")[0].equalsIgnoreCase("Year");
    }

    public static void Assignvalues(String record){


      String[] arr =   record.split(",");

        Year = arr[0];
        Month = arr[1];
        DayofMonth = arr[2];
        DayOfWeek = arr[3];
        DepTime = arr[4];

        CRSDepTime = arr[5];
        ArrTime = arr[6];
        CRSArrTime = arr[7];
        UniqueCarrier = arr[8];
        FlightNum = arr[9];
        TailNum = arr[10];
        ActualElapsedTime = arr[11];
        CRSElapsedTime = arr[12];
        AirTime = arr[13];

        ArrDelay = arr[14];
        DepDelay = arr[15];
        Origin = arr[16];

        Dest = arr[17];
        Distance = arr[18];
        TaxiIn = arr[19];
        TaxiOut = arr[20];
        Cancelled = arr[21];
        CancellationCode = arr[21];
        Diverted = arr[23];
        CarrierDelay = arr[24];
        WeatherDelay = arr[25];
        NASDelay = arr[26];
        SecurityDelay = arr[27];
        LateAircraftDelay = arr[28];
}


    public static void main(String[] args) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("1990_test.csv");
        InputStreamReader x = new InputStreamReader(is,"UTF-8");

        BufferedReader y = new BufferedReader(x);


        for(String p; (p = y.readLine()) != null;)
        {
           if(!isheader(p)){
//               System.out.print(p);
            AirlineDataParse.Assignvalues(p);
               System.out.println(AirlineDataParse.getDayofMonth());
           }


//            System.out.println(p);
//            if(!isheader(p)){
//            //isheader(p);
//
//                try{
//
//                }
//
//                catch (Exception e){
//                    System.out.println("this cannot be printed out");
//                }
//
//            }

    }
    }
}
