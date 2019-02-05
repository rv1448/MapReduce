package mapreduce;


public class Fileparser1 {
    private String Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier;
    private String FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin;
    private String Dest, Distance, TaxiIn, TaxiOut, Cancelled;
    private String CancellationCode, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay;
    private String[] row1;

    public String[] splitrow(String row1) {
        this.row1 = row1.split(",");
        return this.row1;
    }

    public boolean isheader(String[] row1) {
        return row1[0].equals("Year");
    }


    public StringBuilder commaseparated(String[] arr) {
        StringBuilder comma = new StringBuilder(" ");
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

        comma.append(Year).append(",");
        comma.append(DayofMonth).append(",");
        comma.append(DayOfWeek).append(",");
        comma.append(DepTime).append(",");

        comma.append(CRSDepTime).append(",");
        comma.append(ArrTime).append(",");
        comma.append(CRSArrTime).append(",");
        comma.append(UniqueCarrier).append(",");
        comma.append(FlightNum).append(",");
        comma.append(TailNum).append(",");
        comma.append(ActualElapsedTime).append(",");
        comma.append(CRSElapsedTime).append(",");
        comma.append(AirTime).append(",");

        comma.append(ArrDelay).append(",");
        comma.append(DepDelay).append(",");
        comma.append(Origin).append(",");
        comma.append(Dest).append(",");
        comma.append(Distance).append(",");
        comma.append(TaxiIn).append(",");
        comma.append(TaxiOut).append(",");

        comma.append(Cancelled).append(",");
        comma.append(CancellationCode).append(",");
        comma.append(Diverted).append(",");
        comma.append(CarrierDelay).append(",");
        comma.append(WeatherDelay).append(",");

        comma.append(NASDelay).append(",");
        comma.append(SecurityDelay).append(",");
        comma.append(LateAircraftDelay);


        return comma;
    }
}
