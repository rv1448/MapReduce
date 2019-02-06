package mapreduce;


public class Fileparser1 {
    private static  String Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier;
    private static String FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin;
    private static String Dest, Distance, TaxiIn, TaxiOut, Cancelled;
    private static  String CancellationCode, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay;
    private static String[] row1;

    public static  String[] splitrow(String rowt) {
         row1 = rowt.split(",");
        return row1;
    }

    public static boolean isheader(String[] row1) {
        return row1[0].equals("Year");
    }


    public static String getYear() {
        return Year;
    }

    public static void setYear(String year) {
        Year = row1[0];
    }

    public static String getMonth() {
        return Month;
    }

    public static void setMonth(String month) {
        Month =  row1[1];
    }

    public static String getDayofMonth() {
        return DayofMonth;
    }

    public static void setDayofMonth(String dayofMonth) {
        DayofMonth = row1[2];
    }

    public static String getDayOfWeek() {
        return DayOfWeek;
    }

    public static void setDayOfWeek(String dayOfWeek) {
        DayOfWeek = row1[3];
    }

    public static String getDepTime() {
        return DepTime;
    }

    public static void setDepTime(String depTime) {
        DepTime = row1[4];
    }


    public static String getCRSDepTime() {
        return CRSDepTime;
    }

    public static void setCRSDepTime(String CRSDepTime) {
        CRSDepTime = row1[5];
    }

    public static String getArrTime() {
        return ArrTime;
    }

    public static void setArrTime(String arrTime) {
        ArrTime = row1[6];
    }

    public static String getCRSArrTime() {
        return CRSArrTime;
    }

    public static void setCRSArrTime(String CRSArrTime) {
        Fileparser1.CRSArrTime = row1[7];
    }

    public static String getUniqueCarrier() {
        return UniqueCarrier;
    }

    public static void setUniqueCarrier(String uniqueCarrier) {
        UniqueCarrier = row1[8];
    }

    public static String getFlightNum() {
        return FlightNum;
    }

    public static void setFlightNum(String flightNum) {
        FlightNum = row1[9];
    }

    public static String getTailNum() {
        return TailNum;
    }

    public static void setTailNum(String tailNum) {
        TailNum = row1[10];
    }

    public static String getActualElapsedTime() {
        return ActualElapsedTime;
    }

    public static void setActualElapsedTime(String actualElapsedTime) {
        ActualElapsedTime = row1[11];
    }

    public static String getCRSElapsedTime() {
        return CRSElapsedTime;
    }

    public static void setCRSElapsedTime(String CRSElapsedTime) {
        Fileparser1.CRSElapsedTime = row1[12];
    }

    public static String getAirTime() {
        return AirTime;
    }

    public static void setAirTime(String airTime) {
        AirTime = row1[13];
    }

    public static String getArrDelay() {
        return ArrDelay;
    }

    public static void setArrDelay(String arrDelay) {
        ArrDelay = row1[14];
    }

    public static String getDepDelay() {
        return DepDelay;
    }

    public static void setDepDelay(String depDelay) {
        DepDelay = row1[15];
    }

    public static String getOrigin() {
        return Origin;
    }

    public static void setOrigin(String origin) {
        Origin = row1[16];
    }

    public static String getDest() {
        return Dest;
    }

    public static void setDest(String dest) {
        Dest = row1[17];
    }

    public static String getDistance() {
        return Distance;
    }

    public static void setDistance(String distance) {
        Distance = row1[18];
    }

    public static String getTaxiIn() {
        return TaxiIn;
    }

    public static void setTaxiIn(String taxiIn) {
        TaxiIn = row1[19];
    }

    public static String getTaxiOut() {
        return TaxiOut;
    }

    public static void setTaxiOut(String taxiOut) {
        TaxiOut = row1[20];
    }

    public static String getCancelled() {
        return Cancelled;
    }

    public static void setCancelled(String cancelled) {
        Cancelled = row1[21];
    }

    public static String getCancellationCode() {
        return CancellationCode;
    }

    public static void setCancellationCode(String cancellationCode) {
        CancellationCode = row1[22];
    }

    public static String getDiverted() {
        return Diverted;
    }

    public static void setDiverted(String diverted) {
        Diverted = row1[23];
    }

    public static String getCarrierDelay() {
        return CarrierDelay;
    }

    public static void setCarrierDelay(String carrierDelay) {
        CarrierDelay = row1[24];
    }

    public static String getWeatherDelay() {
        return WeatherDelay;
    }

    public static void setWeatherDelay(String weatherDelay) {
        WeatherDelay = row1[25];
    }

    public static String getNASDelay() {
        return NASDelay;
    }

    public static void setNASDelay(String NASDelay) {
        Fileparser1.NASDelay = row1[26];
    }

    public static String getSecurityDelay() {
        return SecurityDelay;
    }

    public static void setSecurityDelay(String securityDelay) {
        SecurityDelay = row1[27];
    }

    public static String getLateAircraftDelay() {
        return LateAircraftDelay;
    }

    public static void setLateAircraftDelay(String lateAircraftDelay) {
        LateAircraftDelay = row1[28];
    }

    public static StringBuilder commaseparated(String[] arr) {
        StringBuilder comma = new StringBuilder(" ");
//        Year = arr[0];
//        Month = arr[1];
//        DayofMonth = arr[2];
//        DayOfWeek = arr[3];
//        DepTime = arr[4];
//
//        CRSDepTime = arr[5];
//        ArrTime = arr[6];
//        CRSArrTime = arr[7];
//        UniqueCarrier = arr[8];
//        FlightNum = arr[9];
//        TailNum = arr[10];
//        ActualElapsedTime = arr[11];
//        CRSElapsedTime = arr[12];
//        AirTime = arr[13];
//
//        ArrDelay = arr[14];
//        DepDelay = arr[15];
//        Origin = arr[16];
//
//        Dest = arr[17];
//        Distance = arr[18];
//        TaxiIn = arr[19];
//        TaxiOut = arr[20];
//        Cancelled = arr[21];
//        CancellationCode = arr[21];
//        Diverted = arr[23];
//        CarrierDelay = arr[24];
//        WeatherDelay = arr[25];
//        NASDelay = arr[26];
//        SecurityDelay = arr[27];
//        LateAircraftDelay = arr[28];

        comma.append(getYear()).append(",");
        comma.append(getDayofMonth()).append(",");
        comma.append(getDayOfWeek()).append(",");
        comma.append(getDepTime()).append(",");

        comma.append(getCRSDepTime()).append(",");
        comma.append(getArrTime()).append(",");
        comma.append(getCRSArrTime()).append(",");
        comma.append(getUniqueCarrier()).append(",");
        comma.append(getFlightNum()).append(",");
        comma.append(getTailNum()).append(",");
        comma.append(getActualElapsedTime()).append(",");
        comma.append(getCRSElapsedTime()).append(",");
        comma.append(getAirTime()).append(",");

        comma.append(getArrDelay()).append(",");
        comma.append(getDepDelay()).append(",");
        comma.append(getOrigin()).append(",");
        comma.append(getDest()).append(",");
        comma.append(getDistance()).append(",");
        comma.append(getTaxiIn()).append(",");
        comma.append(getTaxiOut()).append(",");

        comma.append(getCancelled()).append(",");
        comma.append(getCancellationCode()).append(",");
        comma.append(getDiverted()).append(",");
        comma.append(getCarrierDelay()).append(",");
        comma.append(getWeatherDelay()).append(",");

        comma.append(getNASDelay()).append(",");
        comma.append(getSecurityDelay()).append(",");
        comma.append(getLateAircraftDelay());


        return comma;
    }




}
