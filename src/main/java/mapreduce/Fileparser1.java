package mapreduce;


public  class Fileparser1 {
     String Year, DayofMonth,DayOfWeek,DepTime;
     String[] row1;

     public String[] splitrow(String row1){
       this.row1 = row1.split(",");
       return this.row1;
     }

    public   boolean isheader(String[] row1 ) {  return row1.equals("Year");}


    public  StringBuilder   commaseparated( String[] arr){
        StringBuilder comma  = new StringBuilder(" ");
        Year = arr[0];
        DayofMonth = arr[1];
        DayOfWeek = arr[2];
        DepTime =arr[3];

        comma.append(Year).append(",");
        comma.append(DayofMonth).append(",");
        comma.append(DayOfWeek).append(",");
        comma.append(DepTime);
        return  comma;
    }
}
