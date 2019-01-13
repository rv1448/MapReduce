package Misc;


public class XML_Parser {
    static  String xml = "<row Id=\"6939296\" PostTypeId=\"2\" ParentId=\"6939137\"\n" +
            "CreationDate=\"2011-08-04T09:50:25.043\" Score=\"4\" ViewCount=\"\"\n" +
            "Body=\"&lt;p&gt;You should have imported Poll with &lt;code&gt;\n" +
            "from polls.models import Poll&lt;/code&gt;&lt;/p&gt;&#xA;\"\n" +
            "OwnerUserId=\"634150\" LastActivityDate=\"2011-08-04T09:50:25.043\"\n" +
            "CommentCount=\"1\" />";

    public static void main(String[] args) {
//        StringTokenizer string = new StringTokenizer(xml,"\"");
//
//        while (string.hasMoreTokens()){
//            System.out.println(string.nextToken());
        String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
                .split("\"");

//        for (String x : tokens){
//            for(int i=0;i<2; i++) {
//                System.out.println(x);
//
//            }
//        }
//        for(int i=0; i <=xml.length(); )
        System.out.println(tokens[0]);

    }


}
