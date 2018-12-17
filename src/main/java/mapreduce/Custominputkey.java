package mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class Custominputkey implements WritableComparable<Custominputkey> {

    Text cardtype;

    //String[] k = value.toString().split("\\|");  // Escape for the metacharacter

    public Custominputkey(){
        this.cardtype = new Text();

    }

    public Custominputkey(Text cardtype){
        this.cardtype = cardtype;
    }


    public Text get(){
        return cardtype;
    }

    public void readFields(DataInput in) throws IOException {
        cardtype.readFields(in);

    }

    public void write(DataOutput out) throws  IOException{
        cardtype.write(out);
    }
    public int compareTo(Custominputkey o){
     return 1;
    }
}
