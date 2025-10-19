package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class URLCountryPair implements Writable, WritableComparable<URLCountryPair> {
    
    private final Text URL = new Text();
    private final Text Country = new Text();
    
    public URLCountryPair() {
    }
    
    public URLCountryPair(String URL, String Country) {
        this.URL.set(URL);
        this.Country.set(Country);
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        URL.write(out);
        Country.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        URL.readFields(in);
        Country.readFields(in);
    }
    
    @Override
    public int compareTo(URLCountryPair pair) {
        if (URL.compareTo(pair.getURL()) == 0) {
            return Country.compareTo(pair.Country);
        }
        return URL.compareTo(pair.getURL());
    }
    
    public Text getURL() {
        return URL;
    }
    
    public Text getCountry() {
        return Country;
    }
    
}
