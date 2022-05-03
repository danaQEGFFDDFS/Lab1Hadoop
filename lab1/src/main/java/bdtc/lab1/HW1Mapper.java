package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /** Value for <K,V> output mapper */
    private final static IntWritable one = new IntWritable();

    /** Key for <K,V> output mapper */
    private Text word = new Text();

    private HashMap<String, String> myMap = new HashMap<String, String>() {{
        put("debug", "7");
        put("info", "6");
        put("notice", "5");
        put("warning", "4");
        put("warn", "4");
        put("err", "3");
        put("error", "3");
        put("crit", "2");
        put("alert", "1");
        put("emerg", "0");
        put("panic", "0");
    }};

    /**
     * map
     * @param key
     * @param value input string
     * @param context
     * @return void; Write <K,V> pairs into context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /** parse value to string */
        String line = value.toString();

        /** split value into tokens [month, day, date, -, mygoods] */
        String[] tokens = line.split(" ");

        /** set value into IntWritable container */
        one.set(1);

        /** parse string into hour*/

        SimpleDateFormat parser = new SimpleDateFormat("HH:mm:ss");

        Date date = new Date();

        try {
            date = parser.parse(tokens[2]);
        } catch ( ParseException e) {
            System.out.println(e.getMessage());
        }
        SimpleDateFormat formatter = new SimpleDateFormat("HH");
        String formattedDate = formatter.format(date);

        /** make key from date + hour + logType */
        word.set(tokens[0] + " " + tokens[1] + " "+ formattedDate + " " + myMap.get(tokens[4]));
        context.write(word, one);
    }
}
