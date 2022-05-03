import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class CountersTestDana {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String test1 = "FEB 5 11:35:10 - err SOME TEST SITUATION";
    private final String test2 = "FEB 5 12:35:10 - error SOME TEST SITUATION";
    private final String test3 = "FEB 5 13:35:10 - crit SOME TEST SITUATION";
    private final String test4 = "FEB 5 14:35:10 - panic SOME TEST SITUATION";
    private final String test5 = "FEB 5 15:35:10 - debug SOME TEST SITUATION";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounters() throws IOException {

        mapDriver
                .withInput(new LongWritable(), new Text(test1))
                .withInput(new LongWritable(), new Text(test2))
                .withInput(new LongWritable(), new Text(test3))
                .withInput(new LongWritable(), new Text(test4))
                .withInput(new LongWritable(), new Text(test5))
                .withOutput(new Text("FEB 5 11 3"), new IntWritable(1))
                .withOutput(new Text("FEB 5 12 3"), new IntWritable(1))
                .withOutput(new Text("FEB 5 13 2"), new IntWritable(1))
                .withOutput(new Text("FEB 5 14 0"), new IntWritable(1))
                .withOutput(new Text("FEB 5 15 7"), new IntWritable(1))
                .runTest();
    }
}

