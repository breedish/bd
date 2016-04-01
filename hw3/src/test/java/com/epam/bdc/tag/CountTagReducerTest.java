package com.epam.bdc.tag;

import com.beust.jcommander.internal.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author zenind
 */
public class CountTagReducerTest {

    @Test(dataProvider = "tags-reducer-data")
    public void testCorrectCountCalculation(String inKey, List<IntWritable> inValues, String outKey, IntWritable outValue) throws Exception {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
            .withReducer(new CountTagReducer())
            .withInput(new Text(inKey), inValues)
            .withOutput(new Text(outKey), outValue)
            .runTest();
    }

    @DataProvider(name = "tags-reducer-data")
    public Object[][] tagsReducerData() {
        return new Object[][]{
            { "all", Lists.newArrayList(new IntWritable(1)), "all", new IntWritable(1)},
            { "money", Lists.newArrayList(new IntWritable(1), new IntWritable(1)), "money", new IntWritable(2) },
            { "split", Lists.newArrayList(new IntWritable(1), new IntWritable(1), new IntWritable(1)), "split", new IntWritable(3) }
        };
    }
}
