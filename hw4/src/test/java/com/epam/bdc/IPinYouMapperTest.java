package com.epam.bdc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author zenind
 */
public class IPinYouMapperTest {

    @Test(dataProvider = "mapperData", enabled = false)
    public void testCorrectMapping(String input, IPinYouWritable oKey, String oValue) throws Exception {
        new MapDriver<LongWritable, Text, IPinYouWritable, Text>()
            .withMapper(new IPinYouMapper())
            .withInput(new LongWritable(0), new Text(input))
            .withOutput(oKey, new Text(oValue))
            .runTest();
    }

    @DataProvider(name = "mapper-data")
    public Object[][] mapperData() {
        return new Object[][]{
            {
                "8daf2a8dbcdc1c089724de201086a63a\t20130606000104100\tVh271psHOclWqOB\tmozilla/4.0 (windows; u; windows nt 5.1; zh-tw; rv:1.9.0.11)\t14.110.159.*\t275\t275\t1\ttrqRTuFJBT27Fgc\t6f772ed0fafd1ada95733a9eacefb1be\t\tmm_12431063_3028982_10198617\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282163000648\t0",
                new IPinYouWritable(new Text("Vh271psHOclWqOB"), new Text("20130606000104100")),
                "8daf2a8dbcdc1c089724de201086a63a\t20130606000104100\tVh271psHOclWqOB\tmozilla/4.0 (windows; u; windows nt 5.1; zh-tw; rv:1.9.0.11)\t14.110.159.*\t275\t275\t1\ttrqRTuFJBT27Fgc\t6f772ed0fafd1ada95733a9eacefb1be\t\tmm_12431063_3028982_10198617\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282163000648\t0"
            }
        };
    }
}
