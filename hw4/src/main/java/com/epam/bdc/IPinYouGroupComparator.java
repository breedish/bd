package com.epam.bdc;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zenind
 */
public class IPinYouGroupComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IPinYouWritable i1 = (IPinYouWritable) a;
        IPinYouWritable i2 = (IPinYouWritable) b;
        return i1.compareTo(i2);
    }
}
