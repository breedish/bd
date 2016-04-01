package com.epam.bdc.bid;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author zenind
 */
public class IpBidStatsWritable implements WritableComparable<IpBidStatsWritable> {

    private Integer count;

    private Long bid;

    public IpBidStatsWritable() {
        count = 0;
        bid = 0L;
    }

    public IpBidStatsWritable(Integer count, Long bid) {
        checkArgument(count != null && count >= 0);
        checkArgument(bid != null);
        this.count = count;
        this.bid = bid;
    }

    public Integer getCount() {
        return count;
    }

    public Long getBid() {
        return bid;
    }

    public void incCount(Integer count) {
        this.count = this.count + count;
    }

    public void incBid(Long bid) {
        this.bid = this.bid + bid;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeLong(bid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        bid = dataInput.readLong();
    }

    @Override
    public int compareTo(IpBidStatsWritable o) {
        int cmp = count.compareTo(o.count);
        return cmp == 0 ? cmp : bid.compareTo(o.bid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IpBidStatsWritable that = (IpBidStatsWritable) o;
        return Objects.equals(count, that.count) && Objects.equals(bid, that.bid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, bid);
    }

    @Override
    public String toString() {
        return "IPBidStats{count=" + count + ", bid=" + bid + '}';
    }

}
