package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyDataWritable implements WritableComparable<MyDataWritable> {
    private final DoubleWritable revenue;
    private final IntWritable quantity;


    public MyDataWritable() {
        this.revenue = new DoubleWritable(0);
        this.quantity = new IntWritable(0);
    }

    public MyDataWritable(double revenue, int quantity) {
        this.revenue = new DoubleWritable(revenue);
        this.quantity = new IntWritable(quantity);
    }

    public double getRevenue() {
        return revenue.get();
    }

    public int getQuantity() {
        return quantity.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        revenue.write(out);
        quantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue.readFields(in);
        quantity.readFields(in);
    }

    @Override
    public int compareTo(MyDataWritable o) {
        return Double.compare(o.getRevenue(), this.getRevenue());
    }

    @Override
    public String toString() {
        return revenue + "\t" + quantity;
    }
}
