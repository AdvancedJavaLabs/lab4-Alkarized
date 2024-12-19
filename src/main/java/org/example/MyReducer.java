package org.example;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MyReducer extends Reducer<Text, MyDataWritable, Text, Text> {
    private boolean headerPrinted = false;
    private List<Data> dataList;

    private final DecimalFormat df = new DecimalFormat("0.00");

    @Override
    protected void setup(Context context) {
        dataList = new ArrayList<>();
    }

    @Override
    protected void reduce(Text key, Iterable<MyDataWritable> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        int totalQuantity = 0;

        for (MyDataWritable val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        if (!headerPrinted) {
            context.write(new Text(String.format("%-20s", "Category")), new Text(String.format("%-20s%-20s", "Revenue", "Quantity")));
            headerPrinted = true;
        }

        String formattedKey = String.format("%-20s", key.toString());
        String formattedOutput = String.format("%-20s%-20s", df.format(totalRevenue), totalQuantity);

        dataList.add(new Data(formattedKey, formattedOutput, totalRevenue));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dataList.sort(Comparator.comparing(Data::value).reversed());

        for (Data elem : dataList) {
            context.write(new Text(elem.formattedKey()), new Text(elem.formattedOutput()));
        }

        this.dataList.clear();
    }

    private record Data(String formattedKey, String formattedOutput, Double value) {}
}
