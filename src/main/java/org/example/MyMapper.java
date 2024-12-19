package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, MyDataWritable> {
    private static final int CATEGORY_INDEX = 2;
    private static final int PRICE_INDEX = 3;
    private static final int QUANTITY_INDEX = 4;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length == 5 && !fields[0].equals("transaction_id")) {
            String category = fields[CATEGORY_INDEX];

            double price = Double.parseDouble(fields[PRICE_INDEX]);
            int quantity = Integer.parseInt(fields[QUANTITY_INDEX]);

            double revenue = price * quantity;

            context.write(new Text(category), new MyDataWritable(revenue, quantity));
        }
    }
}
