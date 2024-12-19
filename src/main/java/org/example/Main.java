package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class Main {
    private static final String DEFAULT_HDFS = "hdfs://localhost:9000";
    private static final String INPUT_DIR = "./input";
    private static final String OUTPUT_DIR = "./output";
    private static final String RESULT_FILENAME = "resultsTime.txt";
    private static final Integer WORKERS_NUM = 1;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", DEFAULT_HDFS);

        FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(INPUT_DIR);
        Path outputPath = new Path(OUTPUT_DIR);

        if (!fs.exists(inputPath)) {
            fs.mkdirs(inputPath);
        }
        try (var files = Files.list(Paths.get(INPUT_DIR))){
            for (var file: files.toList()){
                fs.copyFromLocalFile(
                    false,
                    true,
                    new Path(file.toString()),
                    new Path(INPUT_DIR, file.getFileName().toString())
                );
            }
        }

        fs.delete(outputPath, true);
        FileUtils.deleteDirectory(new File(OUTPUT_DIR));

        long timeStart = System.currentTimeMillis();
        Job job = Job.getInstance(conf, "my working job");
        job.setNumReduceTasks(WORKERS_NUM);

        job.setJarByClass(Main.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyDataWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean isDone = job.waitForCompletion(true);
        Files.writeString(
                Paths.get(RESULT_FILENAME),
                LocalDateTime.now() + " | time in process: "+ (System.currentTimeMillis() - timeStart) + "ms" + ", workers: " + WORKERS_NUM + "\n",
                APPEND,
                CREATE
        );

        if (!isDone) {
            System.exit(-1);
        }

        fs.copyToLocalFile(true, outputPath, new Path(OUTPUT_DIR), true);
    }

}