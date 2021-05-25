/*
    Name: Aditya Viswanatham
    NetID: arv160730
    CS 6350.001 Homework-1 Q5
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
    public static class InvertedIndexMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public static int lineCount = 1;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().split(",");
            for (String str: input)
                context.write(new Text(str), new Text(Integer.toString(lineCount)));
            lineCount++;
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text value: values)
                list.add(value.toString());
            Collections.sort(list);
            context.write(key, new Text(list.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
