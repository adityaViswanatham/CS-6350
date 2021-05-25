/*
    Name: Aditya Viswanatham
    NetID: arv160730
    CS 6350.001 Homework-1 Q4
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DirectFriendMaxAge {
    public static class MaxAgeMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final Text userId = new Text();
        private final Text friendsList = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().split("\\t");
            if (input.length < 2)
                return;
            userId.set(input[0]);
            friendsList.set(input[1]);
            context.write(userId, friendsList);
        }
    }

    public static class MaxAgeReducer
            extends Reducer<Text, Text, Text, Text> {
        Map<String, Integer> map = new HashMap<>();

        public int calculateAge(String dob) {
            String[] input = dob.split("/");
            LocalDate today = LocalDate.now();
            LocalDate birthday = LocalDate.of(
                    Integer.parseInt(input[2]),
                    Integer.parseInt(input[0]),
                    Integer.parseInt(input[1]));

            Period p = Period.between(birthday, today);
            return p.getYears();
        }

        // Reads data from userdata.txt to initiate reduce
        @Override
        public void setup(Reducer.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path fPath = new Path(conf.get("reduce.input"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(fPath);
            for(FileStatus s : status) {
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(s.getPath())));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        map.put(arr[0], calculateAge(arr[9]));
                    line = br.readLine();
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value: values) {
                String[] friends = value.toString().split(",");
                int maxAge = Integer.MIN_VALUE;
                for (String friend: friends) {
                    if (map.get(friend) != null)
                        maxAge = Math.max(maxAge, map.get(friend));
                }
                context.write(key, new Text(Integer.toString(maxAge)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("reduce.input", args[1]);
        Job job = new Job(conf, "Direct Friends Max Age");
        job.setJarByClass(DirectFriendMaxAge.class);
        job.setMapperClass(MaxAgeMapper.class);
        job.setReducerClass(MaxAgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
