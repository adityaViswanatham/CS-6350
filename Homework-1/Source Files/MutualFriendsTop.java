/*
    Name: Aditya Viswanatham
    NetID: arv160730
    CS 6350.001 Homework-1 Q2
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriendsTop {
    public static class MapperOne
            extends Mapper<LongWritable, Text, Text, Text> {
        private final Text user = new Text();
        private final Text friendsList = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().split("\\t");
            if (input.length < 2)
                return;
            String userId = input[0];
            String[] friends = input[1].split(",");
            for (String friend: friends) {
                if (userId.equals(friend))
                    continue;
                // For consistency between two friends
                String userKey = (userId.compareTo(friend) < 0) ?
                        userId + "," + friend : friend + "," + userId;
                user.set(userKey);
                friendsList.set(input[1]);
                context.write(user, friendsList);
            }
        }
    }

    public static class ReducerOne
            extends Reducer<Text, Text, Text, Text> {

        private String mutualFriends(String list1, String list2) {
            if (list1 == null || list2 == null)
                return "0";
            Set<String> set1 = new HashSet<>(Arrays.asList(list1.split(",")));
            Set<String> set2 = new HashSet<>(Arrays.asList(list2.split(",")));
            set1.retainAll(set2);
            return Integer.toString(set1.size());
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text value: values)
                list.add(value.toString());
            if (list.size() == 1)
                list.add("");

            String numFriends = mutualFriends(list.get(0), list.get(1));
            context.write(key, new Text(numFriends));
        }
    }

    public static class MapperTwo
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Forcing all key value pairs into the same reducer for max value
            context.write(new Text(""), value);
        }
    }

    public static class ReducerTwo
            extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>();
            for (Text value: values) {
                String[] keyValue = value.toString().split("\\t");
                if (keyValue.length == 2)
                    map.put(keyValue[0], Integer.parseInt(keyValue[1]));
            }

            PriorityQueue<Map.Entry<String, Integer>> q = new PriorityQueue<>((a, b) ->
                b.getValue().compareTo(a.getValue())
            );
            q.addAll(map.entrySet());
            int max = q.peek().getValue();
            while (!q.isEmpty() && q.peek().getValue() == max) {
                context.write(new Text(q.peek().getKey()),
                        new IntWritable(q.peek().getValue()));
                q.remove();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Mutual Friends");
        job.setJarByClass(MutualFriendsTop.class);
        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true))
            System.exit(1);

        conf = new Configuration();
        job = new Job(conf, "MutualFriendsTop");
        job.setJarByClass(MutualFriendsTop.class);
        job.setMapperClass(MapperTwo.class);
        job.setReducerClass(ReducerTwo.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
