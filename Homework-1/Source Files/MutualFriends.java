/*
    Name: Aditya Viswanatham
    NetID: arv160730
    CS 6350.001 Homework-1 Q1
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriends {
    public static class MutualFriendsMapper
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

    public static class MutualFriendsReducer
            extends Reducer<Text, Text, Text, Text> {

        private String mutualFriends(String list1, String list2) {
            if (list1 == null || list2 == null)
                return null;
            Set<String> set1 = new HashSet<>(Arrays.asList(list1.split(",")));
            Set<String> set2 = new HashSet<>(Arrays.asList(list2.split(",")));
            set1.retainAll(set2);
            return set1.toString();
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text value: values)
                list.add(value.toString());
            if (list.size() == 1)
                list.add("");

            String mFriends = mutualFriends(list.get(0), list.get(1));
            if (mFriends != null)
                context.write(key, new Text(mFriends));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Mutual Friends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(MutualFriendsMapper.class);
        job.setReducerClass(MutualFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
