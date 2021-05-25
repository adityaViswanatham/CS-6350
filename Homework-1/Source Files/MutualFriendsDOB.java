/*
    Name: Aditya Viswanatham
    NetID: arv160730
    CS 6350.001 Homework-1 Q3
 */

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriendsDOB {
    public static class MutualFriendsDOBMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        Map<String, String> map = new HashMap<>();
        private final Text user = new Text();
        private final Text friendsList = new Text();

        // Reads data from userdata.txt to initiate map
        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path fPath = new Path(conf.get("map.input"));
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
                        map.put(arr[0], ":" + arr[1] + ": " + arr[9]);
                    line = br.readLine();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().split("\\t");
            // If no friends
            if (input.length < 2)
                return;
            String userId = input[0];
            String[] friends = input[1].split(",");
            String str = "";

            // Loop to perform map-side join
            for (String friend: friends)
                str += map.get(friend) + ",";
            str = str.substring(0, str.length()-1);
            for (String friend: friends) {
                if (userId.equals(friend))
                    continue;
                // For consistency between two friends
                String userKey = (userId.compareTo(friend) < 0) ?
                        userId + "," + friend : friend + "," + userId;
                user.set(userKey);
                friendsList.set(str);
                context.write(user, friendsList);
            }
        }
    }

    public static class MutualFriendsDOBReducer
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
            String[] mFriendsList = mFriends.split(",");
            String mutualFriends = "";
            for (String friend: mFriendsList)
                mutualFriends += friend.substring(friend.indexOf(':')+1) + ", ";
            mutualFriends = mutualFriends.substring(0, mutualFriends.length()-2);
            context.write(key, new Text("[" + mutualFriends));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("map.input", args[1]);
        Job job = new Job(conf, "Mutual Friends DOB");
        job.setJarByClass(MutualFriendsDOB.class);
        job.setMapperClass(MutualFriendsDOBMapper.class);
        job.setReducerClass(MutualFriendsDOBReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
