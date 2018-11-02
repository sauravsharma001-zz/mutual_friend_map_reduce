/**
 * @author: Saurav Sharma
 * MutualFriends: Main Class to start Map Reduce Job
 */

package sxs179830;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {

    /**
     * Main method to start Map Reduce Job
     * @param args argument passed through terminal
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // Creating configuration file for MapReduce Job
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int quest = otherArgs.length == 0 ? -1 : Integer.parseInt(otherArgs[0]);
        if (quest == -1) {
            System.err.println("Usage: <question_number>");
            System.exit(2);
        }

        if(quest < 3) {
            if(quest == 1 && otherArgs.length != 3) {
                System.err.println("Usage: <question_number> <input_files> <output_folder>");
                System.exit(2);
            }
            else if(quest ==  2 && otherArgs.length != 4) {
                System.err.println("Usage: <question_number> <input_files> <temp_folder> <output_folder>");
                System.exit(2);
            }
            // Job 1 -- Calculate Mutual Friends for all Pairs
            {
                // create a job with name "mutual_friend"
                Job job = Job.getInstance(conf, "mutual_friend");
                job.setJarByClass(MutualFriendForAllPairs.class);

                // Setting InputFormat Class on how to split input
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                // Setting Mapper and Reducer
                job.setMapperClass(MutualFriendForAllPairs.MutualFriendForAllPairsMapper.class);
                job.setReducerClass(MutualFriendForAllPairs.MutualFriendForAllPairsReducer.class);
                job.setSortComparatorClass(MutualFriendForAllPairs.MutualFriendsKeyComparator.class);

                // Setting Output format of Mapper and Input format of Reducer
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                // Set the HDFS path of the input data
                FileInputFormat.addInputPath(job, new Path(otherArgs[1]));

                // Set the HDFS path for the output data
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

                if (!job.waitForCompletion(true))
                    System.exit(1);
            }
            if(quest == 2) {
                // Job 2 -- Calculate Top 10 pair with highest number of mutual friends
                {
                    Job job2 = Job.getInstance(conf, "top_10_mutual_friends");
                    job2.setJarByClass(Top10MutualFriends.class);

                    // Setting InputFormat Class on how to split input
                    job2.setInputFormatClass(KeyValueTextInputFormat.class);

                    // Setting Mapper and Reducer
                    job2.setMapperClass(Top10MutualFriends.Top10MutualFriendsMapper.class);
                    job2.setReducerClass(Top10MutualFriends.Top10MutualFriendsReducer.class);

                    // Setting Output format of Mapper and Input format of Reducer
                    job2.setMapOutputKeyClass(LongWritable.class);
                    job2.setMapOutputValueClass(Text.class);
                    job2.setOutputKeyClass(Text.class);
                    job2.setOutputValueClass(Text.class);

                    // Setting number of Reducer. Setting it to 1, so that all key comes to same reducer in order to sort and get top 10 records
                    job2.setNumReduceTasks(1);
                    // Setting Sorting order of Key emitted by Mapper. By Default it is in increasing order.
                    job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

                    // Set the HDFS path of the input data
                    FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));

                    // set the HDFS path for the output data
                    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
                    System.exit(job2.waitForCompletion(true) ? 0 : 1);
                }
            }
        }
        else if(quest == 3) {

            if(otherArgs.length != 6) {
                System.err.println("Usage: <question_number> <input_files_adj_list> <input_files_user_data> <output_folder> <user1_id> <user2_id>");
                System.exit(2);
            }
            // Job 3 -- Calculate mutual friends detail for a given pair of user
            {
                // Adding variable to configuration in order to use those in Mapper class
                conf.set("USER_AGENT_PATH",otherArgs[2]);
                conf.set("USER_1", otherArgs[4]);
                conf.set("USER_2", otherArgs[5]);

                // create a job with name "mutual_friend_for_a_pair"
                Job job3 = Job.getInstance(conf, "mutual_friend_for_a_pair");
                job3.setJarByClass(MutualFriendsForAGivenPair.class);

                // Setting InputFormat Class on how to split input
                job3.setInputFormatClass(KeyValueTextInputFormat.class);

                // Setting Mapper and Reducer
                job3.setMapperClass(MutualFriendsForAGivenPair.MutualFriendsForAGivenPairMapper.class);
                job3.setReducerClass(MutualFriendsForAGivenPair.MutualFriendsForAGivenPairReducer.class);

                // Setting Output format of Mapper and Input format of Reducer
                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(Text.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                // Set the HDFS path of the input data
                FileInputFormat.addInputPath(job3, new Path(otherArgs[1]));
                // Set the HDFS path for the output data
                FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
                System.exit(job3.waitForCompletion(true) ? 0 : 1);
            }
        }
        else if(quest == 4) {
            if(otherArgs.length != 4) {
                System.err.println("Usage: <question_number> <input_files_adj_list> <input_files_user_data> <output_folder>");
                System.exit(2);
            }
            // Job 4 -- Mapping User's Friend's Age
            {
                // create a job with name "user_detail_mapping"
                Job job4 = Job.getInstance(conf, "user_detail_mapping");
                job4.setJarByClass(UserYoungestFriend.class);

                // Setting Multiple Mapper and Reducer
                MultipleInputs.addInputPath(job4, new Path(args[1]), KeyValueTextInputFormat.class, UserYoungestFriend.UserYoungestFriendSplitterMapper.class);
                MultipleInputs.addInputPath(job4, new Path(args[2]), TextInputFormat.class, UserYoungestFriend.UserYoungestFriendDetailMapper.class);
                job4.setReducerClass(UserYoungestFriend.UserYoungestFriendReducer.class);

                // Setting Output format of Mapper and Input format of Reducer
                job4.setMapOutputKeyClass(LongWritable.class);
                job4.setMapOutputValueClass(Text.class);
                job4.setOutputKeyClass(LongWritable.class);
                job4.setOutputValueClass(Text.class);

                // Set the HDFS path of the input data
                FileOutputFormat.setOutputPath(job4, new Path(otherArgs[3] +"/temp"));
                if (!job4.waitForCompletion(true))
                    System.exit(1);
            }
            {
                // create a job with name "user_youngest_friend"
                Job job5 = Job.getInstance(conf, "user_youngest_friend");
                job5.setJarByClass(UserYoungestFriend.class);

                // Setting Multiple Mapper and Reducer
                MultipleInputs.addInputPath(job5, new Path(otherArgs[3] +"/temp/part-r-00000"), KeyValueTextInputFormat.class, UserYoungestFriend.UserYoungestFriendSwapperMapper.class);
                MultipleInputs.addInputPath(job5, new Path(args[2]), TextInputFormat.class, UserYoungestFriend.UserDetailMapper.class);
                job5.setReducerClass(UserYoungestFriend.UserYoungestFriendSwapperReducer.class);

                // Setting Output format of Mapper and Input format of Reducer
                job5.setMapOutputKeyClass(LongWritable.class);
                job5.setMapOutputValueClass(Text.class);
                job5.setOutputKeyClass(LongWritable.class);
                job5.setOutputValueClass(Text.class);
                job5.setSortComparatorClass(LongWritable.DecreasingComparator.class);

                // Set the HDFS path of the input data
                FileOutputFormat.setOutputPath(job5, new Path(otherArgs[3] +"/temp2"));
                if (!job5.waitForCompletion(true))
                    System.exit(1);
            }
            {
                // create a job with name "user_youngest_friend"
                Job job6 = Job.getInstance(conf, "user_youngest_friend");
                job6.setJarByClass(UserYoungestFriend.class);

                // Setting InputFormat Class on how to split input
                job6.setInputFormatClass(KeyValueTextInputFormat.class);

                // Setting Mapper and Reducer
                job6.setMapperClass(UserYoungestFriend.UserAgeSwappingMapper.class);
                job6.setReducerClass(UserYoungestFriend.UserAgeSwappingReducer.class);

                // Setting Output format of Mapper and Input format of Reducer
                job6.setMapOutputKeyClass(LongWritable.class);
                job6.setMapOutputValueClass(Text.class);
                job6.setOutputKeyClass(Text.class);
                job6.setOutputValueClass(Text.class);

                // Setting number of Reducer. Setting it to 1, so that all key comes to same reducer in order to sort and get top 10 records
                job6.setNumReduceTasks(1);
                // Setting Sorting order of Key emitted by Mapper. By Default it is in increasing order.
                job6.setSortComparatorClass(LongWritable.DecreasingComparator.class);

                // Set the HDFS path of the input data
                FileInputFormat.addInputPath(job6, new Path(otherArgs[3] +"/temp2"));

                // Set the HDFS path of the input data
                FileOutputFormat.setOutputPath(job6, new Path(otherArgs[3] +"/out"));
                if (!job6.waitForCompletion(true))
                    System.exit(1);
            }
        }
        else {
            System.err.println("Invalid Question Number provided: " + quest + ". Valid inputs are 1 to 4" );
            System.exit(2);
        }
    }
}
