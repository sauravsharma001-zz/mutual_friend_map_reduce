/**
 * @author: Saurav Sharma
 * Top10MutualFriends: Main Class containing Mapper and Reducer for finding top 10 all pairs of user with maximum number of mutual friends.
 */

package sxs179830;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Top10MutualFriends {

    /**
     * Top10MutualFriendsMapper: Mapper class
     */
    public static class Top10MutualFriendsMapper extends Mapper<Text, Text, LongWritable, Text> {

        private Text word = new Text();

        /**
         * Mapper receive the list of mutual friends for a given pair. Then the mapper just calculate
         * the number of mutual friends and forward this as key and send user pair and their mutual
         * friends as value to the Reducer.
         * @param key pair of user
         * @param value list of mutual friends (comma separated)
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int length = value.toString().split(",").length;
            String res = key + ":" + value;
            word.set(res);
            context.write(new LongWritable(length), word);
        }
    }

    /**
     * Top10MutualFriendsReducer: Reducer class
     */
    public static class Top10MutualFriendsReducer extends Reducer<LongWritable, Text, Text, Text> {

        // variable to keep track of number of records emitted
        private static int count = 0;
        private Text keyOutput = new Text();
        private Text valueOutput = new Text();

        /**
         * As there is only 1 Reducer, it gets all the value emitted by Mapper in descending order
         * of the number of the mutual friends for a pair of users. And it just write top 10 records
         * to the output file.
         * @param key key emitted by mapper
         * @param values list of value emitted by mapper for the key
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if(count < 10) {
                    keyOutput.set(val.toString().split(":")[0]);
                    valueOutput.set(val.toString().split(":")[1]);
                    context.write(keyOutput, valueOutput);
                }
                count++;
            }
        }
    }
}
