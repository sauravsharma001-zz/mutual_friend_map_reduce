/**
 * @author: Saurav Sharma
 * MutualFriendForAllPairs: Main Class containing Mapper and Reducer for calculating mutual friends for all pairs of user
 */

package sxs179830;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class MutualFriendForAllPairs {

    /**
     * MutualFriendForAllPairsMapper: Mapper Class.
     */
    public static class MutualFriendForAllPairsMapper extends Mapper<Text, Text, Text, Text> {

        private Text word = new Text();

        /**
         * Each line provided to the mapper is converted into key value pair, where key contains the user
         * and value contains the list of his friends (separated using ','). Then a list of pair is created
         * containing user and each of his friend. While creating pair, it is followed that the user with smaller id comes first.
         * So, that (1, 2) and (2, 1) goes to the same reducer. Then pair with list of mutual friends are emitted by mapper as key value pair.
         * Key contains the (user, friend id) and value contains the list of all mutual friends.
         * @param key user id
         * @param value list of friends (comma separated)
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] friendList = value.toString().split(",");
            for(int i = 0; i < friendList.length; i++) {
                if(friendList[i].trim().length() > 0 ) {
                    if(key.compareTo(new Text(friendList[i])) > 0)
                        word.set(friendList[i] + "," + key);
                    else
                        word.set(key + "," + friendList[i]);
                    context.write(word, value);
                }
            }
        }
    }

    /**
     * MutualFriendForAllPairsReducer: Reducer class.
     */
    public static class MutualFriendForAllPairsReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Reducer receive two list of value for a pair. And then intersection is taken between the two list to get mutual friends
         * between them. First list contains the list of friends for first user and second list contains the list of friends for
         * second user. So, by taking intersection between the list we get their mutual friends.
         * @param key key emitted by mapper
         * @param values list of value emitted by mapper for the key
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashSet h = new HashSet();
            List<String[]> mutualFriendList = new ArrayList<>();
            int i = 0;
            for(Text tx : values) {
                String[] t = tx.toString().split(",");
                mutualFriendList.add(t);
            }
            if(mutualFriendList.size() == 2) {
                StringBuilder sb = new StringBuilder();
                for(String s : mutualFriendList.get(0)) {
                    h.add(s.trim());
                }
                for(String s : mutualFriendList.get(1)) {
                    if(h.contains(s.trim())) {
                        sb.append(s.trim() +",");
                    }
                }
                if(sb.toString().trim().length() > 0) {
                    sb.deleteCharAt(sb.length()-1);
                    context.write(key, new Text(sb.toString()));
                }
            }
        }
    }

    /**
     * MutualFriendsKeyComparator: Comparator class designed to compare the keys emitted by Mapper.
     */
    public static class MutualFriendsKeyComparator extends WritableComparator {

        protected MutualFriendsKeyComparator() {
            super(Text.class, true);
        }

        /**
         * compares the two object and returns which is smaller. Here object is in (key,value) format. So first key of both the
         * object is compared, in case both keys are same then values are compared.
         * @param w1 object one
         * @param w2 object two
         * @return -1: if w1 is smaller, 0: if equal, 1: if w1 is bigger
         */
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;
            String key1arr[] = w1.toString().split(",");
            String key2arr[] = w2.toString().split(",");
            int a = Integer.parseInt(key1arr[0]);
            int b = Integer.parseInt(key2arr[0]);
            if(a != b) {
                return a > b ? 1 : -1;
            } else {
                a = Integer.parseInt(key1arr[1]);
                b = Integer.parseInt(key2arr[1]);
                if(a == b)
                    return 0;
                else
                    return a > b ? 1 : -1;
            }
        }
    }
}
