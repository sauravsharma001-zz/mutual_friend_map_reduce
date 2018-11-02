/**
 * @author: Saurav Sharma
 * MutualFriendsForAGivenPair: Main Class to start Map Reduce Job
 */

package sxs179830;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class MutualFriendsForAGivenPair {

    /**
     * MutualFriendsForAGivenPairMapper: Mapper Class.
     */
    public static class MutualFriendsForAGivenPairMapper extends Mapper<Text, Text, Text, Text> {

        private Text word = new Text();
        HashMap<String,String> map = new HashMap<>();
        Text user = new Text();

        /**
         * Each line provided to the mapper is converted into key value pair, where key contains the user
         * and value contains the list of his friends (separated using ','). Then a string is constructed which oontains
         * the user's friend's id along with friend's state which is fetched from the map. Then emitting only relevant output
         * by mapper i.e. details of given pair of user stored in user variable. And rest all are discarded.  The value emitted
         * contains the list of friends along with it states.
         * @param key user id
         * @param value list of friends (comma separated)
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            // Getting list of his friends
            String[] friendList = value.toString().split(",");
            StringBuilder sb = new StringBuilder();

            // Creating a string containing id and state, using the map which was populated in setup() method
            for(String f : friendList) {
                sb.append(map.get(f.trim()));
                sb.append(",");
            }

            for(int i = 0; i < friendList.length; i++) {
                if(friendList[i].trim().length() > 0 ) {
                    if(key.compareTo(new Text(friendList[i])) > 0)
                        word.set(friendList[i] + "," + key);
                    else
                        word.set(key + "," + friendList[i]);
                    if(word.equals(user))
                        context.write(word, new Text(sb.toString()));
                }
            }
        }

        /**
         * This method gets invoked before map. In this method, the mapper reads the input file provided as configuration
         * variable "USER_AGENT_PATH" and creates map containing userId and his/her detail (i.e. State).
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path part=new Path(context.getConfiguration().get("USER_AGENT_PATH"));//Location of file in HDFS
            Text user1 = new Text(context.getConfiguration().get("USER_1"));
            Text user2 = new Text(context.getConfiguration().get("USER_2"));

            if(user1.compareTo(user2) > 0) {
                Text temp = user2;
                user2 = user1;
                user1 = temp;
            }
            user = new Text(user1 + "," + user2);

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();

                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    String[] arr=line.split(",");
                    map.put(arr[0].trim(), arr[0] + ":" + arr[1] + ":" + arr[5]);
                    line=br.readLine();
                }
            }
        }
    }

    /**
     * MutualFriendsForAGivenPairReducer: Reducer class.
     */
    public static class MutualFriendsForAGivenPairReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Reducer receive two list of value for a pair. And then intersection is taken between the two list to get mutual friends
         * between them. First list contains the list of friends for first user and second list contains the list of friends for
         * second user. So, by taking intersection between the list we get their mutual friends. The list also contains State from
         * that friend is.
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
                    h.add(s.trim().split(":")[0]);
                }
                for(String s : mutualFriendList.get(1)) {
                    if(h.contains(s.trim().split(":")[0])) {
                        sb.append(s.trim().substring(s.indexOf(":")+1) +", ");
                    }
                }
                if(sb.toString().length() > 2) {
                    sb.deleteCharAt(sb.length()-1);
                    sb.deleteCharAt(sb.length()-1);
                    sb.insert(0, "[");
                    sb.append("]");
                    context.write(key, new Text(sb.toString()));
                }
            }
        }
    }
}
