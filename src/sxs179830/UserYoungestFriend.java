/**
 * @author: Saurav Sharma
 * UserYoungestFriend: Main Class containing Mapper and Reducer for calculating top 10 user with youngest friend in descending order.
 */

package sxs179830;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class UserYoungestFriend {

    /**
     * UserYoungestFriendSplitterMapper: Mapper class
     */
    public static class UserYoungestFriendSplitterMapper extends Mapper<Text, Text, LongWritable, Text> {

        /**
         * Each line provided to the mapper is converted into key value pair, where key contains the user
         * and value contains the list of his friends (separated using ','). Then a list of pair is created
         * containing user and each of his friend. And Mapper emit key (friend's id) and value (user id).
         * @param key user id
         * @param value list of friends (comma separated)
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            for(String s : val) {
                if(s.trim().length() > 0) {
                    context.write(new LongWritable(Long.valueOf(s)), key);
                }
            }
        }
    }

    /**
     * UserYoungestFriendDetailMapper: Mapper class
     */
    public static class UserYoungestFriendDetailMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Text word = new Text();
        private LongWritable k = new LongWritable();

        /**
         * Mapper reads the userdata and create map containing the user id and its age
         * @param key key offset
         * @param value list of friends
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            k = new LongWritable(Long.valueOf(val[0].trim()));
            word.set("age:" + calculateAge(val[9]));
            context.write(k, word);
        }

        /**
         * Calculate age based on a given date
         * @param d date
         * @return age
         */
        private static String calculateAge(String d) {
            String[] date = d.split("/");
            if(date.length != 3)
                return "";
            LocalDate birthdate = new LocalDate (Integer.parseInt(date[2]), Integer.parseInt(date[0]), Integer.parseInt(date[1]));
            LocalDate now = new LocalDate();
            Period period = new Period(birthdate, now, PeriodType.yearMonthDay());
            return String.valueOf(period.getYears());
        }
    }

    /**
     * UserDetailMapper: Mapper class
     */
    public static class UserDetailMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Text word = new Text();
        private LongWritable k = new LongWritable();

        /**
         * Read the files and emit user id with address
         * @param key file offset
         * @param value user detail
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            k = new LongWritable(Long.valueOf(val[0].trim()));
            word.set("address:" + val[1] + " " + val[2] +", " + val[3] +", " + val[4] + ", " + val[5]);
            context.write(k, word);
        }
    }

    /**
     * UserYoungestFriendSwapperMapper: Mapper class
     */
    public static class UserYoungestFriendSwapperMapper extends Mapper<Text, Text, LongWritable, Text> {

        private LongWritable k = new LongWritable();

        /**
         * Reading the input file and converting it into key, value pair
         * @param key user id
         * @param value friend_id with age
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.valueOf(key.toString())), value);
        }
    }

    /**
     * UserAgeSwappingMapper: Mapper class
     */
    public static class UserAgeSwappingMapper extends Mapper<Text, Text, LongWritable, Text> {

        private Text word = new Text();
        private LongWritable k = new LongWritable();

        /**
         * This mapper just swapping the user id with age
         * @param key friend_id
         * @param value user_id and age
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            int age = Integer.parseInt(val[val.length-1].trim());
            word.set(value + ";" + key);
            context.write(new LongWritable(age), word);
        }
    }

    /**
     * UserYoungestFriendReducer: Reducer class
     */
    public static class UserYoungestFriendReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        /**
         * Reducer emit key, value pair where key contains friend_id and value contains the user_id and its age
         * @param key user_id
         * @param values list of friends for that user
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> friends = new LinkedList<>();
            String age = new String();
            for(Text t : values) {
                if(t.toString().contains("age")) {
                    age = t.toString().split(":")[1];
                } else {
                    friends.add(t.toString());
                }
            }

            for(String f : friends) {
                context.write(new LongWritable(Long.valueOf(f)), new Text(age+ ";" + key.toString()));
            }
        }
    }

    /**
     * UserYoungestFriendSwapperReducer: Reducer class
     */
    public static class UserYoungestFriendSwapperReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        /**
         * This function calculates the top 10 friends with youngest friends sorted in descending order
         * @param key file offset
         * @param values user detail
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String address = new String();
            int minAge = Integer.MAX_VALUE, age;
            for(Text t : values) {
                if(t.toString().contains("address")) {
                    address = t.toString().split(":")[1];
                } else {
                    if(t.toString().trim().length() > 0) {
                        age = Integer.valueOf(t.toString().split(";")[0]);
                        if (age < minAge) {
//                        sb = t.toString().split(";")[1];
                            minAge = age;
                        }
                    }
                }
            }
            if(minAge < Integer.MAX_VALUE) {
                Text word = new Text();
                word.set(address + ", " + minAge);
                context.write(key, word);
            }
        }
    }

    /**
     * UserAgeSwappingReducer: Reducer class
     */
    public static class UserAgeSwappingReducer extends Reducer<LongWritable, Text, Text, Text> {

        private static int count = 0;
        private Text keyOutput = new Text();
        private Text valueOutput = new Text();

        /**
         * Reducer calculate top 10 user according to age in decreasing order
         * @param key age
         * @param values user detail with friends id
         * @param context context object to interact with rest of hadoop system
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                if(count < 10) {
                    String[] a = val.toString().split(";");
                    keyOutput.set(a[1]);
                    valueOutput.set(a[0]);
                    context.write(keyOutput, valueOutput);
                }
                count++;
            }
        }
    }
}

