package com.movies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserPairReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<String[]> userRatings = new ArrayList<>();

        // Collect all user-rating pairs for this movie
        for (Text value : values) {
            String[] userRating = value.toString().split(",");
            if (userRating.length == 2) {
                userRatings.add(userRating);
            }
        }

        // Generate all pairs of users who rated this movie
        for (int i = 0; i < userRatings.size(); i++) {
            for (int j = i + 1; j < userRatings.size(); j++) {
                String user1 = userRatings.get(i)[0];
                String rating1 = userRatings.get(i)[1];
                String user2 = userRatings.get(j)[0];
                String rating2 = userRatings.get(j)[1];

                // Ensure consistent ordering: smaller userId first
                if (user1.compareTo(user2) < 0) {
                    context.write(new Text(user1 + "," + user2),
                            new Text(rating1 + "," + rating2));
                } else {
                    context.write(new Text(user2 + "," + user1),
                            new Text(rating2 + "," + rating1));
                }
            }
        }
    }
}
