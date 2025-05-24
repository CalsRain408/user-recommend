package com.movies;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserPairMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("userId") || line.isEmpty()) {
            return; // Skip header and empty lines
        }

        String[] tokens = line.split(",");
        if (tokens.length >= 3) {
            String movieId = tokens[1].trim();
            String userId = tokens[0].trim();
            String rating = tokens[2].trim();

            // Output: movieId -> (userId, rating)
            context.write(new Text(movieId), new Text(userId + "," + rating));
        }
    }
}
