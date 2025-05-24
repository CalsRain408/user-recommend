package com.movies;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        String[] parts = line.split("\t");

        if (parts.length == 2) {
            String userPair = parts[0];
            String ratingPair = parts[1];

            // Pass through: userPair -> ratingPair
            context.write(new Text(userPair), new Text(ratingPair));
        }
    }
}
