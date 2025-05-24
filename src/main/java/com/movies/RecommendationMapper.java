package com.movies;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> userNeighbors = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load user neighbors from distributed cache
        Configuration conf = context.getConfiguration();
        Path neighborsPath = new Path(conf.get("neighbors.path"));
        FileSystem fs = FileSystem.get(conf);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(neighborsPath)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    userNeighbors.put(parts[0], parts[1]);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.startsWith("userId") || line.isEmpty()) {
            return; // Skip header and empty lines
        }

        String[] tokens = line.split(",");
        if (tokens.length >= 3) {
            String userId = tokens[0].trim();
            String movieId = tokens[1].trim();
            String rating = tokens[2].trim();

            // Check if this user has neighbors
            String neighbors = userNeighbors.get(userId);
            if (neighbors != null) {
                String[] neighborPairs = neighbors.split(";");
                for (String neighborPair : neighborPairs) {
                    String[] parts = neighborPair.split(",");
                    if (parts.length == 2) {
                        String neighborId = parts[0];
                        String similarity = parts[1];

                        // Output for each user their neighbor's ratings
                        context.write(new Text(neighborId),
                                new Text(userId + "," + movieId + "," + rating + "," + similarity));
                    }
                }
            }
        }
    }

}
