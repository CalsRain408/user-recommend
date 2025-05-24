package com.movies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NeighborReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<NeighborSimilarity> neighbors = new ArrayList<>();

        // Collect all neighbors with their similarities
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 2) {
                try {
                    String neighborId = parts[0];
                    double similarity = Double.parseDouble(parts[1]);
                    neighbors.add(new NeighborSimilarity(neighborId, similarity));
                } catch (NumberFormatException e) {
                    // Skip invalid similarities
                }
            }
        }

        // Sort by similarity (descending) and select top 10
        Collections.sort(neighbors, new Comparator<NeighborSimilarity>() {
            @Override
            public int compare(NeighborSimilarity a, NeighborSimilarity b) {
                return Double.compare(b.similarity, a.similarity);
            }
        });

        StringBuilder topNeighbors = new StringBuilder();
        int count = Math.min(10, neighbors.size());
        for (int i = 0; i < count; i++) {
            if (i > 0) topNeighbors.append(";");
            topNeighbors.append(neighbors.get(i).neighborId)
                    .append(",")
                    .append(neighbors.get(i).similarity);
        }

        if (topNeighbors.length() > 0) {
            context.write(key, new Text(topNeighbors.toString()));
        }
    }

    private static class NeighborSimilarity {
        String neighborId;
        double similarity;

        NeighborSimilarity(String neighborId, double similarity) {
            this.neighborId = neighborId;
            this.similarity = similarity;
        }
    }
}
