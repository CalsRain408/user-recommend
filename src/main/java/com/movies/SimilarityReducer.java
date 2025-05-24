package com.movies;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<Double> ratings1 = new ArrayList<>();
        List<Double> ratings2 = new ArrayList<>();

        // Collect all rating pairs for this user pair
        for (Text value : values) {
            String[] ratingPair = value.toString().split(",");
            if (ratingPair.length == 2) {
                try {
                    ratings1.add(Double.parseDouble(ratingPair[0]));
                    ratings2.add(Double.parseDouble(ratingPair[1]));
                } catch (NumberFormatException e) {
                    // Skip invalid ratings
                }
            }
        }

        // Calculate Pearson correlation coefficient
        if (ratings1.size() >= 2) { // Need at least 2 common ratings
            double correlation = calculatePearsonCorrelation(ratings1, ratings2);

            if (!Double.isNaN(correlation)) {
                String[] users = key.toString().split(",");
                // Output both directions of similarity
                context.write(new Text(users[0]), new Text(users[1] + "," + correlation));
                context.write(new Text(users[1]), new Text(users[0] + "," + correlation));
            }
        }
    }

    private double calculatePearsonCorrelation(List<Double> x, List<Double> y) {
        int n = x.size();
        if (n != y.size() || n == 0) return Double.NaN;

        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;

        for (int i = 0; i < n; i++) {
            sumX += x.get(i);
            sumY += y.get(i);
            sumXY += x.get(i) * y.get(i);
            sumX2 += x.get(i) * x.get(i);
            sumY2 += y.get(i) * y.get(i);
        }

        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        return denominator == 0 ? 0 : numerator / denominator;
    }
}
