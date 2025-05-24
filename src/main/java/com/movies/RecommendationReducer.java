package com.movies;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Double> movieScores = new HashMap<>();
        Map<String, Double> movieWeights = new HashMap<>();

        // Process all neighbor ratings for this user
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 4) {
                try {
                    String neighborUserId = parts[0];
                    String movieId = parts[1];
                    double rating = Double.parseDouble(parts[2]);
                    double similarity = Double.parseDouble(parts[3]);

                    // Calculate weighted rating
                    if (similarity > 0) { // Only consider positive similarities
                        double weightedRating = rating * similarity;

                        movieScores.put(movieId,
                                movieScores.getOrDefault(movieId, 0.0) + weightedRating);
                        movieWeights.put(movieId,
                                movieWeights.getOrDefault(movieId, 0.0) + similarity);
                    }
                } catch (NumberFormatException e) {
                    // Skip invalid entries
                }
            }
        }

        // Calculate final scores and create recommendations
        List<MovieRecommendation> recommendations = new ArrayList<>();
        for (String movieId : movieScores.keySet()) {
            double totalScore = movieScores.get(movieId);
            double totalWeight = movieWeights.get(movieId);

            if (totalWeight > 0) {
                double finalScore = totalScore / totalWeight;
                recommendations.add(new MovieRecommendation(movieId, finalScore));
            }
        }

        // Sort by score (descending) and select top 5
        Collections.sort(recommendations, new Comparator<MovieRecommendation>() {
            @Override
            public int compare(MovieRecommendation a, MovieRecommendation b) {
                return Double.compare(b.score, a.score);
            }
        });

        StringBuilder topMovies = new StringBuilder();
        int count = Math.min(5, recommendations.size());
        for (int i = 0; i < count; i++) {
            if (i > 0) topMovies.append(",");
            topMovies.append(recommendations.get(i).movieId)
                    .append(":")
                    .append(String.format("%.2f", recommendations.get(i).score));
        }

        if (topMovies.length() > 0) {
            context.write(key, new Text(topMovies.toString()));
        }
    }

    private static class MovieRecommendation {
        String movieId;
        double score;

        MovieRecommendation(String movieId, double score) {
            this.movieId = movieId;
            this.score = score;
        }
    }
}
