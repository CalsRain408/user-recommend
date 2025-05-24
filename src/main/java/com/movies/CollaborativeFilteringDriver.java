package com.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CollaborativeFilteringDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(args.length);
            System.err.println("Usage: com.movies.CollaborativeFilteringDriver <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];

        // Step 1: Generate user pairs
        if (!runUserPairJob(conf, inputPath, outputPath + "/step1")) {
            System.exit(-1);
        }

        // Step 2: Calculate similarities
        if (!runSimilarityJob(conf, outputPath + "/step1", outputPath + "/step2")) {
            System.exit(-1);
        }

        // Step 3: Select neighbors
        if (!runNeighborJob(conf, outputPath + "/step2", outputPath + "/step3")) {
            System.exit(-1);
        }

        // Step 4: Generate recommendations
        conf.set("neighbors.path", outputPath + "/step3/part-r-00000");
        if (!runRecommendationJob(conf, inputPath, outputPath + "/final")) {
            System.exit(-1);
        }

        System.out.println("Collaborative filtering completed successfully!");
    }

    private static boolean runUserPairJob(Configuration conf, String input, String output)
            throws Exception {
        Job job = Job.getInstance(conf, "user pair generation");
        job.setJarByClass(CollaborativeFilteringDriver.class);
        job.setMapperClass(UserPairMapper.class);
        job.setReducerClass(UserPairReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    private static boolean runSimilarityJob(Configuration conf, String input, String output)
            throws Exception {
        Job job = Job.getInstance(conf, "similarity calculation");
        job.setJarByClass(CollaborativeFilteringDriver.class);
        job.setMapperClass(SimilarityMapper.class);
        job.setReducerClass(SimilarityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    private static boolean runNeighborJob(Configuration conf, String input, String output)
            throws Exception {
        Job job = Job.getInstance(conf, "neighbor selection");
        job.setJarByClass(CollaborativeFilteringDriver.class);
        job.setMapperClass(NeighborMapper.class);
        job.setReducerClass(NeighborReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    private static boolean runRecommendationJob(Configuration conf, String input, String output)
            throws Exception {
        Job job = Job.getInstance(conf, "recommendation generation");
        job.setJarByClass(CollaborativeFilteringDriver.class);
        job.setMapperClass(RecommendationMapper.class);
        job.setReducerClass(RecommendationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }
}
