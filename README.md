# Lab Session II: Big Data Storage, Query, and Parallel Recommendation System Experiment

## I. Experimental Objectives
(1) To understand the methods of big data storage and query, master the use of Hadoop and MongoDB, and comprehend the pros and cons of different storage systems in big data processing.
(2) To learn the principles and implementation of parallel recommendation systems, grasp the MapReduce programming model, and be able to implement a parallel user-based collaborative filtering recommendation algorithm on the Hadoop platform.

## II. Experimental Content
1. Install and configure Hadoop and MongoDB.
2. Import the MovieLens dataset into Hadoop and MongoDB.
3. Perform data storage and query operations using Hadoop's HDFS command-line tools and MongoDB's query language.
4. Compare the performance and applicability of Hadoop and MongoDB in big data storage and query.
5. Implement a parallel user-based collaborative filtering recommendation algorithm on the Hadoop platform using the MovieLens dataset.
6. Partition the dataset by user and compute user similarities in parallel on each partition.
7. Merge similarity results from each partition using MapReduce to obtain the final user neighborhood set.
8. Generate recommendation lists for users based on neighbors' rating information.
9. Evaluate the accuracy and recall of recommendation results and compare them with non-parallel recommendation algorithms.

## III. Experimental Steps
1. Environment Preparation
   (1) Linux operating system (Ubuntu or CentOS recommended), Java runtime environment (JDK 1.8 or above), normal network connection.
   (2) Download the installation packages for Hadoop and MongoDB from the official website. Ensure the experimental environment has: Linux OS, Java runtime (JDK 1.8+), properly installed and configured Hadoop platform, and normal network connection.
   (3) Download the MovieLens dataset from the official website or QQ group, selecting a complete dataset with user ratings and movie information.

2. Hadoop Installation and Configuration
   Extract the Hadoop installation package: `tar -zxvf hadoop-3.3.1.tar.gz`.
   Configure Hadoop environment variables: Add `export HADOOP_HOME=/home/user/hadoop-3.3.1` and `export PATH=$PATH:$HADOOP_HOME/bin` to the `~/.bashrc` file, then run `source ~/.bashrc` to activate the configuration.
   Configure Hadoop's `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, and `yarn-site.xml` files, setting parameters for HDFS and YARN such as namenode/datano de addresses, and memory/CPU resource allocation.
   Format the HDFS file system: `hdfs namenode -format`.
   Start Hadoop services: `start-dfs.sh` and `start-yarn.sh`.
   Verify successful installation: Access the Hadoop Web management interface at `http://<your-ip>:9870` to check service status.

3. MongoDB Installation and Configuration
   Extract the MongoDB installation package: `tar -zxvf mongodb-linux-x86_64-5.0.5.tgz`.
   Configure MongoDB environment variables: Add `export PATH=$PATH:/home/user/mongodb-linux-x86_64-5.0.5/bin` to `~/.bashrc`, then run `source ~/.bashrc`.
   Create MongoDB data and log directories: `mkdir -p /data/db` and `mkdir -p /data/log`.
   Start MongoDB service: `mongod --fork --logpath /data/log/mongodb.log --dbpath /data/db`.
   Verify successful installation: Enter the MongoDB command-line interface with `mongo` and run `db.version()` to confirm normal operation.

4. Data Preparation
   Download the MovieLens dataset from the official website, selecting a complete dataset with user ratings and movie information.
   Extract the dataset: `tar -zxvf ml-25m.tar.gz`, obtaining files like `movies.csv` and `ratings.csv`.

5. Data Import to Hadoop
   Upload the dataset to HDFS: `hdfs dfs -put ml-25m /user/hadoop`, placing it in the Hadoop distributed file system for subsequent processing.

6. Data Import to MongoDB
   Enter the MongoDB command-line interface: `mongo`.
   Create databases and collections: `use movielens`, then `db.createCollection("movies")` and `db.createCollection("ratings")`.
   Import data: Use `mongoimport` to import data from `movies.csv` and `ratings.csv` into MongoDB. Example: `mongoimport --db movielens --collection movies --type csv --file /home/user/ml-25m/movies.csv --headerline`.

7. Hadoop Data Storage and Query Operations
   View the file system: `hdfs dfs -ls /user/hadoop`, listing files and folders in the specified Hadoop directory.
   Create a directory: `hdfs dfs -mkdir /user/hadoop/input`, preparing for data storage.
   Upload a file to HDFS: `hdfs dfs -put /home/user/ml-25m/movies.csv /user/hadoop/input`.
   Download a file: `hdfs dfs -get /user/hadoop/input/movies.csv /home/user`, verifying data accessibility and integrity.

8. MongoDB Data Query Operations
   Query all documents: `db.movies.find()`, retrieving all documents in the movies collection.
   Query specific fields: `db.movies.find({}, {title: 1, genres: 1})`, obtaining only movie titles and genres.
   Filter data: `db.movies.find({genres: "Comedy"})`, finding comedy movies.
   Sort data: `db.movies.find().sort({title: 1})`, sorting results by movie title alphabetically.

9. Performance Comparison and Analysis
   Prepare test data: Select a data sample from the MovieLens dataset, e.g., 100,000 user ratings.
   Hadoop query performance test: Execute queries on Hadoop, recording start and end times to calculate query duration. Use HDFS tools or simple MapReduce programs to observe response times with large datasets.
   MongoDB query performance test: Perform similar queries on MongoDB, recording operation times to compare query speeds between the two systems.
   Data volume expansion: Increase test data scale (e.g., to 1 million, 10 million records), repeating tests to observe performance trends and analyze scalability in big data processing.
   Analyze results: Create performance comparison charts to visually display query performance of Hadoop and MongoDB at different data volumes. Consider architectural features and use cases to explain performance differences, such as Hadoop's suitability for batch processing and MongoDB's efficiency in rapid queries.

10. Data Preprocessing
    Preprocess the ratings.csv file in the MovieLens dataset, extracting user IDs, movie IDs, and ratings to create input data suitable for collaborative filtering algorithms. Use Python scripts or Hive SQL for data cleaning and transformation.

11. MapReduce Program Writing
    Write Mapper and Reducer code for computing user similarities:
        Mapper: Read input data, partition by user ID, and output user-rating pairs (e.g., `<user ID, (movie ID, rating)>`).
        Reducer: Calculate user similarities using methods like cosine similarity or Pearson correlation, outputting the top N similar users as neighbor candidates.
    Write Mapper and Reducer code for merging similarity results:
        Mapper: Read user similarity results from each partition, outputting `<user ID, (neighbor user ID, similarity)>`.
        Reducer: Merge results for the same user ID, selecting the top K similar neighbors to form the final user neighborhood set.
    Write Mapper and Reducer code for generating recommendation lists:
        Mapper: Based on user neighborhoods and original ratings, output user-neighbor movie rating information (e.g., `<user ID, (movie ID, neighbor rating, similarity)>`).
        Reducer: Calculate weighted average ratings for each user, generating recommendation lists sorted by rating, selecting the top M movies.

12. Hadoop Task Submission and Execution
    Package the MapReduce program into a JAR file, ensuring it runs on the Hadoop cluster.
    Upload preprocessed data to Hadoop's HDFS, specifying a path like `/user/hadoop/input/recommendation`.
    Submit MapReduce jobs to the Hadoop cluster:
        (1) Submit the user similarity computation job: `hadoop jar recommendation.jar SimilarityCalculator /user/hadoop/input/recommendation /user/hadoop/output/similarity`.
        (2) Submit the similarity result merging job: `hadoop jar recommendation.jar SimilarityMerger /user/hadoop/output/similarity /user/hadoop/output/neighborhood`.
        (3) Submit the recommendation list generation job: `hadoop jar recommendation.jar RecommendationGenerator /user/hadoop/output/neighborhood /user/hadoop/output/recommendation`.
        (4) Monitor job execution: Use the Hadoop Web interface or command-line tools to track progress, resource usage, and address potential issues like task failures or resource shortages.

13. Result Evaluation and Analysis
    Obtain recommendation results from HDFS: `hdfs dfs -get /user/hadoop/output/recommendation /home/user`.
    Prepare a test dataset: Set aside a portion of the MovieLens data as a test set with real user ratings.
    Calculate accuracy and recall:
        Accuracy: Proportion of recommended movies in the test set to total recommended movies.
        Recall: Proportion of test set movies in the recommended list to total test set movies.
    Compare parallel and non-parallel recommendation algorithms: Run the same user-based collaborative filtering algorithm in a single-machine environment (e.g., using Python's scikit-learn), calculate its accuracy and recall, and contrast with the parallel algorithm's results. Analyze the parallel algorithm's performance and efficiency advantages in large-scale data processing, noting any potential errors or differences.

14. Experiment Summary and Report Writing
    Summarize the experiment: Review issues encountered and solutions during installation, data import, MapReduce debugging, data conversion, and job submission.
    Write the experiment report: Detail objectives, content, steps, results, and performance analysis. Include data, charts, and in-depth discussions on Hadoop and MongoDB's roles in big data storage/query, and provide suggestions for experimental design improvements.
