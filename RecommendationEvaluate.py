import pandas as pd
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split


class RecommendationEvaluator:
    def __init__(self, original_ratings_file, recommendations_file, test_ratio=0.2, min_rating_threshold=4.0):
        """
        Initialize the evaluator

        Args:
            original_ratings_file: Path to original MovieLens ratings CSV
            recommendations_file: Path to recommendation results from HDFS
            test_ratio: Proportion of data to use for testing
            min_rating_threshold: Minimum rating to consider as "liked" in test set
        """
        self.original_ratings_file = original_ratings_file
        self.recommendations_file = recommendations_file
        self.test_ratio = test_ratio
        self.min_rating_threshold = min_rating_threshold

        # Data containers
        self.original_ratings = None
        self.train_data = None
        self.test_data = None
        self.recommendations = None
        self.user_test_movies = defaultdict(set)
        self.user_recommendations = defaultdict(list)

    def load_original_data(self):
        """Load and prepare original MovieLens rating data"""
        print("Loading original ratings data...")

        # Load the original ratings
        self.original_ratings = pd.read_csv(self.original_ratings_file)

        # Ensure column names are correct
        if 'userId' not in self.original_ratings.columns:
            self.original_ratings.columns = ['userId', 'movieId', 'rating', 'timestamp']

        print(f"Loaded {len(self.original_ratings)} ratings from {self.original_ratings['userId'].nunique()} users")
        print(f"Rating distribution:\n{self.original_ratings['rating'].value_counts().sort_index()}")

    def create_train_test_split(self):
        """Split data into train and test sets"""
        print(f"\nSplitting data into train ({1 - self.test_ratio:.0%}) and test ({self.test_ratio:.0%}) sets...")

        # Group by user and split each user's ratings
        train_list = []
        test_list = []

        for user_id in self.original_ratings['userId'].unique():
            user_ratings = self.original_ratings[self.original_ratings['userId'] == user_id]

            if len(user_ratings) >= 5:  # Only split users with at least 5 ratings
                train_user, test_user = train_test_split(
                    user_ratings,
                    test_size=self.test_ratio,
                    random_state=42,
                    stratify=None
                )
                train_list.append(train_user)
                test_list.append(test_user)
            else:
                # Put all ratings in train set for users with few ratings
                train_list.append(user_ratings)

        self.train_data = pd.concat(train_list, ignore_index=True)
        self.test_data = pd.concat(test_list, ignore_index=True)

        print(f"Train set: {len(self.train_data)} ratings")
        print(f"Test set: {len(self.test_data)} ratings")

        # Create test set dictionary (only high-rated movies)
        test_high_rated = self.test_data[self.test_data['rating'] >= self.min_rating_threshold]
        for _, row in test_high_rated.iterrows():
            self.user_test_movies[row['userId']].add(row['movieId'])

        print(f"Users with test movies (rating >= {self.min_rating_threshold}): {len(self.user_test_movies)}")

    def load_recommendations(self):
        """Load recommendation results from HDFS output"""
        print(f"\nLoading recommendations from {self.recommendations_file}...")

        with open(self.recommendations_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # Parse format: userId<tab>movieId1:score1,movieId2:score2,...
                parts = line.split('\t')
                if len(parts) == 2:
                    user_id = int(parts[0])
                    recommendations_str = parts[1]

                    # Parse movie recommendations
                    movie_recommendations = []
                    for movie_score in recommendations_str.split(','):
                        if ':' in movie_score:
                            movie_id, score = movie_score.split(':')
                            movie_recommendations.append((int(movie_id), float(score)))

                    self.user_recommendations[user_id] = movie_recommendations

        print(f"Loaded recommendations for {len(self.user_recommendations)} users")

    def calculate_metrics(self):
        """Calculate accuracy and recall for each user and overall"""
        print("\nCalculating accuracy and recall metrics...")

        user_metrics = []
        total_recommended = 0
        total_hits = 0
        total_test_movies = 0
        total_recalled = 0

        for user_id in self.user_recommendations:
            if user_id in self.user_test_movies:
                # Get recommended movies for this user
                recommended_movies = {movie_id for movie_id, _ in self.user_recommendations[user_id]}
                test_movies = self.user_test_movies[user_id]

                # Calculate hits (intersection)
                hits = recommended_movies.intersection(test_movies)

                # Calculate accuracy and recall for this user
                accuracy = len(hits) / len(recommended_movies) if recommended_movies else 0
                recall = len(hits) / len(test_movies) if test_movies else 0

                user_metrics.append({
                    'userId': user_id,
                    'recommended_count': len(recommended_movies),
                    'test_count': len(test_movies),
                    'hits': len(hits),
                    'accuracy': accuracy,
                    'recall': recall
                })

                # Update totals
                total_recommended += len(recommended_movies)
                total_hits += len(hits)
                total_test_movies += len(test_movies)
                total_recalled += len(hits)

        # Calculate overall metrics
        overall_accuracy = total_hits / total_recommended if total_recommended > 0 else 0
        overall_recall = total_recalled / total_test_movies if total_test_movies > 0 else 0

        # Convert to DataFrame for easier analysis
        metrics_df = pd.DataFrame(user_metrics)

        return metrics_df, overall_accuracy, overall_recall

    def generate_report(self):
        """Generate comprehensive evaluation report"""
        print("=" * 60)
        print("MOVIE RECOMMENDATION EVALUATION REPORT")
        print("=" * 60)

        # Calculate metrics
        user_metrics, overall_accuracy, overall_recall = self.calculate_metrics()

        if len(user_metrics) == 0:
            print("No users found with both recommendations and test data!")
            return

        # Overall Statistics
        print(f"\n OVERALL METRICS")
        print(f"{'=' * 40}")
        print(f"Overall Accuracy: {overall_accuracy:.2%}")
        print(f"Overall Recall: {overall_recall:.2%}")
        print(f"Users evaluated: {len(user_metrics)}")

        # Detailed Statistics
        print(f"\n DETAILED STATISTICS")
        print(f"{'=' * 40}")
        print(
            f"Average accuracy per user: {user_metrics['accuracy'].mean():.2%} ± {user_metrics['accuracy'].std():.2%}")
        print(f"Average recall per user: {user_metrics['recall'].mean():.2%} ± {user_metrics['recall'].std():.2%}")
        print(f"Median accuracy: {user_metrics['accuracy'].median():.2%}")
        print(f"Median recall: {user_metrics['recall'].median():.2%}")

        # Distribution analysis
        print(f"\n ACCURACY DISTRIBUTION")
        print(f"{'=' * 40}")
        accuracy_bins = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
        accuracy_counts = pd.cut(user_metrics['accuracy'], bins=accuracy_bins).value_counts().sort_index()
        for interval, count in accuracy_counts.items():
            percentage = count / len(user_metrics) * 100
            print(f"{interval}: {count} users ({percentage:.1f}%)")

        print(f"\n RECALL DISTRIBUTION")
        print(f"{'=' * 40}")
        recall_counts = pd.cut(user_metrics['recall'], bins=accuracy_bins).value_counts().sort_index()
        for interval, count in recall_counts.items():
            percentage = count / len(user_metrics) * 100
            print(f"{interval}: {count} users ({percentage:.1f}%)")

        # Best and worst performers
        print(f"\n TOP PERFORMERS (by accuracy)")
        print(f"{'=' * 40}")
        top_accuracy = user_metrics.nlargest(5, 'accuracy')[
            ['userId', 'accuracy', 'recall', 'hits', 'recommended_count', 'test_count']]
        print(top_accuracy.to_string(index=False, float_format='%.2f'))

        print(f"\n CHALLENGING CASES (lowest accuracy)")
        print(f"{'=' * 40}")
        bottom_accuracy = user_metrics.nsmallest(5, 'accuracy')[
            ['userId', 'accuracy', 'recall', 'hits', 'recommended_count', 'test_count']]
        print(bottom_accuracy.to_string(index=False, float_format='%.2f'))

        return user_metrics, overall_accuracy, overall_recall

    def create_visualizations(self, user_metrics):
        """Create visualizations for the evaluation results"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))

        # Accuracy histogram
        axes[0, 0].hist(user_metrics['accuracy'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].set_title('Distribution of Accuracy Scores')
        axes[0, 0].set_xlabel('Accuracy')
        axes[0, 0].set_ylabel('Number of Users')
        axes[0, 0].axvline(user_metrics['accuracy'].mean(), color='red', linestyle='--',
                           label=f'Mean: {user_metrics["accuracy"].mean():.2%}')
        axes[0, 0].legend()

        # Recall histogram
        axes[0, 1].hist(user_metrics['recall'], bins=20, alpha=0.7, color='lightcoral', edgecolor='black')
        axes[0, 1].set_title('Distribution of Recall Scores')
        axes[0, 1].set_xlabel('Recall')
        axes[0, 1].set_ylabel('Number of Users')
        axes[0, 1].axvline(user_metrics['recall'].mean(), color='red', linestyle='--',
                           label=f'Mean: {user_metrics["recall"].mean():.2%}')
        axes[0, 1].legend()

        # Scatter plot: Accuracy vs Recall
        axes[1, 0].scatter(user_metrics['accuracy'], user_metrics['recall'], alpha=0.6, color='green')
        axes[1, 0].set_title('Accuracy vs Recall')
        axes[1, 0].set_xlabel('Accuracy')
        axes[1, 0].set_ylabel('Recall')
        axes[1, 0].plot([0, 1], [0, 1], 'r--', alpha=0.5, label='Perfect correlation')
        axes[1, 0].legend()

        # Number of hits distribution
        axes[1, 1].hist(user_metrics['hits'], bins=range(0, max(user_metrics['hits']) + 2), alpha=0.7, color='orange',
                        edgecolor='black')
        axes[1, 1].set_title('Distribution of Recommendation Hits')
        axes[1, 1].set_xlabel('Number of Hits')
        axes[1, 1].set_ylabel('Number of Users')
        axes[1, 1].axvline(user_metrics['hits'].mean(), color='red', linestyle='--',
                           label=f'Mean: {user_metrics["hits"].mean():.1f}')
        axes[1, 1].legend()

        plt.tight_layout()
        plt.savefig('recommendation_evaluation.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Correlation matrix
        plt.figure(figsize=(10, 8))
        correlation_data = user_metrics[['accuracy', 'recall', 'hits', 'recommended_count', 'test_count']]
        correlation_matrix = correlation_data.corr()
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, square=True)
        plt.title('Correlation Matrix of Evaluation Metrics')
        plt.tight_layout()
        plt.savefig('correlation_matrix.png', dpi=300, bbox_inches='tight')
        plt.show()

    def run_evaluation(self):
        """Run complete evaluation pipeline"""
        try:
            # Load data
            self.load_original_data()
            self.create_train_test_split()
            self.load_recommendations()

            # Generate report
            user_metrics, overall_accuracy, overall_recall = self.generate_report()

            # Create visualizations
            print(f"\n Generating visualizations...")
            self.create_visualizations(user_metrics)

            # Save detailed results
            user_metrics.to_csv('user_evaluation_metrics.csv', index=False)
            print(f"\n Detailed user metrics saved to 'user_evaluation_metrics.csv'")

            return user_metrics, overall_accuracy, overall_recall

        except Exception as e:
            print(f"Error during evaluation: {str(e)}")
            raise


def sample_evaluation():
    """Sample usage of the evaluator"""
    evaluator = RecommendationEvaluator(
        original_ratings_file = '/Users/lemonseed/Documents/homework/cloud_computing/sessions/user_recommend/ml-latest-small/ratings.csv',  # Original MovieLens ratings
        recommendations_file = '/Users/lemonseed/Documents/homework/cloud_computing/sessions/session2/recommendation_list.txt',  # HDFS output
        test_ratio = 0.6,  # 20% for testing
        min_rating_threshold = 1.0  # Consider ratings >= 4.0 as "liked"
    )

    return evaluator.run_evaluation()


if __name__ == "__main__":
    results = sample_evaluation()