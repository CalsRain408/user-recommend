import csv
import random
import sys
from shutil import copyfile

import pandas as pd


def process_ratings_data(input_csv, output_config):
    with open(input_csv) as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip the topic
        total_rows = sum(1 for _ in reader)

    for output_file, target_size in output_config:
        print(f"Processing {output_file} (target: {target_size} rows)...")

        if target_size == total_rows:
            copyfile(input_csv, output_file)
            print("Created by direct copy")
            continue
        elif target_size > total_rows:
            print(f"Error: Target size {target_size} exceeds total rows {total_rows}")
            continue

        selected_indices = set(random.sample(range(total_rows), target_size))
        with open(input_csv, 'r') as fin, open(output_file, 'w', newline='') as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            writer.writerow(next(reader))

            written = 0
            for idx, row in enumerate(reader):
                if idx in selected_indices:
                    writer.writerow(row)
                    written += 1
                    if written % 10000 == 0:
                        print(f"Written {written}/{target_size} rows...")
            print(f"Finished writing {written} rows to {output_file}")


if __name__ == "__main__":
    input_csv = '/Users/lemonseed/Documents/homework/cloud_computing/ml-latest/ratings.csv'
    output_config = [
        ("ratings_100k.csv", 100000),
        ("ratings_1m.csv", 1000000),
        ("ratings_10m.csv", 10000000)
    ]

    process_ratings_data(input_csv, output_config)