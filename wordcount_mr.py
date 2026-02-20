#!/usr/bin/env python3
"""
Multithreaded MapReduce Word Count

This script counts occurrences of each word (case-insensitive) in a text file,
using a MapReduce approach with Python threading.

Usage:
  python3 wordcount_mr.py --file data.txt --threads 4

If --file is omitted, you’ll be prompted to enter it interactively.
"""

import threading
import time
import argparse
import re
import json
from collections import defaultdict
from mapcount_mt_r import single_threaded_wordcount, multi_threaded_wordcount


def load_data(file_path):
    """Load all lines from the given text file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.readlines()

def single_threaded_wordcount(lines):
    """Count words in one thread."""
    start = time.time()
    counts = defaultdict(int)
    for line in lines:
        for word in re.findall(r"[A-Za-z]+", line.lower()):
            counts[word] += 1
    duration = time.time() - start
    return counts, duration

def map_task(part_lines, intermediate, index):
    """Map function: count words in a partition."""
    counts = defaultdict(int)
    for line in part_lines:
        for word in re.findall(r"[A-Za-z]+", line.lower()):
            counts[word] += 1
    intermediate[index] = counts

def reduce_task(keys, intermediate, reduced, index):
    """Reduce function: sum counts for assigned keys across all map outputs."""
    counts = defaultdict(int)
    for part_counts in intermediate:
        for key in keys:
            if key in part_counts:
                counts[key] += part_counts[key]
    reduced[index] = counts

def combiner_task(reduced, final_counts):
    """Combiner function: merge all reduced dicts into the final result."""
    for part_counts in reduced:
        for word, cnt in part_counts.items():
            final_counts[word] = final_counts.get(word, 0) + cnt

def multi_threaded_wordcount(lines, num_threads):
    """Orchestrate Map, Reduce, and Combiner threads and measure total time."""
    start = time.time()

    # 1. Partition data lines into N chunks
    total = len(lines)
    size = total // num_threads
    partitions = [
        lines[i*size : (i+1)*size if i < num_threads-1 else total]
        for i in range(num_threads)
    ]

    # 2. Launch Map threads
    intermediate = [None] * num_threads
    map_threads = []
    for i, part in enumerate(partitions):
        t = threading.Thread(target=map_task, args=(part, intermediate, i))
        map_threads.append(t)
        t.start()
    for t in map_threads:
        t.join()

    # 3. Shuffle: collect all unique words
    unique_words = set()
    for d in intermediate:
        unique_words.update(d.keys())
    sorted_keys = sorted(unique_words)

    # 4. Partition keys for Reduce
    ksize = len(sorted_keys) // num_threads
    key_groups = [
        sorted_keys[i*ksize : (i+1)*ksize if i < num_threads-1 else len(sorted_keys)]
        for i in range(num_threads)
    ]

    # 5. Launch Reduce threads
    reduced = [None] * num_threads
    reduce_threads = []
    for i, keys in enumerate(key_groups):
        t = threading.Thread(target=reduce_task, args=(keys, intermediate, reduced, i))
        reduce_threads.append(t)
        t.start()
    for t in reduce_threads:
        t.join()

    # 6. Launch Combiner thread
    final_counts = {}
    combiner = threading.Thread(target=combiner_task, args=(reduced, final_counts))
    combiner.start()
    combiner.join()

    duration = time.time() - start
    return final_counts, duration

def main():
    parser = argparse.ArgumentParser(description='MapReduce Word Count with Multithreading')
    parser.add_argument('--file', help='Path to input data file')
    parser.add_argument('--threads', type=int, default=4,
                        help='Number of threads for Map/Reduce (default: 4)')
    args = parser.parse_args()

    # Interactive fallback if no file provided
    file_path = args.file or input("Enter path to data file: ").strip()
    num_threads = args.threads

    print(f"\nSingle-threaded run on “{file_path}”...")
    lines = load_data(file_path)
    single_counts, single_time = single_threaded_wordcount(lines)
    print(f"  → Time: {single_time:.4f} seconds; unique words: {len(single_counts)}")

    print(f"\nMultithreaded MapReduce run with {num_threads} threads...")
    multi_counts, multi_time = multi_threaded_wordcount(lines, num_threads)
    print(f"  → Time: {multi_time:.4f} seconds; unique words: {len(multi_counts)}")

    # Output final counts as JSON
    print("\nFinal word counts (JSON):")
    print(json.dumps(dict(multi_counts.items())))

if __name__ == "__main__":
    main()
