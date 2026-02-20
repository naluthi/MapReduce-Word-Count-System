#!/usr/bin/env python3
"""
Multithreaded MapReduce Word Count

This script counts occurrences of each word (case-insensitive) in a text file,
using a MapReduce approach with Python threading.

Usage:
  python3 wordcount_mt.py --file data.txt --threads 4

If --file is omitted, you’ll be prompted to enter it interactively.
"""

import re
import time
import argparse
import threading
from collections import Counter



#─── Map Thread ──────────────────────────────────────────────────────────────
class MapThread(threading.Thread):
    def __init__(self, chunk_lines, result_list, lock, thread_id):
        super().__init__(name=f"Map-{thread_id}")
        # join the lines into one string for tokenization
        self.text = "\n".join(chunk_lines).lower()
        self.result_list = result_list
        self.lock = lock

    def run(self):
        # tokenize on word characters
        words = re.findall(r"\w+", self.text)
        local_counts = Counter(words)
        # store this thread's result
        with self.lock:
            self.result_list.append(local_counts)


#─── Reduce Thread ────────────────────────────────────────────────────────────
class ReduceThread(threading.Thread):
    def __init__(self, key, partial_counters, result_dict, lock):
        super().__init__(name=f"Reduce-{key}")
        self.key = key
        self.partials = partial_counters
        self.result_dict = result_dict
        self.lock = lock

    def run(self):
        total = sum(d.get(self.key, 0) for d in self.partials)
        with self.lock:
            self.result_dict[self.key] = total


#─── Combiner Thread ──────────────────────────────────────────────────────────
class CombinerThread(threading.Thread):
    def __init__(self, reduced_dict, result_list, lock):
        super().__init__(name="Combiner")
        self.reduced = reduced_dict
        self.result_list = result_list
        self.lock = lock

    def run(self):
        # produce a sorted list of (word, count) descending
        sorted_pairs = sorted(
            self.reduced.items(),
            key=lambda kv: kv[1],
            reverse=True
        )
        with self.lock:
            self.result_list.extend(sorted_pairs)


#─── Single-threaded word count ───────────────────────────────────────────────
def single_thread_count(lines):
    text = "\n".join(lines).lower()
    words = re.findall(r"\w+", text)
    return Counter(words)


#─── Multithreaded Map-Reduce-Combiner ────────────────────────────────────────
def multithreaded_count(lines, num_threads):
    # 1) Map phase
    chunk_size = (len(lines) + num_threads - 1) // num_threads
    partial_maps = []
    map_lock = threading.Lock()
    map_threads = []

    for i in range(num_threads):
        chunk = lines[i*chunk_size : (i+1)*chunk_size]
        t = MapThread(chunk, partial_maps, map_lock, i)
        map_threads.append(t)
        t.start()
    for t in map_threads:
        t.join()

    # 2) Shuffle: collect all unique words
    all_counters = partial_maps
    keys = set().union(*all_counters) if all_counters else set()

    # 3) Reduce phase
    reduced = {}
    reduce_lock = threading.Lock()
    reduce_threads = []
    for key in keys:
        t = ReduceThread(key, all_counters, reduced, reduce_lock)
        reduce_threads.append(t)
        t.start()
    for t in reduce_threads:
        t.join()

    # 4) Combiner phase
    combined_list = []
    comb_lock = threading.Lock()
    combiner = CombinerThread(reduced, combined_list, comb_lock)
    combiner.start()
    combiner.join()

    # build final dict (preserves sorted order in Python 3.7+)
    return dict(combined_list)


def read_lines(filepath):
    """
    Attempts to read a file as UTF-8; if that fails, falls back to Latin-1.
    Returns a list of lines without trailing newlines.
    """
    try:
        with open(filepath, encoding='utf-8') as rf:
            return [line.rstrip('\n') for line in rf]
    except UnicodeDecodeError:
        with open(filepath, encoding='latin-1') as rf:
            return [line.rstrip('\n') for line in rf]

def main():
    p = argparse.ArgumentParser(
        description="Word-Count: single vs Map/Reduce/Combiner multithread"
    )
    p.add_argument("--file", "-f", required=True, help="Path to text file")
    p.add_argument("--threads", "-t", type=int, default=2, help="Number of Map threads")
    args = p.parse_args()

    # Use robust file reading
    lines = read_lines(args.file)

    print(f"\nFile: {args.file!r}, Lines: {len(lines)}, Threads: {args.threads}\n")

    # Single-thread
    t0 = time.perf_counter()
    single_counts = single_thread_count(lines)
    t1 = time.perf_counter()
    print(f"Single-thread: {t1-t0:.3f}s, unique words: {len(single_counts)}")

    # Multithreaded Map/Reduce/Combiner
    t2 = time.perf_counter()
    multi_counts = multithreaded_count(lines, args.threads)
    t3 = time.perf_counter()
    print(f"Multi-thread : {t3-t2:.3f}s, unique words: {len(multi_counts)}\n")

    # Verify correctness
    if dict(single_counts) == multi_counts:
        print("✅ Counts match single-threaded result.")
    else:
        print("⚠️ Counts differ! (unexpected)")

    # Top 20
    print("\nTop 20 words:")
    for idx, (w, c) in enumerate(multi_counts.items(), 1):
        print(f"{idx:2d}. {w!r}: {c}")
        if idx >= 25:
            break


if __name__ == "__main__":
    main()
