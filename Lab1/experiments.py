# -*- coding: utf-8 -*-
import sys
import time
from pyspark import SparkConf, SparkContext

def time_it(lines_rdd):
    start = time.time()
    # Action .count() forces Spark to execute the pipeline
    lines_rdd.flatMap(lambda line: line.strip().split(" ")) \
             .map(lambda w: (w, 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .count()
    return int((time.time() - start) * 1000)

def heavy_transform(w):
    h = hash(w)
    for i in range(300):
        h = (h * 31 + i) & 0xFFFFFFFF
    return w

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: experiments.py <input> <mode>")
        sys.exit(1)

    input_path = sys.argv[1]
    mode = sys.argv[2]

    conf = SparkConf().setAppName("PartitioningExperiment")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Warmup JVM and Spark
    time_it(sc.textFile(input_path))

    if mode == "scan":
        partition_counts = [1, 2, 4, 8, 16, 32, 128]
        for n in partition_counts:
            lines = sc.textFile(input_path, n)
            t = time_it(lines)
            print("RESULT: partitions={} actual={} time_ms={}".format(n, lines.getNumPartitions(), t))

    elif mode == "repartition-coalesce":
        targets = [1, 2, 4, 8, 16]
        base = sc.textFile(input_path)
        print("RESULT: base actual={} time_ms={}".format(base.getNumPartitions(), time_it(base)))

        for n in targets:
            rep = base.repartition(n)
            print("RESULT: repartition({}) actual={} time_ms={}".format(n, rep.getNumPartitions(), time_it(rep)))
            
            coa = base.coalesce(n)
            print("RESULT: coalesce({}) actual={} time_ms={}".format(n, coa.getNumPartitions(), time_it(coa)))

    elif mode == "persist":
        def words_rdd():
            return sc.textFile(input_path).flatMap(lambda l: l.strip().split(" ")).map(heavy_transform)

        # Without cache
        not_cached = words_rdd()
        t1 = time.time()
        not_cached.count()
        run1 = int((time.time() - t1) * 1000)

        t2 = time.time()
        not_cached.count()
        run2 = int((time.time() - t2) * 1000)
        print("RESULT: no_cache run1_ms={} run2_ms={}".format(run1, run2))

        # With cache
        cached = words_rdd().persist()
        t1 = time.time()
        cached.count()
        run1 = int((time.time() - t1) * 1000)

        t2 = time.time()
        cached.count()
        run2 = int((time.time() - t2) * 1000)
        print("RESULT: with_cache run1_ms={} run2_ms={}".format(run1, run2))

    sc.stop()