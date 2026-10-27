import time
from pyspark import SparkConf, SparkContext

def compute_contribs(urls, rank):
    if not urls:
        return []
    return [(url, rank / float(len(urls))) for url in urls]

if __name__ == "__main__":
    conf = SparkConf().setAppName("PageRankWebUI")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    lines = sc.textFile("hdfs://namenode:9000/graph_edges.txt")
    edges = lines.map(lambda s: s.split("\t")).map(lambda parts: (int(parts[0]), int(parts[1])))

    all_vertices = sc.parallelize(range(1000))
    vertices_with_no_outgoing = all_vertices.map(lambda v: (v, []))

    grouped_edges = edges.groupByKey().mapValues(list)
    links = vertices_with_no_outgoing.union(grouped_edges).reduceByKey(lambda a, b: a + b).persist()

    ranks = links.mapValues(lambda _: 1.0)

    for i in range(2):
        links_with_ranks = links.join(ranks)
        contributions = links_with_ranks.flatMap(lambda x: compute_contribs(x[1][0], x[1][1]))
        received = contributions.reduceByKey(lambda a, b: a + b)
        ranks = all_vertices.map(lambda v: (v, 0.0)).union(received).reduceByKey(lambda a, b: a + b).mapValues(lambda s: 0.15 + 0.85 * s)

    ranks.count()

    print("Finished, open http://localhost:4040")
    print("Waiting 300 seconds for Web UI inspection...")
    time.sleep(300)

    sc.stop()
    