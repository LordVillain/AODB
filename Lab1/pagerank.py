import sys
from pyspark import SparkConf, SparkContext

def compute_contribs(urls, rank):
    num_urls = len(urls)
    if num_urls == 0:
        return []
    for url in urls:
        yield (url, rank / float(num_urls))

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: pagerank.py <input> <output> <numVertices> <iterations>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    num_vertices = int(sys.argv[3])
    iterations = int(sys.argv[4])

    conf = SparkConf().setAppName("PageRank")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Read edges: "src \t dst" -> (src, dst)
    lines = sc.textFile(input_path)
    edges = lines.map(lambda s: s.split("\t")).map(lambda parts: (int(parts[0]), int(parts[1])))

    all_vertices = sc.parallelize(range(num_vertices))
    vertices_with_no_outgoing = all_vertices.map(lambda v: (v, []))

    grouped_edges = edges.groupByKey().mapValues(list)
    # Combine real links with vertices that have no links
    links = vertices_with_no_outgoing.union(grouped_edges).reduceByKey(lambda a, b: a + b).persist()

    # Initial rank 1.0 for all vertices
    ranks = links.mapValues(lambda _: 1.0)

    for i in range(iterations):
        links_with_ranks = links.join(ranks)
        contributions = links_with_ranks.flatMap(lambda x: compute_contribs(x[1][0], x[1][1]))
        received = contributions.reduceByKey(lambda a, b: a + b)
        
        # Dampening formula: 0.15 + 0.85 * sum
        ranks = all_vertices.map(lambda v: (v, 0.0)).union(received).reduceByKey(lambda a, b: a + b).mapValues(lambda s: 0.15 + 0.85 * s)

    # Sort descending by rank
    sorted_ranks = ranks.sortBy(lambda x: x[1], ascending=False)
    sorted_ranks.saveAsTextFile(output_path)

    sc.stop()