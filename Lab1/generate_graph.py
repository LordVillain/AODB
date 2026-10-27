import random

num_vertices = 1000
num_edges = 6000

edges = set()
while len(edges) < num_edges:
    src = random.randint(0, num_vertices - 1)
    if random.random() < 0.3:
        dst = random.randint(0, 15)
    else:
        dst = random.randint(0, num_vertices - 1)
    
    if src != dst:
        edges.add((src, dst))

with open("graph_edges.txt", "w") as f:
    for src, dst in edges:
        f.write("{}\t{}\n".format(src, dst))

print("Created graph_edges.txt with {} edges.".format(len(edges)))