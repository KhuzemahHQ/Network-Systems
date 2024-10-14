from networkx import Graph,bellman_ford_path


global_view = Graph(undirected = True)

global_view.add_edge("A","B", weight=2)

global_view.add_edge("D","C",weight=1)

global_view.add_edge("B","C",weight=8)

x = bellman_ford_path(global_view,"A","C")

print(x)