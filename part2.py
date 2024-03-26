import math
import osmnx as ox
import networkx as nx

# Specify the name of the place
place_name = "Kamppi, Helsinki, Finland"

# Fetch the road network
graph = ox.graph_from_place(place_name)

# Convert the network into GeoDataFrames
nodes, edges = ox.graph_to_gdfs(graph)

# Now, nodes and edges are GeoDataFrames that you can process with standard pandas methods


# Fetch the road network
place_name = "Kamppi, Helsinki, Finland"
graph = ox.graph_from_place(place_name)

# Get the nodes closest to two points
point1 = (60.165, 24.93)
point2 = (60.175, 24.94)
node1 = ox.get_nearest_nodes(graph, point1)
node2 = ox.get_nearest_nodes(graph, point2)

# Calculate the shortest path
path = nx.shortest_path(graph, node1, node2)

# Convert the path into a list of points
path_points = [(graph.nodes[node]['y'], graph.nodes[node]['x']) for node in path]