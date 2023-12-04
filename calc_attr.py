import psycopg2
import shapely.wkt
import networkx as nx
from math import comb
import dotenv
import os

dotenv.load_dotenv()

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

query = f"""
    SELECT ogc_fid, source, target, ST_AsText(wkb_geometry), reliability FROM {os.getenv('GEOM_TABLE')};
"""

cursor.execute(query)

graph = nx.DiGraph()

# Add edges for evety row in the table
for row in cursor.fetchall():
    edge_id, source, target, geom_text, reliability = row
    
    geom = shapely.wkt.loads(geom_text)
    graph.add_edge(source, target, id=edge_id, failure_probability=1-reliability)

cursor.close()
conn.close()

total_failure_probability = 0
num_pairs = comb(len(graph.nodes()), 2)

for source in graph.nodes():
    for target in graph.nodes():
        if source != target:
            try:
                # Given the reliability of each edge, calculate the reliability of the path
                failure_probability = nx.shortest_path_length(graph, source=source, target=target, weight='failure_probability')
                total_failure_probability += failure_probability
            except nx.NetworkXNoPath:
                pass

if num_pairs > 0:
    attr = total_failure_probability / num_pairs
    print(f"Average Two-Terminal Reliability (ATTR): {attr}")
else:
    print("No pairs of terminals found for reliability calculation.")
