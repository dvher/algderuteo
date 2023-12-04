import json
import psycopg2
import shapely.wkt
import networkx as nx
import math
import dotenv

dotenv.load_dotenv()

# Calculate the reliability of a link based on its distance to the epicenter of the earthquake
def seismic_reliability(distance, decay_factor=0.1):
    decay_factor = max(0, decay_factor)
    reliability = math.exp(-decay_factor * distance)
    reliability = max(0, min(1, reliability))
    return reliability

db_params = {
    'dbname': dotenv.get('DB_NAME'),
    'user': dotenv.get('DB_USER'),
    'password': dotenv.get('DB_PASS'),
    'host': dotenv.get('DB_HOST'),
    'port': dotenv.get('DB_PORT'),
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

query = f"""
    SELECT ogc_fid, source, target, ST_AsText(wkb_geometry) FROM {dotenv.get('GEOM_TABLE')};
"""

cursor.execute(query)

graph = nx.DiGraph()

with open('query.geojson', 'r') as file:
    earthquake_data = json.load(file)

for row in cursor.fetchall():
    edge_id, source, target, geom_text = row
    geom = shapely.wkt.loads(geom_text)
    
    center_point = geom.centroid.coords[0]
    
    i, j = int(center_point[0]), int(center_point[1])
    
    magnitude = float(earthquake_data['features'][0]['properties']['mag'])

    epicenter_i, epicenter_j = int(earthquake_data['features'][0]['geometry']['coordinates'][0]), int(earthquake_data['features'][0]['geometry']['coordinates'][1])
    
    distance_to_epicenter = math.sqrt((i - epicenter_i)**2 + (j - epicenter_j)**2)
    
    probability_of_failure = seismic_reliability(distance_to_epicenter, decay_factor=0.1)
    
    graph.add_edge(source, target, id=edge_id, probability_of_failure=probability_of_failure)

cursor.close()
conn.close()

for edge in graph.edges(data=True):
    print(edge)

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

for edge in graph.edges(data=True):
    source, target, properties = edge
    edge_id = properties['id']
    probability_of_failure = properties['probability_of_failure']
    query = f"""
        UPDATE {dotenv.get('GEOM_TABLE')}
        SET reliability = {probability_of_failure}
        WHERE ogc_fid = {edge_id};
    """
    cursor.execute(query)

conn.commit()

cursor.close()
conn.close()