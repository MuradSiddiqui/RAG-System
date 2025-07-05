# This file is for the for the connection between neo4j and UI 
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load .env values
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test1234")

# Initialize Neo4j driver
def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def convert_neo4j_types(obj):
    """Convert Neo4j types to Python native types"""
    from neo4j.graph import Node, Relationship
    
    if isinstance(obj, Node):
        # Convert Node to dictionary with its properties
        result = dict(obj)
        # Add metadata if needed
        result['_neo4j_labels'] = list(obj.labels)
        result['_neo4j_element_id'] = obj.element_id
        return result
    elif isinstance(obj, Relationship):
        # Convert Relationship to dictionary with its properties
        result = dict(obj)
        result['_neo4j_type'] = obj.type
        result['_neo4j_element_id'] = obj.element_id
        return result
    elif isinstance(obj, dict):
        # Recursively process dictionary values
        return {k: convert_neo4j_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        # Recursively process list/tuple items
        return [convert_neo4j_types(item) for item in obj]
    else:
        # Return as-is for primitive types
        return obj

# Run any Cypher query and return list of results
def run_query(cypher_query):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher_query)
        records = []
        for record in result:
            # Convert the record to a dictionary and process Neo4j types
            record_dict = dict(record)
            converted_record = convert_neo4j_types(record_dict)
            records.append(converted_record)
        return records
