# This file is for neo4j ingestion, and i have loaded extended doubles
import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, ConstraintError
import spacy
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load spaCy models
nlp_en = spacy.load("en_core_web_sm")
nlp_de = spacy.load("de_core_news_sm")

# Load environment variables
load_dotenv()

# Config
CSV_PATH = "/Users/muradsiddiqui/Desktop/dataset/RAG_System/2000K_final_extended_double_data.csv"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test1234")

class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str):
        self._driver = None
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            # Test the connection
            self._driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j database")
        except ServiceUnavailable as e:
            logger.error(f"Failed to connect to Neo4j database: {e}")
            raise
        except Exception as e:
            logger.error(f"An error occurred while connecting to Neo4j: {e}")
            raise

    def close(self):
        if self._driver:
            self._driver.close()
            logger.info("Neo4j connection closed")

    def get_driver(self):
        return self._driver

# Negation Detection
def inject_negation(text: str, lang: str = "en") -> str:
    if not isinstance(text, str):
        return ""
    try:
        nlp = nlp_en if lang == "en" else nlp_de
        doc = nlp(text)
        negation_words = {"not", "no", "never", "none", "n't", "nicht", "kein", "keine", "keinen", "keiner", "keines"}
        negated_heads = {tok.head.i for tok in doc if tok.dep_ == "neg" or tok.text.lower() in negation_words}
        return " ".join(["NEG_" + tok.text if i in negated_heads else tok.text for i, tok in enumerate(doc)])
    except Exception as e:
        logger.error(f"Error in negation detection for text: {text[:50]}... Error: {e}")
        return text

# Product mapping with type hints
product_mapping: Dict[str, tuple[str, list[str]]] = {
    "p_i_has_occu_pension": ("OccuPension", ["b_fund_based_occu_dir_product_id", "p_pens_sav"]),
    "p_i_has_private_pension": ("PrivatePension", ["b_cldirectinsurance_product_id", "p_pens_sav"]),
    "p_i_whole_life_insur": ("Insurance", ["b_classic_basic_product_id", "p_insur_exp"]),
    "p_i_savings_for_securities": ("InvestmentAccount", ["b_fund_basic_product_id", "p_inv_sav", "p_val_investment"]),
    "p_i_homeowner": ("Property", ["b_prop_sav", "p_prop_sav", "p_prop_total_value"]),
    "p_i_has_savings_acct": ("BankAccount", ["b_holding_bank_deposits_2023", "p_bank_sav"])
}

def create_double_graph(tx, row: pd.Series) -> None:
    """
    Create or update a Double node and its relationships in Neo4j.
    
    Args:
        tx: Neo4j transaction object
        row: Pandas Series containing the double data
    """
    try:
        uid = int(row['unique_identifier']) if not pd.isnull(row['unique_identifier']) else None
        if uid is None:
            logger.warning(f"Skipping row with null unique_identifier")
            return

        # Create Double node with parameters
        double_params = {
            "id": uid,
            "description_en": row.get("description_en", ""),
            "description_de": row.get("description_de", ""),
            "neg_en": row['negation_description_en_final'],
            "neg_de": row['negation_description_de_final'],
            "has_neg_en": bool(row['has_negation_en']),
            "has_neg_de": bool(row['has_negation_de']),
            "tags_en": row.get("tags_en", ""),
            "tags_de": row.get("tags_de", ""),
            "p_age_2023": row.get("p_age_2023", 0),
            "p_i_male": row.get("p_i_male", 0),
            "p_gross_income": row.get("p_gross_income", 0),
            "p_expenses": row.get("p_expenses", 0),
            "p_bank_sav": row.get("p_bank_sav", 0),
            "p_i_homeowner": row.get("p_i_homeowner", 0)
        }

        # Create Double node
        create_double_query = """
        MERGE (d:Double {id: $id})
        SET d += $properties
        """
        tx.run(create_double_query, id=uid, properties=double_params)

        # Create Product nodes and relationships
        for flag, (label, attributes) in product_mapping.items():
            if row.get(flag, 0) == 1:
                product_params = {
                    "pid": f"{label}_{uid}",
                    "did": uid,
                    **{attr: row.get(attr, None) for attr in attributes}
                }
                
                create_product_query = f"""
                MERGE (p:{label} {{id: $pid}})
                SET p += $properties
                WITH p
                MATCH (d:Double {{id: $did}})
                MERGE (d)-[:OWNS]->(p)
                """
                tx.run(create_product_query, 
                      pid=product_params["pid"],
                      did=uid,
                      properties={attr: product_params[attr] for attr in attributes})

    except Exception as e:
        logger.error(f"Error creating graph for double {uid}: {str(e)}")
        raise

def main():
    try:
        # Initialize Neo4j connection
        neo4j_conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        driver = neo4j_conn.get_driver()

        # Load CSV
        logger.info(f"Loading CSV from {CSV_PATH}")
        df = pd.read_csv(CSV_PATH, sep=";", engine="python", on_bad_lines='skip')
        logger.info(f"Successfully loaded {len(df)} rows")

        # Apply negation
        logger.info("Applying negation detection")
        df['negation_description_en_final'] = df['description_en'].apply(lambda x: inject_negation(x, lang='en'))
        df['negation_description_de_final'] = df['description_de'].apply(lambda x: inject_negation(x, lang='de'))
        df['has_negation_en'] = df['negation_description_en_final'].str.contains("NEG_")
        df['has_negation_de'] = df['negation_description_de_final'].str.contains("NEG_")

        # Write to Neo4j
        logger.info("Starting Neo4j ingestion")
        with driver.session() as session:
            for idx, row in df.iterrows():
                try:
                    session.execute_write(create_double_graph, row)
                    if (idx + 1) % 1000 == 0:
                        logger.info(f"Processed {idx + 1} rows")
                except ConstraintError as ce:
                    logger.error(f"Constraint violation for row {idx}: {ce}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing row {idx}: {e}")
                    continue

        logger.info("Ingestion completed for Neo4j")

    except Exception as e:
        logger.error(f"An error occurred during the ingestion process: {e}")
        raise
    finally:
        if 'neo4j_conn' in locals():
            neo4j_conn.close()

if __name__ == "__main__":
    main()
