from typing import Dict, Any, List, Optional
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Neo4j Configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test1234")

# Product type to property mapping
PRODUCT_VALUE_MAPPING = {
    "Property": "p_prop_total_value",
    "InvestmentAccount": "p_val_investment",
    "BankAccount": "p_holding_bank_deposits_2023",
    "OccuPension": "p_pens_sav",
    "PrivatePension": "p_pens_sav",
    "Insurance": "p_insur_exp"
}

class Neo4jProductQueries:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def close(self):
        if self.driver:
            self.driver.close()

    def _build_product_query(self, product_type: str, value_condition: str) -> str:
        """
        Build a Cypher query for product-related searches
        
        Args:
            product_type: The type of product (e.g., "Property", "InvestmentAccount")
            value_condition: The condition for the value (e.g., "> 200000")
            
        Returns:
            str: The complete Cypher query
        """
        value_field = PRODUCT_VALUE_MAPPING.get(product_type)
        if not value_field:
            raise ValueError(f"Unknown product type: {product_type}")

        query = f"""
        MATCH (d:Double)-[:OWNS]->(p:{product_type})
        WHERE p.{value_field} {value_condition}
        RETURN DISTINCT d
        """
        return query

    def query_property_value(self, min_value: float = None, max_value: float = None) -> List[Dict]:
        """
        Query doubles who own properties within a value range
        """
        conditions = []
        if min_value is not None:
            conditions.append(f">= {min_value}")
        if max_value is not None:
            conditions.append(f"<= {max_value}")
            
        value_condition = " AND ".join([f"p.p_prop_total_value {cond}" for cond in conditions])
        if not value_condition:
            value_condition = "IS NOT NULL"  # Default condition if no range specified
            
        query = self._build_product_query("Property", value_condition)
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record["d"]) for record in result]

    def query_investment_value(self, min_value: float = None, max_value: float = None) -> List[Dict]:
        """
        Query doubles who have investments within a value range
        """
        conditions = []
        if min_value is not None:
            conditions.append(f">= {min_value}")
        if max_value is not None:
            conditions.append(f"<= {max_value}")
            
        value_condition = " AND ".join([f"p.p_val_investment {cond}" for cond in conditions])
        if not value_condition:
            value_condition = "IS NOT NULL"
            
        query = self._build_product_query("InvestmentAccount", value_condition)
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record["d"]) for record in result]

    def query_bank_deposits(self, min_value: float = None, max_value: float = None) -> List[Dict]:
        """
        Query doubles who have bank deposits within a value range
        """
        conditions = []
        if min_value is not None:
            conditions.append(f">= {min_value}")
        if max_value is not None:
            conditions.append(f"<= {max_value}")
            
        value_condition = " AND ".join([f"p.p_holding_bank_deposits_2023 {cond}" for cond in conditions])
        if not value_condition:
            value_condition = "IS NOT NULL"
            
        query = self._build_product_query("BankAccount", value_condition)
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record["d"]) for record in result]

    def query_pension_savings(self, min_value: float = None, max_value: float = None, pension_type: str = None) -> List[Dict]:
        """
        Query doubles who have pension savings within a value range
        pension_type can be 'OccuPension' or 'PrivatePension'
        """
        conditions = []
        if min_value is not None:
            conditions.append(f">= {min_value}")
        if max_value is not None:
            conditions.append(f"<= {max_value}")
            
        value_condition = " AND ".join([f"p.p_pens_sav {cond}" for cond in conditions])
        if not value_condition:
            value_condition = "IS NOT NULL"
            
        product_type = pension_type if pension_type in ["OccuPension", "PrivatePension"] else "OccuPension"
        query = self._build_product_query(product_type, value_condition)
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record["d"]) for record in result]

    def query_insurance_expenses(self, min_value: float = None, max_value: float = None) -> List[Dict]:
        """
        Query doubles who have insurance expenses within a value range
        """
        conditions = []
        if min_value is not None:
            conditions.append(f">= {min_value}")
        if max_value is not None:
            conditions.append(f"<= {max_value}")
            
        value_condition = " AND ".join([f"p.p_insur_exp {cond}" for cond in conditions])
        if not value_condition:
            value_condition = "IS NOT NULL"
            
        query = self._build_product_query("Insurance", value_condition)
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record["d"]) for record in result]

    def query_multiple_products(self, conditions: Dict[str, Dict[str, float]]) -> List[Dict]:
        """
        Query doubles who have multiple products meeting specific conditions
        
        Args:
            conditions: Dictionary mapping product types to their value conditions
            Example: {
                "Property": {"min": 200000},
                "InvestmentAccount": {"max": 50000},
                "BankAccount": {"min": 10000, "max": 100000}
            }
        """
        match_clauses = []
        where_clauses = []
        
        for product_type, value_range in conditions.items():
            value_field = PRODUCT_VALUE_MAPPING.get(product_type)
            if not value_field:
                continue
                
            alias = f"p_{product_type.lower()}"
            match_clauses.append(f"MATCH (d)-[:OWNS]->({alias}:{product_type})")
            
            if "min" in value_range:
                where_clauses.append(f"{alias}.{value_field} >= {value_range['min']}")
            if "max" in value_range:
                where_clauses.append(f"{alias}.{value_field} <= {value_range['max']}")
        
        query = "\n".join(match_clauses)
        if where_clauses:
            query += "\nWHERE " + " AND ".join(where_clauses)
        query += "\nRETURN DISTINCT d"
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record["d"]) for record in result]

# Example usage:
if __name__ == "__main__":
    # Example queries
    neo4j_products = Neo4jProductQueries()
    
    try:
        # Example 1: Query properties worth more than 200,000
        property_results = neo4j_products.query_property_value(min_value=200000)
        print(f"Found {len(property_results)} doubles with properties > 200,000")
        
        # Example 2: Query investments between 10,000 and 50,000
        investment_results = neo4j_products.query_investment_value(min_value=10000, max_value=50000)
        print(f"Found {len(investment_results)} doubles with investments between 10,000 and 50,000")
        
        # Example 3: Query multiple products
        multiple_conditions = {
            "Property": {"min": 200000},
            "InvestmentAccount": {"max": 50000},
            "BankAccount": {"min": 10000, "max": 100000}
        }
        multiple_results = neo4j_products.query_multiple_products(multiple_conditions)
        print(f"Found {len(multiple_results)} doubles matching multiple product conditions")
        
    finally:
        neo4j_products.close() 