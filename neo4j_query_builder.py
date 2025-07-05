# neo4j_query_builder.py

from langdetect import detect
from typing import Dict, Any, List, Optional
import re

# Product type to property mapping
PRODUCT_VALUE_MAPPING = {
    "Property": "p_prop_total_value",
    "InvestmentAccount": "p_val_investment",
    "BankAccount": "p_holding_bank_deposits_2023",
    "OccuPension": "p_pens_sav",
    "PrivatePension": "p_pens_sav",
    "Insurance": "p_insur_exp"
}

def safe_detect_lang(text: str) -> str:
    """
    Try to detect 'en' or 'de'; default to 'en' otherwise.
    """
    try:
        lang = detect(text)
        return lang if lang in ("en", "de") else "en"
    except:
        return "en"

def build_cypher_query(
    filters: dict,
    language: str = "en",
    product_map: dict = None
) -> str:
    """
    Build a Cypher query that:
      - MATCHes (d:Double)
      - MATCHes any product nodes needed (not optional for product queries)
      - WHEREs on both double‐level fields and product‐level numeric filters
      - Filters by keywords over tags_en/tags_de (but not for product-only queries)
      - Returns DISTINCT d
    """
    if product_map is None:
        product_map = PRODUCT_VALUE_MAPPING

    logical = filters.get("logical_filters", {})
    keywords = filters.get("keywords", [])

    # choose tags field
    tag_field = "tags_de" if language == "de" else "tags_en"

    # Separate product filters from double filters
    product_filters = {}
    double_filters = {}

    # Track if we have any product-related queries
    has_product_query = False

    for field, condition in logical.items():
        if not condition or str(condition).lower() in ("none", "null", ""):
            continue

        # Check if this is a product-related field
        product_type = None
        for prod_type, field_name in product_map.items():
            if field == field_name:
                product_type = prod_type
                has_product_query = True
                break

        if product_type:
            # Extract numeric value from condition
            match = re.search(r'([<>]=?|=)\s*(\d+(?:\.\d+)?)', str(condition))
            if match:
                operator, value = match.groups()
                if product_type not in product_filters:
                    product_filters[product_type] = {}
                
                if operator in ['>', '>=']:
                    product_filters[product_type]['min'] = float(value)
                elif operator in ['<', '<=']:
                    product_filters[product_type]['max'] = float(value)
                elif operator == '=':
                    product_filters[product_type]['min'] = float(value)
                    product_filters[product_type]['max'] = float(value)
        else:
            # This is a double-level filter
            double_filters[field] = condition

    # Start building the query
    match_clauses = []
    where_clauses = []

    # If we have product filters, start with those matches
    if product_filters:
        for product_type, conditions in product_filters.items():
            alias = f"p_{product_type.lower()}"
            # Use MATCH instead of OPTIONAL MATCH for product queries
            match_clauses.append(f"MATCH (d:Double)-[:OWNS]->({alias}:{product_type})")
            
            field = product_map[product_type]
            if 'min' in conditions:
                where_clauses.append(f"{alias}.{field} >= {conditions['min']}")
            if 'max' in conditions:
                where_clauses.append(f"{alias}.{field} <= {conditions['max']}")
            # Add non-null check
            where_clauses.append(f"{alias}.{field} IS NOT NULL")
    else:
        # If no product filters, start with Double match
        match_clauses.append("MATCH (d:Double)")

    # Handle double-level filters
    for field, condition in double_filters.items():
        cond = str(condition).strip().lower()
        if cond in ("true", "false"):
            # Special handling for boolean fields that are stored as integers in Neo4j
            if field == "p_i_homeowner":
                # Convert boolean to integer: true -> 1, false -> 0
                int_value = 1 if cond == "true" else 0
                where_clauses.append(f"d.{field} = {int_value}")
            else:
                # Standard boolean handling for other fields
                where_clauses.append(f"d.{field} = {cond}")
        else:
            where_clauses.append(f"d.{field} {condition}")

    # Handle keyword filters - but only if we don't have product queries OR if we have double filters too
    # For pure product queries, skip keyword filtering as it's too restrictive
    should_use_keywords = keywords and (not has_product_query or double_filters)
    
    # Skip keyword filtering for structural/logical terms that aren't interests
    structural_terms = {"property", "euros", "homeowner", "investment", "pension", "insurance", 
                       "income", "age", "savings", "expenses", "value", "worth", "money"}
    
    if should_use_keywords:
        # Filter out structural terms from keywords
        interest_keywords = [kw for kw in keywords if kw.lower() not in structural_terms]
        
        if interest_keywords:
            kw_conds = [
                f"toLower(d.{tag_field}) CONTAINS toLower('{kw}')"
                for kw in interest_keywords
            ]
            where_clauses.append("(" + " OR ".join(kw_conds) + ")")
        # If no interest keywords remain, skip keyword filtering entirely

    # Assemble the query
    query = "\n".join(match_clauses)
    if where_clauses:
        query += "\nWHERE " + " AND ".join(where_clauses)
    query += "\nRETURN DISTINCT d"

    return query
