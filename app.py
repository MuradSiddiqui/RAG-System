import streamlit as st
import json
import pandas as pd
from query_parser_llama_groq import parse_query
from neo4j_query_builder import build_cypher_query
from neo4j_connector import run_query
from langdetect import detect
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

# Qdrant Setup
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
COLLECTION_EN = "doubles_semantic"
COLLECTION_DE = "doubles_semantic_de"
qdrant_client = QdrantClient(
    url="https://42fb40a4-e46d-4d7f-96d3-6b4e9309cc4b.europe-west3-0.gcp.cloud.qdrant.io:6333",
    api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.a-Gbv6aEgZWAG6eoKRhaAQsklfA5Oa_gtaAcX_R9HTg",
    timeout=120.0
)
model = SentenceTransformer(EMBEDDING_MODEL)

# Page Configuration
st.set_page_config(
    page_title="RAG Search",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Header
st.title(" Unified Doubles Search Assistant")
st.markdown("---")
st.markdown("""
**Welcome to the Doubles Search System!** 
Search through our database using natural language queries. The system combines:
- **Structured filtering** through Neo4j graph database
- **Semantic search** through Qdrant vector database
""")

def safe_detect_lang(text):
    try:
        if len(text.strip()) <= 5:
            probable_german = ['gerne', 'gut', 'danke', 'bitte', 'ja', 'nein', 'nicht']
            if text.lower() in probable_german:
                return 'de'
        lang = detect(text)
        if lang not in ['en', 'de']:
            return 'en'
        return lang
    except:
        return 'en'

def search_qdrant(query, lang):
    collection = COLLECTION_DE if lang == 'de' else COLLECTION_EN
    query_vector = model.encode(query).tolist()
    results = qdrant_client.search(collection_name=collection, query_vector=query_vector, limit=5)
    return results

# Product mapping for structured filter routing
product_field_to_label = {
    "Property":          "p_prop_total_value",
    "InvestmentAccount": "p_val_investment", 
    "BankAccount":       "p_holding_bank_deposits_2023",
    "OccuPension":       "p_pens_sav",
}

# Sidebar with example queries
with st.sidebar:
    st.header(" Example Queries")
    st.markdown("**Property Queries:**")
    st.code("Find people who own a property worth more than 200000 euros")
    
    st.markdown("**Investment Queries:**")
    st.code("Find people with investment accounts worth more than 50000 euros")
    
    st.markdown("**Demographic Queries:**")
    st.code("Find doubles older than 30 who are interested in books")
    
    st.markdown("**Combined Queries:**")
    st.code("Find people over 40 who like sports and have bank deposits more than 100000 euros")
    
    st.markdown("---")
    st.markdown("Tips:")
    st.markdown("• Use natural language")
    st.markdown("• Include specific amounts for financial queries") 
    st.markdown("• Mix demographics with interests")
    st.markdown("• Supports English and German")
    st.markdown("")
    st.markdown("**Semantic Search Scores:**")
    st.markdown("🟢 Good match (>0.4)")
    st.markdown("🟡 Moderate match (>0.2)")
    st.markdown("🔴 Poor match (≤0.2)")

# Main search interface
st.markdown("###  Search Query")
user_input = st.text_input(
    "Enter your natural language query:",
    placeholder="e.g. Find people over 40 who like books and have properties worth more than 300000 euros",
    help="Use natural language to describe what you're looking for"
)

# Enable debug mode toggle
debug_mode = st.checkbox(" Enable Debug Mode", help="Show technical details and query information")

if user_input:
    # Detect language
    lang = safe_detect_lang(user_input)
    lang_display = " German" if lang == 'de' else " English"
    
    # Parse query
    with st.spinner(" Parsing your query using LLaMA..."):
        parsed = parse_query(user_input, lang=lang)

    if parsed:
        # Clean up logical filters
        if "logical_filters" in parsed:
            parsed["logical_filters"] = {
                k: v for k, v in parsed["logical_filters"].items()
                if v not in [None, "", "null", "None"]
            }

        # DEBUG SECTION (Hidden by default)
        if debug_mode:
            with st.expander(" Debug Information", expanded=False):
                st.subheader("Parsed Query")
                st.json(parsed)
                
                st.subheader("Technical Details")
                st.json({
                    "Language": lang,
                    "Product Map": product_field_to_label,
                    "Parsed Filters": parsed
                })

        # === NEO4J STRUCTURED SEARCH ===
        st.markdown("##  Structured Search Results (Neo4j)")

        
        with st.spinner("Searching Neo4j database..."):
            cypher_query = build_cypher_query(parsed, language=lang, product_map=product_field_to_label)
            
            # Always show Cypher query
            with st.expander("Generated Cypher Query", expanded=False):
                st.code(cypher_query, language="cypher")

            # Get total count
            count_query = cypher_query.replace("RETURN DISTINCT d", "RETURN count(DISTINCT d) AS total_matches")
            count_result = run_query(count_query)
            total_matches = count_result[0]["total_matches"] if count_result else 0

            # Show count query in debug mode only
            if debug_mode:
                with st.expander(" Count Query", expanded=False):
                    st.code(count_query, language="cypher")
                    st.markdown(f"**Total matches found:** {total_matches}")

        # Display results
        if total_matches > 0:
            # Get total database count for context
            total_db_query = "MATCH (d:Double) RETURN count(d) as total"
            total_db_result = run_query(total_db_query)
            total_doubles = total_db_result[0]['total'] if total_db_result else 0
            
            st.success(f"Found **{total_matches}** matches (out of {total_doubles} total doubles in database)")
            
            # Get limited results for display
            limited_query = cypher_query + "\nLIMIT 5"
            records = run_query(limited_query)
            
            if records:
                # Results summary
                cols = st.columns([3, 1])
                with cols[0]:
                    st.markdown(f"**Showing first 5 results** (out of {total_matches} matches)")
                with cols[1]:
                    # Download button
                    filtered_records = []
                    for r in records:
                        if isinstance(r, dict) and "d" in r:
                            filtered_records.append(r["d"])
                        else:
                            filtered_records.append(r if isinstance(r, dict) else {})
                    
                    df = pd.DataFrame(filtered_records)
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download CSV",
                        csv,
                        f"search_results_{len(filtered_records)}_records.csv",
                        "text/csv",
                        help=f"Download all {len(filtered_records)} displayed records"
                    )

                # Display individual results
                for idx, r in enumerate(records[:5]):
                    if isinstance(r, dict) and "d" in r:
                        props = r["d"]
                    else:
                        props = r if isinstance(r, dict) else {"error": f"Could not process record type: {type(r)}"}
                    
                    with st.expander(f" Double #{idx + 1} (ID: {props.get('id', 'N/A')})", expanded=False):
                        # Create columns for better layout
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            st.markdown("**Description (English):**")
                            st.write(props.get("description_en", "No English description available"))
                            
                            if lang == 'de' or debug_mode:
                                st.markdown("**Description (German):**")
                                st.write(props.get("description_de", "No German description available"))
                        
                        with col2:
                            st.markdown("Key Information:")
                            st.markdown(f"**ID:** {props.get('id', 'N/A')}")
                            st.markdown(f"**Age:** {props.get('p_age_2023', 'N/A')}")
                            st.markdown(f"**Gender:** {'Male' if props.get('p_i_male') == 1 else 'Female' if props.get('p_i_male') == 0 else 'N/A'}")
                            st.markdown(f"**Income:** {props.get('p_gross_income', 'N/A')} €")
                            
                            # Show tags
                            tags_en = props.get("tags_en", "")
                            if tags_en:
                                st.markdown("Tags:")
                                st.markdown(f"_{tags_en}_")
                        
                        # Show all metadata in debug mode
                        if debug_mode:
                            st.markdown(" All Metadata:")
                            filtered_props = {k: v for k, v in props.items() 
                                           if k not in ["description_en", "description_de"]}
                            st.json(filtered_props)
        else:
            st.warning("No matches found in Neo4j database")
            
            # DEBUG: Show database stats when no results
            if debug_mode:
                with st.expander(" Database Debug Information", expanded=False):
                    debug_query = "MATCH (d:Double) RETURN count(d) as total"
                    debug_result = run_query(debug_query)
                    st.markdown(f"**Total Doubles in database:** {debug_result[0]['total'] if debug_result else 0}")
                    
                    if product_field_to_label:
                        st.markdown("**Product ownership statistics:**")
                        for label, prop in product_field_to_label.items():
                            debug_query = f"MATCH (d:Double)-[:OWNS]->(p:{label}) RETURN count(DISTINCT d) as total"
                            debug_result = run_query(debug_query)
                            st.markdown(f"• Doubles with {label}: {debug_result[0]['total'] if debug_result else 0}")

        # === QDRANT SEMANTIC SEARCH ===
        st.markdown("---")
        st.markdown("##  Semantic Search Results (Qdrant)")
        
        if parsed.get("similarity_query"):
            with st.spinner(" Running semantic search..."):
                qdrant_results = search_qdrant(parsed["similarity_query"], lang)

            if qdrant_results:
                # Get total database count for context (reuse from Neo4j section if available)
                if 'total_doubles' not in locals():
                    total_db_query = "MATCH (d:Double) RETURN count(d) as total"
                    total_db_result = run_query(total_db_query)
                    total_doubles = total_db_result[0]['total'] if total_db_result else 0
                
                st.success(f"Found **{len(qdrant_results)}** semantic matches (out of {total_doubles} total doubles in database)")
                
                for idx, res in enumerate(qdrant_results):
                    with st.expander(f" Semantic Match #{idx + 1} (Score: {res.score:.4f})", expanded=False):
                        # Show relevance score
                        score_color = "🟢" if res.score > 0.4 else "🟡" if res.score > 0.2 else "🔴"
                        st.markdown(f"**{score_color} Relevance Score:** {res.score:.4f}")
                        
                        # Show description
                        description = res.payload.get("description_de") if lang == 'de' else res.payload.get("description_en")
                        st.markdown("Description:")
                        st.write(description)
                        
                        # Always show complete payload data
                        st.markdown("Complete Data:")
                        st.json(res.payload)
            else:
                st.info(" No semantic matches found")
        else:
            st.info("No semantic query generated for this search")

    else:
        st.error("Could not parse query. Please try rephrasing your request.")
        
        if debug_mode:
            st.markdown("** Suggestions:**")
            st.markdown("• Make sure to include specific criteria")
            st.markdown("• Use clear demographic or financial terms")
            st.markdown("• Check the example queries in the sidebar")

# Footer
# st.markdown("---")
# st.markdown("**🔍 Doubles Search System** | Powered by Neo4j + Qdrant + LLaMA")
