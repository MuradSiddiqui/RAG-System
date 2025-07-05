<<<<<<< HEAD
# RAG System for Double Matching

A Retrieval-Augmented Generation (RAG) system that combines semantic search and structured queries to find and match doubles based on various criteria. The system integrates Neo4j for graph database operations and Qdrant for vector similarity search.

## 🌟 Features

- **Doubles Management**
  - Store and manage doubles with profile attributes in Neo4j
  - Link doubles with various product nodes (Property, Insurance, BankAccount, etc.)
  - Support for complex product-based filtering and value range queries

- **AI-Powered Search**
  - Natural language query parsing using LLaMA model (via Groq API)
  - Conversion of free-form text to structured JSON queries
  - Semantic search capabilities using SentenceTransformers and Qdrant

- **Hybrid Search System**
  - Structured queries through Neo4j Cypher
  - Vector similarity search via Qdrant
  - Parallel execution of semantic and structured searches

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Neo4j Database
- Qdrant Vector Database
- Groq API access

### Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd RAG_System
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
# Create a .env file with:
NEO4J_URI=your_neo4j_uri
NEO4J_USER=your_neo4j_user
NEO4J_PASSWORD=your_neo4j_password
GROQ_API_KEY=your_groq_api_key
QDRANT_URL=your_qdrant_url
QDRANT_API_KEY=your_qdrant_api_key
```

## 🏗️ Project Structure

- `app.py` - Main application file with Streamlit UI
- `neo4j_connector.py` - Neo4j database connection handler
- `neo4j_product_queries.py` - Product-related Neo4j queries
- `neo4j_query_builder.py` - Cypher query builder
- `qdrantnew.py` - Qdrant vector database integration
- `query_parser_llama_groq.py` - Natural language query parser using LLaMA

## 💻 Usage

1. Start the application:
```bash
streamlit run app.py
```

2. Access the web interface at `http://localhost:8501`

3. Enter your search query in natural language or use structured filters

### Example Queries

```python
# Natural Language Query
"Find doubles who own property worth more than 200,000 and have savings accounts"

# Structured Query
{
    "products": {
        "Property": {"min": 200000},
        "BankAccount": {"exists": true}
    }
}
```

## 🔄 Data Flow

1. User inputs query (natural language or structured)
2. If natural language:
   - Query is parsed by LLaMA model into structured format
3. System executes:
   - Structured search in Neo4j
   - Semantic search in Qdrant
4. Results are combined and presented to user

## 🛠️ Current Status

The system is currently in a proof-of-concept stage with:
- Core infrastructure (Neo4j + Qdrant) operational
- Basic query processing pipeline implemented
- Parallel execution of semantic and structured searches
- Active development and experimentation ongoing

## 📊 Visualization

Currently supports:
- Structured query results viewing in Neo4j browser
- Semantic search results through Qdrant interface
- Basic web UI for query input and results display


## 👥 Contributor

- Muhammad Murad Siddiqui

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 
=======
# RAG-System
>>>>>>> 90c4c2625d7657fdc39958fa29fb5665f6f87d86
