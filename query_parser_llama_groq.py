# query_parser_llama_groq.py

from openai import OpenAI
from dotenv import load_dotenv
import os
import json
import re
from langdetect import detect

# Load API key from .env
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")
client = OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")

# Synonyms → your Neo4j field names
SYNONYM_MAP = {
    # Double-level
    "age": "p_age_2023",
    "income": "p_gross_income",
    "expenses": "p_expenses",
    "leisure spending": "p_leis_exp",
    "savings": "p_bank_sav",
    "bank balance": "p_bank_sav",
    "account balance": "p_bank_sav",
    # Property ownership (boolean)
    "homeowner": "p_i_homeowner",
    "property owner": "p_i_homeowner",
    "own property": "p_i_homeowner",
    "owns property": "p_i_homeowner",
    "property ownership": "p_i_homeowner",
    "home ownership": "p_i_homeowner",
    # Product-level
    "investment": "p_val_investment",
    "investment value": "p_val_investment",
    "pension": "p_pens_sav",
    "pension savings": "p_pens_sav",
    "occupational pension": "p_pens_sav",
    "insurance": "p_insur_exp",
    "insurance cost": "p_insur_exp",
    "property value": "p_prop_total_value",
    "property savings": "p_prop_total_value",
    "bank deposits": "p_holding_bank_deposits_2023",
}

# Allowed fields list
APPROVED_FIELDS = sorted(set(SYNONYM_MAP.values()))


def safe_detect_lang(text: str) -> str:
    """Return 'en' or 'de' based on the text, defaulting to 'en'."""
    try:
        lang = detect(text)
        return lang if lang in ("en", "de") else "en"
    except:
        return "en"


def validate_and_fix_age_detection(query: str, parsed_result: dict) -> dict:
    """
    Post-processing validation to catch age patterns that LLaMA might miss.
    This serves as a backup safety net for age detection.
    """
    if not parsed_result or "logical_filters" not in parsed_result:
        return parsed_result
    
    # Check if age is already detected
    if "p_age_2023" in parsed_result["logical_filters"]:
        return parsed_result
    
    # Age patterns to look for
    age_patterns = [
        r'over\s+(\d+)',
        r'above\s+(\d+)', 
        r'older\s+than\s+(\d+)',
        r'more\s+than\s+(\d+)\s+years?\s+old',
        r'>?\s*(\d+)\s+years?\s+old',
        r'under\s+(\d+)',
        r'below\s+(\d+)',
        r'younger\s+than\s+(\d+)',
        r'less\s+than\s+(\d+)\s+years?\s+old'
    ]
    
    query_lower = query.lower()
    
    for pattern in age_patterns:
        match = re.search(pattern, query_lower)
        if match:
            age_value = int(match.group(1))
            
            # Determine operator based on pattern
            if any(word in pattern for word in ['over', 'above', 'older', 'more']):
                parsed_result["logical_filters"]["p_age_2023"] = f">{age_value}"
            elif any(word in pattern for word in ['under', 'below', 'younger', 'less']):
                parsed_result["logical_filters"]["p_age_2023"] = f"<{age_value}"
            
            print(f"🔧 Post-processing: Added missing age filter p_age_2023 {parsed_result['logical_filters']['p_age_2023']}")
            break
    
    return parsed_result


def parse_query(query: str, lang: str = None) -> dict | None:
    """
    Send a GROQ/LLama prompt to extract:
      - keywords: tags-level interests
      - logical_filters: {field: operator+value}
      - similarity_query: same-language summary for semantic search
    """
    # detect language if not passed
    if lang is None:
        lang = safe_detect_lang(query)

    # instruct similarity_query language
    lang_instruction = {
        "en": "Produce the similarity_query in *English*.",
        "de": "Gib die similarity_query auf *Deutsch* aus."
    }[lang]

    # build prompt
    if lang == "de":
        prompt = f"""
Du bist ein Suchassistent für eine Graphdatenbank von Nutzerprofilen ("Doubles").
Extrahiere bitte als JSON drei Teile aus dem Query:

1. keywords: 
   Extrahiere NUR Wörter, die persönliche INTERESSEN, HOBBYS oder FREIZEITAKTIVITÄTEN darstellen.
   NIEMALS diese strukturellen/finanziellen Begriffe extrahieren: "eigentum", "investition", "pension", "versicherung", "einkommen", "geld", "euro", "ersparnisse", "ausgaben", "alter", "hausbesitzer"
   Diese Begriffe werden zu logischen Filtern, NICHT zu Keywords.
   Wenn eine Anfrage nur strukturelle Begriffe und keine Interessen enthält, gib ein leeres Keywords-Array zurück: []
   
   Beispiele:
   - "Finde Leute die Sport und Reisen mögen" → ["sport", "reisen"]
   - "Finde Leute die Eigentum besitzen" → [] (leer - keine Interessen erwähnt)
   - "Finde Leute die Bücher mögen und Eigentum besitzen" → ["bücher"] (nur das Interesse)
   
   Nur inkludieren: Hobbys, Sport, Kunst, Unterhaltung, persönliche Interessen die wörtlich im Text erscheinen.
   VERMEIDE Verben (besitzen, haben, mögen), Präpositionen (über, unter), und Verbindungswörter (und, oder, wer).

2. logical_filters: 
   Nur diese Felder: {APPROVED_FIELDS}
   
   WICHTIG für Altersfilter:
   - "über X", "älter als X", "mehr als X Jahre" → p_age_2023 > X
   - "unter X", "jünger als X", "weniger als X Jahre" → p_age_2023 < X
   - Beispiele: "über 40" → p_age_2023 > 40, "älter als 30" → p_age_2023 > 30
   
   Gib numerische oder boolesche Bedingungen an (z.B. p_age_2023 > 40, p_i_homeowner = true).
   Für Eigentumsbesitz wie "Eigentum besitzen", "Hausbesitzer", verwende p_i_homeowner = true.
   Für Eigentumswert verwende p_prop_total_value mit numerischen Bedingungen.

3. similarity_query:
   Eine kurze deutsche Zusammenfassung. {lang_instruction}

Antwort nur mit gültigem JSON:

{{
  "keywords": ["reisen", "bücher"],
  "logical_filters": {{ "p_age_2023": ">40", "p_pens_sav": ">3000" }},
  "similarity_query": "Profile über 40, die gerne reisen und Pension ansparen"
}}

Query: "{query}"
"""
    else:
        prompt = f"""
You are a search assistant for a graph of user profiles ("Doubles").
Extract three parts as JSON:

1. keywords:
   Extract ONLY words that represent personal INTERESTS, HOBBIES, or RECREATIONAL ACTIVITIES.
   NEVER extract these structural/financial terms: "property", "investment", "pension", "insurance", "income", "money", "euros", "savings", "expenses", "age", "homeowner"
   These terms become logical filters, NOT keywords.
   If a query only contains structural terms and no interests, return an empty keywords array: []
   
   COMMON VALID INTERESTS: sports, books, travel, music, art, cooking, pets, animals, technology, gaming, fitness, reading, dancing, photography, gardening, movies, theater, outdoors, hiking, cycling, swimming, crafts, cars, food, wine, fashion, etc.
   
   Examples:
   - "Find people who like sports and travel" → ["sports", "travel"]
   - "Find people who own property" → [] (empty - no interests mentioned)
   - "Find people who like books and own property" → ["books"] (only the interest)
   - "Find people who like pets, sports, and books" → ["pets", "sports", "books"] (all three interests)
   
   Only include: hobbies, sports, arts, entertainment, personal interests that appear literally in the text.
   AVOID verbs (own, have, like), prepositions (over, under), and connecting words (and, or, who).

2. logical_filters:
   Only use these fields: {APPROVED_FIELDS}
   
   CRITICAL - Always detect AGE conditions:
   - "over X", "older than X", "above X", "more than X years old" → p_age_2023 > X
   - "under X", "younger than X", "below X", "less than X years old" → p_age_2023 < X
   - Examples: "over 40" → p_age_2023 > 40, "older than 30" → p_age_2023 > 30
   
   For property ownership terms like "own property", "homeowner", use p_i_homeowner = true.
   For property VALUE, use p_prop_total_value with numeric conditions.
   
   Age Examples:
   - "Find people over 40 who own property" → {{"p_age_2023": ">40", "p_i_homeowner": "true"}}
   - "Find people older than 25 with savings" → {{"p_age_2023": ">25"}}

3. similarity_query:
   A concise English summary of the search intent. {lang_instruction}

Output only valid JSON in this form:

{{
  "keywords": ["books", "travel"],
  "logical_filters": {{ "p_age_2023": ">40", "p_val_investment": ">200000" }},
  "similarity_query": "profiles over 40 who save and invest"
}}

Query: "{query}"
"""

    # call GROQ
    response = client.chat.completions.create(
        model="llama3-8b-8192",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )

    raw = response.choices[0].message.content.strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        print("⚠️ Couldn't find JSON:\n", raw)
        return None

    try:
        parsed = json.loads(match.group())
    except json.JSONDecodeError as e:
        print("⚠️ JSON decode error:", e)
        print("Extracted:\n", match.group())
        return None

    # map synonyms to actual fields
    logical = {}
    for k, v in parsed.get("logical_filters", {}).items():
        # translate any natural-language key into your field name
        field = SYNONYM_MAP.get(k.lower(), k)
        logical[field] = v
    parsed["logical_filters"] = logical

    return validate_and_fix_age_detection(query, parsed)


if __name__ == "__main__":
    q = input("Enter query: ")
    out = parse_query(q)
    print(json.dumps(out, indent=2, ensure_ascii=False))
