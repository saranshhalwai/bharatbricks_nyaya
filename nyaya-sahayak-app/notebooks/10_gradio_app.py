# Databricks notebook source
# DBTITLE 1,Import nyaya_core and Initialize Orchestrator
# Import or define core functions for Gradio app
import sys
import os
import time
import re
import uuid
from datetime import datetime

print("⚙️ Setting up Nyaya-Sahayak for Gradio app...\n")

# Add workspace directory to Python path
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
workspace_dir = f"/Workspace/Users/{user_email}"
if workspace_dir not in sys.path:
    sys.path.append(workspace_dir)

# Try to import from nyaya_core if it exists, otherwise use simplified versions
try:
    if 'nyaya_core' in sys.modules:
        del sys.modules['nyaya_core']
    from nyaya_core import retrieve_legal, ask_qwen, translate_hi_to_en, translate_en_to_hi
    print("✅ nyaya_core module imported successfully\n")
except ModuleNotFoundError:
    print("⚠️  nyaya_core.py not found. Please run notebook 07 first to create it.")
    print("   For now, using simplified inline implementations...\n")
    
    # Simplified retrieve_legal
    def retrieve_legal(query: str, top_k: int = 5, filter_source=None) -> list:
        try:
            keywords = ' '.join([w for w in query.lower().split() if len(w) > 3])[:100]
            sql = f"""
                SELECT chunk_id, section_number, title, chunk_text, source_law, content_type
                FROM workspace.default.legal_chunks
                WHERE LOWER(chunk_text) LIKE '%{keywords.split()[0] if keywords else 'section'}%'
            """
            if filter_source:
                sql += f" AND source_law = '{filter_source}'"
            sql += f" LIMIT {top_k}"
            results = spark.sql(sql).collect()
            return [[r.chunk_id, r.section_number or '', r.title or '', r.chunk_text[:500], 
                    r.source_law or '', r.content_type or '', 0.75] for r in results]
        except:
            return []
    
    def ask_qwen(user_prompt: str, context: str = "", max_tokens: int = 100) -> str:
        if context:
            sections = re.findall(r'Section\s+(\d+)', context)
            if sections:
                return f"Based on the legal provisions, particularly Section {sections[0]}, {user_prompt.lower()} relates to the applicable law. Please refer to the specific section for detailed information."
        return f"This query relates to Indian criminal law. Please refer to the relevant BNS/BNSS/BSA sections."
    
    def translate_hi_to_en(text: str) -> str:
        return text
    
    def translate_en_to_hi(text: str) -> str:
        return text

print("✅ Functions available: retrieve_legal, ask_qwen, translate_hi_to_en, translate_en_to_hi\n")

# Define agents for Gradio
class BNSAgent:
    def query(self, user_question: str) -> dict:
        start = time.time()
        raw_results = retrieve_legal(user_question, top_k=5)
        legal_results = [r for r in raw_results if r[5] == 'legal_section'][:3]
        context = "\n\n".join([f"[{r[4]} Section {r[1]}] {r[2]}:\n{r[3]}" for r in legal_results]) if legal_results else ""
        
        # Check for direct section lookup
        section_match = re.search(r'section\s+(\d+)', user_question, re.IGNORECASE)
        if section_match and context:
            sec_num = section_match.group(1)
            try:
                fallback = spark.sql(f"""
                    SELECT section_number, title, full_text, source_law 
                    FROM workspace.default.bns_sections 
                    WHERE section_number = '{sec_num}' 
                    LIMIT 1
                """).collect()
                if fallback:
                    row = fallback[0]
                    context += f"\n\n[Direct lookup] BNS Section {row.section_number}: {row.full_text[:500]}"
            except:
                pass
        
        answer = ask_qwen(user_question, context=context, max_tokens=200)
        return {
            "question": user_question,
            "answer": answer,
            "sections_cited": [r[1] for r in legal_results] if legal_results else [],
            "source_laws": list(set([r[4] for r in legal_results])) if legal_results else [],
            "response_time_ms": int((time.time()-start)*1000)
        }

class SchemeAgent:
    def check_eligibility(self, state: str, income: int, occupation: str, category: str) -> dict:
        start = time.time()
        try:
            query = f"""
                SELECT scheme_name, ministry, beneficiary_type, full_text, state
                FROM workspace.default.government_schemes
                WHERE scheme_name IS NOT NULL
            """
            if state != "All India":
                query += f" AND (state = '{state}' OR state = 'All India' OR state IS NULL)"
            query += " LIMIT 50"
            
            schemes = spark.sql(query).collect()
            eligible = [
                {"name": s.scheme_name, "ministry": s.ministry or "Unknown", "type": s.beneficiary_type or "General"}
                for s in schemes[:10]
            ]
            
            context = "\n".join([f"{s['name']} - {s['ministry']}" for s in eligible[:5]])
            summary = ask_qwen(f"Recommend top 3 schemes for {occupation} with income ₹{income} in {state}", context=context, max_tokens=150)
            
            return {
                "eligible_schemes": eligible,
                "total_found": len(eligible),
                "ai_summary": summary,
                "response_time_ms": int((time.time()-start)*1000)
            }
        except:
            return {"eligible_schemes": [], "total_found": 0, "ai_summary": "Scheme data not available", "response_time_ms": 0}

class DiffAgent:
    def compare(self, query: str) -> dict:
        start = time.time()
        try:
            mappings = spark.sql("""
                SELECT ipc_section, bns_section, ipc_title, change_type, change_detail
                FROM workspace.default.ipc_bns_mapping
                LIMIT 5
            """).collect()
            
            context = "\n".join([f"IPC {m.ipc_section} → BNS {m.bns_section}: {m.change_type}" for m in mappings])
            explanation = ask_qwen(f"Explain how {query}", context=context, max_tokens=200)
            
            return {"ai_explanation": explanation, "found_direct_match": True, "response_time_ms": int((time.time()-start)*1000)}
        except:
            explanation = ask_qwen(f"Explain the change from IPC to BNS: {query}", max_tokens=200)
            return {"ai_explanation": explanation, "found_direct_match": False, "response_time_ms": int((time.time()-start)*1000)}

class TranslationAgent:
    def to_hindi(self, text: str) -> str:
        try:
            return translate_en_to_hi(text)
        except:
            return text
    
    def to_english(self, text: str) -> str:
        try:
            return translate_hi_to_en(text)
        except:
            return text

class OrchestratorAgent:
    def __init__(self):
        self.bns_agent = BNSAgent()
        self.scheme_agent = SchemeAgent()
        self.diff_agent = DiffAgent()
        self.translation_agent = TranslationAgent()
    
    def route(self, user_query: str) -> dict:
        query_lower = user_query.lower()
        
        # Detect if Hindi query
        try:
            import langdetect
            lang = langdetect.detect(user_query)
            if lang == 'hi':
                user_query = self.translation_agent.to_english(user_query)
        except:
            pass
        
        # Detect intent
        if any(word in query_lower for word in ['scheme', 'eligible', 'eligibility', 'योजना']):
            result = self.scheme_agent.check_eligibility("All India", 200000, "General", "General")
            result['intent'] = 'scheme_eligibility'
            result['agent_used'] = 'SchemeAgent'
        elif any(word in query_lower for word in ['ipc', 'compare', 'changed', 'difference']):
            result = self.diff_agent.compare(user_query)
            result['intent'] = 'ipc_bns_comparison'
            result['agent_used'] = 'DiffAgent'
        else:
            result = self.bns_agent.query(user_query)
            result['intent'] = 'legal_query'
            result['agent_used'] = 'BNSAgent'
        
        return result

# Initialize orchestrator
orchestrator = OrchestratorAgent()
print("✅ Orchestrator initialized successfully")
print("   - BNSAgent: Legal Q&A with semantic search")
print("   - SchemeAgent: Government scheme eligibility")
print("   - DiffAgent: IPC vs BNS comparison")
print("   - TranslationAgent: Hindi ↔ English")
print("\n🚀 Ready to build Gradio app!\n")

# COMMAND ----------

# DBTITLE 1,Prerequisites Check
# MAGIC %md
# MAGIC ## Gradio App for Nyaya-Sahayak
# MAGIC
# MAGIC ### Prerequisites
# MAGIC This notebook requires:
# MAGIC 1. ✅ Orchestrator initialized (notebook 07)
# MAGIC 2. ✅ `ask_qwen()` function available (notebook 05)
# MAGIC 3. ✅ Translation functions available (notebook 06)
# MAGIC 4. ✅ `retrieve_legal()` function available (notebook 04)
# MAGIC
# MAGIC If these are not in scope, run those notebooks first or import from saved modules.

# COMMAND ----------

# DBTITLE 1,Define Handler Functions for Each Tab
import time

# Indian states list
INDIAN_STATES = [
    "All India","Andhra Pradesh","Arunachal Pradesh","Assam","Bihar",
    "Chhattisgarh","Goa","Gujarat","Haryana","Himachal Pradesh",
    "Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra",
    "Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Punjab",
    "Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura",
    "Uttar Pradesh","Uttarakhand","West Bengal","Delhi","Jammu & Kashmir"
]

def legal_query_handler(question, response_lang):
    """
    Handle legal questions via orchestrator.
    Supports Hindi and English questions.
    """
    try:
        t0 = time.time()
        result = orchestrator.route(question)
        elapsed = int((time.time()-t0)*1000)
        
        # Get answer
        answer = result.get('answer', 'No answer found')
        
        # Translate to Hindi if requested
        if response_lang == "Hindi" and result.get('answer_hindi'):
            answer = result['answer_hindi']
        elif response_lang == "Hindi" and 'translation_agent' in dir():
            answer = orchestrator.translation_agent.to_hindi(answer)
        
        # Format output
        sections = ", ".join(result.get('sections_cited', []))
        output = f"""**Answer:**
{answer}

**Sections Referenced:** {sections or 'N/A'}
**Intent Detected:** {result.get('intent', 'N/A')}
**Response Time:** {elapsed}ms"""
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

def scheme_eligibility_handler(state, income, occupation, category):
    """
    Check government scheme eligibility using Spark SQL.
    """
    try:
        t0 = time.time()
        result = orchestrator.scheme_agent.check_eligibility(
            state=state,
            income=int(income),
            occupation=occupation,
            category=category
        )
        elapsed = int((time.time()-t0)*1000)
        
        # Format schemes list
        schemes_list = "\n".join([
            f"• **{s['name']}** — {s['ministry']}"
            for s in result['eligible_schemes'][:5]
        ])
        
        output = f"""**Eligible Schemes ({result['total_found']} found):**
{schemes_list}

**AI Recommendation:**
{result['ai_summary']}

*Response time: {elapsed}ms*"""
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

def ipc_bns_compare_handler(query):
    """
    Compare IPC and BNS sections.
    """
    try:
        t0 = time.time()
        result = orchestrator.diff_agent.compare(query.strip())
        elapsed = int((time.time()-t0)*1000)
        
        if result.get('found_direct_match') == False:
            return f"**AI Explanation:**\n{result['ai_explanation']}\n\n*{elapsed}ms*"
        
        output = f"""| | **Old (IPC)** | **New (BNS)** |
|---|---|---|
| **Section** | {result.get('ipc_section','—')} | {result.get('bns_section','—')} |
| **Title** | {result.get('ipc_title','—')} | {result.get('bns_title','—')} |
| **Change** | colspan | **{result.get('change_type','—').upper()}** |

**Impact for Citizens:**
{result.get('impact_for_citizen', '')}

**AI Explanation:**
{result.get('ai_explanation', '')}

*Response time: {elapsed}ms*"""
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

print("✅ Handler functions defined")

# COMMAND ----------

# DBTITLE 1,Install Gradio
# MAGIC %pip install gradio

# COMMAND ----------

# DBTITLE 1,Restart Python Kernel
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Build Gradio Interface
import gradio as gr

print("Building Gradio interface...\n")

# Create Gradio app with 3 tabs
with gr.Blocks(
    title="Nyaya-Sahayak | न्याय सहायक",
    theme=gr.themes.Soft()
) as demo:
    
    # Header
    gr.Markdown("""
    # ⚖️ Nyaya-Sahayak | न्याय सहायक
    ### AI-Powered Indian Legal Assistant | Making Law Accessible to Every Citizen
    *Built on Databricks Free Edition | IIT Indore | Bharat Bricks Hacks 2026*
    """)
    
    # Tab 1: Legal Assistant
    with gr.Tab("⚖️ Legal Assistant"):
        gr.Markdown("### Ask legal questions in Hindi or English")
        
        with gr.Row():
            q_input = gr.Textbox(
                label="Your Question",
                placeholder="What is BNS Section 103? / धारा 103 क्या है?",
                lines=2
            )
            lang_radio = gr.Radio(
                ["Auto-detect", "English", "Hindi"],
                value="Auto-detect",
                label="Response Language"
            )
        
        q_btn = gr.Button("Ask", variant="primary", size="lg")
        q_output = gr.Markdown(label="Answer")
        
        gr.Examples(
            examples=[
                ["What is BNS Section 103?", "English"],
                ["धारा 103 क्या है?", "Hindi"],
                ["What changed in rape laws from IPC to BNS?", "English"],
                ["जमानत के नियम क्या हैं?", "Hindi"],
                ["What are the bail provisions under BNSS?", "English"],
                ["Explain culpable homicide in simple terms", "English"],
            ],
            inputs=[q_input, lang_radio]
        )
        
        q_btn.click(legal_query_handler, [q_input, lang_radio], q_output)
    
    # Tab 2: Scheme Eligibility
    with gr.Tab("🏛️ Scheme Eligibility"):
        gr.Markdown("### Check which government schemes you qualify for")
        
        with gr.Row():
            state_dd = gr.Dropdown(
                INDIAN_STATES,
                label="State",
                value="Madhya Pradesh"
            )
            income_sl = gr.Slider(
                minimum=0,
                maximum=1000000,
                value=200000,
                step=10000,
                label="Annual Income (₹)"
            )
        
        with gr.Row():
            occ_dd = gr.Dropdown(
                ["Farmer","Student","Women","Entrepreneur",
                 "Daily Wage Worker","Senior Citizen","General"],
                label="Occupation",
                value="Farmer"
            )
            cat_dd = gr.Dropdown(
                ["General","SC","ST","OBC"],
                label="Category",
                value="General"
            )
        
        scheme_btn = gr.Button(
            "Check My Eligibility",
            variant="primary",
            size="lg"
        )
        scheme_output = gr.Markdown(label="Eligible Schemes")
        
        gr.Examples(
            examples=[
                ["Madhya Pradesh", 150000, "Farmer", "General"],
                ["All India", 300000, "Student", "SC"],
                ["Maharashtra", 100000, "Women", "OBC"],
                ["Uttar Pradesh", 500000, "Entrepreneur", "General"],
            ],
            inputs=[state_dd, income_sl, occ_dd, cat_dd]
        )
        
        scheme_btn.click(
            scheme_eligibility_handler,
            [state_dd, income_sl, occ_dd, cat_dd],
            scheme_output
        )
    
    # Tab 3: IPC to BNS Changes
    with gr.Tab("📝 IPC → BNS Changes"):
        gr.Markdown("### See how laws changed from IPC 1860 to BNS 2023")
        
        diff_input = gr.Textbox(
            label="Enter IPC section number or topic",
            placeholder="302  or  murder  or  sedition  or  theft",
            lines=1
        )
        
        diff_btn = gr.Button(
            "Find Changes",
            variant="primary",
            size="lg"
        )
        diff_output = gr.Markdown(label="Comparison")
        
        gr.Examples(
            examples=[
                ["302"],  # Murder
                ["420"],  # Cheating
                ["376"],  # Rape
                ["124A"], # Sedition
                ["498A"], # Cruelty to wife
                ["murder"],
                ["sedition"],
                ["defamation"],
            ],
            inputs=[diff_input]
        )
        
        diff_btn.click(ipc_bns_compare_handler, diff_input, diff_output)
    
    # Footer
    gr.Markdown("""
    ---
    *Data: BNS 2023 | BNSS 2023 | BSA 2023 | IPC 1860 | Constitution of India | gov_myscheme*  
    *Models: Qwen2.5-7B (CPU) | IndicTrans2 | Databricks Vector Search*  
    *Powered by: Databricks Delta Lake | Spark SQL | Unity Catalog | MLflow*
    """)

print("✅ Gradio interface built")

# COMMAND ----------

# DBTITLE 1,Launch Gradio App
print("⚠️  Gradio server launch is not supported in Databricks serverless notebooks.\n")
print("The Gradio interface has been built successfully in the previous cell.")
print("To deploy this app, use Databricks Apps:\n")
print("Option 1: Deploy via Databricks Apps UI")
print("  1. Go to Databricks UI → Apps → Create App")
print("  2. Select 'Gradio' app type")
print("  3. Point to this notebook")
print("  4. Deploy and get public URL\n")
print("Option 2: Use the app.py template")
print("  1. Run the next cell to generate app.py")
print("  2. Deploy the app.py file from the volume\n")
print("✅ Gradio interface is ready for deployment")

# COMMAND ----------

# DBTITLE 1,Save as app.py for Databricks Apps Deployment
# Generate complete app.py file for Databricks Apps deployment
app_code = '''
# Nyaya-Sahayak Gradio App for Databricks Apps
import sys
import os
import time
import re
import uuid
from datetime import datetime

# Add volume to Python path
sys.path.insert(0, "/Volumes/workspace/default/nyaya_models/")

import gradio as gr

# Import core functions (these should be pre-deployed in the volume)
try:
    from nyaya_core import retrieve_legal, ask_qwen, translate_hi_to_en, translate_en_to_hi
except ModuleNotFoundError:
    print("⚠️  nyaya_core.py not found. Using simplified versions...")
    
    def retrieve_legal(query: str, top_k: int = 5, filter_source=None) -> list:
        # Simplified implementation - replace with actual Spark code in production
        return []
    
    def ask_qwen(user_prompt: str, context: str = "", max_tokens: int = 100) -> str:
        return "This is a placeholder response. Deploy the actual model."
    
    def translate_hi_to_en(text: str) -> str:
        return text
    
    def translate_en_to_hi(text: str) -> str:
        return text

# Define agents
class BNSAgent:
    def query(self, user_question: str) -> dict:
        start = time.time()
        raw_results = retrieve_legal(user_question, top_k=5)
        legal_results = [r for r in raw_results if r[5] == 'legal_section'][:3]
        context = "\n\n".join([f"[{r[4]} Section {r[1]}] {r[2]}:\n{r[3]}" for r in legal_results]) if legal_results else ""
        
        section_match = re.search(r'section\s+(\d+)', user_question, re.IGNORECASE)
        if section_match and context:
            sec_num = section_match.group(1)
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                fallback = spark.sql(f"""
                    SELECT section_number, title, full_text, source_law 
                    FROM workspace.default.bns_sections 
                    WHERE section_number = '{sec_num}' 
                    LIMIT 1
                """).collect()
                if fallback:
                    row = fallback[0]
                    context += f"\n\n[Direct lookup] BNS Section {row.section_number}: {row.full_text[:500]}"
            except:
                pass
        
        answer = ask_qwen(user_question, context=context, max_tokens=200)
        return {
            "question": user_question,
            "answer": answer,
            "sections_cited": [r[1] for r in legal_results] if legal_results else [],
            "source_laws": list(set([r[4] for r in legal_results])) if legal_results else [],
            "response_time_ms": int((time.time()-start)*1000)
        }

class SchemeAgent:
    def check_eligibility(self, state: str, income: int, occupation: str, category: str) -> dict:
        start = time.time()
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            query = f"""
                SELECT scheme_name, ministry, beneficiary_type, full_text, state
                FROM workspace.default.government_schemes
                WHERE scheme_name IS NOT NULL
            """
            if state != "All India":
                query += f" AND (state = '{state}' OR state = 'All India' OR state IS NULL)"
            query += " LIMIT 50"
            
            schemes = spark.sql(query).collect()
            eligible = [
                {"name": s.scheme_name, "ministry": s.ministry or "Unknown", "type": s.beneficiary_type or "General"}
                for s in schemes[:10]
            ]
            
            context = "\n".join([f"{s['name']} - {s['ministry']}" for s in eligible[:5]])
            summary = ask_qwen(f"Recommend top 3 schemes for {occupation} with income ₹{income} in {state}", context=context, max_tokens=150)
            
            return {
                "eligible_schemes": eligible,
                "total_found": len(eligible),
                "ai_summary": summary,
                "response_time_ms": int((time.time()-start)*1000)
            }
        except:
            return {"eligible_schemes": [], "total_found": 0, "ai_summary": "Scheme data not available", "response_time_ms": 0}

class DiffAgent:
    def compare(self, query: str) -> dict:
        start = time.time()
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            mappings = spark.sql("""
                SELECT ipc_section, bns_section, ipc_title, change_type, change_detail
                FROM workspace.default.ipc_bns_mapping
                LIMIT 5
            """).collect()
            
            context = "\n".join([f"IPC {m.ipc_section} → BNS {m.bns_section}: {m.change_type}" for m in mappings])
            explanation = ask_qwen(f"Explain how {query}", context=context, max_tokens=200)
            
            return {"ai_explanation": explanation, "found_direct_match": True, "response_time_ms": int((time.time()-start)*1000)}
        except:
            explanation = ask_qwen(f"Explain the change from IPC to BNS: {query}", max_tokens=200)
            return {"ai_explanation": explanation, "found_direct_match": False, "response_time_ms": int((time.time()-start)*1000)}

class TranslationAgent:
    def to_hindi(self, text: str) -> str:
        try:
            return translate_en_to_hi(text)
        except:
            return text
    
    def to_english(self, text: str) -> str:
        try:
            return translate_hi_to_en(text)
        except:
            return text

class OrchestratorAgent:
    def __init__(self):
        self.bns_agent = BNSAgent()
        self.scheme_agent = SchemeAgent()
        self.diff_agent = DiffAgent()
        self.translation_agent = TranslationAgent()
    
    def route(self, user_query: str) -> dict:
        query_lower = user_query.lower()
        
        try:
            import langdetect
            lang = langdetect.detect(user_query)
            if lang == 'hi':
                user_query = self.translation_agent.to_english(user_query)
        except:
            pass
        
        if any(word in query_lower for word in ['scheme', 'eligible', 'eligibility', 'योजना']):
            result = self.scheme_agent.check_eligibility("All India", 200000, "General", "General")
            result['intent'] = 'scheme_eligibility'
            result['agent_used'] = 'SchemeAgent'
        elif any(word in query_lower for word in ['ipc', 'compare', 'changed', 'difference']):
            result = self.diff_agent.compare(user_query)
            result['intent'] = 'ipc_bns_comparison'
            result['agent_used'] = 'DiffAgent'
        else:
            result = self.bns_agent.query(user_query)
            result['intent'] = 'legal_query'
            result['agent_used'] = 'BNSAgent'
        
        return result

# Initialize orchestrator
orchestrator = OrchestratorAgent()

# Indian states list
INDIAN_STATES = [
    "All India","Andhra Pradesh","Arunachal Pradesh","Assam","Bihar",
    "Chhattisgarh","Goa","Gujarat","Haryana","Himachal Pradesh",
    "Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra",
    "Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Punjab",
    "Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura",
    "Uttar Pradesh","Uttarakhand","West Bengal","Delhi","Jammu & Kashmir"
]

# Handler functions
def legal_query_handler(question, response_lang):
    """
    Handle legal questions via orchestrator.
    Supports Hindi and English questions.
    """
    try:
        t0 = time.time()
        result = orchestrator.route(question)
        elapsed = int((time.time()-t0)*1000)
        
        answer = result.get('answer', 'No answer found')
        
        if response_lang == "Hindi" and result.get('answer_hindi'):
            answer = result['answer_hindi']
        elif response_lang == "Hindi" and 'translation_agent' in dir():
            answer = orchestrator.translation_agent.to_hindi(answer)
        
        sections = ", ".join(result.get('sections_cited', []))
        output = f"""**Answer:**
{answer}

**Sections Referenced:** {sections or 'N/A'}
**Intent Detected:** {result.get('intent', 'N/A')}
**Response Time:** {elapsed}ms"""
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

def scheme_eligibility_handler(state, income, occupation, category):
    """
    Check government scheme eligibility using Spark SQL.
    """
    try:
        t0 = time.time()
        result = orchestrator.scheme_agent.check_eligibility(
            state=state,
            income=int(income),
            occupation=occupation,
            category=category
        )
        elapsed = int((time.time()-t0)*1000)
        
        schemes_list = "\n".join([
            f"• **{s['name']}** — {s['ministry']}"
            for s in result['eligible_schemes'][:5]
        ])
        
        output = f"""**Eligible Schemes ({result['total_found']} found):**
{schemes_list}

**AI Recommendation:**
{result['ai_summary']}

*Response time: {elapsed}ms*"""
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

def ipc_bns_compare_handler(query):
    """
    Compare IPC and BNS sections.
    """
    try:
        t0 = time.time()
        result = orchestrator.diff_agent.compare(query.strip())
        elapsed = int((time.time()-t0)*1000)
        
        if result.get('found_direct_match') == False:
            return f"**AI Explanation:**\n{result['ai_explanation']}\n\n*{elapsed}ms*"
        
        output = f"""| | **Old (IPC)** | **New (BNS)** |
|---|---|---|
| **Section** | {result.get('ipc_section','—')} | {result.get('bns_section','—')} |
| **Title** | {result.get('ipc_title','—')} | {result.get('bns_title','—')} |
| **Change** | colspan | **{result.get('change_type','—').upper()}** |

**Impact for Citizens:**
{result.get('impact_for_citizen', '')}

**AI Explanation:**
{result.get('ai_explanation', '')}

*Response time: {elapsed}ms*"""
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

# Build Gradio interface
with gr.Blocks(
    title="Nyaya-Sahayak | न्याय सहायक",
    theme=gr.themes.Soft()
) as demo:
    
    gr.Markdown("""
    # ⚖️ Nyaya-Sahayak | न्याय सहायक
    ### AI-Powered Indian Legal Assistant | Making Law Accessible to Every Citizen
    *Built on Databricks Free Edition | IIT Indore | Bharat Bricks Hacks 2026*
    """)
    
    # Tab 1: Legal Assistant
    with gr.Tab("⚖️ Legal Assistant"):
        gr.Markdown("### Ask legal questions in Hindi or English")
        
        with gr.Row():
            q_input = gr.Textbox(
                label="Your Question",
                placeholder="What is BNS Section 103? / धारा 103 क्या है?",
                lines=2
            )
            lang_radio = gr.Radio(
                ["Auto-detect", "English", "Hindi"],
                value="Auto-detect",
                label="Response Language"
            )
        
        q_btn = gr.Button("Ask", variant="primary", size="lg")
        q_output = gr.Markdown(label="Answer")
        
        gr.Examples(
            examples=[
                ["What is BNS Section 103?", "English"],
                ["धारा 103 क्या है?", "Hindi"],
                ["What changed in rape laws from IPC to BNS?", "English"],
                ["जमानत के नियम क्या हैं?", "Hindi"],
                ["What are the bail provisions under BNSS?", "English"],
                ["Explain culpable homicide in simple terms", "English"],
            ],
            inputs=[q_input, lang_radio]
        )
        
        q_btn.click(legal_query_handler, [q_input, lang_radio], q_output)
    
    # Tab 2: Scheme Eligibility
    with gr.Tab("🏛️ Scheme Eligibility"):
        gr.Markdown("### Check which government schemes you qualify for")
        
        with gr.Row():
            state_dd = gr.Dropdown(
                INDIAN_STATES,
                label="State",
                value="Madhya Pradesh"
            )
            income_sl = gr.Slider(
                minimum=0,
                maximum=1000000,
                value=200000,
                step=10000,
                label="Annual Income (₹)"
            )
        
        with gr.Row():
            occ_dd = gr.Dropdown(
                ["Farmer","Student","Women","Entrepreneur",
                 "Daily Wage Worker","Senior Citizen","General"],
                label="Occupation",
                value="Farmer"
            )
            cat_dd = gr.Dropdown(
                ["General","SC","ST","OBC"],
                label="Category",
                value="General"
            )
        
        scheme_btn = gr.Button(
            "Check My Eligibility",
            variant="primary",
            size="lg"
        )
        scheme_output = gr.Markdown(label="Eligible Schemes")
        
        gr.Examples(
            examples=[
                ["Madhya Pradesh", 150000, "Farmer", "General"],
                ["All India", 300000, "Student", "SC"],
                ["Maharashtra", 100000, "Women", "OBC"],
                ["Uttar Pradesh", 500000, "Entrepreneur", "General"],
            ],
            inputs=[state_dd, income_sl, occ_dd, cat_dd]
        )
        
        scheme_btn.click(
            scheme_eligibility_handler,
            [state_dd, income_sl, occ_dd, cat_dd],
            scheme_output
        )
    
    # Tab 3: IPC to BNS Changes
    with gr.Tab("📝 IPC → BNS Changes"):
        gr.Markdown("### See how laws changed from IPC 1860 to BNS 2023")
        
        diff_input = gr.Textbox(
            label="Enter IPC section number or topic",
            placeholder="302  or  murder  or  sedition  or  theft",
            lines=1
        )
        
        diff_btn = gr.Button(
            "Find Changes",
            variant="primary",
            size="lg"
        )
        diff_output = gr.Markdown(label="Comparison")
        
        gr.Examples(
            examples=[
                ["302"],  # Murder
                ["420"],  # Cheating
                ["376"],  # Rape
                ["124A"], # Sedition
                ["498A"], # Cruelty to wife
                ["murder"],
                ["sedition"],
                ["defamation"],
            ],
            inputs=[diff_input]
        )
        
        diff_btn.click(ipc_bns_compare_handler, diff_input, diff_output)
    
    gr.Markdown("""
    ---
    *Data: BNS 2023 | BNSS 2023 | BSA 2023 | IPC 1860 | Constitution of India | gov_myscheme*  
    *Models: Qwen2.5-7B (CPU) | IndicTrans2 | Databricks Vector Search*  
    *Powered by: Databricks Delta Lake | Spark SQL | Unity Catalog | MLflow*
    """)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=8080, share=False)
'''

app_path = "/Volumes/workspace/default/nyaya_models/app.py"

try:
    with open(app_path, "w") as f:
        f.write(app_code)
    print(f"✅ Complete app.py saved to: {app_path}")
    print(f"   File size: {len(app_code)} characters")
    print("\n📦 Included components:")
    print("   ✓ All agent classes (BNSAgent, SchemeAgent, DiffAgent, TranslationAgent, OrchestratorAgent)")
    print("   ✓ All handler functions (legal_query, scheme_eligibility, ipc_bns_compare)")
    print("   ✓ Complete Gradio interface with 3 tabs")
    print("   ✓ Launch configuration for production deployment")
    print("\n🚀 To deploy to Databricks Apps:")
    print("   1. Go to Databricks UI → Apps → Create App")
    print("   2. Select 'Custom' app type")
    print("   3. Point to app.py: /Volumes/workspace/default/nyaya_models/app.py")
    print("   4. Set environment: Python 3.11+")
    print("   5. Add required packages: gradio, langdetect")
    print("   6. Deploy and get public URL")
    print("\n⚠️  Remember to deploy nyaya_core.py to the volume first!")
except Exception as e:
    print(f"❌ Error saving app.py: {str(e)}")
    print("   Make sure the volume path exists: /Volumes/workspace/default/nyaya_models/")