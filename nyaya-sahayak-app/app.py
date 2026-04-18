# Nyaya-Sahayak Gradio App for Databricks Apps
import sys
import os
import time
import re
import uuid
from datetime import datetime

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gradio as gr

# Import core functions
try:
    from nyaya_core import retrieve_legal, ask_qwen, translate_hi_to_en, translate_en_to_hi
except ImportError:
    print("⚠️  nyaya_core.py not found or import failed. Using simplified versions...")
    
    def retrieve_legal(query: str, top_k: int = 5, filter_source=None) -> list:
        return []
    
    def ask_qwen(user_prompt: str, context: str = "", max_tokens: int = 100) -> str:
        return "Simplified response: " + user_prompt
    
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
    try:
        t0 = time.time()
        result = orchestrator.route(question)
        elapsed = int((time.time()-t0)*1000)
        
        answer = result.get('answer', 'No answer found')
        
        if response_lang == "Hindi":
            answer = orchestrator.translation_agent.to_hindi(answer)
        
        sections = ", ".join(result.get('sections_cited', []))
        output = f"**Answer:**\n{answer}\n\n**Sections Referenced:** {sections or 'N/A'}\n**Intent Detected:** {result.get('intent', 'N/A')}\n**Response Time:** {elapsed}ms"
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

def scheme_eligibility_handler(state, income, occupation, category):
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
        
        output = f"**Eligible Schemes ({result['total_found']} found):**\n{schemes_list}\n\n**AI Recommendation:**\n{result['ai_summary']}\n\n*Response time: {elapsed}ms*"
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

def ipc_bns_compare_handler(query):
    try:
        t0 = time.time()
        result = orchestrator.diff_agent.compare(query.strip())
        elapsed = int((time.time()-t0)*1000)
        
        if result.get('found_direct_match') == False:
            return f"**AI Explanation:**\n{result['ai_explanation']}\n\n*{elapsed}ms*"
        
        output = f"| | **Old (IPC)** | **New (BNS)** |\n|---|---|---|\n| **Section** | {result.get('ipc_section','—')} | {result.get('bns_section','—')} |\n| **Title** | {result.get('ipc_title','—')} | {result.get('bns_title','—')} |\n| **Change** | colspan | **{result.get('change_type','—').upper()}** |\n\n**Impact for Citizens:**\n{result.get('impact_for_citizen', '')}\n\n**AI Explanation:**\n{result.get('ai_explanation', '')}\n\n*Response time: {elapsed}ms*"
        
        return output
    except Exception as e:
        return f"Error: {str(e)}"

# Build Gradio interface
with gr.Blocks(
    title="Nyaya-Sahayak | न्याय सहायक",
    theme=gr.themes.Soft()
) as demo:
    
    gr.Markdown("# ⚖️ Nyaya-Sahayak | न्याय सहायक\n### AI-Powered Indian Legal Assistant | Making Law Accessible to Every Citizen\n*Built on Databricks Free Edition | IIT Indore | Bharat Bricks Hacks 2026*")
    
    with gr.Tab("⚖️ Legal Assistant"):
        gr.Markdown("### Ask legal questions in Hindi or English")
        with gr.Row():
            q_input = gr.Textbox(label="Your Question", placeholder="What is BNS Section 103? / धारा 103 क्या है?", lines=2)
            lang_radio = gr.Radio(["Auto-detect", "English", "Hindi"], value="Auto-detect", label="Response Language")
        q_btn = gr.Button("Ask", variant="primary", size="lg")
        q_output = gr.Markdown(label="Answer")
        gr.Examples(examples=[["What is BNS Section 103?", "English"], ["धारा 103 क्या है?", "Hindi"]], inputs=[q_input, lang_radio])
        q_btn.click(legal_query_handler, [q_input, lang_radio], q_output)
    
    with gr.Tab("🏛️ Scheme Eligibility"):
        gr.Markdown("### Check which government schemes you qualify for")
        with gr.Row():
            state_dd = gr.Dropdown(INDIAN_STATES, label="State", value="Madhya Pradesh")
            income_sl = gr.Slider(minimum=0, maximum=1000000, value=200000, step=10000, label="Annual Income (₹)")
        with gr.Row():
            occ_dd = gr.Dropdown(["Farmer","Student","Women","Entrepreneur","Daily Wage Worker","Senior Citizen","General"], label="Occupation", value="Farmer")
            cat_dd = gr.Dropdown(["General","SC","ST","OBC"], label="Category", value="General")
        scheme_btn = gr.Button("Check My Eligibility", variant="primary", size="lg")
        scheme_output = gr.Markdown(label="Eligible Schemes")
        scheme_btn.click(scheme_eligibility_handler, [state_dd, income_sl, occ_dd, cat_dd], scheme_output)
    
    with gr.Tab("📝 IPC → BNS Changes"):
        gr.Markdown("### See how laws changed from IPC 1860 to BNS 2023")
        diff_input = gr.Textbox(label="Enter IPC section number or topic", placeholder="302  or  murder  or  sedition  or  theft", lines=1)
        diff_btn = gr.Button("Find Changes", variant="primary", size="lg")
        diff_output = gr.Markdown(label="Comparison")
        diff_btn.click(ipc_bns_compare_handler, diff_input, diff_output)
    
    gr.Markdown("---\n*Data: BNS 2023 | BNSS 2023 | BSA 2023 | IPC 1860 | Constitution of India | gov_myscheme*  \n*Models: Qwen2.5-7B (CPU) | IndicTrans2 | Databricks Vector Search*  \n*Powered by: Databricks Delta Lake | Spark SQL | Unity Catalog | MLflow*")

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", 8000))
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)
