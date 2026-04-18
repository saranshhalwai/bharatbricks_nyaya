"""nyaya_core.py - Production module for Nyaya-Sahayak
Fixed for Databricks Apps environment.
"""

__version__ = '4.0.1-production-fixed-app'
__author__ = 'Nyaya-Sahayak Team'

import os
import requests
import json
import torch
import gc
from typing import Optional, List, Tuple
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Global state (Models loaded lazily)
_chroma_client = None
_chroma_collection = None
_embedding_model = "databricks-bge-large-en"
_source_table = "workspace.default.legal_chunks"
_qwen_model = None
_qwen_tokenizer = None
_indic_en_model = None
_indic_en_tokenizer = None
_en_indic_model = None
_en_indic_tokenizer = None

def _get_db_creds():
    """Get host and token using databricks-sdk Config"""
    cfg = Config()
    # In Databricks Apps, host is usually in DATABRICKS_HOST
    host = os.environ.get("DATABRICKS_HOST") or cfg.host
    # Token can be obtained via authenticate() which returns a header or similar
    # For simple requests.post, we need the raw token.
    # Databricks Apps provides DATABRICKS_CLIENT_ID/SECRET for M2M, 
    # and x-forwarded-access-token for user auth.
    # If we are using App Auth (Service Principal), we might need to exchange.
    # However, WorkspaceClient() should handle it.
    w = WorkspaceClient()
    return w.config.host, w.config.token

def _init_chromadb():
    """Initialize ChromaDB client and collection (called on first use)"""
    global _chroma_client, _chroma_collection
    if _chroma_collection is not None:
        return
    print("🔧 Initializing ChromaDB (first use)...")
    import os
    os.environ["ANONYMIZED_TELEMETRY"] = "False"
    import chromadb
    from chromadb.config import Settings
    
    # Try to find chroma_db in the workspace
    # Since we are in an app, we might need to use a Volume for shared data
    workspace_dir = "/Workspace/Users/mems230005017@iiti.ac.in"
    if not os.path.exists(workspace_dir):
        # Fallback to current dir if not found (for local testing or different environment)
        workspace_dir = os.path.dirname(os.path.abspath(__file__))
        
    chroma_dirs = [d for d in os.listdir(workspace_dir) if d.startswith('chroma_db_full_')]
    if not chroma_dirs:
        # Check if it's in a volume
        volume_path = "/Volumes/workspace/default/nyaya_models/"
        if os.path.exists(volume_path):
            chroma_dirs = [d for d in os.listdir(volume_path) if d.startswith('chroma_db_full_')]
            if chroma_dirs:
                workspace_dir = volume_path
    
    if not chroma_dirs:
        print("⚠️ ChromaDB not found. Vector search will be disabled.")
        return

    chroma_dirs.sort(reverse=True)
    chroma_path = os.path.join(workspace_dir, chroma_dirs[0])
    _chroma_client = chromadb.PersistentClient(path=chroma_path, settings=Settings(anonymized_telemetry=False))
    _chroma_collection = _chroma_client.get_or_create_collection(name="legal_chunks_vectors", metadata={"embedding_model": _embedding_model, "source_table": _source_table})
    print(f"✅ ChromaDB initialized ({_chroma_collection.count()} vectors)")

def _init_qwen():
    """Initialize Qwen 2.5 0.5B LLM (called on first use)"""
    global _qwen_model, _qwen_tokenizer
    if _qwen_model is not None:
        return
    print("⚡ Loading Qwen 2.5 (0.5B) for ultra-fast CPU inference...")
    from transformers import AutoModelForCausalLM, AutoTokenizer
    import torch
    import psutil
    torch.set_num_threads(psutil.cpu_count(logical=False))
    MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"
    _qwen_tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    _qwen_model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.float32, device_map="cpu", low_cpu_mem_usage=True)
    print("✅ Qwen 2.5 loaded successfully")

def _init_translation():
    """Initialize IndicTrans2 models (called on first use)"""
    global _indic_en_model, _indic_en_tokenizer, _en_indic_model, _en_indic_tokenizer
    if _indic_en_model is not None:
        return
    print("🔧 Loading IndicTrans2 models (first use, ~1 minute)...")
    from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
    
    INDIC_EN_PATH = "/Volumes/workspace/default/nyaya_models/indictrans2/indic_en/"
    EN_INDIC_PATH = "/Volumes/workspace/default/nyaya_models/indictrans2/en_indic/"
    INDIC_EN_MODEL = "ai4bharat/indictrans2-indic-en-dist-200M"
    EN_INDIC_MODEL = "ai4bharat/indictrans2-en-indic-dist-200M"
    
    # Try loading from local files first
    use_local = os.path.exists(INDIC_EN_PATH) and os.path.exists(EN_INDIC_PATH)
    
    if use_local:
        try:
            print("  Attempting to load from local volume...")
            _indic_en_tokenizer = AutoTokenizer.from_pretrained(INDIC_EN_PATH, trust_remote_code=True, local_files_only=True)
            _indic_en_model = AutoModelForSeq2SeqLM.from_pretrained(INDIC_EN_PATH, trust_remote_code=True, local_files_only=True)
            _en_indic_tokenizer = AutoTokenizer.from_pretrained(EN_INDIC_PATH, trust_remote_code=True, local_files_only=True)
            _en_indic_model = AutoModelForSeq2SeqLM.from_pretrained(EN_INDIC_PATH, trust_remote_code=True, local_files_only=True)
            print("✅ IndicTrans2 models loaded from local volume")
            return
        except Exception as e:
            print(f"  ⚠️ Local loading failed: {e}")
            use_local = False
    
    # Download from HuggingFace
    if not use_local:
        print("  Downloading from HuggingFace...")
        _indic_en_tokenizer = AutoTokenizer.from_pretrained(INDIC_EN_MODEL, trust_remote_code=True)
        _indic_en_model = AutoModelForSeq2SeqLM.from_pretrained(INDIC_EN_MODEL, trust_remote_code=True, device_map="cpu")
        _en_indic_tokenizer = AutoTokenizer.from_pretrained(EN_INDIC_MODEL, trust_remote_code=True)
        _en_indic_model = AutoModelForSeq2SeqLM.from_pretrained(EN_INDIC_MODEL, trust_remote_code=True, device_map="cpu")
        print("✅ IndicTrans2 models downloaded and loaded successfully")

def retrieve_legal(query: str, top_k: int = 5, filter_source: Optional[str] = None) -> List[Tuple]:
    """Retrieve relevant legal chunks using semantic search via ChromaDB"""
    _init_chromadb()
    if _chroma_collection is None:
        return []
        
    try:
        host, token = _get_db_creds()
    except Exception as e:
        print(f"Error getting Databricks credentials: {e}")
        return []

    embedding_url = f"{host}/serving-endpoints/{_embedding_model}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        response = requests.post(embedding_url, headers=headers, json={"input": [query]}, timeout=30)
        response.raise_for_status()
        query_embedding = response.json()['data'][0]['embedding']
    except Exception as e:
        print(f"Error getting query embedding from {embedding_url}: {e}")
        return []
        
    try:
        where_clause = {"source_law": filter_source} if filter_source else None
        results = _chroma_collection.query(query_embeddings=[query_embedding], n_results=top_k, where=where_clause, include=["documents", "metadatas", "distances"])
        output = []
        for i in range(len(results['ids'][0])):
            chunk_id = results['ids'][0][i]
            metadata = results['metadatas'][0][i]
            document = results['documents'][0][i]
            distance = results['distances'][0][i]
            similarity = 1 / (1 + distance)
            output.append([chunk_id, metadata.get('section_number', ''), metadata.get('title', ''), document, metadata.get('source_law', ''), metadata.get('content_type', ''), round(similarity, 4)])
        return output
    except Exception as e:
        print(f"Error searching ChromaDB: {e}")
        return []

def ask_qwen(user_prompt: str, context: str = "", max_tokens: int = 100) -> str:
    """Ask Qwen 2.5 0.5B (supports Hindi + English)"""
    _init_qwen()
    SYSTEM_PROMPT = "You are Nyaya-Sahayak (न्याय सहायक), an AI legal assistant for Indian law. You help with BNS 2023, BNSS 2023, and BSA 2023. Be concise and cite section numbers. Answer in Hindi if asked in Hindi."
    if context:
        full_prompt = f"{SYSTEM_PROMPT}\n\nसंदर्भ/Context:\n{context[:500]}\n\nप्रश्न/Question: {user_prompt}\n\nउत्तर/Answer:"
    else:
        full_prompt = f"{SYSTEM_PROMPT}\n\nप्रश्न/Question: {user_prompt}\n\nउत्तर/Answer:"
    
    inputs = _qwen_tokenizer(full_prompt, return_tensors="pt", truncation=True, max_length=512)
    with torch.no_grad():
        outputs = _qwen_model.generate(**inputs, max_new_tokens=max_tokens, max_length=None, temperature=0.3, do_sample=True, top_p=0.85, pad_token_id=_qwen_tokenizer.eos_token_id, eos_token_id=_qwen_tokenizer.eos_token_id)
    response = _qwen_tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    if "Answer:" in response:
        answer = response.split("Answer:")[-1].strip()
    elif "उत्तर:" in response:
        answer = response.split("उत्तर:")[-1].strip()
    else:
        answer = response[len(full_prompt):].strip()
    
    del inputs, outputs
    gc.collect()
    return answer

def translate_hi_to_en(text: str) -> str:
    """Translate Hindi text to English using IndicTrans2"""
    _init_translation()
    if _indic_en_model is None: return text
    tagged_text = "hin_Deva eng_Latn " + text
    inputs = _indic_en_tokenizer(tagged_text, return_tensors="pt", padding=True, truncation=True, max_length=256)
    with torch.no_grad():
        outputs = _indic_en_model.generate(input_ids=inputs['input_ids'], attention_mask=inputs['attention_mask'], max_new_tokens=256, num_beams=4, early_stopping=True, use_cache=False)
    translation = _indic_en_tokenizer.decode(outputs[0], skip_special_tokens=True)
    return translation.strip()

def translate_en_to_hi(text: str) -> str:
    """Translate English text to Hindi using IndicTrans2"""
    _init_translation()
    if _en_indic_model is None: return text
    tagged_text = "eng_Latn hin_Deva " + text
    inputs = _en_indic_tokenizer(tagged_text, return_tensors="pt", padding=True, truncation=True, max_length=256)
    with torch.no_grad():
        outputs = _en_indic_model.generate(input_ids=inputs['input_ids'], attention_mask=inputs['attention_mask'], max_new_tokens=256, num_beams=4, early_stopping=True, use_cache=False)
    translation = _en_indic_tokenizer.decode(outputs[0], skip_special_tokens=True)
    return translation.strip()
