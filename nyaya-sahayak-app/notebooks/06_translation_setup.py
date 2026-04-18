# Databricks notebook source
import requests
import json

# Get Databricks credentials automatically from the notebook context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# 1. Create the 'huggingface' scope (it's okay if it already exists)
print("Creating scope...")
scope_resp = requests.post(
    f"{host}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": "huggingface"}
)

if scope_resp.status_code == 200:
    print("✅ Scope 'huggingface' created.")
elif "ALREADY_EXISTS" in scope_resp.text:
    print("ℹ️ Scope 'huggingface' already exists.")
else:
    print(f"⚠️ Scope creation note: {scope_resp.text}")

# 2. Store the token secret securely
print("\nStoring token...")
secret_resp = requests.post(
    f"{host}/api/2.0/secrets/put",
    headers=headers,
    json={
        "scope": "huggingface",
        "key": "token",
        "string_value": "hf_token"
    }
)

if secret_resp.status_code == 200:
    print("✅ Success! The Hugging Face token is securely stored in Databricks Secrets.")
    print("🧹 You can now safely delete this notebook cell!")
else:
    print(f"❌ Failed to store secret: {secret_resp.text}")

# COMMAND ----------

# DBTITLE 1,Cell 2
# --- RUN THIS IN CELL 1 ---
%pip install -q "transformers<5.0.0" torch psutil langdetect sentencepiece accelerate
dbutils.library.restartPython()

# --- RUN THIS IN CELL 2 ---
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
import torch
import psutil
import os

print("Loading IndicTrans2 models...\n")
print("Model 1: Indic → English (200M distilled)")
print("Model 2: English → Indic (200M distilled)\n")

# Paths for saving models to Unity Catalog Volumes (writable on serverless)
INDIC_EN_PATH = "/Volumes/workspace/default/nyaya_models/indictrans2/indic_en/"
EN_INDIC_PATH = "/Volumes/workspace/default/nyaya_models/indictrans2/en_indic/"

# Retrieve HuggingFace token from Databricks secrets
hf_token = dbutils.secrets.get(scope="huggingface", key="token")

print("Loading Indic → English model...")
indic_en_tokenizer = AutoTokenizer.from_pretrained(
    "ai4bharat/indictrans2-indic-en-dist-200M",
    trust_remote_code=True,
    token=hf_token
)
indic_en_model = AutoModelForSeq2SeqLM.from_pretrained(
    "ai4bharat/indictrans2-indic-en-dist-200M",
    trust_remote_code=True,
    device_map="cpu",
    token=hf_token
)
indic_en_model.eval()
print("✅ Indic → English loaded")

print("\nLoading English → Indic model...")
en_indic_tokenizer = AutoTokenizer.from_pretrained(
    "ai4bharat/indictrans2-en-indic-dist-200M",
    trust_remote_code=True,
    token=hf_token
)
en_indic_model = AutoModelForSeq2SeqLM.from_pretrained(
    "ai4bharat/indictrans2-en-indic-dist-200M",
    trust_remote_code=True,
    device_map="cpu",
    token=hf_token
)
en_indic_model.eval()
print("✅ English → Indic loaded")
print(f"\n✅ Both IndicTrans2 models loaded successfully on CPU!")

# Check RAM
ram = psutil.virtual_memory()
print(f"\nRAM Status:\n  Available: {ram.available/1e9:.1f} GB\n  Used: {ram.used/1e9:.1f} GB ({ram.percent}%)")

def translate_hi_to_en(text: str) -> str:
    tagged_text = "hin_Deva eng_Latn " + text
    
    inputs = indic_en_tokenizer(
        tagged_text,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=256
    )
    
    with torch.no_grad():
        outputs = indic_en_model.generate(
            input_ids=inputs['input_ids'],
            attention_mask=inputs['attention_mask'],
            max_new_tokens=256,
            num_beams=4,
            early_stopping=True,
            use_cache=False
        )
    
    translation = indic_en_tokenizer.decode(outputs[0], skip_special_tokens=True)
    return translation.strip()

def translate_en_to_hi(text: str) -> str:
    tagged_text = "eng_Latn hin_Deva " + text
    
    inputs = en_indic_tokenizer(
        tagged_text,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=256
    )
    
    with torch.no_grad():
        outputs = en_indic_model.generate(
            input_ids=inputs['input_ids'],
            attention_mask=inputs['attention_mask'],
            max_new_tokens=256,
            num_beams=4,
            early_stopping=True,
            use_cache=False
        )
    
    translation = en_indic_tokenizer.decode(outputs[0], skip_special_tokens=True)
    return translation.strip()

print("✅ Translation functions defined")

from langdetect import detect, LangDetectException

def detect_language(text: str) -> str:
    try:
        lang = detect(text)
        return 'hi' if lang == 'hi' else 'en'
    except LangDetectException:
        return 'en'

print("✅ Language detection function defined")

print("Testing translations...\n")
english = translate_hi_to_en('हत्या की सजा क्या है?')
hindi = translate_en_to_hi('What is the punishment for murder?')
print(f"HI->EN: {english}")
print(f"EN->HI: {hindi}")

import os

if 'indic_en_model' not in globals() or 'en_indic_model' not in globals():
    print("❌ Models not loaded yet!")
else:
    print("Saving IndicTrans2 models to Unity Catalog Volume...\n")
    
    os.makedirs(INDIC_EN_PATH, exist_ok=True)
    indic_en_model.save_pretrained(INDIC_EN_PATH)
    indic_en_tokenizer.save_pretrained(INDIC_EN_PATH)
    print(f"✅ Saved to: {INDIC_EN_PATH}")
    
    os.makedirs(EN_INDIC_PATH, exist_ok=True)
    en_indic_model.save_pretrained(EN_INDIC_PATH)
    en_indic_tokenizer.save_pretrained(EN_INDIC_PATH)
    print(f"✅ Saved to: {EN_INDIC_PATH}")
    
    print("\n✅ All models saved to Unity Catalog Volume!")

import mlflow
print("Logging translation models to MLflow (Local Workspace)...\n")
try:
    user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
    mlflow.set_experiment(f"/Users/{user_email}/nyaya-sahayak")
    with mlflow.start_run(run_name="indictrans2-translation-models") as run:
        mlflow.log_param("model_family", "IndicTrans2")
        mlflow.log_param("model_size", "200M-distilled")
        mlflow.log_param("directions", "hi<->en")
        print(f"✅ Translation models logged to MLflow Workspace. Run ID: {run.info.run_id}")
except Exception as e:
    print(f"⚠️  MLflow logging warning: {e}")

# COMMAND ----------

