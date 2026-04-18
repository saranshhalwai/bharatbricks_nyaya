# Databricks notebook source
# --- RUN THIS IN CELL 1 ---
# Install required libraries
%pip install -q transformers torch accelerate psutil
dbutils.library.restartPython()

# --- RUN THIS IN CELL 2 ---
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import psutil
import gc

# Force PyTorch to max out the CPU cores for faster generation
torch.set_num_threads(psutil.cpu_count(logical=False))

print("⚡ Loading Qwen 2.5 (0.5B) for ultra-fast CPU inference...\n")

MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"

# Load tokenizer and model - OPTIMIZED FOR FAST CPU
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    torch_dtype=torch.float32,
    device_map="cpu",
    low_cpu_mem_usage=True
)

SYSTEM_PROMPT = """You are Nyaya-Sahayak (न्याय सहायक), an AI legal assistant for Indian law.
You help with BNS 2023, BNSS 2023, and BSA 2023.
Be concise and cite section numbers. Answer in Hindi if asked in Hindi."""

def ask_legal_assistant(user_prompt: str, context: str = "", max_tokens: int = 100) -> str:
    """
    Ask Qwen (supports Hindi + English)
    Memory-optimized and ultra-fast for Serverless CPU
    """
    if context:
        full_prompt = f"{SYSTEM_PROMPT}\n\nसंदर्भ/Context:\n{context[:500]}\n\nप्रश्न/Question: {user_prompt}\n\nउत्तर/Answer:"
    else:
        full_prompt = f"{SYSTEM_PROMPT}\n\nप्रश्न/Question: {user_prompt}\n\nउत्तर/Answer:"
    
    inputs = tokenizer(full_prompt, return_tensors="pt", truncation=True, max_length=512)
    
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_tokens,
            max_length=None, # Suppresses length warnings constraint
            temperature=0.3,
            do_sample=True,
            top_p=0.85,
            pad_token_id=tokenizer.eos_token_id,
            eos_token_id=tokenizer.eos_token_id
        )
    
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    if "Answer:" in response:
        answer = response.split("Answer:")[-1].strip()
    elif "उत्तर:" in response:
        answer = response.split("उत्तर:")[-1].strip()
    else:
        answer = response[len(full_prompt):].strip()
    
    del inputs, outputs
    gc.collect()
    
    return answer

print("✅ Qwen 2.5 loaded successfully!")
print(f"\n📦 Model: Qwen2.5-0.5B")
print(f"⚡ CPU Limit Optimized (Blazing Fast)")

# Simulate RAG retrieval with mock context
print("\nTesting RAG pattern with context...\n")

mock_context = """[BNS_2023 Section 103] Murder:
Whoever commits murder shall be punished with death or imprisonment for life, and shall also be liable to fine.

[BNS_2023 Section 101] Culpable homicide:
Whoever causes death by doing an act with the intention of causing death,
or with the intention of causing such bodily injury as is likely to cause death,
commits the offence of culpable homicide."""

question = "What is the punishment for murder under BNS 2023?"

print(f"Question: {question}\n")
print("Context provided:")
print(mock_context[:200] + "...\n" + "-" * 70)

import time
start = time.time()
answer = ask_legal_assistant(question, context=mock_context)
end = time.time()

print(f"Answer with context:\n{answer}")
print("\n" + "="*70)
print(f"✅ RAG pattern working! (Response took: {end-start:.2f} seconds)")


# COMMAND ----------

