# Databricks notebook source
# DBTITLE 1,Install Required Libraries
# Install all required packages
# Note: Using AI Gateway with Qwen3 Next Instruct instead of local llama-cpp-python
%pip install pymupdf requests tqdm langdetect sentence-transformers faiss-cpu transformers sentencepiece sacremoses gradio mlflow huggingface_hub

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Verify RAM and System Resources
import psutil
import os

ram = psutil.virtual_memory()
print(f"Total RAM: {ram.total/1e9:.1f} GB")
print(f"Available: {ram.available/1e9:.1f} GB")
print(f"Used: {ram.used/1e9:.1f} GB ({ram.percent}%)")

if ram.available < 8e9:
    print("\n⚠️  WARNING: Less than 8GB free RAM")
    print("Consider reducing model context window or batch sizes")
else:
    print("\n✅ Sufficient RAM available for CPU-based inference")

# COMMAND ----------

# DBTITLE 1,Create Volume Directory Structure
import os

# Define all required directories in Unity Catalog Volumes
# Note: No qwen directory needed - using AI Gateway endpoint instead
dirs = [
    "/Volumes/workspace/default/nyaya_models/indictrans2/en_indic/",
    "/Volumes/workspace/default/nyaya_models/indictrans2/indic_en/",
    "/Volumes/workspace/default/nyaya_models/embedder/",
    "/Volumes/workspace/default/nyaya_models/faiss/",
    "/Volumes/workspace/default/legal-data/schemes/",
]

print("Creating directory structure...\n")
for d in dirs:
    try:
        os.makedirs(d, exist_ok=True)
        print(f"✅ Created: {d}")
    except Exception as e:
        print(f"❌ Failed: {d} | Error: {e}")

print("\n✅ Directory structure complete!")

# COMMAND ----------

# DBTITLE 1,Verify PDF Files in Volume
import fitz  # PyMuPDF
import os

pdfs = {
    "BNS 2023": "/Volumes/workspace/default/legal-data/BNS_English_30-04-2024.pdf",
    "BNSS 2023": "/Volumes/workspace/default/legal-data/250883_english_01042024.pdf",
    "BSA Part1": "/Volumes/workspace/default/legal-data/a202345.pdf",
    "BSA Part2": "/Volumes/workspace/default/legal-data/Hh202345.pdf",
    "IPC 1860":  "/Volumes/workspace/default/legal-data/THE-INDIAN-PENAL-CODE-1860.pdf",
    "Constitution": "/Volumes/workspace/default/legal-data/constitution_of_india.pdf",
}

print("Verifying legal PDF files...\n")

all_good = True
for name, path in pdfs.items():
    try:
        doc = fitz.open(path)
        size_mb = os.path.getsize(path) / 1e6
        print(f"✅ {name:15} | {doc.page_count:4} pages | {size_mb:6.1f} MB")
        doc.close()
    except FileNotFoundError:
        print(f"❌ {name:15} | File not found: {path}")
        all_good = False
    except Exception as e:
        print(f"❌ {name:15} | Error: {e}")
        all_good = False

if all_good:
    print("\n✅ All PDF files verified successfully!")
else:
    print("\n⚠️  Some PDF files are missing or corrupted")

# COMMAND ----------

# DBTITLE 1,Test Serverless SQL Warehouse Connection
from databricks.sdk import WorkspaceClient

print("Testing Databricks Workspace connection...\n")

try:
    w = WorkspaceClient()
    
    # List warehouses
    warehouses = list(w.warehouses.list())
    
    if warehouses:
        print(f"Found {len(warehouses)} SQL warehouse(s):\n")
        for wh in warehouses:
            print(f"Warehouse: {wh.name}")
            print(f"  State: {wh.state}")
            print(f"  Size: {wh.cluster_size}")
            print(f"  Type: {wh.warehouse_type}")
            print()
    else:
        print("⚠️  No SQL warehouses found")
    
    # Test Spark SQL connection
    print("Testing Spark SQL connection...")
    result = spark.sql("SELECT current_user() as user, current_database() as db").collect()
    user = result[0]['user']
    db = result[0]['db']
    print(f"✅ Connected as: {user}")
    print(f"✅ Current database: {db}")
    
    print("\n✅ Environment setup complete! Ready for data ingestion.")
    
except Exception as e:
    print(f"❌ Connection error: {e}")
    import traceback
    traceback.print_exc()