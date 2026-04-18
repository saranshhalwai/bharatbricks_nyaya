# Databricks notebook source
# DBTITLE 1,Install Required Packages
# MAGIC %pip install huggingface_hub PyMuPDF requests

# COMMAND ----------

# DBTITLE 1,Get File List from HuggingFace
from huggingface_hub import HfFileSystem
import requests, time, os, json
from datetime import datetime
import fitz  # PyMuPDF

print("Fetching file list from HuggingFace dataset...\n")

try:
    fs = HfFileSystem()
    all_files = fs.ls("datasets/shrijayan/gov_myscheme/text_data", detail=True)
    pdf_files = [f for f in all_files if f['name'].endswith('.pdf')]
    filenames = [os.path.basename(f['name']) for f in pdf_files]
    
    print(f"✅ Found {len(filenames)} PDF files to process")
    print(f"\nFirst 5 files:")
    for f in filenames[:5]:
        print(f"  - {f}")
    
except Exception as e:
    print(f"❌ Error fetching file list: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Batch Download + Extract + Delete Function
BASE_URL = "https://huggingface.co/datasets/shrijayan/gov_myscheme/resolve/main/text_data/"
TEMP_DIR = "/Volumes/workspace/default/legal-data/schemes/"

def process_batch(batch_filenames, batch_num):
    """
    Download PDFs, extract text immediately, delete PDF to save space.
    Returns list of extracted records ready for Delta Lake.
    """
    results = []
    
    for fname in batch_filenames:
        url = BASE_URL + fname
        local_path = TEMP_DIR + fname
        
        try:
            # Download PDF
            r = requests.get(url, timeout=30, 
                           headers={"User-Agent": "Mozilla/5.0"})
            
            if r.status_code != 200:
                print(f"  ⚠️  Skip {fname}: HTTP {r.status_code}")
                continue
            
            # Write to volume temporarily
            with open(local_path, 'wb') as f:
                f.write(r.content)
            
            # Extract text immediately using PyMuPDF
            doc = fitz.open(local_path)
            text = " ".join([page.get_text() for page in doc])
            text = " ".join(text.split())  # clean whitespace
            doc.close()
            
            # Delete PDF immediately to conserve storage
            os.remove(local_path)
            
            # Only keep if we got meaningful text
            if len(text) > 100:
                results.append({
                    "filename": fname,
                    "text": text[:50000],  # cap at 50k chars to fit Delta
                    "char_count": len(text),
                    "batch_num": batch_num,
                    "extracted_at": datetime.now().isoformat()
                })
            
        except Exception as e:
            print(f"  ❌ Error {fname}: {str(e)[:50]}")
        
        # Rate limit to avoid overloading HuggingFace
        time.sleep(0.5)
    
    return results

print("✅ Batch processing function defined")

# COMMAND ----------

# DBTITLE 1,Process All Batches and Write to Delta
# Ensure the temporary directory exists (use dbutils for volumes)
dbutils.fs.mkdirs(TEMP_DIR)

BATCH_SIZE = 50
all_results = []

total_batches = (len(filenames) + BATCH_SIZE - 1) // BATCH_SIZE
print(f"Starting batch processing: {total_batches} batches of {BATCH_SIZE} files each\n")
print("=" * 70)

for i in range(0, len(filenames), BATCH_SIZE):
    batch_num = i // BATCH_SIZE + 1
    batch = filenames[i:i+BATCH_SIZE]
    
    print(f"\n[Batch {batch_num}/{total_batches}] Processing {len(batch)} files...")
    
    # Process batch
    batch_results = process_batch(batch, batch_num)
    
    # Write to Delta immediately (append mode)
    if batch_results:
        batch_df = spark.createDataFrame(batch_results)
        batch_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("workspace.default.scheme_raw_text")
        
        print(f"  ✅ Batch {batch_num}: {len(batch_results)} schemes extracted and written to Delta")
    else:
        print(f"  ⚠️  Batch {batch_num}: No valid extractions")
    
    # Rate limit between batches
    time.sleep(2)

print("\n" + "=" * 70)
print("\n🎉 Ingestion complete! Checking final count...\n")

# Final count
total = spark.sql("SELECT COUNT(*) as cnt FROM workspace.default.scheme_raw_text").collect()[0]['cnt']
print(f"✅ Total schemes in Delta table: {total:,}")

# Sample data
print("\nSample records:")
spark.sql("SELECT filename, char_count, batch_num FROM workspace.default.scheme_raw_text LIMIT 5").show(truncate=False)