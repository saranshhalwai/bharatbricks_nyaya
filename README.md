# Nyaya-Sahayak Notebooks

This folder contains the development notebooks for the Nyaya-Sahayak project.

## Notebook Sequence

Run these notebooks in order to set up the complete system:

1. **01_environment_setup.ipynb** - Initial setup, dependencies, Unity Catalog configuration
2. **02_ingest_govscheme.ipynb** - Ingest government schemes data from myscheme.gov.in
3. **03_extract_legal_text.ipynb** - Extract and parse BNS/BNSS/BSA legal sections
4. **04_vector_search_setup.ipynb** - Set up Databricks Vector Search with embeddings
5. **05_load_qwen_model.ipynb** - Load and configure Qwen2.5-7B LLM
6. **06_translation_setup.ipynb** - Set up IndicTrans2 for Hindi-English translation
7. **07_agents_orchestrator.ipynb** - Build multi-agent system with MCP pattern
8. **08_mlflow_evaluation.ipynb** - Evaluate system with BhashaBench test suite
9. **09_ai_functions.ipynb** - SQL AI functions integration
10. **10_gradio_app.ipynb** - Build and deploy Gradio interface

## Data Pipeline

```
Raw Data Sources
    ↓
[Notebooks 01-03] Data Ingestion & Processing
    ↓
Delta Lake Tables (Unity Catalog)
    ↓
[Notebook 04] Vector Search Index
    ↓
[Notebooks 05-07] ML Models & Agents
    ↓
[Notebook 08] Evaluation
    ↓
[Notebook 10] Production App
```

## Requirements

- Databricks Workspace (Free Edition compatible)
- Python 3.10+
- See `../requirements.txt` for package dependencies
