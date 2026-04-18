# Databricks notebook source
# DBTITLE 1,Introduction to Databricks AI Functions
# MAGIC %md
# MAGIC # Databricks AI Functions for Legal AI
# MAGIC
# MAGIC ## What are AI Functions?
# MAGIC
# MAGIC Databricks AI Functions allow you to call LLMs directly from SQL queries using:
# MAGIC - `ai_query()` - General purpose LLM inference
# MAGIC - `ai_generate_text()` - Text generation
# MAGIC - `ai_classify()` - Classification tasks
# MAGIC - `ai_extract()` - Information extraction
# MAGIC - `ai_similarity()` - Text similarity
# MAGIC
# MAGIC ## Benefits
# MAGIC - **Zero infrastructure** - No model deployment needed
# MAGIC - **SQL-native** - Use in SELECT, WHERE, GROUP BY
# MAGIC - **Built-in models** - Access to Llama, MPT, and more
# MAGIC - **Pay-per-token** - No idle compute costs
# MAGIC
# MAGIC ## Use Cases for Nyaya-Sahayak
# MAGIC 1. Summarize legal sections for citizens
# MAGIC 2. Extract structured data from scheme descriptions
# MAGIC 3. Classify scheme beneficiary types
# MAGIC 4. Answer legal questions in SQL dashboards
# MAGIC 5. Multilingual legal assistance

# COMMAND ----------

# DBTITLE 1,Introduction to Databricks AI Functions
# MAGIC %md
# MAGIC # Databricks AI Functions for Legal AI
# MAGIC
# MAGIC ## What are AI Functions?
# MAGIC
# MAGIC Databricks AI Functions allow you to call LLMs directly from SQL queries using:
# MAGIC - `ai_query()` - General purpose LLM inference
# MAGIC - `ai_generate_text()` - Text generation
# MAGIC - `ai_classify()` - Classification tasks
# MAGIC - `ai_extract()` - Information extraction
# MAGIC - `ai_similarity()` - Text similarity
# MAGIC
# MAGIC ## Benefits
# MAGIC - **Zero infrastructure** - No model deployment needed
# MAGIC - **SQL-native** - Use in SELECT, WHERE, GROUP BY
# MAGIC - **Built-in models** - Access to Llama, MPT, and more
# MAGIC - **Pay-per-token** - No idle compute costs
# MAGIC
# MAGIC ## Use Cases for Nyaya-Sahayak
# MAGIC 1. Summarize legal sections for citizens
# MAGIC 2. Extract structured data from scheme descriptions
# MAGIC 3. Classify scheme beneficiary types
# MAGIC 4. Answer legal questions in SQL dashboards
# MAGIC 5. Multilingual legal assistance

# COMMAND ----------

# DBTITLE 1,Demo 1: Summarize BNS Sections for Citizens
# MAGIC %sql
# MAGIC -- Use AI Functions to create citizen-friendly summaries of BNS sections
# MAGIC -- This can power dashboards without deploying models
# MAGIC
# MAGIC SELECT 
# MAGIC     section_number,
# MAGIC     title,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'Summarize this Indian criminal law section in 2 simple sentences for a common citizen: ',
# MAGIC             'Section ', section_number, ' - ', title, ': ',
# MAGIC             LEFT(full_text, 500)
# MAGIC         )
# MAGIC     ) as citizen_summary,
# MAGIC     source_law
# MAGIC FROM workspace.default.bns_sections
# MAGIC -- Removed filter to see all available data
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Demo 2: Extract Structured Info from Schemes
# MAGIC %sql
# MAGIC -- Extract key information from government schemes using AI
# MAGIC -- This shows structured data extraction via SQL
# MAGIC
# MAGIC SELECT 
# MAGIC     scheme_name,
# MAGIC     ministry,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'Extract from this scheme description: beneficiary type, income limit, and top 2 required documents. ',
# MAGIC             'Scheme: ', scheme_name, ' ',
# MAGIC             LEFT(full_text, 400)
# MAGIC         )
# MAGIC     ) as extracted_info
# MAGIC FROM workspace.default.government_schemes
# MAGIC WHERE scheme_name IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Demo 3: Classify Scheme Beneficiary Types
# MAGIC %sql
# MAGIC -- Classify schemes by beneficiary category using AI
# MAGIC -- Useful for scheme recommendation engines
# MAGIC
# MAGIC SELECT 
# MAGIC     scheme_name,
# MAGIC     ministry,
# MAGIC     beneficiary_type as original_type,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'Classify this government scheme beneficiary into ONE category: Farmer, Student, Women, Youth, Senior Citizen, or General. ',
# MAGIC             'Scheme: ', scheme_name, ' ',
# MAGIC             'Description: ', LEFT(full_text, 300)
# MAGIC         )
# MAGIC     ) as ai_classified_category
# MAGIC FROM workspace.default.government_schemes
# MAGIC WHERE ministry LIKE '%Agriculture%'
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Demo 4: Compare IPC and BNS in SQL
# MAGIC %sql
# MAGIC -- Generate natural language comparisons of IPC vs BNS sections
# MAGIC -- This can power interactive legal comparison dashboards
# MAGIC
# MAGIC SELECT 
# MAGIC     ipc_section,
# MAGIC     bns_section,
# MAGIC     ipc_title,
# MAGIC     change_type,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT(
# MAGIC             'Explain in 2 sentences how this law changed from IPC to BNS and what it means for citizens: ',
# MAGIC             'IPC Section ', ipc_section, ' (', ipc_title, ') changed to BNS Section ', bns_section, '. ',
# MAGIC             'Change type: ', change_type, '. ',
# MAGIC             'Details: ', change_detail
# MAGIC         )
# MAGIC     ) as change_explanation
# MAGIC FROM workspace.default.ipc_bns_mapping
# MAGIC WHERE change_type IN ('modified', 'deleted')
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Demo 5: Multilingual Legal Q&A in SQL
# MAGIC %sql
# MAGIC -- Answer legal questions in Hindi directly from SQL
# MAGIC -- Enables multilingual dashboards and reports
# MAGIC
# MAGIC SELECT 
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         'हत्या के लिए BNS 2023 के तहत क्या सजा है? Hindi में 2 वाक्यों में जवाब दें।'
# MAGIC     ) as hindi_answer;

# COMMAND ----------

# DBTITLE 1,Demo 6: Legal Section Search with AI Similarity
# MAGIC %sql
# MAGIC -- Use AI similarity for semantic search within SQL
# MAGIC -- Alternative to Vector Search for smaller datasets
# MAGIC
# MAGIC SELECT 
# MAGIC     section_number,
# MAGIC     title,
# MAGIC     source_law,
# MAGIC     LEFT(full_text, 200) as preview,
# MAGIC     ai_similarity(
# MAGIC         'What are the laws about theft and stealing?',
# MAGIC         CONCAT(title, ' ', full_text)
# MAGIC     ) as similarity_score
# MAGIC FROM workspace.default.bns_sections
# MAGIC -- Removed similarity threshold filter to see all scores
# MAGIC ORDER BY similarity_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Demo 7: Aggregate Analytics with AI
# MAGIC %sql
# MAGIC -- Combine SQL aggregations with AI for business intelligence
# MAGIC -- Example: Count schemes by AI-classified category
# MAGIC
# MAGIC WITH classified_schemes AS (
# MAGIC     SELECT 
# MAGIC         scheme_name,
# MAGIC         ministry,
# MAGIC         ai_query(
# MAGIC             'databricks-meta-llama-3-3-70b-instruct',
# MAGIC             CONCAT(
# MAGIC                 'Classify beneficiary as ONE word: Farmer, Student, Women, Youth, Rural, or General. Scheme: ',
# MAGIC                 LEFT(full_text, 200)
# MAGIC             )
# MAGIC         ) as category
# MAGIC     FROM workspace.default.government_schemes
# MAGIC     WHERE scheme_name IS NOT NULL
# MAGIC     LIMIT 100
# MAGIC )
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) as scheme_count,
# MAGIC     COLLECT_LIST(scheme_name) as schemes
# MAGIC FROM classified_schemes
# MAGIC GROUP BY category
# MAGIC ORDER BY scheme_count DESC;

# COMMAND ----------

# DBTITLE 1,Summary and Use Cases
# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### ✅ What We Demonstrated
# MAGIC 1. **SQL-native AI** - Call LLMs directly in SELECT statements
# MAGIC 2. **Zero deployment** - No model endpoints or infrastructure
# MAGIC 3. **Multilingual** - Hindi and English queries work seamlessly
# MAGIC 4. **Structured extraction** - Parse unstructured legal text into fields
# MAGIC 5. **Semantic search** - AI similarity as alternative to vector search
# MAGIC 6. **Business intelligence** - Combine AI with SQL aggregations
# MAGIC
# MAGIC ### 📊 Performance Notes
# MAGIC - AI Functions use Databricks Foundation Model API
# MAGIC - Pay-per-token pricing (use for ad-hoc queries, not high-volume)
# MAGIC - For production RAG, prefer Vector Search + dedicated models
# MAGIC - Ideal for dashboards, reports, and exploratory analysis
# MAGIC
# MAGIC ### 🚀 Production Recommendations
# MAGIC - **For dashboards**: Use AI Functions (no infrastructure)
# MAGIC - **For chatbots**: Use deployed models (lower latency)
# MAGIC - **For RAG**: Use Vector Search (better accuracy)
# MAGIC - **For batch jobs**: Use AI Functions (cost-effective)
# MAGIC
# MAGIC ### 📝 Nyaya-Sahayak Integration
# MAGIC AI Functions complement our multi-agent system:
# MAGIC - **Agents**: Real-time conversational UI (Gradio app)
# MAGIC - **AI Functions**: Analytical dashboards and reports
# MAGIC - **Vector Search**: High-accuracy RAG retrieval
# MAGIC - **Spark SQL**: Scheme eligibility filtering
# MAGIC
# MAGIC ✅ **AI Functions enable zero-infrastructure legal AI at the SQL layer!**