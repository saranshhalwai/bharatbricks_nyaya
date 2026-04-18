# Nyaya-Sahayak | न्याय सहायक

⚖️ **AI-Powered Indian Legal Assistant**

Making Indian law accessible to every citizen in Hindi and English.

## Features

- 🔍 **Legal Assistant**: Ask questions about BNS 2023, BNSS, BSA in Hindi or English
- 🏛️ **Scheme Eligibility**: Check which government schemes you qualify for
- 📝 **IPC → BNS Changes**: Compare how laws changed from IPC 1860 to BNS 2023

## Tech Stack

- **Frontend**: Gradio
- **Backend**: Databricks (Delta Lake, Spark SQL, Unity Catalog)
- **ML Models**: Qwen2.5-7B, IndicTrans2
- **Deployment**: Databricks Apps (Serverless)

## Data Sources

- Bharatiya Nyaya Sanhita (BNS) 2023
- Bharatiya Nagarik Suraksha Sanhita (BNSS) 2023
- Bharatiya Sakshya Adhiniyam (BSA) 2023
- Indian Penal Code (IPC) 1860
- Constitution of India
- Government Schemes (myscheme.gov.in)

## Deployment

### Deploy to Databricks Apps

1. Create a new app in Databricks UI → Apps
2. Point to this GitHub repository
3. Set entry point: `app.py`
4. Add environment: Python 3.11+
5. Deploy and access via public URL

### Local Development

```bash
pip install -r requirements.txt
python app.py
```

## Project Structure

```
nyaya-sahayak/
├── app.py              # Main Gradio application
├── nyaya_core.py       # Core legal query functions
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

## Built With

- Databricks Free Edition
- Built for Bharat Bricks Hacks 2026
- IIT Indore

## License

MIT License

---

Made with ❤️ for making Indian law accessible to all
