# Flask Admin for PostgreSQL

This app lets you explore your PostgreSQL database with a simple UI:

- Choose a schema and table
- Instantly get a CRUD interface in Flask-Admin

## Setup

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env  # and fill in your DB connection
python app.py
