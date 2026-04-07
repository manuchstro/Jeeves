import json
import os
import sqlite3
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_PATH = BASE_DIR / "credentials.json"
TOKEN_PATH = BASE_DIR / "gmail_token.json"
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "jeeves.db"))


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gmail_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            token_json TEXT NOT NULL,
            scopes_json TEXT NOT NULL,
            connected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()


def load_credentials():
    creds = None

    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if not CREDENTIALS_PATH.exists():
            raise FileNotFoundError(f"Missing credentials file at {CREDENTIALS_PATH}")
        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        creds = flow.run_local_server(port=0)

    TOKEN_PATH.write_text(creds.to_json())
    return creds


def fetch_profile_email(creds):
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    return profile.get("emailAddress", "").strip().lower()


def upsert_gmail_account(email, creds):
    token_payload = json.loads(creds.to_json())
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO gmail_accounts (email, token_json, scopes_json)
        VALUES (?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            token_json = excluded.token_json,
            scopes_json = excluded.scopes_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            email,
            json.dumps(token_payload),
            json.dumps(SCOPES),
        ),
    )
    conn.commit()
    conn.close()


def main():
    init_db()
    creds = load_credentials()
    email = fetch_profile_email(creds)
    if not email:
        raise RuntimeError("Failed to resolve Gmail account email")
    upsert_gmail_account(email, creds)
    print(json.dumps({
        "ok": True,
        "email": email,
        "db_path": DB_PATH,
        "token_path": str(TOKEN_PATH),
    }, indent=2))


if __name__ == "__main__":
    main()
