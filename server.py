from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import requests

client = OpenAI()

SYSTEM_PROMPT = """You are Jeeves. Be concise and decision-focused."""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")
FRED_API_KEY = os.environ.get("FRED_API_KEY")


# -------- DEBUG HELPERS --------

def debug_massive(ticker):
    try:
        url = f"https://api.massive.com/v1/stocks/{ticker.upper()}"
        headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
        r = requests.get(url, headers=headers, timeout=10)
        return {"status": r.status_code, "json": r.json()}
    except Exception as e:
        return {"error": str(e)}


def debug_fred(series):
    try:
        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series}&api_key={FRED_API_KEY}&file_type=json"
        )
        r = requests.get(url, timeout=10)
        return {"status": r.status_code, "json": r.json()}
    except Exception as e:
        return {"error": str(e)}


@app.route("/sms", methods=["POST"])
def sms():
    raw = request.form.get("Body", "").strip()
    lower = raw.lower()

    from_number = request.form.get("From", "").replace("whatsapp:", "")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    # ---- DEBUG COMMANDS ----
    if lower.startswith("debug massive "):
        ticker = raw.split(" ")[-1]
        out = debug_massive(ticker)
        resp.message(str(out)[:1500])
        return str(resp)

    if lower.startswith("debug fred "):
        series = raw.split(" ")[-1]
        out = debug_fred(series)
        resp.message(str(out)[:1500])
        return str(resp)

    # ---- NORMAL (no price yet) ----
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": raw}
            ]
        )
        reply = completion.choices[0].message.content
    except:
        reply = "Temporary error."

    resp.message(reply)
    return str(resp)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
