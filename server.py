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


# -------- MASSIVE (FIXED) --------

def get_stock_price(ticker):
    try:
        url = f"https://api.massive.com/v1/stocks/{ticker.upper()}"
        headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}

        r = requests.get(url, headers=headers)
        data = r.json()

        return data.get("last")
    except:
        return None


# -------- FRED TEST --------

def get_fred(series):
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={FRED_API_KEY}&file_type=json"
        r = requests.get(url)
        data = r.json()

        obs = data.get("observations", [])
        if not obs:
            return None

        return obs[-1].get("value")
    except:
        return None


@app.route("/sms", methods=["POST"])
def sms():
    raw = request.form.get("Body", "").strip()
    lower = raw.lower()

    from_number = request.form.get("From", "").replace("whatsapp:", "")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    # PRICE
    if lower.startswith("price "):
        ticker = raw.split(" ")[-1]
        price = get_stock_price(ticker)

        if price is None:
            resp.message(f"{ticker.upper()}: N/A")
        else:
            resp.message(f"{ticker.upper()}: ${price}")

        return str(resp)

    # FRED TEST
    if lower.startswith("fred "):
        series = raw.split(" ")[-1]
        val = get_fred(series)

        if val is None:
            resp.message(f"{series}: N/A")
        else:
            resp.message(f"{series}: {val}")

        return str(resp)

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
