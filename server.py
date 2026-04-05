from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os
import sqlite3
import requests

client = OpenAI()

SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.

User profile:
- UC Berkeley economics student
- Focus: energy markets, especially uranium
- Strong interest in geopolitical catalysts and supply dynamics
- Prefers high-signal, actionable insights over theory
- Strongly dislikes noise, fluff, and irrelevant macro commentary
- Values speed, clarity, and decisiveness
- Comfortable with risk analysis and probabilistic thinking
- Wants alerts that are timely and meaningful, not obvious or delayed

Behavior:
- Be direct, efficient, and precise
- Default to short responses unless depth is clearly needed
- Prioritize usefulness over completeness
- Surface implications, not just facts
- Highlight what actually matters
- When uncertain, always default to saying "I don't know"

Objective:
Help the user make better decisions, faster.
"""

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")
DB_PATH = "jeeves.db"

MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")


def get_stock_price(ticker):
    ticker = ticker.upper()

    try:
        url = f"https://api.massive.com/v1/market/quote/{ticker}?apikey={MASSIVE_API_KEY}"
        r = requests.get(url)
        data = r.json()

        return data.get("price")

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

    if lower.startswith("price "):
        ticker = raw.split(" ")[-1]
        price = get_stock_price(ticker)

        if price is None:
            resp.message(f"{ticker.upper()}: N/A")
        else:
            resp.message(f"{ticker.upper()}: ${round(price,2)}")

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
