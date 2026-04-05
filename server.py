from openai import OpenAI
from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os

client = OpenAI()

SYSTEM_PROMPT = """
You are Jeeves, a high-functioning personal assistant.
Be concise, sharp, and decision-focused.
"""

conversation_history = []

app = Flask(__name__)

MY_NUMBER = os.environ.get("MY_NUMBER")

@app.route("/", methods=["GET"])
def home():
    return "Jeeves is running"

@app.route("/sms", methods=["POST"])
def sms():
    incoming = request.form.get("Body", "").lower()
    from_number = request.form.get("From", "").replace("whatsapp:", "")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    # ADD MEMORY
    conversation_history.append({"role": "user", "content": incoming})

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT}
    ] + conversation_history[-10:]

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages
    )

    reply = completion.choices[0].message.content

    conversation_history.append({"role": "assistant", "content": reply})

    resp.message(reply)
    return str(resp)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
