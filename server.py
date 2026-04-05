from openai import OpenAI
client = OpenAI()

from flask import Flask, request
from twilio.twiml.messaging_response import MessagingResponse
import os

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

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are Jeeves, a concise, intelligent assistant that helps improve financial decision making and alert quality."},
            {"role": "user", "content": incoming}
        ]
    )

    reply = completion.choices[0].message.content
    resp.message(reply)

    return str(resp)

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
    
