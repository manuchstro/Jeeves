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
    from_number = request.form.get("From")

    resp = MessagingResponse()

    if from_number != MY_NUMBER:
        return ""

    if "hello" in incoming:
        resp.message("System active.")
    elif "good alert" in incoming:
        resp.message("Noted.")
    elif "too much noise" in incoming:
        resp.message("Reducing noise.")
    elif "more like this" in incoming:
        resp.message("Increasing sensitivity.")
    elif "late" in incoming:
        resp.message("Improving timing.")
    else:
        resp.message("Received.")

    return str(resp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
