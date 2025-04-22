import json
import os
import requests
import sys

# Read configuration from environment variables
NOMAD_SERVER = os.environ.get("NOMAD_SERVER")
NOMAD_TOKEN = os.environ.get("NOMAD_TOKEN")

# Set headers
headers = {
    "X-Nomad-Token": NOMAD_TOKEN,
}

URL_API_PATH = "/v1/event/stream"
URL_QUERY_STRING = ""
# URL_QUERY_STRING = "?topic=Node:*"

url = NOMAD_SERVER + URL_API_PATH + URL_QUERY_STRING


def handle_event(event):
    print("topic: " + event["Topic"] + " type: " + event["Type"])


def handle_data(response):
    for line in response.iter_lines():
        if line:  # filter out keep-alive new lines
            object = json.loads(line.decode("utf-8"))
            if len(object) > 1:  # has Events
                for event in object["Events"]:
                    handle_event(event)


def connect(url):
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        handle_data(response)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


def start():
    try:
        connect(url)
    except KeyboardInterrupt:
        print("Received keyboard interrupt. Stopping.")
        SystemExit()


start()
