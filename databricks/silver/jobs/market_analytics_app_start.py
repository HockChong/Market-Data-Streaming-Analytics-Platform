# Databricks notebook source
# DBTITLE 1,Start market-analytics-dashboard App
# market-analytics-dashboard — Start
# Triggered by a Databricks Job at 9:30 AM Mon–Fri (America/New_York)
# Starts the app if STOPPED. Safe to run if already RUNNING — no-op.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppState

APP_NAME = "market-analytics-dashboard"

w = WorkspaceClient()
app = w.apps.get(name=APP_NAME)
state = app.app_status.state

print(f"Current app state: {state}")

if state == AppState.RUNNING:
    print("App is already running. No action taken.")
elif state == AppState.STOPPED:
    w.apps.start(name=APP_NAME)
    print("App start requested successfully.")
else:
    print(f"App is in state '{state}' (e.g. STARTING/STOPPING). No action taken — check again shortly.")
