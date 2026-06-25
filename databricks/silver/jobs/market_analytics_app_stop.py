# Databricks notebook source
# DBTITLE 1,Stop market-analytics-dashboard App
# market-analytics-dashboard — Stop
# Triggered by a Databricks Job at 5:00 PM Mon–Fri (America/New_York)
# Stops the app if RUNNING. Safe to run if already STOPPED — no-op.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppState

APP_NAME = "market-analytics-dashboard"

w = WorkspaceClient()
app = w.apps.get(name=APP_NAME)
state = app.app_status.state

print(f"Current app state: {state}")

if state == AppState.RUNNING:
    w.apps.stop(name=APP_NAME)
    print("App stop requested successfully.")
elif state == AppState.STOPPED:
    print("App is already stopped. No action taken.")
else:
    print(f"App is in state '{state}' (e.g. STARTING/STOPPING). No action taken — check again shortly.")
