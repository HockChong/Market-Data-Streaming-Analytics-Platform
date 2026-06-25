# Databricks notebook source
# DBTITLE 1,Stop OHLCV Silver Pipeline
# OHLCV Silver Pipeline — Stop
# Triggered by a Databricks Job at 5:00 PM Mon–Fri (America/New_York)
# Requests the pipeline to stop its active continuous update.
# Safe to run if already IDLE — no error is raised.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState

PIPELINE_ID = "da840128-1d31-49d9-b035-831693f5234f"

w = WorkspaceClient()
info = w.pipelines.get(pipeline_id=PIPELINE_ID)
state = info.state

print(f"Current pipeline state: {state}")

if state == PipelineState.RUNNING:
    w.pipelines.stop(pipeline_id=PIPELINE_ID)
    print("Pipeline stop requested successfully.")
elif state == PipelineState.IDLE:
    print("Pipeline is already idle. No action taken.")
else:
    print(f"Pipeline is in state '{state}'. No stop action taken.")
