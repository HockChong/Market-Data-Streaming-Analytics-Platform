# Databricks notebook source
# DBTITLE 1,Stop Gold Pipeline
# Gold Pipeline — Stop
# Triggered by a Databricks Job at 5:00 PM Mon–Fri (America/New_York)
# Requests the pipeline to stop its active continuous update.
# Safe to run if already IDLE — no error is raised.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState

PIPELINE_ID = "891f5401-86a8-41e8-bf23-8ca91ac43cd4"

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
