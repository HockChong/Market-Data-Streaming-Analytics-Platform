# Databricks notebook source
# DBTITLE 1,Start News Silver Pipeline
# News Silver Pipeline — Start
# Triggered by a Databricks Job at 9:30 AM Mon–Fri (America/New_York)
# Starts a new continuous update if the pipeline is IDLE or FAILED.
# Safe to run if already RUNNING — no duplicate update is created.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState

PIPELINE_ID = "de513865-fae0-419a-b687-809b158c5825"

w = WorkspaceClient()
info = w.pipelines.get(pipeline_id=PIPELINE_ID)
state = info.state

print(f"Current pipeline state: {state}")

if state in (PipelineState.IDLE, PipelineState.FAILED):
    result = w.pipelines.start_update(pipeline_id=PIPELINE_ID)
    print(f"Pipeline update started. Update ID: {result.update_id}")
elif state == PipelineState.RUNNING:
    print("Pipeline is already running. No action taken.")
else:
    print(f"Unexpected state '{state}'. Attempting to start anyway...")
    result = w.pipelines.start_update(pipeline_id=PIPELINE_ID)
    print(f"Pipeline update started. Update ID: {result.update_id}")
