# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import json
import requests
import importlib
import config_files.config_files
importlib.reload(config_files.config_files)
from config_files.config_files import CONFIG
from pyspark.sql import Row
from pyspark.sql import functions as F


# COMMAND ----------

def job_details(config, job_id):
    try:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        job_api = config["databricks_job_details"]["job_details_api"].format(job_id=job_id)
        response = requests.get( job_api,headers={"Authorization": f"Bearer {token}"})
        return response.json()
    except Exception as e:
        print(f"Error fetching job details for job_id {job_id}: {e}")
        return {}


# COMMAND ----------

def list_jobs(config):
    try:
        jobs_api = config["databricks_job_details"]["list_job_api"]
        data = requests.get(jobs_api, headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()})
        json_output = data.json()
        failed_run_ids = list()
        success_run_ids = list()
        for run in json_output['runs']:
            if run['state']["result_state"] == 'FAILED':
                failed_job_run_id = run['run_id']
                failed_run_ids.append(failed_job_run_id)
            if run['state']["result_state"] == 'SUCCESS':
                success_job_run_id = run['run_id']
                success_run_ids.append(success_job_run_id)
        return failed_run_ids, success_run_ids
    except Exception as e:
        print(e)



# COMMAND ----------

def get_failed_parent_run(config):
    try:
        failed_run_ids, success_run_ids = list_jobs(CONFIG)
        task_failed_run_id = list()
        for failed in failed_run_ids:
            details_api = config["databricks_job_details"]["run_details_api"].format(run_id=failed)
            response = requests.get(details_api, headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()})
            data = response.json()
            task_failed_run_id.append(data['tasks'][0]['run_id'])
        return task_failed_run_id
    except Exception as e:
        print(e)



# COMMAND ----------

def get_error_message(config):
    try:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        task_run_ids = get_failed_parent_run(config)
        error_details = []
        for run_id in task_run_ids:
            error_api = config["databricks_job_details"]["error_api"].format(run_id=run_id)             
            error_output = requests.get(error_api,headers={"Authorization": f"Bearer {token}"}).json()
            metadata = error_output.get("metadata", {})
            job_id = metadata.get("job_id")
            job_metadata = job_details(config, job_id)
            settings = job_metadata.get("settings", {})
            error_info = {
                "job_id": job_id,
                "job_name": settings.get("name"),
                "run_id": metadata.get("run_id"),
                "job_run_id": metadata.get("job_run_id"),
                "task_key": metadata.get("task_key"),
                "error_message": error_output.get("error"),
                "error_trace": error_output.get("error_trace"),
                "state_message": metadata.get("state", {}).get("state_message"),
                "notebook_path": (
                    metadata.get("tasks", [{}])[0]
                    .get("notebook_task", {})
                    .get("notebook_path")
                ),
                "git_source": str(
                    settings.get("git_source", {})
                )
            }
            error_details.append(error_info)
        if not error_details:
            return None
        error_df = spark.createDataFrame(
            [Row(**error) for error in error_details]
        )
        error_df = error_df.withColumn("ingested_at", F.current_timestamp()).withColumn("_ingest_date",F.current_date())
        error_df.write.mode("overwrite").saveAsTable("job_error_details")
        return error_df
    except Exception as e:
        print(f"Error fetching error details: {e}")
        return None

# COMMAND ----------

if __name__ == "__main__":
    failed_run_ids, success_run_ids = list_jobs(CONFIG)
    task_failed_run_id = get_failed_parent_run(CONFIG)
    error_details = get_error_message(CONFIG)
    error_details.display()