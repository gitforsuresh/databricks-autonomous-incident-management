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

def job_details(config):
    try:
        job_api = config["databricks_job_details"]["job_details_api"]
        data = requests.get(job_api, headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()})
        json_output = data.json()
        return json_output
    except Exception as e:
        print(e)

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
        task_run_id = get_failed_parent_run(config)
        error_details = []
        for run_id in task_run_id:
            error_api = config["databricks_job_details"]["error_api"].format(run_id=run_id)
            error_output = requests.get(error_api, headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}).json()
            error_info = {
            "job_id": error_output["metadata"]["job_id"],
            "run_id": error_output["metadata"]["run_id"],
            "job_run_id": error_output["metadata"]["job_run_id"],
            "task_key": error_output["metadata"]["task_key"],
            "error_message": error_output["error"],
            "error_trace": error_output["error_trace"],
            "state_message": error_output["metadata"]["state"]["state_message"],
            "notebook_path": error_output["metadata"]["tasks"][0]["notebook_task"]["notebook_path"]
            }
            error_details.append(error_info)
        if error_details:
            error_df = spark.createDataFrame([Row(**error) for error in error_details])
            error_df = error_df.withColumn("ingested_at", F.current_timestamp()).withColumn("_ingest_date", F.current_date())
            # error_df.write.mode("append").saveAsTable("job_error_details")
        return error_df

    except Exception as e:
        print(e)





# COMMAND ----------

if __name__ == "__main__":
    failed_run_ids, success_run_ids = list_jobs(CONFIG)
    task_failed_run_id = get_failed_parent_run(CONFIG)
    error_details = get_error_message(CONFIG)
    print(failed_run_ids)
    print(success_run_ids)
    print(task_failed_run_id)
    print(error_details)

# COMMAND ----------

error_details = get_error_message(CONFIG)

# COMMAND ----------



# COMMAND ----------

error_details.display()