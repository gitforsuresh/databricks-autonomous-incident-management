# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from langchain.agents import create_agent
from langchain_databricks import ChatDatabricks

# COMMAND ----------

def get_failed_jobs(status="FAILED"):
    """Return failed jobs as JSON"""

    error_df = spark.read.table("job_error_details")

    return [
        row.asDict()
        for row in error_df.limit(20).collect()
    ]

# COMMAND ----------



agent = create_agent(
    model=ChatDatabricks(endpoint="databricks-gpt-oss-20b", max_tokens=1000),
    tools=[get_failed_jobs],
    system_prompt="""
    You are a Databricks Incident Analysis Assistant.

    Always use the get_failed_jobs tool whenever a user asks:
    - failed jobs
    - error details
    - job failures
    - error_df
    - job error table
    - github details

    Do not make up information.
    Only answer from the tool output.
    always provide github details, job_id, job_name, task_key, error_message, error_trace while reponding with other details and avoid '>' from the git url
    error details should be displayed properly and avoid using this hexa values and other things, convert it into image and produce
    when users are asking questions out of error_df, kindly mention "I am an assistant who could help with the failed job details, if you have any questions related to failed job, you can ask."
    """
    )

result = agent.invoke({"messages": [("user", "can you provide me the details about the failed jobs")]})
print(result["messages"][-1].content)
agent

# COMMAND ----------



chat = ChatDatabricks(endpoint="databricks-gpt-oss-20b", max_tokens=1000)
result = chat.invoke("hello, hi, how are you")


# COMMAND ----------

from langchain.agents import create_agent
from langchain_databricks import ChatDatabricks
model = create_agent(model = ChatDatabricks(endpoint="databricks-gpt-oss-20b", max_tokens=1000),
                     tools = [get_weather],
                     system_prompt = 'you are a weather assistance')

model

# COMMAND ----------

response = model.invoke({"messages": [("user", "what is the weather in new york")]})
print(response["messages"][-1].content)
