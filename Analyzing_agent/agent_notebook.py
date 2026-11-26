# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from langchain.agents import create_agent
from langchain_databricks import ChatDatabricks

# COMMAND ----------

def get_weather(city:str)->str:
    """
    This function is a tool for the agent to get the weather in a given city.
    The input is a city name, and the output is a string describing the weather in that city.
    Example: get_weather("New York") -> "It's 25 degrees in New York"""
    return f"It's 25 degrees in {city}"

# COMMAND ----------



agent = create_agent(
    model=ChatDatabricks(endpoint="databricks-gpt-oss-20b", max_tokens=1000),
    tools=[get_weather],
    system_prompt="You are a helpful assistant",
)

result = agent.invoke({"messages": [("user", "what's the weather today in chennai?")]})
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
