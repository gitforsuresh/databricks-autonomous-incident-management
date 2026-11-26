# Databricks notebook source
def even_or_odd(num)
    if num%2 == 0:
        return "even"
    else:
        return "odd"

# COMMAND ----------

if __name__ == "__main__":
    even_or_odd(2)