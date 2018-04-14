import pyspark

lines = pyspark.SparkContext.textFile("README.md")
lines.count()