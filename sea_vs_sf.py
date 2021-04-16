import pyspark

from azureml.opendatasets import SeattleSafety
from azureml.opendatasets import SanFranciscoSafety

from datetime import datetime
from dateutil import parser

ENV = 'dev'
SPARK_URL = None

if ENV is 'dev' or '':
  SPARK_URL = "spark://localhost:7077"
# else:
  
end_date = parser.parse('2016-01-01')
start_date = parser.parse('2015-05-01')
safety = SeattleSafety(start_date=start_date, end_date=end_date)
safety = safety.to_spark_dataframe()

sc = pyspark.SparkContext(
  master=SPARK_URL,
  appName="SeattleVsSF"
)