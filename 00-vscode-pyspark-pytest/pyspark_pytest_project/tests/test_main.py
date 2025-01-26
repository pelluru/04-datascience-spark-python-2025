import pytest
from app.main import create_spark_session

def test_create_spark_session():
    app_name = 'TestApp'
    spark = create_spark_session(app_name)
    assert spark is not None
    assert spark.sparkContext.appName == app_name