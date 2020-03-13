# test-application

This is marketing analytics task.

Look for EDA and sample data [here](https://github.com/odaykhovskaya/test-application/blob/master/src/main/resources/jupyter-notebooks/EDA.ipynb).

Solutions for both tasks can be found [here](https://github.com/odaykhovskaya/test-application/tree/master/src/main/scala/solution). Every task was solved in both ways: with Spark DataFrame API and SparkSQL. In each package (task1, task2) there are 2 files: 'DataFrameAPI' and 'SQL' with solutions. Every solution was commented step by step.

All solutions are tested [here](https://github.com/odaykhovskaya/test-application/tree/master/src/test/scala).

Answers for sample data can be found [here](https://github.com/odaykhovskaya/test-application/blob/master/src/main/resources/jupyter-notebooks/Result.ipynb).

How to run locally:

* Make sure you have [Spark](https://spark.apache.org/downloads.html) installed and properly configured environment with $SPARK_HOME variable
* Configure environment variables (You may skip this step. In this case sample data will be used):
    ```
  PATH_TO_DATA_PURCHASES="<path to .xlsx file with purchases data>"
  PATH_TO_DATA_CLICKSTREAM="<path to .xlsx file with clickstream data>"
    ```
* In the root directory package run script ```run.sh```, which uses spark-submit to run all the solutions. In case of any problems with script permissions run ``` sudo chmod +x ./run.sh``` in project's root directory.
