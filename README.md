# test-application

This is marketing analytics task.

Look for EDA and sample data [here](https://github.com/odaykhovskaya/test-application/blob/master/src/main/resources/jupyter-notebooks/EDA.ipynb).

Solution for both tasks can be found [here](https://github.com/odaykhovskaya/test-application/tree/master/src/main/scala/solution). In each package (task1, task2) there are 2 files: 'DataFrameAPI' and 'SQL', so every task was solved in both ways.

How to run locally:

* Make sure you have [Spark](https://spark.apache.org/downloads.html) installed and properly configured
* Config environment variables:
    ```
  PATH_TO_DATA_PURCHASES="<path to .xlsx file with purchases data>"
  PATH_TO_DATA_CLICKSTREAM="<path to .xlsx file with clickstream data>"
    ```
* In the root directory package run command
    ```
    sbt assembly
    ```
