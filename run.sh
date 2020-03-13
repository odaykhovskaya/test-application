sbt package

export $RESULT_FOLDER_NAME="result"
mkdir $RESULT_FOLDER_NAME

export $PATH_TO_RESULT="./$RESULT_FOLDER_NAME"

spark-submit --class solution.task1.DataFrameAPI --master local[*] target/scala-2.11/test-application_2.11-0.1.jar