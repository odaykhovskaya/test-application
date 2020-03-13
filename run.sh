sbt 'set test in assembly := {}' assembly

spark-submit --class solution.task1.Main --packages 'com.crealytics:spark-excel_2.11:0.12.5' target/scala-2.11/test-application-assembly-0.1.jar

spark-submit --class solution.task2.Main --packages 'com.crealytics:spark-excel_2.11:0.12.5' target/scala-2.11/test-application-assembly-0.1.jar
