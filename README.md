# PIG-Latin-Custom-Load-Function

Run the following script on PIG grunt shell to Invoke this Custom Load Function on Apache log files

REGISTER '/home/cloudera/logloader.jar';

input_data = load '/home/cloudera/logdata/input.txt' using com.custom.loadfunction.LogLoaderFunc() as (output:chararray);

DUMP input_data;
