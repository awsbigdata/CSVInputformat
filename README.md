# CSVInputformat
to skip new line if it is in  column


Introduction :
   By Default, TextInputFormat in hive will terminate the line if new line present in between columns.eventhough CSV serde does support it.I have created a inputformat which skip the column if it has newline with in columns and it should be enclosed with double quotes.
   
   Ex. below record will be counted as 6 rows in hive however, It has only 3 rows .
   
2,"Selvam","I live in Sydney
I like to play criket"
3,"Sankari","new line comment
seconde line "
2,"Panner","new line comment
seconde line "


Create table with custom inputformat  in hive to read CSV.

CREATE TABLE my_table_nw(id int, name string,comment string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
STORED AS  INPUTFORMAT 'org.apache.hadoop.mapred.lib.input.CSVTextInputFormat'
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://testwest2/csv';

Steps:

1. Clone the git hub code to your local 

2. Execute mvn clean install

3. copy the jar from target to hive

4. add the jar using  add jar CSVInputFormat-1.0.jar;

now, you should be able use CSVTextInputFormat.

I have attached test.csv in resource folder for reference.





