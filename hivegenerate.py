import pandas as pd
import re


def generate_hive_meta(table):
    table.name = 'jams' 
    dict = {'int64':'int','object':'string','float64':'double',}
    col_type_temp = [ ]
    for i in range(len(table.keys())):
        a = table.keys()[i] +" "+ dict[str(table.dtypes[i])]
        col_type_temp.append(a)
    col_type = re.sub("\'|\[|\]", "", str(col_type_temp))
    command = "CREATE EXTERNAL TABLE IF NOT EXISTS df_test."+table.name+"(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location 'hdfs:///user/husni/muhammadrizki/%s';" % (col_type, table.name)
    return command