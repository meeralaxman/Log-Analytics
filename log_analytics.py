#importing Libraries
import re as re
import sys
import datetime

from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from pyspark import SparkContext



sc = SparkContext()
HiveContext = HiveContext(sc)

#Pattern matching for the log file lines using Regular Expressions
ELB_LOG_PATTERN = re.compile(r"""
                             \s*
                             ([^ ]*)
                             \s*
                             ([^ ]*)
                             \s*
                             ([^ ]*)[:-]([0-9]*)   
                             \s*
                             ([^ ]*)[:-]([0-9]*)
                             \s*
                             ([-.0-9]*)
                             \s*
                             ([-.0-9]*)
                             \s*
                             ([-.0-9]*)
                             \s*
                             (|[-0-9]*)
                             \s*
                             (-|[-0-9]*)
                             \s*
                             ([-0-9]*)
                             \s*
                             ([-0-9]*)     
                             \s*
                             "(\S+)
                             \s*
                             (\S+)
                             \s*
                             (\S+)"    
                             \s*
                             \"([^\"]*)\"
                             \s*
                             ([A-Z0-9-]+)
                             \s*
                             ([A-Za-z0-9.-]*)                         
                             \s*$
                             """,re.VERBOSE)


def parse_elb_log_line(logline):
	logline=logline.encode('utf-8')    #converting from unicode to utf-8 format
	match = re.search(ELB_LOG_PATTERN, logline)
	if match is None:
	   raise Error("Invalid logline: %s" % logline)

        return Row(
		timestamp = match.group(1),
		elb_name = match.group(2),
		client_ipadd = match.group(3),
		client_port=match.group(4),
		backend_ip = match.group(5),
		backend_port=match.group(6),
		request_processing_time = match.group(7),
		backend_processing_time = match.group(8),
		response_processing_time = match.group(9),
		elb_status_code=match.group(10),
		backend_status_code=match.group(11),
		received_bytes=match.group(12),
		sent_bytes=match.group(13),
		request_method=match.group(14),
		end_point=match.group(15),
		protocol=match.group(16),
		user_agent=match.group(17),
		ssl_cipher=match.group(18),
		ssl_protocol=match.group(19)
        )
	
	

#Reading the input file into an RDD
input=sc.textFile("hdfs://prcsparkprod/user/skprccm/2015_07_22_mktplace_shop_web_log_sample.log.gz")

#calling the function for log file parsing
input_parse=input.map(lambda line:parse_elb_log_line(line)).cache()

#creating a dataframe for input_parse rdd
df=HiveContext.createDataFrame(input_parse)

#dropping duplicates from the input dataset
df.dropDuplicates()

count=df.count()
print("\n\n Number of Processing Records are:::::%d"%count)

#filtering the source dataframe for only required columns
df_filter = (df.select(df.client_ipadd.alias("ip_address"),
               df.timestamp,
               df.end_point.alias("hostname")
                )).repartition(120)


print("\n\n\nSample Records of Filtering are\n\n\n")
df_filter.show(10, False)


df1=df_filter.withColumn("prevTimestamp",lag(df_filter.timestamp,1).over(Window.partitionBy(df_filter.ip_address).orderBy(df_filter.timestamp)))

#definging the time frame window duration of 15 minutes in sesocnds and time format
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
timeWindow = 900

#Adding new columns that are needed in calcualtions
df2= df1.withColumn("diff_seconds",F.unix_timestamp(df1.timestamp,format=timeFmt)- F.unix_timestamp(df1.prevTimestamp,format=timeFmt))

df3=df2.withColumn("isNewSession",F.when(df2.diff_seconds <= timeWindow,0).otherwise(1))	

df4=df3.withColumn("session_id",sum(df3.isNewSession).over(Window.partitionBy(df3.ip_address).orderBy(df3.ip_address, df3.timestamp)))

df5=df4.withColumn("session_count_distinct",max(df4.session_id).over(Window.partitionBy(df4.ip_address)))

#cache the df5 as it is used many times
df5.cache()	

df6=df5.groupby(df5.ip_address,df5.session_id).count()
print("************** Number of Hits per each Ip Address during each session *********\n\n")
df6.write.format("orc").saveAsTable("elb_logs_hits")
df6.show(20,False)   #this is to show to a sample of 20 records on the console


df7=df5.withColumn("min_time_stamp",min(df5.timestamp).over(Window.partitionBy(df5.ip_address)))
df8=df7.withColumn("max_time_stamp",max(df7.timestamp).over(Window.partitionBy(df7.ip_address)))
df9=df8.withColumn("total_session_duration",F.unix_timestamp(df8.max_time_stamp,format=timeFmt)-  F.unix_timestamp(df8.min_time_stamp,format=timeFmt))

df10=df9.withColumn("avg_session_time",df9.total_session_duration / df9.session_count_distinct)
print("*************Avg Session Time is ***************************************************\n\n")
df10.select('ip_address','session_id','avg_session_time').write.format("orc").saveAsTable("elb_logs_avg_session_times")
df10.show(20,False)

print(" ***********************************Unique URL visits per session are ******************************************* \n\n")
df11=df10.select('ip_address','session_id','hostname').distinct()
df11.write.format("orc").saveAsTable("elb_logs_unique_url_visits")
df11.show(20,False)

print("****************** most enegabed users are **********************\n\n")
df12=df10.select('ip_address','total_session_duration').dropDuplicates().sort(df10.total_session_duration.desc())
df12.write.format("orc").saveAsTable("elb_logs_engaged_users")
df12.show(20,False)


