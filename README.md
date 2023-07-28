kafka_to_hdfs_data_transfer
pyspark dataframe, data (or) file transfer to HDFS location using kafka streaming

For sending the data into the HDFS from kafka we have to set configure first.
for those cofiguration important aspects are given below -permission from HDFS to send the
data to HDFS -namenode and datanode are up and running -kafka server and zookeeper server also had to be up and running For the permission
to HSDF have to change in hadoop file for the reference purpose i am sharing the HDFS and core site XML files
