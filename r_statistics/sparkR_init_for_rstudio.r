### sparkR init for Rstudio ###
### taken from the official documentation: ###
### https://spark.apache.org/docs/latest/sparkr.html#starting-up-from-rstudio ###

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/usr/local/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))


### read csvs after init ###

#create item_properties.csv schema [mainly to reaf timestamp as string]
item_prop_schema <- structType(structField("timestamp","string"),structField("itemid","integer"),structField("property","string"),structField("value","string"))

#read properties csvs
item_properties <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/item_properties_part1.csv",source="csv",header="true",schema = item_prop_schema)
item_properties_2 <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/item_properties_part2.csv",source="csv",header="true",schema = item_prop_schema)

#union the 2 dataframes
item_properties = union(item_properties,item_properties_2)

#convert unix epoch timestamp to datetime string in a new dataframe
item_props_with_ts <- withColumn(item_properties, "ts", from_unixtime(item_properties$timestamp/1000))

#read events.csv
events <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/events.csv",source="csv",header="true",delimiter=",", na.strings="NA",schema = structType(structField("timestamp","string"),structField("visitorid","integer"),structField("event","string"),structField("itemid","integer"),structField("transactionid","integer")))
events <- withColumn(events, "timestamp", from_unixtime(events$timestamp/1000))

#read category_tree.csv
category_tree <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/category_tree.csv",source="csv",header="true",inferSchema="true")


### prepare data for visualization ###
#get distinct itemids in events
createOrReplaceTempView(events,"events_table")
items_in_events <- sql("SELECT distinct(itemid) from events_table")

createOrReplaceTempView(items_in_events,"items_table")
createOrReplaceTempView(item_properties,"properties_table")

#get items with category from properties
items_with_category <- sql("SELECT itemid,value from properties_table WHERE property LIKE 'categoryid'")

createOrReplaceTempView(items_with_category,"item_cats_from_properties")
createOrReplaceTempView(items_in_events,"used_items_table")
items_in_events_categories <- sql("SELECT distinct(i.itemid) as itemid,p.value FROM used_items_table i, item_cats_from_properties p WHERE i.itemid=p.itemid")

createOrReplaceTempView(category_tree,"categories_table")
#get parent categories
parent_categories <- sql("SELECT categoryid FROM categories_table WHERE parentid LIKE ''")

#get parent items ###NOT NEEDED###
createOrReplaceTempView(items_in_events_categories,"used_items_cats")
createOrReplaceTempView(parent_categories,"parents_table")
parent_items <- sql("SELECT itemid,value FROM used_items_cats WHERE value IN(SELECT * FROM parents_table)")


#get distinct items
used_items_categories <- sql("SELECT distinct(e.itemid), p.value FROM events e, item_cats_from_properties p WHERE e.itemid=p.itemid AND p.property LIKE 'categoryid'")

