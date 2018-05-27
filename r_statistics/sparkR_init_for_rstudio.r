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
item_prop_schema <- structType(structField("timestamp","string"),structField("itemid","string"),structField("property","string"),structField("value","string"))
cats_ancestors_schema <- structType(structField("categoryid","string"),structField("ancestorid","string"))
events_schema <- structType(structField("timestamp","string"),structField("visitorid","string"),structField("event","string"),structField("itemid","string"),structField("transactionid","integer"))

#read properties csvs
item_properties <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/item_properties_part1.csv",source="csv",header="true",schema = item_prop_schema)
item_properties_2 <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/item_properties_part2.csv",source="csv",header="true",schema = item_prop_schema)
cats_ancestors_df <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/exported_csv/ancestor_per_catid.csv",source="csv",header="true",schema = cats_ancestors_schema)

#union the 2 dataframes
item_properties = union(item_properties,item_properties_2)

#convert unix epoch timestamp to datetime string in a new dataframe
item_props_with_ts <- withColumn(item_properties, "ts", from_unixtime(item_properties$timestamp/1000))

#read events.csv
events <- read.df("/media/myrto/Files/Documents/Master/DI UOA/BigData/project/Retailrocket_recom_sys_dataset/events.csv",source="csv",header="true",delimiter=",", na.strings="NA",schema = events_schema)
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

createOrReplaceTempView(cats_ancestors_df, "categories_with_ancestors")

#get items and their general categories per time
used_items_categories <- sql("SELECT e.itemid, e.timestamp, c.ancestorid FROM events e, item_cats_from_properties p, categories_with_ancestors c WHERE e.itemid=p.itemid AND p.property LIKE 'categoryid' AND p.value=c.categoryid")

#get weighted property values per item per event
props_per_event <- sql("SELECT e.timestamp, e.itemid, e.visitorid, i.property, max( ( 86400000/(e.timestamp-i.timestamp)) ) as weight FROM events_table e, properties_table i WHERE e.itemid=i.itemid AND i.property NOT IN('categoryid','available') AND i.timestamp < e.timestamp GROUP BY e.timestamp, e.itemid, e.visitorid, i.property")


## NEW VISUALISATION user/item implicit ratings scatter plot ##
## requires that item_properties, events and categor_ancestors have already been loaded and timestamp converted (optional) ##

## new events schema that includes 'ratings' ##
events_with_ratings_schema <- structType(structField("timestamp","string"),structField("visitorid","string"),structField("event","string"),structField("itemid","string"),structField("transactionid","integer"), structField("eventRating","double"))

## lambda expression on events to create the new column according to event type [map-like function called dapply]
events_with_ratings <- dapply(events, function(x) { x <- { a <- 0.0
	if (x$event == "view") {
		a = 0.3
	} else if (x$event == "addtocart") {
		a = 0.7
	} else {
		a = 1.0
	}
	cbind(x, a ) }
	}, events_with_ratings_schema)

createOrReplaceTempView(events_with_ratings, "events_with_ratings_table")


## create a df with ratings, itemid, visitorid, category and ancstor of every event
user_item_ratings_with_cats <- sql("SELECT e.visitorid, e.itemid, sum(e.eventRating), c.categoryid, c.ancestorid FROM events_with_ratings_table e, item_cats_from_properties p, categories_with_ancestors c WHERE e.itemid = p.itemid AND p.value LIKE c.categoryid GROUP BY e.visitorid, e.itemid, c.categoryid, c.ancestorid")

# convert to r dataframe in order to plot
user_item_ratings_with_cats <- collect(user_item_ratings_with_cats)