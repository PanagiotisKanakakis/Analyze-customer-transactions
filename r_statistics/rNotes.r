### BIG DATA PROJECT 2018 ###
### R CODE FOR INITIAL STATISTICS ###

### LIBRARIES: readr, dplyr, plyr, ggplot2 ###

### load csv to dataframe ###
events = read.csv("events.csv")

### convert UNIX epoch timestamp column to date string ###
events$timestamp <- as.POSIXct(as.numeric(events$timestamp)/1000,origin='1970-01-01')

##############################################################
##################### EVENTS PER MONTH #######################

### create dataframe with timestamp in the form 'Jun 2015' ###
eventsWithCharDate <- data.frame("timestamp"=format(events$timestamp,"%b %Y"), "visitorid"=events$visitorid, "event" = events$event, "itemid" = events$itemid)

### create month array to use for ordering the dataframe ###
monthsOrder <- c("Μάι 2015", "Ιούν 2015", "Ιούλ 2015", "Αύγ 2015", "Σεπ 2015")

### create dataframe with count of each event type per month ###
eventsPerMonth <- count(eventsWithCharDate,c("timestamp","event"))

### properly order by month ###
eventsPerMonth$timestamp <- factor(eventsPerMonth$timestamp,levels=monthsOrder)
eventsPerMonth <- eventsPerMonth[order(eventsPerMonth$timestamp),]

### bar chart of addToCart and transaction events per month ###
eventsPerMonthBarChart <- ggplot(subset(eventsTimeline,event !='view'),aes(timestamp,freq,fill=event))+geom_bar(stat='identity',position = position_dodge())+xlab("")+ylab("counted events")

### dataframe with total events per month ###
visitsTimeline <- count(eventsWithCharDate,timestamp)

### properly order by month ###
visitsTimeline$timestamp <- factor( as.character(visitsTimeline$timestamp), levels=monthsOrder)
visitsTimeline <- visitsTimeline[order(visitsTimeline$timestamp),]

### bar chart of total events per month ###
visitsBarChart <- ggplot(visitsTimeline,aes(x=timestamp,y=n/1000))+geom_bar(stat='identity')+xlab("")+ylab("number of visits (in thousands)")


##############################################################
##################### EVENTS PER DAY #########################

### create events dataframe with timestamp per day ###
eventsPerDay <- data.frame("timestamp"=as.Date(as.character(events$timestamp,"%Y-%m-%d")), "visitorid"=events$visitorid, "event" = events$event, "itemid" = events$itemid)

### create dataframe with count of each event type per day ###
eventsCountPerDay <- count(eventsPerDay,c("timestamp","event"))

### create line chart with the above counts ###
eventLinesPerDay <- ggplot(eventsCountPerDay,aes(x=timestamp,y=freq,color=event))+geom_line()+xlab("Date")+ylab("Counted events")

### linechart with just addToCart and transactions per day ###
cartOrTransactionLinesPerDay <- ggplot(subset(eventsCountPerDay,event != 'view'),aes(x=timestamp,y=freq,color=event))+geom_line()+xlab("")+ylab("Counted events")

