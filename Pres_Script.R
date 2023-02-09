library(RODBC)
library(igraph)
library(RColorBrewer)
library('plot.matrix')
set.seed(12345)

#####################
# Loading the data
#####################
run_querries <- function(){
  db = odbcConnect("mysql_server_64", uid="root", pwd="NicoSQL00!")
  sqlQuery(db, "USE ma_charity")
  
  query_year = "WITH don_20%1$s AS (SELECT contact_id 
                                  FROM acts 
                                  WHERE (act_type_id = 'DO') AND (YEAR(act_date)=20%1$s) AND
                                  (campaign_id IN ('LAL%1$s','SO%1$s','SA%1$s','SJ%1$s','SV%1$s','SF%1$s')) GROUP BY 1),
                  info_don AS (SELECT contact_id, 
                                  COUNT(amount) AS frequency, 
                                  AVG(amount) AS avg_amount, 
                                  MAX(amount) AS max_amount,
                                  SUM(amount) AS tot_amount,
                                  MAX(act_date) AS date_last_don,
                                  MIN(act_date) AS date_first_don
                                  FROM acts
                                  WHERE (YEAR(act_date) <= 20%1$s) AND
                                  (act_type_id = 'DO') AND (campaign_id IN ('LA%1$s','SO%1$s','SA%1$s','SJ%1$s','SV%1$s','SF%1$s'))
                                  GROUP BY contact_id),     
                  enc_campaign AS (SELECT contact_id,
                                  CASE WHEN campaign_id='LAL%1$s' then 1 else 0 end as act_lal%1$s,
                                  CASE WHEN campaign_id='SO%1$s' then 1 else 0 end as act_so%1$s,
                                  CASE WHEN campaign_id='SA%1$s' then 1 else 0 end as act_sa%1$s,
                                  CASE WHEN campaign_id='SJ%1$s' then 1 else 0 end as act_sj%1$s,
                                  CASE WHEN campaign_id='SV%1$s' then 1 else 0 end as act_sv%1$s,
                                  CASE WHEN campaign_id='SF%1$s' then 1 else 0 end as act_sf%1$s
                                  FROM acts),
                  enc_solicit AS (SELECT contact_id,
                                  CASE WHEN campaign_id='LAL%1$s' then 1 else 0 end as sol_lal%1$s,
                                  CASE WHEN campaign_id='SO%1$s' then 1 else 0 end as sol_so%1$s,
                                  CASE WHEN campaign_id='SA%1$s' then 1 else 0 end as sol_sa%1$s,
                                  CASE WHEN campaign_id='SJ%1$s' then 1 else 0 end as sol_sj%1$s,
                                  CASE WHEN campaign_id='SV%1$s' then 1 else 0 end as sol_sv%1$s,
                                  CASE WHEN campaign_id='SF%1$s' then 1 else 0 end as sol_sf%1$s
                                  FROM actions),
                  contact_info AS (SELECT c.id as id,
                                  IF (c.prefix_id IN ('ASSO', 'SOC'), 1, 0) AS pref_group,
                                  IF (c.prefix_id IN ('MLLE', 'MME'), 1, 0) AS pref_fem,
                                  IF (c.prefix_id IN ('DR', 'MR', 'ME', 'MMME'), 1, 0) AS pref_male
                                  FROM contacts c) 
                SELECT d%1$s.contact_id, c.pref_group, c.pref_fem, c.pref_male,
                MAX(e.act_lal%1$s) as act_lal, MAX(e.act_so%1$s) as act_so, MAX(e.act_sa%1$s) as act_sa, 
                MAX(e.act_sj%1$s) as act_sj, MAX(e.act_sv%1$s) as act_sv, MAX(e.act_sf%1$s) as act_sf,
                MAX(s.sol_lal%1$s) as sol_lal, MAX(s.sol_so%1$s) as sol_so, MAX(s.sol_sa%1$s) as sol_sa, 
                MAX(s.sol_sj%1$s) as sol_sj, MAX(s.sol_sv%1$s) as sol_sv, MAX(s.sol_sf%1$s) as sol_sf,
                i.frequency, i.avg_amount, i.max_amount, i.tot_amount,
                DATEDIFF(20%1$s1231, i.date_last_don)/365 as recency,
                DATEDIFF(20%1$s1231, i.date_first_don)/365 as first_don
                FROM don_20%1$s d%1$s INNER JOIN info_don i ON (d%1$s.contact_id = i.contact_id) 
                          INNER JOIN enc_campaign e ON (d%1$s.contact_id = e.contact_id) 
                          INNER JOIN enc_solicit s ON (d%1$s.contact_id = s.contact_id) 
                          INNER JOIN contact_info c ON (d%1$s.contact_id = c.id) GROUP BY 1;"
                        
  data_2019 <<- sqlQuery(db, sprintf(query_year, 19))
  data_2020 <<- sqlQuery(db, sprintf(query_year, 20))
  data_2021 <<- sqlQuery(db, sprintf(query_year, 21))
  
  odbcClose(db)
  
  # No need anymore to select only rows that have no nan values (individuals that didn't donate at all have nan for targetamount)
  # inner join has been used in initial query, starting from the subset of people of type "DO" that donated
  # in 2019 to one of the considered 2019 campaigns
  
  # to this has been added the usual general information (frequency, avg amount, max...) considering "DO" donations
  # that have been made by this subset in all the years <=2019 (recency computed in deviation from 31-12-2019)
  # considering only "LA" or "S" campaigns
  
  # + donor prefix information for this subset, with group that integrate (asso & soc), fem that integrates (mlle and mme)
  # and male that integrates (dr, mr, me, mmme) (sorry for madame&monsieur but they are very few, no homo tho)
  
  # + two sets of 6 columns each: ("act_" + each of ["lal", "so", "sa", "sj", "sv", "sf"]) if they donated to the campaign
  # of the particular year (here 2019) or not (even though not necessarily in 2019, I noticed there is a small
  # probability that someone donates to one of the 19 campaigns, so that he/she passes the first filter
  # and then donates to the specific 19campaign considered by the encoding in one of the following years, i think it's not a problem);
  # and the second set ("sol_" + each of ["lal", "so", "sa", "sj", "sv", "sf"]) if they were solicited
  # for that campaign of the particular year (here 2019) or not
  
  dir.create("./market_3")
  write.table(data_2019, "./market_3/data_2019.txt", sep="\t")
  write.table(data_2020, "./market_3/data_2020.txt", sep="\t")
  write.table(data_2021, "./market_3/data_2021.txt", sep="\t")
}

########################
# Uncomment this to get run the querry
########################

run_querries()

########################
# Load the files created to avoid aving to run such a long querry each time.
########################

setwd("C:/Users/nicol/Desktop/Project_mark/new")

setwd("C:/Users/nicol/Desktop/Project_mark/new/market_31")

data_2019 = read.table("./market_3/data_2019.txt", sep="\t")
data_2020 = read.table("./market_3/data_2020.txt", sep="\t")
data_2021 = read.table("./market_3/data_2021.txt", sep="\t")

#######################
# Creating an adjacency matrix for this sample.
#######################
# We create an upper triangular matrix: Mij = 1 if individual i and j donated to the same campaign
# Donating to the same campaign implies some similarity
# In terms of our subsample: Mij is one if in one of the six columns, they both have 1

create_adjacency <- function(sample, n){
  rownames(sample) <- NULL # Just letting the indices go from one to 200 to make comparison easier
  M = matrix(0, nrow=n, ncol=n)
  
  for (row in 1:(nrow(M)-1)){
    for (column in (row+1): nrow(M)){
      if((sample[row,"act_lal"] == sample[column,"act_lal"] & sample[row,"act_lal"] ==  1)
         | (sample[row,"act_so"] == sample[column,"act_so"] & sample[row,"act_so"]== 1)
         | (sample[row,"act_sa"] == sample[column,"act_sa"] & sample[row,"act_sa"]== 1)
         | (sample[row,"act_sj"] == sample[column,"act_sj"] & sample[row,"act_sj"]== 1)
         | (sample[row,"act_sv"] == sample[column,"act_sv"] & sample[row,"act_sv"]== 1)
         | (sample[row,"act_sf"] == sample[column,"act_sf"] & sample[row,"act_sf"]== 1))
      {
        M[row,column] = 1
      }
    }
  }
  return(M)
}

#######################
# Convert an adjacency matrix into graph, and print it.
#######################

create_graph <- function(M){
  
  G = graph_from_adjacency_matrix(M, mode = "undirected")
  
  plot(G, Glayout = layout.fruchterman.reingold,
       vertex.size = 10,
       vertex.color = "blue",
       vertex.frame.color = "black",
       vertex.label.color = "grey",
       vertex.label.font = 1,
       vertex.label.cex = 0.6,
       vertex.label.family = "sans",
       edge.width=1,
       edge.color = "grey")
  
  # Use full graph to compute characteristics, input has to be adjacency matrix
  return(G)
}

#######################
# Segment the graph G
#######################

create_segments <- function(G, subs){
  segments <- cluster_walktrap(G, steps = 4)
  subs <- cbind(subs, membership=segments$membership)
  classes <- unique(subs$membership)
  print(classes)
  plot(x=segments, y=G,
       vertex.size = 20,
       vertex.label.color = "white",
       vertex.label.cex = 0.55,
       vertex.label.family = "sans",
       edge.width=1)  # plot network with clustering
  
  for (i in classes){
    sub = subs[which(subs$membership == i),]
    count=dim(sub)[1]
    cat(sprintf("Class %s, frequency = %s \n", i, count))
  }
  return(subs)
}

for (i in classes){
  sub = subs_2019[which(subs_2019$membership == i),]
  count=dim(sub)[1]
  cat(sprintf("Class %s, frequency = %s \n", i, count))
}

########################
# Returns information on each segment
########################

segments_info <- function(subs){
  # Compute stats
  cols = c("pref_group",
           "pref_fem",
           "pref_male",
           "frequency",
           "act_lal",
           "act_so",
           "act_sa",
           "act_sj",
           "act_sv",
           "act_sf",
           "sol_lal",
           "sol_so",
           "sol_sa",
           "sol_sj",
           "sol_sv",
           "sol_sf",
           "avg_amount",
           "max_amount",
           "tot_amount",
           "recency",
           "first_don",
           "profitability",
           "degree",
           "between")
  
  # could also print proportion of solicited individuals per campaign (per segment)
  # it is enough to insert them above, the below store will have the proportion of
  # solicitations for a given segment for given 19 campaign among those considered
  classes <- unique(subs$membership)
  store_des = matrix(0, nrow=length(classes), ncol=length(cols))
  for (i in seq_along(classes)){
    sub = subs[which(subs$membership == classes[i]),]
    stats = colMeans(sub[,cols])
    store_des[i,] = as.numeric(stats)
  }
  
  cols_store = c("%_group",
                 "%_fem",
                 "%_male",
                 "frequency",
                 "%act_lal",
                 "%act_so",
                 "%act_sa",
                 "%act_sj",
                 "%act_sv",
                 "%act_sf",
                 "%sol_lal",
                 "%sol_so",
                 "%sol_sa",
                 "%sol_sj",
                 "%sol_sv",
                 "%sol_sf",
                 "avg_amount",
                 "max_amount",
                 "tot_amount",
                 "recency",
                 "first_don",
                 "profitability",
                 "degree",
                 "between")
  
  colnames(store_des) = cols_store
  rownames(store_des) = classes
  return(store_des)
}


visualize_info <- function(segments_info, year, cmap){
  slice_act = c("%act_lal", "%act_so", "%act_sa", "%act_sj", "%act_sv", "%act_sf")
  slice_sol = c("%sol_lal", "%sol_so", "%sol_sa", "%sol_sj", "%sol_sv", "%sol_sf")
  row_new = c("all", "sf", "sa", "sv", "so")
  col_new = c("lal", "so", "sa", "sj", "sv", "sf")
  
  act_info = segments_info[, slice_act]
  sol_info = segments_info[, slice_sol]
  
  acts_year = matrix(act_info, ncol=6)
  sol_year = matrix(sol_info, ncol=6)
  
  rownames(acts_year) = rownames(act_info)
  colnames(acts_year) = col_new
  rownames(sol_year) = rownames(act_info)
  colnames(sol_year) = col_new
  rownames(acts_year) = row_new
  rownames(sol_year) = row_new
  
  par(mfrow=c(2,1), mar=c(5.1, 5.1, 4.1, 4.1)) 
  plot(acts_year, col=cmap, main=sprintf("Donations %s", year), xlab="Campaign type", ylab="Segments")
  plot(sol_year, col=cmap[3:length(cmap)], main=sprintf("Solicitations %s", year), xlab="Campaign type", ylab="Segments")
}



########################
# Subsampling the data
########################

## We only do this subsampling once so that we have the same subsample on all our local computers. 
## To get the contact id's from this one sample, we created a text file that we will import now
## Like this, we have the same results all the time, as this is the only source of randomness


# Number of samples we select
n = 150
cost = 0.85

subs_2019 = read.table("subs_2019.txt", sep="\t")
subs_2019$profitability = subs_2019$tot_amount - cost * (subs_2019$sol_lal + subs_2019$sol_so + subs_2019$sol_sa + subs_2019$sol_sj + subs_2019$sol_sv + subs_2019$sol_sf)

#########################
# Creating the adjacency matrix for our sample
#########################

M_2019 = create_adjacency(subs_2019, n)

#########################
# Creating a network and corresponding segments
#########################

par(mfrow=c(1,1))
G_2019 = create_graph(M_2019)

# # Use full graph to compute characteristics, input has to be adjacency matrix, M created inside first function
subs_2019$degree  = degree(G_2019) # the number of connections
subs_2019$between = betweenness(G_2019) # the number of shortest paths going through an actor

subs_2019 = create_segments(G_2019, subs_2019)
subs_2019 = subs_2019[1:(length(subs_2019)-1)]
subs_2019 = subs_2019[order(subs_2019$contact_id),]

segments_info_2019 = segments_info(subs_2019)
segments_info_2019 = segments_info_2019[order(rownames(segments_info_2019)),]

visualize_info(segments_info_2019, 2019, brewer.pal(4, "Greens"))


########################
# Function to compute the information for other years.
########################

create_adjacency_year <- function(studied){
  n = nrow(studied)
  print(n)
  M_temp = matrix(0, nrow=n, ncol=n)
  for (row in 1:(n-1)){
    for (column in (row+1): n){
      if((studied[row,"act_lal"] == studied[column,"act_lal"] & studied[row,"act_lal"] ==  1)
         | (studied[row,"act_so"] == studied[column,"act_so"] & studied[row,"act_so"]== 1)
         | (studied[row,"act_sa"] == studied[column,"act_sa"] & studied[row,"act_sa"]== 1)
         | (studied[row,"act_sj"] == studied[column,"act_sj"] & studied[row,"act_sj"]== 1)
         | (studied[row,"act_sv"] == studied[column,"act_sv"] & studied[row,"act_sv"]== 1)
         | (studied[row,"act_sf"] == studied[column,"act_sf"] & studied[row,"act_sf"]== 1))
      {
        M_temp[row,column] = 1
      }
    }
  }
  return(M_temp)
}


#######################
# Segment the graph G for other years.
#######################

create_segments_year <- function(G, subs){
  segments <- cluster_walktrap(G, steps = 4)
  subs <- cbind(subs, membership=cut_at(segments, 5))
  classes <- sort(unique(subs$membership))
  print(classes)
  plot(x=segments, y=G,
       vertex.size = 20,
       vertex.label.color = "white",
       vertex.label.cex = 0.55,
       vertex.label.family = "sans",
       edge.width=1)  # plot network with clustering
  
  for (i in classes){
    sub = subs[which(subs$membership == i),]
    count=dim(sub)[1]
    cat(sprintf("Class %s, frequency = %s \n", i, count))
  }
  return(subs)
}


study_year <- function(data, list_selected){
  subs = data.frame(contact_id=list_selected)
  subs = merge(x = data, y = subs, by = "contact_id", all.y = TRUE)
  subs = subs[!is.na(subs$tot_amount ), ]
  subs$profitability = subs$tot_amount - cost * (subs$sol_lal + subs$sol_so + subs$sol_sa + subs$sol_sj + subs$sol_sv + subs$sol_sf)
  
  M_temp = create_adjacency_year(subs)
  G_temp = create_graph(M_temp)
  
  # Use full graph to compute characteristics, input has to be adjacency matrix, M created inside first function
  subs$degree  = degree(G_temp) # the number of connections
  subs$between = betweenness(G_temp) # the number of shortest paths going through an actor
  
  subs = create_segments_year(G_temp, subs)
  
  subs_final = data.frame(contact_id=list_selected)   # to have segment of people going out
  subs_final = merge(x = subs, y = subs_final, by = "contact_id", all.y = TRUE)
  subs_final[is.na(subs_final)] = 0   
  subs_final = subs_final[order(subs_final$contact_id),]
  return(subs_final)
}

par(mfrow=c(1,1))
subs_2020 = study_year(data_2020, subs_2019$contact_id)
subs_2020 = read.table("subs_2020.txt", sep="\t")
segments_info_2020 = segments_info(subs_2020)
segments_info_2020 = segments_info_2020[order(rownames(segments_info_2020)),]
visualize_info(segments_info_2020[-1,], 2020, brewer.pal(3, "BuGn"))

par(mfrow=c(1,1))
subs_2021 = study_year(data_2021, subs_2019$contact_id)
segments_info_2021 = segments_info(subs_2021)
segments_info_2021 = segments_info_2021[order(rownames(segments_info_2021)),]

visualize_info(segments_info_2021[-1,], 2021, brewer.pal(3, "YlGn"))

write.table(segments_info_2019, "./market_3/segments_info_2019.txt", sep="\t")
write.table(segments_info_2020, "./market_3/segments_info_2020.txt", sep="\t")
write.table(segments_info_2021, "./market_3/segments_info_2021.txt", sep="\t")
write.table(subs_2019, "./market_3/subs_2019.txt", sep="\t")
write.table(subs_2020, "./market_3/subs_2020.txt", sep="\t")
write.table(subs_2021, "./market_3/subs_2021.txt", sep="\t")

#######################
# Categorize
#######################

# Categorize the customers we sampled in 2019 to their correct segmented, also computed in 2019

categorize <- function(segments, subs, name){
  subs$temp = NA
  for (row in 1:nrow(subs)){
    if(subs[row, "sol_so"] == 1
       & subs[row, "sol_sa"] == 1
       & subs[row, "sol_sj"] == 1
       & subs[row, "sol_sv"] == 1
       & subs[row, "sol_sf"] == 1
       & subs[row, "sol_lal"] == 1
       & subs[row, "act_so"] == 0
       & subs[row, "act_sa"] == 0
       & subs[row, "act_sv"] == 1
       & subs[row, "act_sf"] == 0){
      subs[row, "temp"] = 4
    }
    else if(subs[row, "sol_so"] == 1
            & subs[row, "sol_sa"] == 1
            & subs[row, "sol_sj"] == 1
            & subs[row, "sol_sv"] == 1
            & subs[row, "sol_sf"] == 1
            & subs[row, "sol_lal"] == 1
            & subs[row, "act_so"] == 0
            & subs[row, "act_sf"] == 1){
      subs[row, "temp"] = 2
    }
    else if(subs[row, "sol_so"] == 1
            & subs[row, "act_so"] == 1
            & subs[row, "act_sa"] == 0
            & subs[row, "act_sj"] == 0
            & subs[row, "act_sv"] == 0
            & subs[row, "act_lal"] == 0
            & subs[row, "act_sf"] == 0){
      subs[row, "temp"] = 5
    }
    else if(subs[row, "sol_sa"] == 1
            & subs[row, "sol_sj"] == 1
            & subs[row, "sol_sv"] == 1
            & subs[row, "sol_sf"] == 1
            & subs[row, "sol_lal"] == 1
            & subs[row, "act_sa"] == 1
            & subs[row, "act_sf"] == 0){
      subs[row, "temp"] = 3
    }
    else if(subs[row, "tot_amount"] == 0){
      subs[row, "temp"] = 0
    }else subs[row, "temp"] = 1
  }
  
  segments <- cbind(segments, new_cat=subs$temp)
  names(segments)[names(segments) == 'new_cat'] <- name
  return(segments)
}

segments = data.frame(contact_id=subs_2019$contact_id, cat_2019=subs_2019$membership)
segments = categorize(segments, subs_2020, "cat_2020")
segments = categorize(segments, subs_2021, "cat_2021")

segments = read.table("segments.txt", sep="\t")
subs_2020 = read.table("subs_2020.txt", sep="\t")
subs_2019 = read.table("subs_2019.txt", sep="\t")
segments_info_2019 = read.table("segments_info_2019.txt", sep="\t")
segments_info_2020 = read.table("segments_info_2020.txt", sep="\t")


#####
#Creating transition matrix from 2019 to 2020
trans_ma_to2020<-prop.table(with(segments, table(cat_2019, cat_2020)), 1)
trans_ma_to2021<-prop.table(with(segments, table(cat_2020, cat_2021)), 1)
trans_ma_to2020<-round(trans_ma_to2020, digits=2)
trans_ma_to2021<-round(trans_ma_to2021, digits=2)
print(trans_ma_to2020)
print(trans_ma_to2021)

#segment predictions
num_periods=5
discount_rate=0.1

# create transition matrix in a markov chain: the transition matrix we computed from 2019 to 2020
# is the transition matrix we assume to be representative for the whole period.

# Absorption state here is final: once you leave it, there's no way you can come back. 
# This is not empirically supported though. 
transition_theory=cbind(c(1,0,0,0,0,0),
                 c(0.37, 0.26, 0.13, 0.11, 0.05, 0.08),
                 c(0.42, 0.12, 0.25, 0.05, 0.10, 0.05),
                 c(0.24, 0.24, 0.14, 0.17, 0.14, 0.07),
                 c(0.11, 0.17, 0.28, 0.06, 0.39, 0.00),
                 c(0.36, 0.28, 0.16, 0.08, 0.04, 0.08))

# Init the segments, populate first period
segments_ = matrix(nrow = 6, ncol = num_periods)
segments_[1:6, 1] = c(0, 38, 40, 29, 18, 25)

# Compute for each an every period (and round output for clarity)
for (i in 2:num_periods) {
  # If we multiply /*/ A mxn and B nxz, then the result is mxn
  segments_[, i] = segments_[, i-1] %*% t(transition_theory)
}
plot(x = seq(2019,2023) ,segments_[1, ] ,ylim = c(0, 120), col = 1, type = 'l', 
     xlab = "Year", ylab = "Number of people")
lines(x = seq(2019,2023), segments_[2, ], col = 2)
lines(x = seq(2019,2023), segments_[3, ], col = 3)
lines(x = seq(2019,2023), segments_[4, ], col = 4)
lines(x = seq(2019,2023), segments_[5, ], col  = 5)
lines(x = seq(2019,2023), segments_[6, ], col = 6)
#lines(segments_[6, ], col = 7)
legend(x = "topleft",
  legend = c('Absorption State', 'Segment 1', 'Segment 2', 'Segment 3', 'Segment 4', 'Segment 5'),
  fill = c(1,2,3,4,5,6,7))
title('Evolution of the number of people in each segment')


print(round(segments_))


#############
# Empirical transition matrix form 2020-2021, one can leave the absorption state!
#############


transition_empirical=cbind(c(.76,.08,.04,.04,.04,.04),
                        c(.28, 0.28, 0.16, .06, .09, .12),
                        c(.36, .25, 0.18, 0.11, 0.04, 0.07),
                        c(0.14, 0.21, 0.29, 0.07, 0.21, 0.07),
                        c(0.22, 0.17, 0.17, 0.22, 0.11, 0.11),
                        c(0.44, 0.11, 0.11, 0.0, 0.22, 0.11))


# Init the segments, populate first period
segments_ = matrix(nrow = 6, ncol = num_periods)
segments_[1:6, 1] = c(0,38, 40, 29, 18, 25)

# Compute for each an every period (and round output for clarity)
for (i in 2:num_periods) {
  # If we multiply /*/ A mxn and B nxz, then the result is mxn
  segments_[, i] = segments_[, i-1] %*% t(transition_empirical)
}
par(mfrow=c(1,1))
plot(x = seq(2019,2023) ,segments_[1, ] ,ylim = c(0, 120), col = 1, type = 'l', 
     xlab = "Year", ylab = "Number of people")
lines(x = seq(2019,2023), segments_[2, ], col = 2)
lines(x = seq(2019,2023), segments_[3, ], col = 3)
lines(x = seq(2019,2023), segments_[4, ], col = 4)
lines(x = seq(2019,2023), segments_[5, ], col  = 5)
lines(x = seq(2019,2023), segments_[6, ], col = 6)
#lines(segments_[6, ], col = 7)

legend(x = "topleft",
       legend = c('absorption state', 'segm. ALL', 'segm. SF', 'segm. SA', 'segm. SV', 'segm. SO'),
       fill = c(1,2,3,4,5,6,7), cex=0.85)
title('Evolution of the number of people in each segment')

# profitability

segments_info_2019 = segments_info_2019[order(rownames(segments_info_2019)),]
prof = segments_info_2019$profitability

seg_pr = t(segments_[-1,])

prof_mat = matrix(nrow=num_periods, ncol=dim(seg_pr)[2])
for (i in 1:nrow(seg_pr)){
  prof_mat[,i] = seg_pr[,i]*prof[i]
}
prof_mat = apply(prof_mat, 2, cumsum)

par(mfrow=c(1,1))  #ylim = c(0, 120)
plot(x = seq(2019,2023) ,prof_mat[,1 ] , ylim=c(0, 15000),col = 2, type = 'l', 
     xlab = "Year", ylab = "Cumulative revenues")
lines(x = seq(2019,2023), prof_mat[,2 ], col = 3)
lines(x = seq(2019,2023), prof_mat[,3 ], col = 4)
lines(x = seq(2019,2023), prof_mat[,4 ], col = 5)
lines(x = seq(2019,2023), prof_mat[,5 ], col  = 6)

legend(x = "topleft",
       legend = c('absorption state', 'segm. ALL', 'segm. SF', 'segm. SA', 'segm. SV', 'segm. SO'),
       fill = c(1,2,3,4,5,6,7), cex=0.85)
title('Evolution of cumulative revenues per segment')



print(round(segments_))


# plots

library(ggplot2)

db = odbcConnect("mysql_server_64", uid="root", pwd="NicoSQL00!")

query = "USE ma_charity"

sqlQuery(db, query)

query <- '
SELECT YEAR(action_date) as year, LEFT(campaign_id, 2) as campaign, count(contact_id) as count
from actions where YEAR(action_date)>2018 
group by 1, 2 ORDER BY 1, 2;'

data = sqlQuery(db, query, max = 0)

fixed = 15000
vc= 0.85

library(scales)

data$tot = data$count*vc + fixed

first = c("SA", "SO", "LA")
for (i in seq_along(first)){
  proj = diff(data[which(data$campaign==first[i]),"tot"])
  proj = mean(proj[(length(proj)-1):length(proj)])
  last = data[which(data$campaign==first[i] & data$year==2021),"tot"]
  data = rbind(data, data[which(data$campaign==first[i] & data$year==2021),])
  data[nrow(data),"year"] = 2022
  data[nrow(data),"count"] = 5
  data[nrow(data),"tot"] = last + proj
}

data %>%
  ggplot(aes(x = year,
             y = tot,
             colour = campaign))+
  geom_line(size=1.5, alpha=0.9, linetype=1)+
  scale_y_continuous("Distribiution per year")+
  scale_x_continuous("Donation amount")+
  geom_point(size=3)+
  scale_colour_brewer(palette = "Greens")+
  theme(panel.background = element_blank(),
        panel.grid.minor.y = element_line(colour="gray", size=0.1),
        panel.grid.major.y = element_line(colour="gray", size=0.5),
        text = element_text(size = 15))


first = c("LA","SA","SF","SJ","SO","SV")
for (i in seq_along(first)){
  proj = diff(data[which(data$campaign==first[i]),"tot"])
  proj = mean(proj[(length(proj)-1):length(proj)])
  last = data[which(data$campaign==first[i] & data$year==2022),"tot"]
  data = rbind(data, data[which(data$campaign==first[i] & data$year==2022),])
  data[nrow(data),"year"] = 2023
  data[nrow(data),"count"] = 5
  data[nrow(data),"tot"] = last + proj
}

data %>%
  ggplot(aes(x = year,
             y = tot,
             colour = campaign))+
  labs(title="Historical and projected \ntotal costs per campaign",
        x ="Year", y = "Total costs per campaign")+
  geom_line(size=1.5, alpha=0.9, linetype=1)+
  geom_point(size=3)+
  scale_colour_brewer(palette = "Greens")+
  theme(panel.background = element_blank(),
        panel.grid.minor.y = element_line(colour="gray", size=0.1),
        panel.grid.major.y = element_line(colour="gray", size=0.5),
        text = element_text(size = 15))



###########################################################@
# Ignore from here
###########################################################@



########################
# We get really high numbers of communities in the clustering, maybe scaling betweenness helps
########################

# subs_2019$between = (subs_2019$between - min(subs_2019$between)) / (max(subs_2019$between) -min(subs_2019$between))

# betweenness is very high if they donated to all campaigns
# betwenness probably the most important, we can analize specific with idea that may donate in a 
# across-campaign pattern, since they have connection with segments of donours (or not) but stand-alone

# CLUSTERING
# function description in manual: "Many networks consist of modules which are densely connected themselves but sparsely connected to other modules
# The edge betweenness score of an edge measures the number of shortest paths through it. The idea of the edge betweenness based community 
# structure detection is that it is likely that edges connecting separate modules have high edge betweenness as all the shortest paths from 
# one module to another must traverse through them.  So if we gradually remove the edge with the highest edge betweenness score we will get a 
# hierarchical map, a rooted tree, called a dendrogram of the graph"
# final clustering == membership vector corresponding to highest modularity score (==considering all possibly community structures following
# the betweenness-based edge removal)
# also https://stackoverflow.com/questions/9471906/what-are-the-differences-between-community-detection-algorithms-in-igraph

# creating full matrix necessary for this thing
segments <- cluster_walktrap(G, steps = 4) 
memb_v = segments$membership
classes <- unique(segments$membership)
print(classes)
plot(x=segments, y=G, 
     vertex.size = 20, 
     vertex.label.color = "white", 
     vertex.label.cex = 0.55,
     vertex.label.family = "sans",
     edge.width=1)  # plot network with clustering


#ceb <- cluster_edge_betweenness(G, directed=FALSE) 
# ceb2 <- edge.betweenness.community(G, directed=FALSE) equivalent

# this gives clusters
#memb_v = ceb$membership
#classes = unique(ceb$membership)
#print(length(memb_v))  # corresponds to nodes
#print(classes) # number of classes

#dendPlot(ceb, mode="hclust")   # plots as hierarchical clustering
#plot(x=ceb, y=G, 
#     vertex.size = 20, 
#     vertex.label.color = "white", 
#    vertex.label.cex = 0.55,
#    vertex.label.family = "sans",
#    edge.width=1)  # plot network with clustering


# print frequency
for (i in classes){
  sub = subs_2019[which(memb_v == i),]
  count=dim(sub)[1]
  cat(sprintf("Class %s, frequency = %s \n", i, count))
}

# Compute stats
cols = c("pref_group", "pref_fem", "pref_male", "frequency", "act_lal", "act_so", "act_sa", "act_sj", "act_sv", "act_sf",
         "sol_lal", "sol_so", "sol_sa", "sol_sj", "sol_sv", "sol_sf",
         "avg_amount",
         "max_amount", "tot_amount", "recency", "first_don", "profitability" ,"degree", "between", "close")  
# could also print proportion of solicitated individuals per campaign (per segment)
# it is enough to insert them above, the below store will have the proportion of
# solicitations for a given segment for given 19 campaign among those considered

store_des = matrix(0, nrow=length(classes), ncol=length(cols))

for (i in seq_along(classes)){
  sub = subs_2019[which(memb_v == classes[i]),]
  stats = colMeans(sub[,cols])
  store_des[i,] = as.numeric(stats)
}

cols_store = c("%_group", "%_fem", "%_male", "frequency", "%act_lal", "%act_so", "%act_sa", "%act_sj", "%act_sv", "%act_sf",
               "%sol_lal", "%sol_so", "%sol_sa", "%sol_sj", "%sol_sv", "%sol_sf",
               "avg_amount",
               "max_amount", "tot_amount", "recency", "first_don", "profitability","degree", "between", "close")

colnames(store_des) = cols_store
rownames(store_des) = classes
print(store_des)
# store with as row: classes, 
# as columns: average of predetermined variables by column



# ----------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# here I selected those with highest betweenness, group them in a single segment
# and replot everything by substituting ceb$membership with the newly defined membership
# but actually seeing the results above the most profitable donors are those with lowest betweenness
# we could do the inverse

# we try to select those with highest betwenness
hist(subs_2019$between, col = 'skyblue3', breaks = 30)
idx = which(subs_2019$between>90)   # select > 90, I found 10% of customers
print(length(idx))  # how many

# set this subset to a unique class, the number is the greatest class number so far + 1
# idea: no clusters should disappear, cause we are selecting people that are for sure not alone in the net
# (they are in-between)
ceb$membership[idx] = sort(unique(ceb$membership))[length(unique(ceb$membership))] + 1  
memb_v = ceb$membership  # new membership
n_classes = unique(ceb$membership)   # new classes (expected: as before + 1)
print(length((unique(ceb$membership))))  # print new n. class

plot(x=ceb, y=G, 
     vertex.size = 20, 
     vertex.label.color = "white", 
     vertex.label.cex = 0.55,
     vertex.label.family = "sans",
     edge.width=1)  # plot network with clustering


# comparison with before
# print frequency
for (i in classes){
  sub = subs_2019[which(memb_v == i),]
  count=dim(sub)[1]
  cat(sprintf("Class %s, frequency = %s \n", i, count))
}

# Compute stats
cols = c("pref_group", "pref_fem", "pref_male", "frequency", "avg_amount",
         "max_amount", "tot_amount", "recency", "first_don", "degree", "between", "close")  
# could also print proportion of solicitated individuals per campaign (per segment)
# it is enough to insert them above, the below store will have the proportion of
# solicitations for a given segment for given 19 campaign among those considered

store_des = matrix(0, nrow=length(classes), ncol=length(cols))

for (i in seq_along(classes)){
  sub = subs_2019[which(memb_v == classes[i]),]
  stats = colMeans(sub[,cols])
  store_des[i,] = as.numeric(stats)
}

cols_store = c("%_group", "%_fem", "%_male", "frequency", "avg_amount",
               "max_amount", "tot_amount", "recency", "first_don", "degree", "between", "close")

colnames(store_des) = cols_store
rownames(store_des) = classes
print(store_des)
# ----------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------

# SAME FOR 2020
# we select same subgroup of people as in 2019 but compute all the statistics
# + the 12 variables of donation/solicitation are computed for the 6 campaigns in 2020
# clearly contact information are the same

# there is possibility any source of combination for donation/solicitaions relative to 2020 campaigns considered
# for what concerns our 2019 sample. There is possibility that some of them neither donate nor are solicitated 
# (even) if improbable they are not solicitated
# We expect 20517 out of the initial 27772 to donate also in 2020 (out of the 35647) distinct donours of 2020
# projected towards 2021, we expect 15384 donours to remain out of the 30780 distinct donours of that year
# seems good since we have a peak of donours in 2020 most likely Web donations for Covid that are difficult to re-trace
# or keep tight (even if critial in other contexts) that we do not consider (as we start from 2019),
# and then we see that the total number goes down for 2021 but we still have 50% of them

# if we change the initial subset we can simply modify the first temporary table
# otherwise for 2021 we can simply change the year and use same query




# FAI LO STESSO SU 2021
# PRENDENDO GLI STESSI CUSTOMER ID
# SPIEGA RAGIONAMENTO DI QUANTI CUSTOMERS TIENI A CAVALLO DI QUEGLI ANNI



# Based on this, what can we all do? 

# Try to make clusters with some kmeans or sth
# Run regression with betweenness as target: high betweenness means donates to most of the campaigns
# Add attributes, like number of times the person was solicited out of all 6 campaigns
