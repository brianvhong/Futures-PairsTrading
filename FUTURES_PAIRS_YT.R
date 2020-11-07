# ******************************************************************************************************************************************************
#                                                 COINTEGRATION MODELS
# ******************************************************************************************************************************************************
# LOAD DATASET
require("quantmod");require("timeDate");require("data.table"); require("pbapply");require("stringr")
require("odbc"); require("tseries");require("dplyr")
# FUTURES TICKERS
NOMS <- c("ZC","A6","E6","ZW","B6","GLB","ZS","GE","ZT","ZF","ZQ","ZL","ZO",
          "QG","GC","ZN","ZM","LE","YM","ES","NQ")

# get data from Database
DB2XTSFUTURES = function(x){
  con <- dbConnect(odbc(), Driver = "/usr/local/mysql-connector-odbc-8.0.11-macos10.13-x86-64bit/lib/libmyodbc8a.so", 
                   Server = "localhost", Database = "DATA", UID = "root", PWD = "ENTER PASSWORD HERE", 
                   Port = 3306)
  tmp <- data.table(dbGetQuery(con,paste0("SELECT DISTINCT * FROM DATA.TOSFUTURESd WHERE ShortName= '",x,
                                          "' ORDER BY DateTime;")))
  dbDisconnect(con)
  tmp <- xts(tmp[,2:5],order.by = as.Date(sapply(tmp$DateTime,as.character),format="%Y-%m-%d"))
  colnames(tmp) <- paste0(x,".",names(tmp))
  Cl(tmp)
}

ALL <- do.call(merge,pblapply(as.list(NOMS),function(x){
  tmp <- try(DB2XTSFUTURES(x))
  if(!inherits(tmp,'try-error'))
    tmp
}))
# Data from 2008-03-31 to 2018-12-31
ALL <- ALL["20080331/20181231"]

# index formatting
DATES <- index(ALL)
# EXCLUDE TRADING NYSE HOLIDAYS & WEEKENDS
ALL <- ALL[isBizday(as.timeDate(DATES), holidays=holidayNYSE(year=2008:2018),wday = 1:5)]
ALL <- na.locf(ALL)
ALL <- na.locf(ALL,fromLast = TRUE)
RETS1 <- ALL
colnames(RETS1)<- gsub(".Close","",names(RETS1))

# EXTRACT NAMES
NOMS <- names(RETS1)

# create combinations of tickers
n <- combn(NOMS,4)
n <- t(n)
n <- na.omit(n)

# NUMBER OF ROWS
NR<- nrow(n)

# MUST BE PRICE SERIES
PAIRS <- pblapply(as.list(c(1:NR)), function(i){
  A <- n[i,1]
  B <- n[i,2]
  C <- n[i,3]
  D <- n[i,4]
  ASSET1 <- RETS1[,paste(A)]
  ASSET2 <- RETS1[,paste(B)]
  ASSET3 <- RETS1[,paste(C)]
  ASSET4 <- RETS1[,paste(D)]
  # RUN THE REGRESSION
  m <- lm(ASSET1 ~ ASSET2 + ASSET3 + ASSET4 + 0)
  beta1 <- coef(m)[1]
  beta2 <- coef(m)[2]
  beta3 <- coef(m)[3]
  # CREATE THE SPREAD AND TEST FOR COINTEGRATION
  sprd <- ASSET1 - (beta1*ASSET2 + beta2*ASSET3 + beta3*ASSET4)
  p <- try(suppressWarnings(adf.test(sprd, alternative="stationary", k=0)$p.value), silent = TRUE)
  if(!inherits(p,"try-error"))
    as.data.frame(cbind(paste(A), paste(B), paste(C),paste(D), round(beta1,4), 
                        round(beta2,4), round(beta3,4),round(p,4)),stringsAsFactors=FALSE)
})

# rowbind Pairs
Ps <- rbindlist(PAIRS)
colnames(Ps)<- c("asset1","asset2","asset3","asset4","beta1","beta2","beta3", "pval")
# ***********************************************************************************
# subset only desired cases
Ps$pval <- sapply(Ps$pval, as.numeric)
pairs2 <- subset(Ps, Ps$pval <= 0.01)
pairs2 <- pairs2[order(pairs2$pval), ,drop=FALSE]
# SUBSET FOR DESIRED PRICES
pairs2 <- as.data.frame(pairs2)
# *******************************************************************************************************
#                                           STRATEGY TO BACKTEST
# *******************************************************************************************************
# Minimum Tick value for each contract
TICK <- as.data.frame(c(0.0001,0.0001,0.0001,0.0001,0.001,0.01,0.01,0.1,0.25,0.25,1,0.0005,0.005,0.005,
                        0.005,0.1,0.1,0.1,0.03125,0.0078125,0.015625,0.0001,0.00005,0.0000005,0.00005,
                        0.0001,0.00001,0.0078125,0.25,0.25,0.0025,0.25,0.25,0.025),
                      row.names = c("N6","S6","RB","HO","NG","ZL","CL","ZM","ES","NQ","YM","HG","SI",
                                    "GE","ZQ","PL","PA","GC","ZB","ZF","ZN","A6","D6","J6","E6","B6",
                                    "M6","ZT","ZW","ZC","GLB","ZS","ZO","LE"))

# TICK VALUE for each contract
TICKV <- as.data.frame(c(10.00,12.50,4.20,4.20,10.00,6.00,10.00,10.00,12.50,5.00,5.00,12.50,25.00,12.50,
                         20.84,5.00,10.00,10.00,31.25,7.81,15.63,10.00,5.00,6.25,6.25,6.25,5.00,15.63,
                         12.50,12.50,6.25,12.50,12.50,10.00),
                       row.names = c("N6","S6","RB","HO","NG","ZL","CL","ZM","ES","NQ","YM","HG","SI",
                                     "GE","ZQ","PL","PA","GC","ZB","ZF","ZN","A6","D6","J6","E6","B6",
                                     "M6","ZT","ZW","ZC","GLB","ZS","ZO","LE"))
# Function to apply strategy & Generate Signal
RETURNS  = function(ii){
  # get the future contract tickers
  tics <- as.character(c(pairs2[ii,1:4],collapse=""))[1:4]
  # subset Prices for desired futures contracts
  s<- RETS1[,tics]
  # get the weights/Betas
  BETAS <- as.numeric(pairs2[ii,5:7])
  # SPX is the contract we are trying to predict
  SPX <- s[,1]
  # Y-Hat is the predicted value of SPX
  YHAT <- reclass((BETAS[1]*coredata(s[,2])) + (BETAS[2]*coredata(s[,3])) + (BETAS[3]*coredata(s[,4])),
                  match.to = s)
  colnames(YHAT) <- "YHAT"
  x <- merge.xts(s,YHAT)
  
  #generate signals
  x$sig <- NA
  # LONG SPX if below the basket of futures contracts represented by Y-HAT & SHORT the Basket
  # SHORT SPX if above the basket of futures contracts represented by Y-HAT & LONG the Basket 
  # We close out the contracts when the Predicted Series (Y-HAT) crosses SPX
  x$sig[c(FALSE, diff(sign(x[,1] - x$YHAT), na.pad=FALSE) != 0)] <- 0
  x$sig[x[,1] > x$YHAT] <- -1 
  x$sig[x[,1] < x$YHAT] <- 1 
  x$sig[1] <- 0 # flat on the first day
  x$sig[nrow(x)] <- 0 # flat on the last day
  
  # Fill in the signal for other times
  x$sig <- na.locf(x$sig) 
  
  # Now Lag your signal to reflect that you can't trade on the same bar that 
  # your signal fires
  x$sig <- Lag(x$sig)
  x$sig[1] <- 0 # replace NA with zero position on first row
  x
}

# This function will generate the returns of the strategy along with:
# Number of trades, Drawdowns, Entry/Exits, and keep track of our equity curve
FUTURES_STRAT = function(x, EQT=50000){
  # Pass in our strategy & get the betas
  datset <- x
  NN <- names(datset)[1:4]
  beta <- subset(pairs2,pairs2$asset1 == NN[1] & pairs2$asset2 == NN[2] & 
                   pairs2$asset3 == NN[3] & pairs2$asset4 == NN[4])[5:7]
  
  datset$ASSET1 <- NA
  datset$ASSET2 <- NA
  datset$ASSET3 <- NA
  datset$ASSET4 <- NA
  
  datset$ASSET1[1] <- datset[1,1]
  datset$ASSET2[1] <- datset[1,2]
  datset$ASSET3[1] <- datset[1,3]
  datset$ASSET4[1] <- datset[1,4]
  
  # TRADE PRICE FOR ASSET 1
  for(k in 2:(nrow(datset))){
    datset[k,7] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                          datset[(k-1),7],datset[k,1])
  }
  # TRADE PRICE FOR ASSET 2
  for(k in 2:(nrow(datset))){
    datset[k,8] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                          datset[(k-1),8],datset[k,2])
  }
  # TRADE PRICE FOR ASSET 3
  for(k in 2:(nrow(datset))){
    datset[k,9] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                          datset[(k-1),9],datset[k,3])
  }
  # TRADE PRICE FOR ASSET 4
  for(k in 2:(nrow(datset))){
    datset[k,10] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                           datset[(k-1),10],datset[k,4])
  }
  # P&L POINTS FOR ASSET 1
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS1 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,1])),])[1]
  TICKval <-coredata(TICKV[rownames(TICKV)== paste(names(datset[,1])),])[1]
  SIGN <- ifelse(beta$beta1 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,11] <- ifelse(coredata(datset[(k),7])!=coredata(datset[(k-1),7]),
                           reclass((((coredata(datset[k,7]) - coredata(datset[(k-1),7]))*coredata(datset[k-1,"sig"])*SIGN)/minTICK*TICKval)-TICKval,
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # P&L POINTS FOR ASSET 2
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS2 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,2])),])[1]
  TICKval <-coredata(TICKV[rownames(TICKV)== paste(names(datset[,2])),])[1]
  SIGN <- ifelse(beta$beta1 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,12] <- ifelse(coredata(datset[(k),8])!=coredata(datset[(k-1),8]),
                           reclass((((coredata(datset[k,8]) - coredata(datset[(k-1),8]))*coredata(datset[k-1,"sig"])*-SIGN)/minTICK*TICKval)-TICKval, 
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # P&L POINTS FOR ASSET 3
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS3 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,3])),])[1]
  TICKval <-coredata(TICKV[rownames(TICKV)== paste(names(datset[,3])),])[1]
  SIGN <- ifelse(beta$beta2 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,13] <- ifelse(coredata(datset[(k),9])!=coredata(datset[(k-1),9]),
                           reclass((((coredata(datset[k,9]) - coredata(datset[(k-1),9]))*coredata(datset[k-1,"sig"])*-SIGN)/minTICK*TICKval)-TICKval,
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # P&L POINTS FOR ASSET 4
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS4 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,4])),])[1]
  TICKval <- coredata(TICKV[rownames(TICKV)== paste(names(datset[,4])),])[1]
  SIGN <- ifelse(beta$beta3 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,14] <- ifelse(coredata(datset[(k),10])!=coredata(datset[(k-1),10]),
                           reclass((((coredata(datset[k,10]) - coredata(datset[(k-1),10]))*coredata(datset[k-1,"sig"])*-SIGN)/minTICK*TICKval)-TICKval,
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # CALCULATE GROSS PROFIT
  datset$GROSS <- round(rowSums(datset[,11:14]),2)
  
  # CALCULATE NET PROFIT
  # 4 * 2.25 == COMMISSIONS
  # You can adjust them below
  datset$NET <- 0
  for(k in 2:nrow(datset)){
    datset[k,"NET"] <- ifelse(coredata(datset[k,"GROSS"]) != 0, 
                              reclass((coredata(datset[k,"GROSS"]) - (2.25*8)), 
                                      match.to=datset[k,"GROSS"]), 0)
  }
  
  # CALCULATE EQUITY
  datset$EQT <- EQT
  for(k in 2:nrow(datset)){
    datset[k,"EQT"] <- ifelse(coredata(datset[(k),"NET"])!=0,
                              reclass(coredata(datset[k,"NET"]) + coredata(datset[(k-1),"EQT"]), 
                                      match.to=datset)
                              ,datset[(k-1),"EQT"])
  }
  
  # CALCULATE DRAWDOWNS
  datset$DD <- round(-(cummax(datset[,"EQT"])-datset[,"EQT"])/datset[,"EQT"],4)
  
  # COUNT THE NUMBER TRADES
  datset$Trades <- 0
  for(k in 2:nrow(datset)){
    datset[k,"Trades"] <- ifelse(coredata(datset[k,"NET"]) != 0 , 
                                 reclass(1+coredata(datset[k-1,"Trades"]),
                                         match.to=datset), 
                                 datset[k-1,"Trades"])
  }
  # COUNT DRAWDOWN DURATION
  datset$duration <- 0
  for(k in 2:nrow(datset)){
    datset[k,"duration"] <- ifelse(coredata(datset[k,"DD"]) != 0, 
                                   reclass(sum(coredata(datset[(k-1),"duration"])+1), 
                                           match.to=datset[k,1]), 0)
  }
  return(datset)
}
# Apply the Strategy to all pairs
# Takes the longest...
ALL <- pblapply(as.list(1:nrow(pairs2)),function(ii){
  x <- RETURNS(ii)
  tmp <- FUTURES_STRAT(x=x,EQT = 50000)
  tmp
})
#saveRDS(ALL,"FUT2020.rds")
# Extract the Equity Curve of all pairs
EQT <- pblapply(as.list(1:length(ALL)),function(ii){
  tmp <- ALL[[ii]][,"EQT"]
  colnames(tmp) <- paste(names(ALL[[ii]])[1:4],collapse="-")
  tmp
})
# merge them together
EQT <- do.call(merge.xts,EQT)

# Location of the Best and Worst ending Equity Curves
BEST  <- which.max(tail(EQT,1))
WORST <- which.min(tail(EQT,1))

# Plot all Equity curves
plot(EQT[,1],col="black",ylim=c(last(EQT[,WORST]),last(EQT[,BEST])))
for(ii in 2:ncol(EQT)){
  lines(EQT[,ii],col="black")
}
lines(EQT[,BEST],col="blue")
# View the Best Pairs
View(ALL[[BEST]])

# Order to extract the Top 10 Pairs
TOP10 <- EQT[,order(tail(EQT,1),decreasing = TRUE)]
TOP10 <- TOP10[,1:10]
NOM2 <- names(TOP10)

# Formatting to get the tickers of the TOP 10 contracts
topPairsN <- do.call(rbind,do.call(c,lapply(as.list(NOM2),function(x) str_split(x,pattern = "\\."))))
NOM2 <- unique(do.call(c,str_split(NOM2,"\\.")))
# get the pairs betas:
pairsFWD <- do.call(rbind,lapply(as.list(1:nrow(topPairsN)), function(ii){
  pairsFWD = subset(pairs2, pairs2$asset1 == topPairsN[ii,1] &
                      pairs2$asset2 == topPairsN[ii,2] &
                      pairs2$asset3 == topPairsN[ii,3] &
                      pairs2$asset4 == topPairsN[ii,4])
  pairsFWD
}))
# I don't have GLB data: :/
pairsFWD <- pairsFWD[-5,]
NOM2 <- NOM2[!str_detect(NOM2,"GLB")]
# ************************************************************************************************************************************
# READ IN FILES - I GET THIS DATA FROM BARCHART AND SAVE IT LOCALLY
# LIST THE PATH TO THE LOCATION WHERE THESE FILES ARE SAVED
LIST <- list.files("/Volumes/3TB/BARCHART/FUTURES/DAILIES/DATABASE",full.names = TRUE)
LIST <- rbindlist(lapply(as.list(LIST),readRDS),use.names = TRUE)

dt <- pblapply(as.list(NOM2),function(N){
  tst <- subset(LIST, LIST$ShortName == N)
  tst <- tst %>% group_by(Date) %>% filter(OpenInterest == max(OpenInterest))
  tst <- tst %>% group_by(Date) %>% filter(Volume == max(Volume))
  unique(tst)
})
dt <- rbindlist(dt,use.names = TRUE)

# function to convert to XTS
fwdXTS = function(ticker)
{
  dt0 <- subset(dt, dt$ShortName == ticker)
  toDate <- sapply(dt0[,"Date"], as.character)
  toDate <- as.Date(toDate, format="%Y-%m-%d")
  dt0 <- xts(dt0[,c("Open","High","Low","Close","Volume","OpenInterest")], 
             order.by = toDate)
  colnames(dt0) <- paste0(ticker,".",names(dt0))
  Cl(dt0)
}
# merge all Closes
futFWD = do.call(merge,lapply(as.list(NOM2), fwdXTS))
futFWD=futFWD["2019::"]
# make index unique i.e. strip time from Dates
futFWD <- make.index.unique(futFWD,drop = TRUE)

# EXCLUDE TRADING NYSE HOLIDAYS & WEEKENDS
futFWD <- futFWD[isBizday(as.timeDate(index(futFWD)), holidays=holidayNYSE(year=2019:2020),wday = 1:5)]
futFWD <- na.locf(futFWD)
futFWD <- na.locf(futFWD,fromLast = TRUE)
colnames(futFWD)<- gsub(".Close","",names(futFWD))
RETS1 <- RETS1[,NOM2]
RETS1 <- rbind(RETS1, futFWD)

# Minor adjustments to the RETURNS and FUTURES_STRAT functions
# replaced pairs2 with pairsFWD which only contain the best pairs
RETURNS  = function(ii){
  tics <- as.character(c(pairsFWD[ii,1:4],collapse=""))[1:4]
  s<- RETS1[,tics]
  BETAS <- as.numeric(pairsFWD[ii,5:7])
  SPX <- s[,1]
  YHAT <- reclass((BETAS[1]*coredata(s[,2])) + (BETAS[2]*coredata(s[,3])) + (BETAS[3]*coredata(s[,4])),
                  match.to = s)
  colnames(YHAT) <- "YHAT"
  x <- merge.xts(s,YHAT)
  x$sig <- NA
  x$sig[c(FALSE, diff(sign(x[,1] - x$YHAT), na.pad=FALSE) != 0)] <- 0
  x$sig[x[,1] > x$YHAT] <- -1
  x$sig[x[,1] < x$YHAT] <- 1 
  x$sig[1] <- 0 
  x$sig[nrow(x)] <- 0 
  x$sig <- na.locf(x$sig)
  x$sig <- Lag(x$sig)
  x$sig[1] <- 0
  x
}

FUTURES_STRAT = function(x, EQT=50000){
  
  datset <- x
  NN <- names(datset)[1:4]
  beta <- subset(pairsFWD,pairsFWD$asset1 == NN[1] & pairsFWD$asset2 == NN[2] & 
                   pairsFWD$asset3 == NN[3] & pairsFWD$asset4 == NN[4])[5:7]
  
  datset$ASSET1 <- NA
  datset$ASSET2 <- NA
  datset$ASSET3 <- NA
  datset$ASSET4 <- NA
  
  datset$ASSET1[1] <- datset[1,1]
  datset$ASSET2[1] <- datset[1,2]
  datset$ASSET3[1] <- datset[1,3]
  datset$ASSET4[1] <- datset[1,4]
  
  # TRADE PRICE FOR ASSET 1
  for(k in 2:(nrow(datset))){
    datset[k,7] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                          datset[(k-1),7],datset[k,1])
  }
  # TRADE PRICE FOR ASSET 2
  for(k in 2:(nrow(datset))){
    datset[k,8] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                          datset[(k-1),8],datset[k,2])
  }
  # TRADE PRICE FOR ASSET 3
  for(k in 2:(nrow(datset))){
    datset[k,9] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                          datset[(k-1),9],datset[k,3])
  }
  # TRADE PRICE FOR ASSET 4
  for(k in 2:(nrow(datset))){
    datset[k,10] <- ifelse(coredata(datset[(k-1),"sig"])==coredata(datset[k,"sig"]), 
                           datset[(k-1),10],datset[k,4])
  }
  # P&L POINTS FOR ASSET 1
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS1 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,1])),])[1]
  TICKval <-coredata(TICKV[rownames(TICKV)== paste(names(datset[,1])),])[1]
  SIGN <- ifelse(beta$beta1 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,11] <- ifelse(coredata(datset[(k),7])!=coredata(datset[(k-1),7]),
                           reclass((((coredata(datset[k,7]) - coredata(datset[(k-1),7]))*coredata(datset[k-1,"sig"])*SIGN)/minTICK*TICKval)-TICKval,
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # P&L POINTS FOR ASSET 2
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS2 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,2])),])[1]
  TICKval <-coredata(TICKV[rownames(TICKV)== paste(names(datset[,2])),])[1]
  SIGN <- ifelse(beta$beta1 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,12] <- ifelse(coredata(datset[(k),8])!=coredata(datset[(k-1),8]),
                           reclass((((coredata(datset[k,8]) - coredata(datset[(k-1),8]))*coredata(datset[k-1,"sig"])*-SIGN)/minTICK*TICKval)-TICKval, 
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # P&L POINTS FOR ASSET 3
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS3 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,3])),])[1]
  TICKval <-coredata(TICKV[rownames(TICKV)== paste(names(datset[,3])),])[1]
  SIGN <- ifelse(beta$beta2 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,13] <- ifelse(coredata(datset[(k),9])!=coredata(datset[(k-1),9]),
                           reclass((((coredata(datset[k,9]) - coredata(datset[(k-1),9]))*coredata(datset[k-1,"sig"])*-SIGN)/minTICK*TICKval)-TICKval,
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # P&L POINTS FOR ASSET 4
  # CONSIDERS BID/OFFER COSTS
  datset$POINTS4 <-0
  minTICK <- coredata(TICK[rownames(TICK)== paste(names(datset[,4])),])[1]
  TICKval <- coredata(TICKV[rownames(TICKV)== paste(names(datset[,4])),])[1]
  SIGN <- ifelse(beta$beta3 > 0,1,-1)
  for(k in 2:nrow(datset)){
    datset[k,14] <- ifelse(coredata(datset[(k),10])!=coredata(datset[(k-1),10]),
                           reclass((((coredata(datset[k,10]) - coredata(datset[(k-1),10]))*coredata(datset[k-1,"sig"])*-SIGN)/minTICK*TICKval)-TICKval,
                                   match.to=datset)
                           ,reclass(0,match.to=datset))
  }
  # CALCULATE GROSS PROFIT
  datset$GROSS <- round(rowSums(datset[,11:14]),2)
  
  # CALCULATE NET PROFIT
  # 4 * 2.25 == COMMISSIONS
  datset$NET <- 0
  for(k in 2:nrow(datset)){
    datset[k,"NET"] <- ifelse(coredata(datset[k,"GROSS"]) != 0, 
                              reclass((coredata(datset[k,"GROSS"]) - (2.25*8)), 
                                      match.to=datset[k,"GROSS"]), 0)
  }
  # CALCULATE EQUITY
  datset$EQT <- EQT
  for(k in 2:nrow(datset)){
    datset[k,"EQT"] <- ifelse(coredata(datset[(k),"NET"])!=0,
                              reclass(coredata(datset[k,"NET"]) + coredata(datset[(k-1),"EQT"]), 
                                      match.to=datset)
                              ,datset[(k-1),"EQT"])
  }
  
  # CALCULATE DRAWDOWNS
  datset$DD <- round(-(cummax(datset[,"EQT"])-datset[,"EQT"])/datset[,"EQT"],4)
  
  # COUNT THE NUMBER TRADES
  datset$Trades <- 0
  for(k in 2:nrow(datset)){
    datset[k,"Trades"] <- ifelse(coredata(datset[k,"NET"]) != 0 , 
                                 reclass(1+coredata(datset[k-1,"Trades"]),
                                         match.to=datset), 
                                 datset[k-1,"Trades"])
  }
  # COUNT DRAWDOWN DURATION
  datset$duration <- 0
  for(k in 2:nrow(datset)){
    datset[k,"duration"] <- ifelse(coredata(datset[k,"DD"]) != 0, 
                                   reclass(sum(coredata(datset[(k-1),"duration"])+1), 
                                           match.to=datset[k,1]), 0)
  }
  return(datset)
}

# Apply the strategy to the entire data
ALL <- pblapply(as.list(1:nrow(pairsFWD)),function(ii){
  x <- RETURNS(ii)
  tmp <- FUTURES_STRAT(x=x,EQT = 50000)
  tmp
})
# Extract Equity Curves
EQT <- pblapply(as.list(1:length(ALL)),function(ii){
  tmp <- ALL[[ii]][,"EQT"]
  colnames(tmp) <- paste(names(ALL[[ii]])[1:4],collapse="-")
  tmp
})
EQT <- do.call(merge.xts,EQT)
# subset Equity Returns for the look Forward:
EQT <- EQT["2019::"]
BEST  <- which.max(tail(EQT,1))
WORST <- which.min(tail(EQT,1))

plot(EQT[,1],col="black",ylim=c(last(EQT[,WORST]),last(EQT[,BEST])))
for(ii in 2:ncol(EQT)){
  lines(EQT[,ii],col="black")
}
lines(EQT[,BEST],col="blue")

View(ALL[[BEST]])
write.zoo(ALL[[1]], "~/Desktop/BEST1.csv",sep=",")
