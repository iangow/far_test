
Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE="crsp")
Sys.setenv(PGUSER="far", PGPASSWORD="honours_2019")
library(DBI)
library(dplyr, warn.conflicts = FALSE)
pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET work_mem TO '8GB'")                   #q2: ?
# rs <- dbExecute(pg, "SET maintance_work_mem TO '1GB'")   #q1: ?
rs <- dbExecute(pg, "SET search_path TO crsp, public")

library(DBI)
library(dplyr)
library(dbplyr)

#1.1 Use tbl() function to pull the following virtual data frames:
#crsp.ccmxpf_linktable, crsp.dsf, crsp.dsi,
#comp.funda, comp.fundq, comp.company.

funda<-tbl(pg, sql("SELECT * FROM comp.funda "))  #annual financial
fundq<-tbl(pg, sql("SELECT * FROM comp.fundq "))   # quarterly financial
comp<-tbl(pg, sql("SELECT * FROM comp.company "))  # company basic
dsf<-tbl(pg, sql("SELECT * FROM crsp.dsf "))     # daily stock firm
dsi<-tbl(pg, sql("SELECT * FROM crsp.dsi "))     # daily stock index
ccmxpf_linktable<-tbl(pg,sql("SELECT * FROM crsp.ccmxpf_linktable"))


time <- tbl(pg,sql("SELECT * FROM generate_series(-361,180,1)")) %>%
  mutate(i=1) %>%
  rename (series = generate_series)

######################################Directly from SAS
# setwd("C:/Users/WuYiyang/Desktop/Data Scientist/comp/R-1")
# library(haven)
# library(stringr)
# library(purrr)
# funda<-read_sas("funda.sas7bdat")
# names(funda)<-map_chr(names(funda), tolower)
# str(funda)
#
# fundq<-read_sas("fundq.sas7bdat")
# names(fundq)<-map_chr(names(fundq), tolower)
# str(fundq)
#
# dsi<-read_sas("dsi.sas7bdat")
# names(dsi)<-map_chr(names(dsi), tolower)
# str(dsi)
#
# memory.limit()
# dsf<-read_sas("dsfsmall.sas7bdat")
# names(dsf)<-map_chr(names(dsf), tolower)
# str(dsf)
#
# #ccmxpf_linktable <-ccmxpf_linktable %>%
# #  collect()
# load(file = "ccmxpf_linktable.RData")
# load(file="time.RData")

#########################1.2 Merge standard (STD) Compustat data of funda and fundq to get necessary Earnings related variables.
funda1 <-
  funda %>%
  filter(indfmt == 'INDL' & datafmt == 'STD',
         popsrc == 'D' & consol == 'C') %>%   #STD
  filter(between(fyear, 1970, 2018)) %>%    # fiscal year end in December
  select(gvkey, cik, cusip, fyear, fyr,apdedate,pddur, epspi, prcc_f) %>%
  rename(enddate = apdedate)

fundq1 <-
  fundq %>%
  filter(indfmt == 'INDL' & datafmt == 'STD',
         popsrc == 'D' & consol == 'C') %>%   #STD
  filter(between(fyearq, 1970, 2018),fqtr==4) %>%  #only keep fiscal quarter 4
  select(gvkey, cik, cusip,apdedateq,fyearq,rdq) %>%
  rename(enddate = apdedateq,fyear=fyearq)  %>%
  filter(!is.na(rdq))

#prepare for next merge
#####SAS:attr(fundq1$fyear,"label")<-attr(funda1$fyear,"label")

library(lubridate)
fund1.2 <-funda1 %>%
  semi_join(fundq1,by=c("gvkey","cusip","cik","fyear")) %>%
  inner_join(fundq1,by=c("gvkey","cusip","cik","fyear"))

#check if na
#####SAS:table(is.na(fund1.2$pddur))
#####SAS:table(is.na(fund1.2$fyr))
#fiscal year at 2nd half of December, fiscal year = cover 12 months
fund1.2=fund1.2%>%
  filter(fyr==12,pddur==12)
#check for duplication
#####SAS:sum(fund1.2 %>% duplicated())  #0 good！


#########################1.3 Classifying the earnings signal as #conveying “Good” or “Bad” news to stock market investors

fund1.3 <-fund1.2 %>%
  group_by(gvkey) %>%
  arrange(fyear) %>%
  mutate(lagfyear = lag(fyear),lagepspi=lag(epspi))%>%
  filter(!is.na(lagfyear) & !is.na(lagepspi)&!is.na(epspi)) %>%
  mutate(ifgoodnews = epspi>lagepspi) %>%
  mutate(ifgoodnews = if_else(ifgoodnews==TRUE,"good","bad")) %>%
  filter(!is.na(rdq)) %>%
  filter(lagfyear == (fyear-1)) %>%    #two successive fiscal years
  ungroup()

#########################1.4 Get permno, which is the unique firm identifier in CRSP.


ccmxpf_linktable1 = ccmxpf_linktable %>%
  filter(!is.na(lpermco)|!is.na(lpermno)) %>%
  filter(usedflag == 1) %>%
  filter(linktype %in% c("LU","LC","LD","LN","LS","LX")) %>%
  mutate(linkdt = as.Date(as.character(linkdt)),linkenddt=as.Date(as.character(linkenddt))) %>%
  distinct()


#####SAS: load(file = "time.RData")
#####SAS: fund1.3= fund1.3 %>% filter(fyear==2016)
#####SAS: attributes(fund1.3$gvkey)<-NULL
#####SAS：str(fund1.3$rdq)

fund1.4<-fund1.3 %>%
  mutate(i=1) %>%
  inner_join(time,by="i") %>%
  select(-i) %>%
  mutate(date = series + rdq) %>%
  inner_join(ccmxpf_linktable1, by="gvkey") %>%
  filter(linkdt <= date) %>%
  filter((is.na(linkenddt))|(!is.na(linkenddt)&linkenddt>=date)) %>%
  rename(permno = lpermno,permco= lpermco)

#########################1.5 Merge with dsi to get necesary Stock Price related variables.

dsi1 = dsi %>%
  select(date, ewretd) %>%
  mutate(ewretd = log(1+ewretd))

dsf1 = dsf %>%
  select(cusip, permno, date, ret) %>%
  mutate (ret = log(1+ret)) %>%
  inner_join(dsi1,by="date") %>%
  mutate(abr = ret - ewretd) %>%
  filter(date>as.Date('1968-01-01') & date<as.Date('2019-01-01'))

dsf1 = dsf1 %>%
  mutate(permno=as.integer(permno))

fund1.5<-fund1.4 %>%
  inner_join(dsf1,by=c("date","permno"))%>%
  filter(!is.na(ret))

#########################1.6 Calculate unadjusted and market-adjusted returns, name the dataframe as avg_rets.

library(tidyr)

avg_rets = fund1.5%>%
  select(ifgoodnews,series,ret,abr) %>%
  group_by(ifgoodnews,series) %>%
  summarize(avgrets = mean(ret,na.rm = TRUE),avgabrs = mean(abr,na.rm=TRUE))  %>%
  ungroup() %>%
  arrange(ifgoodnews, series) %>%
  group_by(ifgoodnews) %>%
  mutate(cumrets = cumsum(avgrets),cumabrs = cumsum(avgabrs)) %>%
  ungroup()

avg_ret = avg_ret %>% collect()
save(file="avg_ret.RData")

library(tidyr)
picture=avg_rets%>%
  select(series,ifgoodnews,cumrets,cumabrs) %>%
  pivot_wider(names_from = ifgoodnews,values_from = c("cumrets","cumabrs")) %>%
  mutate(cumretsavg = cumrets_good/2-cumrets_bad/2,cumabrsavg = cumabrs_good/2-cumabrs_bad/2)



####################################save and picture BB2019
library(ggplot2)
ggplot(data = picture) +
  geom_line(aes(x = series,y=cumabrsavg),color="red") +
  geom_line(aes(x = series,y=-cumabrsavg),color="blue")









