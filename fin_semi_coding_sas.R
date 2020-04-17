Sys.setenv(PGHOST = "10.101.13.99", PGDATABASE="crsp")

library(DBI)
library(dplyr, warn.conflicts = FALSE)

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET work_mem TO '2GB'")              
rs <- dbExecute(pg, "SET search_path TO crsp, public")

# 1.1 Create virtual data frames ----
# crsp.ccmxpf_linktable, crsp.dsf, crsp.dsi,
# comp.funda, comp.fundq, comp.company.

funda <- tbl(pg, sql("SELECT * FROM comp.funda "))   # annual financial
fundq <- tbl(pg, sql("SELECT * FROM comp.fundq "))   # quarterly financial
comp <- tbl(pg, sql("SELECT * FROM comp.company "))  # company basic
dsf <- tbl(pg, sql("SELECT * FROM crsp.dsf "))       # daily stock firm
dsi <- tbl(pg, sql("SELECT * FROM crsp.dsi "))       # daily stock index
ccmxpf_linktable <- tbl(pg,sql("SELECT * FROM crsp.ccmxpf_linktable"))

time <- tbl(pg,sql("SELECT * FROM generate_series(-361,180,1)")) %>%
  mutate(i=1) %>%
  rename (series = generate_series)

# 1.2 Merge Compustat to get earnings-related variables. ----
funda1 <-
  funda %>%
  filter(indfmt == 'INDL' & datafmt == 'STD',
         popsrc == 'D' & consol == 'C') %>% 
  # fiscal year-end in December
  filter(between(fyear, 1970, 2018)) %>%    
  select(gvkey, cik, cusip, fyear, fyr, apdedate, pddur, epspi, prcc_f) %>%
  rename(enddate = apdedate) 

fundq1 <-
  fundq %>%
  filter(indfmt == 'INDL' & datafmt == 'STD',
         popsrc == 'D' & consol == 'C') %>%   #STD
  filter(between(fyearq, 1970, 2018),fqtr==4) %>%  #only keep fiscal quarter 4
  select(gvkey, cik, cusip, apdedateq, fyearq, rdq) %>%
  rename(enddate = apdedateq, fyear=fyearq)  %>%
  filter(!is.na(rdq)) 

# Prepare for next merge
library(lubridate)
fund1.2 <- funda1 %>%
  inner_join(fundq1,by=c("gvkey","cusip","cik","fyear")) %>%
  # Fiscal year at 2nd half of December, fiscal year = cover 12 months
  filter(fyr==12, pddur==12)

# 1.3 Classify the earnings signal as "Good" or "Bad" news ----
fund1.3 <- fund1.2 %>%
  group_by(gvkey) %>%
  arrange(fyear) %>%
  mutate(lagfyear = lag(fyear), lagepspi=lag(epspi)) %>%
  filter(!is.na(lagfyear) & !is.na(lagepspi) & !is.na(epspi)) %>%
  mutate(ifgoodnews = if_else(epspi > lagepspi, "good", "bad")) %>%
  filter(!is.na(rdq)) %>%
  filter(lagfyear == (fyear-1)) %>%    # two successive fiscal years
  ungroup()

# 1.4 Get permno, which is the unique firm identifier in CRSP. ----
ccmxpf_linktable1 <-
  ccmxpf_linktable %>%
  filter(!is.na(lpermco) | !is.na(lpermno)) %>%
  filter(usedflag == 1) %>%
  filter(linktype %in% c("LU","LC","LD","LN","LS","LX")) %>%
  distinct() 

fund1.4 <-
  fund1.3 %>%
  mutate(i=1) %>%
  inner_join(time, by="i") %>%
  select(-i) %>%
  mutate(date = series + rdq) %>%
  inner_join(ccmxpf_linktable1, by="gvkey") %>%
  filter(linkdt <= date) %>%
  filter(is.na(linkenddt) | (!is.na(linkenddt) & linkenddt >= date)) %>%
  rename(permno = lpermno, permco = lpermco) 

# 1.5 Merge with dsi to get stock price-related variables. ----
dsi1 <-
  dsi %>%
  select(date, ewretd) %>%
  mutate(ewretd = log(1+ewretd))

dsf1 <- 
  dsf %>%
  select(permno, date, ret) %>%
  mutate(ret = log(1+ret)) %>%
  inner_join(dsi1, by = "date") %>%
  mutate(abr = ret - ewretd) %>%
  filter(date > as.Date('1968-01-01') & date < as.Date('2019-01-01')) 

fund1.5 <- 
  fund1.4 %>%
  inner_join(dsf1, by=c("date", "permno")) %>%
  filter(!is.na(ret)) 

# 1.6 Calculate unadjusted and market-adjusted returns ----
avg_rets <- 
  fund1.5 %>%
  select(ifgoodnews, series, ret, abr) %>%
  group_by(ifgoodnews, series) %>%
  summarize(avgrets = mean(ret, na.rm = TRUE),
            avgabrs = mean(abr, na.rm=TRUE))  %>%
  ungroup() %>%
  arrange(ifgoodnews, series) %>%
  group_by(ifgoodnews) %>%
  mutate(cumrets = cumsum(avgrets),
         cumabrs = cumsum(avgabrs)) %>%
  ungroup() %>%
  compute()

# avg_rets <- avg_rets %>% collect()
# save(file="avg_ret.RData")

library(tidyr)
picture <-
  avg_rets %>%
  select(series, ifgoodnews, cumrets, cumabrs) %>%
  collect() %>%
  pivot_wider(names_from = ifgoodnews,
              values_from = c("cumrets","cumabrs")) %>%
  mutate(cumretsavg = cumrets_good/2-cumrets_bad/2,
         cumabrsavg = cumabrs_good/2-cumabrs_bad/2)

# Save and picture BB2019 ----
library(ggplot2)
ggplot(data = picture) +
  geom_line(aes(x = series, y = cumabrsavg), color = "red") +
  geom_line(aes(x = series, y = -cumabrsavg), color = "blue")
