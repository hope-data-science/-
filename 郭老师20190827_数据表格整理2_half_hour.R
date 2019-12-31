

library(pacman)
p_load(tidyverse,rio,lubridate,data.table)


### 14-16年

import("G:\\博士期间小项目\\郭老师数据处理\\CMCW_2014_2018_level320190827.xls",
       sheet = "2014-2016") %>% as_tibble()  -> d1416_raw

d1416_raw %>% 
  slice(1:2) %>% 
  t() %>% 
  as.data.frame() %>% 
  rownames_to_column() %>% 
  filter(rowname %in% c("TIMESTAMP","UR_Avg","Rn_Avg",
                        "PAR_Den_Avg","hfp01sc_2_Avg") |
           str_detect(rowname,"soil")) %>% 
  rename(var = rowname,note = V1,unit = V2) -> var_table_1416

d1416_raw %>% 
  slice(3:n()) %>% 
  select(TIMESTAMP,UR_Avg,Rn_Avg,
         PAR_Den_Avg,hfp01sc_2_Avg,
         contains("soil")) %>% 
  as.data.table() %>% 
  .[str_detect(TIMESTAMP,"-"),datetime:=dmy_hm(TIMESTAMP)] %>% 
  .[!str_detect(TIMESTAMP,"-"),datetime:=mdy_hm(TIMESTAMP)] %>% 
  as_tibble() %>% 
  mutate(year = year(datetime),
         month = month(datetime),
         day = day(datetime),
         hour = hour(datetime)) %>% 
  select(-TIMESTAMP,-contains("soil")) -> data_1416
  
###  17年
import("G:\\博士期间小项目\\郭老师数据处理\\CMCW_2014_2018_level320190827.xls",
       sheet = "2017") %>% as_tibble()  -> d17_raw

d17_raw %>% 
  slice(1:2) %>% 
  t() %>% 
  as.data.frame() %>% 
  rownames_to_column() %>% 
  filter(rowname %in% c("TIMESTAMP","UR_Avg","Rn_Avg",
                        "PAR_Den_Avg","hfp01sc_2_Avg") |
           str_detect(rowname,"soil")) %>% 
  rename(var = rowname,note = V1,unit = V2) -> var_table_17

d17_raw %>% 
  slice(3:n()) %>% 
  select(TIMESTAMP,UR_Avg,Rn_Avg,
         PAR_Den_Avg,hfp01sc_2_Avg,
         contains("soil")) %>% 
  as.data.table() %>% 
  .[str_detect(TIMESTAMP,"-"),datetime:=dmy_hm(TIMESTAMP)] %>% 
  .[!str_detect(TIMESTAMP,"-"),datetime:=mdy_hm(TIMESTAMP)] %>% 
  as_tibble() %>% 
  mutate(year = year(datetime),
         month = month(datetime),
         day = day(datetime),
         hour = hour(datetime)) %>% 
  select(-TIMESTAMP,-contains("soil")) -> data_17

### 18年  
import("G:\\博士期间小项目\\郭老师数据处理\\CMCW_2014_2018_level320190827.xls",
       sheet = "2018",col_types = "text")  %>% as_tibble -> d18_raw

d18_raw %>% 
  slice(1:2) %>% 
  t() %>% 
  as.data.frame() %>% 
  rownames_to_column() %>% 
  filter(rowname %in% c("TIMESTAMP","UR_Avg","Rn_Avg",
                        "PAR_Den_Avg","hfp01sc_2_Avg") |
           str_detect(rowname,"soil")) %>% 
  rename(var = rowname,note = V1,unit = V2) -> var_table_18

d18_raw %>% 
  slice(3:n()) %>% 
  select(TIMESTAMP,UR_Avg,
         PAR_Den_Avg,hfp01sc_2_Avg,
         contains("soil")) %>% 
  mutate(datetime = ymd_h(TIMESTAMP)) %>% 
  mutate(year = year(datetime),
         month = month(datetime),
         day = day(datetime),
         hour = hour(datetime)) %>%  
  as_tibble() %>% 
  select(-TIMESTAMP,-contains("soil")) -> data_18

## merge
data_1416 %>% 
  bind_rows(data_17) %>% 
  bind_rows(data_18) -> data_1418


setwd("G:\\博士期间小项目\\郭老师数据处理")

export(data_1418,"data_1418_half_hour.csv")







        

