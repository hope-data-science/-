
library(pacman)
p_load(tidyverse,data.table,lubridate,rio)

setwd("G:\\博士期间小项目\\郭老师数据处理")
load("guo.rdata")

data_1418
var_table

# D系列

## D51 多年自动站每日逐时太阳辐射和累计值 

# 2              UR_Avg       总辐射      W/m2
# 3              Rn_Avg       净辐射      W/m2
# 4         PAR_Den_Avg 光合有效辐射 umol/m2 s
# 5       hfp01sc_2_Avg   土壤热通量      W/m2

# 总辐射辐照度(W/m^2)
# 净辐射辐照度(W/m^2)
# 光合有效辐射光量子(μmol/m^2/s)
# Ht瞬时土壤热通量(W/m^2)

# 总辐射总量曝辐量(MJ/m^2)  
# 净辐射总量曝辐量(MJ/m^2)
# 光合有效辐射总量通量密度(mol/m^2)
# 累计土壤热通量总量(MJ/m^2)

data_1418 %>% 
  select(-contains("soil")) %>% 
  group_by(year,month,day,hour) %>% 
  mutate_at(vars(-group_cols()),list(total = ~ .*3600/1e6)) %>% 
  ungroup() -> D_HOUR

D_HOUR %>% 
  add_column(c1 = "",c2 = "",.after = "UR_Avg") %>% 
  add_column(c3 = "",c4 = "",c5 = "",.after = "hfp01sc_2_Avg") %>%   
  add_column(c6 = "",c7 = "",.after = "UR_Avg_total") %>% 
  mutate_all(list(~replace_na(.,""))) -> D51

## D32 自动站逐日太阳辐射总量及其累计值      
D_HOUR %>% 
  group_by(year,month,day) %>% 
  select(ends_with("total")) %>% 
  summarise_all(sum) %>% 
  ungroup() -> dt1
D_HOUR %>% 
  group_by(year,month,day) %>% 
  filter(PAR_Den_Avg > 10) %>% 
  summarise(sun_hour = n()) %>% 
  ungroup() %>% 
  mutate(sun_min = sun_hour*60) -> dt2
dt1 %>% 
  left_join(dt2)%>% 
  add_column(c1 = "",c2 = "",.after = "UR_Avg_total") %>% 
  add_column(c3 = "",c4 = "",c5 = "",.after = "hfp01sc_2_Avg_total") %>% 
  mutate_all(list(~replace_na(.,""))) -> D32

## D33 自动站逐月太阳辐射总量及其累计值                     
D_HOUR %>% 
  group_by(year,month) %>% 
  select(ends_with("total")) %>% 
  summarise_all(sum) %>% 
  ungroup() -> dt1
D_HOUR %>% 
  group_by(year,month) %>% 
  filter(PAR_Den_Avg > 10) %>% 
  summarise(sun_hour = n()) %>% 
  ungroup() %>% 
  mutate(sun_min = sun_hour*60) -> dt2
dt1 %>% 
  left_join(dt2)%>% 
  add_column(c1 = "",c2 = "",.after = "UR_Avg_total") %>% 
  add_column(c3 = "",c4 = "",c5 = "",.after = "hfp01sc_2_Avg_total") %>% 
  mutate_all(list(~replace_na(.,""))) -> D33

## D42 自动站逐日太阳辐射极值及其出现时间       

import("data_1418_half_hour.csv") %>% as_tibble %>% 
  filter(year != 2019) %>% 
  mutate(datetime = ymd_hms(datetime))-> data_1418_half_hour

data_1418_half_hour %>% 
  select(-hour) %>% 
  group_by(year,month,day) %>% 
  summarise_if(is.numeric,max,na.rm = T) %>% 
  left_join(data_1418_half_hour %>% 
              select(year,month,day,UR_Avg,datetime) %>% 
              distinct(year,month,day,UR_Avg,.keep_all = T)) %>% 
  mutate(UR_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>% 
  left_join(data_1418_half_hour %>% 
              select(year,month,day,Rn_Avg,datetime) %>% 
              distinct(year,month,day,Rn_Avg,.keep_all = T)) %>% 
  mutate(Rn_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>%    
  left_join(data_1418_half_hour %>% 
              select(year,month,day,PAR_Den_Avg,datetime) %>% 
              distinct(year,month,day,PAR_Den_Avg,.keep_all = T)) %>% 
  mutate(PAR_Den_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>% 
  left_join(data_1418_half_hour %>% 
              select(year,month,day,hfp01sc_2_Avg,datetime) %>% 
              distinct(year,month,day,hfp01sc_2_Avg,.keep_all = T)) %>% 
  mutate(hfp01sc_2_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>% 
  select(year,month,day,contains("UR_"),contains("Rn_"),contains("PAR_"),contains("hfp")) %>% 
  add_column(c1="",c2="",c3="",c4="",.after = "UR_Avg_time") %>% 
  mutate(MEM000 = "极大值") %>% 
  add_column(c5="",c6="",c7="",c8="",c9="",c10="",.before = "MEM000") %>% 
  ungroup() %>% 
  mutate_all(list(~replace_na(.,""))) -> d42_max

data_1418_half_hour %>% 
  select(-hour) %>% 
  group_by(year,month,day) %>% 
  summarise_if(is.numeric,min,na.rm = T) %>% 
  left_join(data_1418_half_hour %>% 
              select(year,month,day,UR_Avg,datetime) %>% 
              distinct(year,month,day,UR_Avg,.keep_all = T)) %>% 
  mutate(UR_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>% 
  left_join(data_1418_half_hour %>% 
              select(year,month,day,Rn_Avg,datetime) %>% 
              distinct(year,month,day,Rn_Avg,.keep_all = T)) %>% 
  mutate(Rn_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>%    
  left_join(data_1418_half_hour %>% 
              select(year,month,day,PAR_Den_Avg,datetime) %>% 
              distinct(year,month,day,PAR_Den_Avg,.keep_all = T)) %>% 
  mutate(PAR_Den_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>% 
  left_join(data_1418_half_hour %>% 
              select(year,month,day,hfp01sc_2_Avg,datetime) %>% 
              distinct(year,month,day,hfp01sc_2_Avg,.keep_all = T)) %>% 
  mutate(hfp01sc_2_Avg_time = str_c(hour(datetime),minute(datetime),sep = ":") %>% 
           if_else(str_detect(.,":0$"),str_c(.,"0"),.)) %>% 
  select(-datetime) %>% 
  select(year,month,day,contains("UR_"),contains("Rn_"),contains("PAR_"),contains("hfp")) %>% 
  add_column(c1="",c2="",c3="",c4="",.after = "UR_Avg_time") %>% 
  mutate(MEM000 = "极小值") %>% 
  add_column(c5="",c6="",c7="",c8="",c9="",c10="",.before = "MEM000") %>% 
  ungroup() %>% 
  mutate_all(list(~replace_na(.,""))) -> d42_min

bind_rows(d42_max,d42_min) -> D42

## D43

D42 %>% 
  select(year,month,day,UR_Avg,UR_Avg_time,MEM000) %>% 
  mutate(UR_Avg = as.numeric(UR_Avg)) %>% 
  group_by(year,month) %>% 
  filter(UR_Avg == max(UR_Avg) | UR_Avg == min(UR_Avg)) %>% 
  distinct(year,month,UR_Avg,.keep_all = T) %>% 
  mutate(UR_Avg_date = paste(year,month,day,sep = "-")) %>% 
  select(year,month,UR_Avg,UR_Avg_date,UR_Avg_time,MEM000) %>% 
  ungroup() -> d43_UR_Avg

D42 %>% 
  select(year,month,day,Rn_Avg,Rn_Avg_time,MEM000) %>% 
  mutate(Rn_Avg = as.numeric(Rn_Avg)) %>% 
  group_by(year,month) %>% 
  filter(Rn_Avg == max(Rn_Avg) | Rn_Avg == min(Rn_Avg)) %>% 
  distinct(year,month,Rn_Avg,.keep_all = T) %>% 
  mutate(Rn_Avg_date = paste(year,month,day,sep = "-")) %>% 
  select(year,month,Rn_Avg,Rn_Avg_date,Rn_Avg_time,MEM000) %>% 
  ungroup() -> d43_Rn_Avg

D42 %>% 
  select(year,month,day,PAR_Den_Avg,PAR_Den_Avg_time,MEM000) %>% 
  mutate(PAR_Den_Avg = as.numeric(PAR_Den_Avg)) %>% 
  group_by(year,month) %>% 
  filter(PAR_Den_Avg == max(PAR_Den_Avg) | PAR_Den_Avg == min(PAR_Den_Avg)) %>% 
  distinct(year,month,PAR_Den_Avg,.keep_all = T) %>% 
  mutate(PAR_Den_Avg_date = paste(year,month,day,sep = "-")) %>% 
  select(year,month,PAR_Den_Avg,PAR_Den_Avg_date,PAR_Den_Avg_time,MEM000) %>% 
  ungroup() -> d43_PAR_Den_Avg

d43_UR_Avg %>% 
  inner_join(d43_Rn_Avg)%>% 
  inner_join(d43_PAR_Den_Avg) %>% 
  select(year,month,contains("UR_"),contains("Rn_"),contains("PAR_")) %>% 
  add_column(c5="",c6="",c7="",c8="",c9="",c10="",.after = "UR_Avg_time")  %>% 
  mutate_all(list(~replace_na(.,""))) -> D43

## export to excel
ls(pattern = "^D[0-9]+$") -> D_names

D_names %>% 
  lapply(.,get)  -> a

names(a) = D_names

export(a,col.names = F,"D.xlsx")

