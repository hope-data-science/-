
library(pacman)
p_load(tidyverse,data.table,lubridate,rio)

setwd("G:\\博士期间小项目\\郭老师数据处理")
load("guo.rdata")

data_1418
var_table

data_1418 %>% 
  mutate(datetime = ymd_h(paste(year, month, day, hour, sep= ' '))) %>% 
  as.data.table() %>% 
  .[hour == 0,
    `:=`(year = year(datetime - days(1)),
         month = month(datetime - days(1)),
         day = day(datetime - days(1)),
         hour = 24)] %>% 
  .[between(year,2014,2018)] %>% 
  as_tibble() %>% 
  arrange(year,month,day,hour)-> dat

c(5,10,15,20,40) -> Tg_list

# Tg系列

## 1 小时
for(n in Tg_list){
  var_name = str_c("soil_Temp_",n,"cm_Avg")
  dat %>% 
    select(year,month,day,hour,var_name) %>% 
    reshape2::dcast(year + month + day ~ hour,value.var = var_name) %>% 
    as_tibble() %>% 
    select(year,month,day,`21`,`22`,`23`,`24`,everything()) %>% 
    mutate_all(list(~replace_na(.,""))) -> target
  assign(paste0("Tg",n,"1"),target)
}

## 2 日
for(n in Tg_list){
  var_name = str_c("soil_Temp_",n,"cm_Avg")
  dat %>% 
    select(year,month,day,hour,var_name) %>% 
    group_by(year,month,day) %>% 
    summarise(mean = mean(.data[[var_name]],na.rm = T),
              max = max(.data[[var_name]],na.rm = T),
              min = min(.data[[var_name]],na.rm = T)) %>% 
    ungroup()%>% 
    mutate_all(list(~replace_na(.,""))) -> target
  assign(paste0("Tg",n,"2"),target)
}

## 3 月
for(n in Tg_list){
  var_name = str_c("soil_Temp_",n,"cm_Avg")
  dat %>% 
    select(year,month,day,hour,var_name) %>% 
    group_by(year,month,day) %>% 
    summarise(mean = mean(.data[[var_name]],na.rm = T),
              max = max(.data[[var_name]],na.rm = T),
              min = min(.data[[var_name]],na.rm = T)) %>% 
    select(-day) %>% 
    mutate(max = ifelse(is.infinite(max),NA,max)) %>% 
    mutate(min = ifelse(is.infinite(min),NA,min)) %>% 
    group_by(year,month) %>% 
    summarise_all(list(mean),na.rm = T) %>% 
    rename(AVGMEAN = mean,MAXMEAN = max,MINMEAN = min) %>% 
    ungroup() -> dt1
  dat %>% 
    select(year,month,day,var_name) %>% 
    group_by(year,month) %>% 
    filter(.data[[var_name]] == max(.data[[var_name]],na.rm = T)) %>% 
    slice(1) %>% 
    mutate(TIME1 = str_c(year,month,day,sep  = "/")) %>% 
    select(year,month,MAX000 = var_name,TIME1) -> dt2
  dat %>% 
    select(year,month,day,var_name) %>% 
    group_by(year,month) %>% 
    filter(.data[[var_name]] == min(.data[[var_name]],na.rm = T)) %>% 
    slice(1) %>% 
    mutate(TIME2 = str_c(year,month,day,sep  = "/")) %>% 
    select(year,month,MIN000 = var_name,TIME2) -> dt3
  dt1 %>% 
    left_join(dt2) %>% 
    left_join(dt3) %>% 
    mutate_all(list(~replace_na(.,""))) -> target
  assign(paste0("Tg",n,"3"),target)
}

## write to excel
ls(pattern = "^Tg[0-9]+$") -> Tg_names

# lapply(Tg_names,get)

replace_inf = function(x){
  x %>% 
    lapply(., function(x) gsub("-Inf","",x)) %>% 
    lapply(., function(x) gsub("Inf","",x)) %>% 
    as_tibble() 
}

Tg_names %>% 
  lapply(.,get) %>% 
  lapply(.,replace_inf) -> a

names(a) = Tg_names

export(a,col.names = F,"Tg.xlsx")

#####################################################################################