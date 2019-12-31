
### 环境设置：计算最大值等汇总数据时忽略缺失值
Funs <- Filter(is.function,sapply(ls(baseenv()),get,baseenv()))
na.rm.f <- names(Filter(function(x) any(names(formals(args(x)))%in% 'na.rm'),Funs))

ll <- lapply(na.rm.f,function(x)
{
  tt <- get(x)
  ss = body(tt)
  if (class(ss)!="{") ss = as.call(c(as.name("{"), ss))
  if(length(ss) < 2) print(x)
  else{
    if (!length(grep("na.rm = TRUE",ss[[2]],fixed=TRUE))) {
      ss = ss[c(1,NA,2:length(ss))]
      ss[[2]] = parse(text="na.rm = TRUE")[[1]]
      body(tt)=ss
      (unlockBinding)(x,baseenv())
      assign(x,tt,envir=asNamespace("base"),inherits=FALSE)
      lockBinding(x,baseenv())
    }
  }
})
### 环境设置完毕

### 读入
pacman::p_load(tidyverse,openxlsx,lubridate)
setwd("G:\\博士期间小项目\\郭老师数据处理")
load("meta.rdata")
read_rds("temp_wb.rds") -> wb

##########################################################

### 中间工作站

raw_dt
template_info

template_info %>% 
  filter(str_detect(title,"露点")) %>% 
  select(title) %>% 
  print(n = Inf)

template_info %>% 
  filter(str_detect(title,"露点")) %>% pull(cn)

## TD1/TD2/TD3

### TD1
table_name = "TD1"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names

raw_dt %>% 
  select(station,year,month,day,hour,露点) %>% 
  pivot_wider(names_from = hour,values_from = 露点) %>% 
  mutate(station = "CJM") %>% 
  select(station,year,month,day,`21`,`22`,`23`,`24`,everything()) %>% 
  mutate(beizhu = "") %>% 
  setNames(col_names) %>% 
  mutate_all(list(~replace_na(.,""))) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

### TD2
table_name = "TD2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names

raw_dt %>% 
  select(station,year,month,day,hour,露点) %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),avg = mean(露点),max = max(露点),min = min(露点)) %>% 
  ungroup %>% select(station,everything()) %>% 
  setNames(col_names) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

### TD3
table_name = "TD3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names

raw_dt %>% 
  group_by(year,month,day) %>% 
  summarise(日平均 = mean(露点),日最大 = max(露点),日最小 = min(露点)) %>% 
  select(-day) %>% 
  group_by(year,month) %>% 
  summarise_all(mean) %>% 
  ungroup -> dt1

raw_dt %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),
            max = max(露点),max_datetime = datetime[露点==max(露点)][1] %>% as.Date(),
            min = min(露点),min_datetime = datetime[露点==min(露点)][1] %>% as.Date()) %>% 
  ungroup() -> dt2

dt1 %>% inner_join(dt2) %>% 
  select(station,everything()) %>% 
  mutate_if(is.Date,as.character) %>% 
  setNames(col_names) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

### 中间工作站

raw_dt
template_info

template_info %>% 
  filter(str_detect(title,"气压")) %>% 
  select(title) %>% 
  print(n = Inf)

template_info %>% 
  filter(str_detect(title,"大气压")) %>% pull(cn) 

## P1/P2/P3
### P1
table_name = "P1"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names

raw_dt %>% 
  select(station,year,month,day,hour,气压) %>% 
  pivot_wider(names_from = hour,values_from = 气压) %>% 
  mutate(station = "CJM") %>% 
  select(station,year,month,day,`21`,`22`,`23`,`24`,everything()) %>% 
  mutate(beizhu = "") %>% 
  setNames(col_names) %>% 
  mutate_all(list(~replace_na(.,""))) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

### P2

table_name = "P2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector() -> t2_names
raw_dt %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),avg = mean(气压),
            max = max(气压),max_time = hour[气压==max(气压)][1],
            min = min(气压),min_time = hour[气压==min(气压)][1]) %>% 
  ungroup() %>% 
  na.omit()%>% 
  mutate_at(vars(matches("time")),list(~str_c(.,":00"))) %>% 
  select(station,everything())%>% 
  setNames(t2_names) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

### P3
table_name = "P3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names

raw_dt %>% 
  group_by(year,month,day) %>% 
  summarise(日平均 = mean(气压),日最大 = max(气压),日最小 = min(气压)) %>% 
  select(-day) %>% 
  group_by(year,month) %>% 
  summarise_all(mean) %>% 
  ungroup -> dt1

raw_dt %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),
            max = max(气压),max_datetime = datetime[气压==max(气压)][1] %>% as.Date(),
            min = min(气压),min_datetime = datetime[气压==min(气压)][1] %>% as.Date()) %>% 
  ungroup() -> dt2

dt1 %>% inner_join(dt2) %>% 
  select(station,everything()) %>% 
  mutate_if(is.Date,as.character) %>% 
  setNames(col_names) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

##########################################################

#########################
##通用函数设计
fast_table = function(raw_dt = raw_dt,table_name,varName,mode){
  
  template_info %>% 
    filter(sheet_name == table_name) %>% pull(cn) %>% 
    str_split(";",simplify = T) %>% 
    as.vector()-> col_names
  
  if(mode == 1){
    raw_dt %>% 
      select(station,year,month,day,hour,{{varName}}) %>% 
      pivot_wider(names_from = hour,values_from = {{varName}}) %>% 
      mutate(station = "CJM") %>% 
      select(station,year,month,day,`21`,`22`,`23`,`24`,everything()) %>% 
      mutate(beizhu = "") %>% 
      setNames(col_names) %>% 
      mutate_all(list(~replace_na(.,""))) -> target_table
  }
  if(mode == 2){
    raw_dt %>% 
      group_by(year,month,day) %>% 
      summarise(station = unique(station),avg = mean({{varName}}),
                max = max({{varName}}),max_time = hour[{{varName}}==max({{varName}})][1],
                min = min({{varName}}),min_time = hour[{{varName}}==min({{varName}})][1]) %>% 
      ungroup() %>% 
      na.omit()%>% 
      mutate_at(vars(matches("time")),list(~str_c(.,":00"))) %>% 
      select(station,everything())%>% 
      setNames(col_names) -> target_table
  }
  if(mode == 3){
    raw_dt %>% 
      group_by(year,month,day) %>% 
      summarise(日平均 = mean({{varName}}),日最大 = max({{varName}}),日最小 = min({{varName}})) %>% 
      select(-day) %>% 
      group_by(year,month) %>% 
      summarise_all(mean) %>% 
      ungroup -> dt1
    
    raw_dt %>% 
      group_by(year,month) %>% 
      summarise(station = unique(station),
                max = max({{varName}}),max_datetime = datetime[{{varName}}==max({{varName}})][1] %>% as.Date(),
                min = min({{varName}}),min_datetime = datetime[{{varName}}==min({{varName}})][1] %>% as.Date()) %>% 
      ungroup() -> dt2
    
    dt1 %>% inner_join(dt2) %>% 
      select(station,everything()) %>% 
      mutate_if(is.Date,as.character) %>% 
      setNames(col_names) -> target_table
  }
  
  target_table
}

#########################
### 海平面气压 = 大气压 * 10^(6/18400*(1+T/273));T为温度

template_info %>% 
  filter(str_detect(title,"海平面气压")) %>% 
  select(title) %>% 
  print(n = Inf)

raw_dt %>% 
  mutate(气压 = 气压* 10^(6/18400*(1+温度/273))) -> raw_dt2

table_name = "P01"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = 气压,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "P02"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = 气压,mode = 2) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "P03"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = 气压,mode = 3) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

### 写出
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

write_rds(wb,path = "temp_wb.rds")

saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)





