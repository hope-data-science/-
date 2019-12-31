
## 雨量-> R1/R2/R3
## 湿度-> RH1/RH2/RH3
## 温度,相对湿度 -> 水气压 HB 1-3

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

### 读入
pacman::p_load(tidyverse,openxlsx,lubridate)
setwd("G:\\博士期间小项目\\郭老师数据处理")
load("meta.rdata")
read_rds("temp_wb.rds") -> wb

### 中间工作站

raw_dt
template_info

template_info %>% 
  filter(str_detect(title,"湿度")) %>% 
  select(title) %>% 
  print(n = Inf)

template_info %>% 
  filter(str_detect(title,"降水")) %>% pull(cn)

## 雨量-> R1/R2/R3
table_name = "R1"
fast_table(raw_dt = raw_dt,table_name = table_name,varName = 一小时雨量,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "R2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt %>% 
  select(station,year,month,day,hour,一小时雨量) %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),sum = sum(一小时雨量),max = max(一小时雨量)) %>% 
  ungroup() %>% select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "R3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt %>% 
  select(station,year,month,day,hour,datetime,一小时雨量) %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),sum = sum(一小时雨量),max = max(一小时雨量),
            max_datetime = datetime[一小时雨量==max(一小时雨量)][1] %>% as.Date()) %>% 
  ungroup %>% select(station,everything()) %>% 
  mutate_if(is.Date,as.character) %>% 
  setNames(col_names) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

write_rds(wb,path = "temp_wb.rds")
saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)

## 温度,相对湿度 -> 水气压 HB 1-3
### 水气压E=6.11×10^(7.5*温度/(237+温度))*相对湿度
### https://www.zybang.com/question/5cc141e04190ee0158c754b76d9b6258.html

raw_dt %>% 
  mutate(水气压 = 6.11*10^(7.5*温度/(237+温度))*相对湿度) -> raw_dt2

table_name = "HB1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = 水气压,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "HB2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt2 %>% 
  select(station,year,month,day,hour,datetime,水气压) %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),avg = mean(水气压),max = max(水气压),min = min(水气压)) %>% 
  ungroup %>% 
  select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "HB3"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = 水气压,mode = 3) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

## 相对湿度/最小相对湿度 RH 1-3

table_name = "RH1"
fast_table(raw_dt = raw_dt,table_name = table_name,varName = 相对湿度,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "RH2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),avg = mean(相对湿度),min = min(最小相对湿度),
            min_time = hour[最小相对湿度==min(最小相对湿度)][1]) %>% 
  mutate_at(vars(matches("time")),list(~str_c(.,":00"))) %>% 
  ungroup %>% 
  select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "RH3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt %>% 
  group_by(year,month,day) %>% 
  summarise(日平均 = mean(相对湿度),日最小 = min(最小相对湿度)) %>% 
  select(-day) %>% 
  group_by(year,month) %>% 
  summarise_all(mean) %>% 
  ungroup -> dt1
raw_dt %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),min = min(最小相对湿度),
            min_datetime = datetime[最小相对湿度==min(最小相对湿度)][1] %>% as.Date()) %>% 
  ungroup() %>% 
  mutate_if(is.Date,as.character)-> dt2
dt1 %>% 
  inner_join(dt2) %>% 
  select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

template_info %>% 
  filter(str_detect(title,"湿度")) %>% 
  #pull(cn)
  select(title) %>% 
  print(n = Inf)

write_rds(wb,path = "temp_wb.rds")
saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)



