
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
      #mutate(beizhu = "") %>% 
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
pacman::p_load(tidyverse,openxlsx,lubridate,dtplyr)
setwd("G:\\博士期间小项目\\郭老师数据处理")
load("meta.rdata")
read_rds("temp_wb.rds") -> wb

## 2分钟风速/风向 -> W2V1/W2V2/W2V3/W2W1
raw_dt %>% rename(ws = `2分钟风速`,wd = `2分钟风向`) -> raw_dt2

table_name = "W2V1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = ws,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W2V2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt2 %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),max = max(ws),
            wd = wd[ws==max(ws)][1],avg = mean(ws)) %>% 
  ungroup() %>% select(station,everything()) %>% 
  setNames(col_names)-> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)
  
table_name = "W2V3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
raw_dt2 %>% 
  lazy_dt() %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),avg = mean(ws),max_wd = Mode(wd),
            max_ws = max(ws),max_ws_wd = wd[ws==max(ws)][1],
            max_ws_datetime = datetime[ws==max(ws)][1]) %>% 
  as_tibble() %>% 
  mutate(max_ws_datetime = as.character(max_ws_datetime)) %>% 
  separate(max_ws_datetime,c("date","time"),sep = " ") %>% 
  mutate(time = str_sub(time,1,5)) %>% 
  select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W2W1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = wd,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

## 10分钟平均风速/风向 -> W10AV1/W10AV2/W10AV3/W10AW1
raw_dt %>% rename(ws = `10分钟风速`,wd = `10分钟风向`) -> raw_dt2

table_name = "W10AV1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = ws,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W10AV2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt2 %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),max = max(ws),
            wd = wd[ws==max(ws)][1],avg = mean(ws)) %>% 
  ungroup() %>% select(station,everything()) %>% 
  setNames(col_names)-> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W10AV3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
raw_dt2 %>% 
  lazy_dt() %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),avg = mean(ws),max_wd = Mode(wd),
            max_ws = max(ws),max_ws_wd = wd[ws==max(ws)][1],
            max_ws_datetime = datetime[ws==max(ws)][1]) %>% 
  as_tibble() %>% 
  mutate(max_ws_datetime = as.character(max_ws_datetime)) %>% 
  separate(max_ws_datetime,c("date","time"),sep = " ") %>% 
  mutate(time = str_sub(time,1,5)) %>% 
  select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W10AW1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = wd,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

## 10分钟极大风速/风向 -> W10MV1/W10MV2/W10MV3/W10MW1
raw_dt %>% rename(ws = `极大风速`,wd = `极大风向`) -> raw_dt2

table_name = "W10MV1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = ws,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W10MV2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
raw_dt2 %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),max = max(ws),
            max_time = hour[ws==max(ws)][1] %>% str_c(":00")) %>% 
  ungroup() %>% select(station,everything()) %>% 
  setNames(col_names)-> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W10MV3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> col_names
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
raw_dt2 %>% 
  group_by(station,year,month) %>% 
  summarise(ws = max(ws),wd = wd[ws==max(ws)][1],max_ws_datetime = datetime[ws==max(ws)][1]) %>% 
  as_tibble() %>% 
  mutate(max_ws_datetime = as.character(max_ws_datetime)) %>% 
  separate(max_ws_datetime,c("date","time"),sep = " ") %>% 
  mutate(time = str_sub(time,1,5)) %>% 
  select(station,everything()) %>% 
  setNames(col_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

table_name = "W10MW1"
fast_table(raw_dt = raw_dt2,table_name = table_name,varName = wd,mode = 1) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)




raw_dt

template_info %>% 
  filter(str_detect(title,"10分钟极大")) %>% 
  #select(sheet_name)
  #filter(sheet_name == "W60W1") %>% 
  select(title) %>% 
  print(n = Inf)

write_rds(wb,path = "temp_wb.rds")
saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)



