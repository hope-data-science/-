

library(pacman)
p_load(tidyverse,readxl,openxlsx,lubridate,data.table)

setwd("G:\\博士期间小项目\\郭老师数据处理")

# template整理

dir() %>% 
  str_extract("^tem.+xls$") %>% 
  discard(is.na) -> template_name

excel_sheets(template_name) %>% 
  setdiff(c("D61new",str_c("Sheet",1:3)))-> template_sheet_name

all = tibble()
for(i in template_sheet_name) {
  sheet_name = i
  
  read_excel(template_name,sheet = sheet_name,col_names = F,n_max = 1) %>% 
    .[[1]] -> title
  
  read_excel(template_name,sheet = sheet_name,skip = 1,col_names = F,n_max = 1) %>% 
    apply(1,str_c,collapse = ";") -> cn_explanation
  
  read_excel(template_name,sheet = sheet_name,skip = 2,col_names = F,n_max = 1) %>% 
    apply(1,str_c,collapse = ";") -> en_abbreviation
  
  tibble(sheet_name,title,cn_explanation,en_abbreviation) %>% 
    bind_rows(all,.) -> all
}


all %>% 
  rename(cn = cn_explanation,en = en_abbreviation) -> template_info

# 原始数据整理merge -> template

merge_na = function(a,b){
  if(is.na(a) & is.na(b)) 
    return(NA)
  if(is.na(a)) return(b)
  else return(a)
}

excel_sheets("气象站.xlsx") -> sheet_names

all = tibble()
for(i in sheet_names){
  read_excel("气象站.xlsx",sheet = i,skip = 2) %>% 
    filter(!str_detect(时间,"^[(崇明)(总共)]")) %>% 
    bind_rows(all,.) -> all
}

all %>% 
  mutate(time1 = lubridate::ymd_hms(时间)) %>% 
  mutate(time2 = janitor::excel_numeric_to_date(时间 %>% as.numeric(),include_time = T)) %>% 
  #select(time1,time2) %>% 
  mutate(time1 = as.character(time1),time2=as.character(time2)) %>%
  rowwise() %>% 
  mutate(time = merge_na(time1,time2)) %>% 
  ungroup() %>% 
  mutate(时间 = time,datetime = parse_datetime(time)) %>% 
  select(-time,-time1,-time2) %>% 
  drop_na(datetime) %>% 
  distinct(datetime,.keep_all = T) %>% 
  mutate_if(is.character,as.numeric) %>% 
  mutate(时间 = as.character(datetime)) %>% 
  mutate(year = year(datetime),
         month = month(datetime),
         day = day(datetime),
         hour = hour(datetime),
         station = "CJM") %>% 
  as.data.table() %>% 
  .[hour == 0,
    `:=`(year = year(datetime - days(1)),
         month = month(datetime - days(1)),
         day = day(datetime - days(1)),
         hour = 24)] %>% 
  .[between(year,2010,2018)] %>%
  as_tibble() %>% 
  arrange(datetime)-> raw_dt

save(template_info,raw_dt,file = "meta.rdata")
rm(list = ls())
#############################################################################################

###站点代码CJM，缺失值空着

## Round 1 温度/最高温度/最低温度 -> T1/T2/T3

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

pacman::p_load(tidyverse,openxlsx,maditr)
setwd("G:\\博士期间小项目\\郭老师数据处理")
load("meta.rdata")

template_info %>% 
  filter(str_detect(title,"温")) %>% 
  select(title) %>% 
  print(n = Inf)

template_info %>% 
  filter(str_detect(title,"气温")) 

### T1
table_name = "T1"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector()-> t1_names

raw_dt %>% 
  select(station,year,month,day,hour,温度) %>% 
  pivot_wider(names_from = hour,values_from = 温度) %>% 
  mutate(station = "CJM") %>% 
  select(station,year,month,day,`21`,`22`,`23`,`24`,everything()) %>% 
  mutate(beizhu = "") %>% 
  setNames(t1_names) %>% 
  mutate_all(list(~replace_na(.,""))) -> target_table

loadWorkbook(file = "template_meter_2010_2018.xlsx") -> wb
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)
# saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)
write_rds(wb,path = "temp_wb.rds")

### T2
table_name = "T2"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector() -> t2_names
raw_dt %>% 
  group_by(year,month,day) %>% 
  summarise(station = unique(station),avg = mean(温度),
            max = max(最高气温),max_time = hour[最高气温==max(最高气温)][1],
            min = min(最低气温),min_time = hour[最低气温==min(最低气温)][1]) %>% 
  ungroup() %>% 
  na.omit()%>% 
  mutate_at(vars(matches("time")),list(~str_c(.,":00"))) %>% 
  select(station,everything())%>% 
  setNames(t2_names) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)
write_rds(wb,path = "temp_wb.rds")

### T3
table_name = "T3"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector() -> t3_names

raw_dt %>% 
  group_by(year,month) %>% 
  summarise(station = unique(station),avg = mean(温度),
            max_avg = mean(最高气温),min_avg = mean(最低气温),
            max = max(最高气温),max_datetime = datetime[最高气温==max(最高气温)][1] %>% as.Date(),
            min = min(最低气温),min_datetime = datetime[最低气温==min(最低气温)][1] %>% as.Date()) %>% 
  ungroup() %>% 
  na.omit()%>% 
  select(station,everything()) %>% 
  mutate_if(is.Date,as.character) %>% 
  setNames(t3_names) -> target_table

writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)
write_rds(wb,path = "temp_wb.rds")



