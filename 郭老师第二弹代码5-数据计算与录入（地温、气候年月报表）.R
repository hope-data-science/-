
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

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

#########################

### 读入
pacman::p_load(tidyverse,openxlsx,lubridate,dtplyr)
setwd("G:\\博士期间小项目\\郭老师数据处理")
load("meta.rdata")
read_rds("temp_wb.rds") -> wb

#################################地温数据全体缺失！！！

raw_dt %>% select(contains("地")) %>% summary()

## 地表温度/最高地表温度/最低地表温度 -> Tg01/Tg02/Tg03  不用继续

##### 检查空着的sheet

pacman::p_load(readxl)

sheet_names = template_info %>% pull(sheet_name)

all = vector()
for(i in sheet_names){
  read_excel("temp_wb.xlsx",range = str_c(i,"!A3:A4")) %>% .[[1]] -> to_test
  if(is.na(to_test)) all = c(all,i)
  else if(to_test == "") all = c(all,i)

}

tibble(sheet_name = all) %>% 
  inner_join(template_info) %>% 
  select(sheet_name,title) %>% 
  #filter(!str_detect(title,"地|人工|风")) %>% 
  print(n = Inf)



# writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

# saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)
# write_rds(wb,path = "temp_wb.rds")

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

table_name = "QX4"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector() -> col_names
raw_dt %>% 
  mutate(水汽压 = 6.11*10^(7.5*温度/(237+温度))*相对湿度) %>% 
  mutate(海平面气压 = 气压* 10^(6/18400*(1+温度/273))) %>% 
  mutate(气温 = 温度) %>% 
  mutate(露点温度=露点) %>% 
  mutate(ws2 = `2分钟风速`,wd2 = `2分钟风向`) %>% 
  mutate(ws10 = `10分钟风速`,wd10 = `10分钟风向`) %>% 
  mutate(旬 = case_when(
    day %in% 1:10 ~ "上",
    day %in% 11:20 ~ "中",
    T ~ "下"
  )) -> raw_dt2
raw_dt2 %>%
  group_by(生态站代码=station,年=year,月=month) %>% 
  summarise(
    `气压日平均值上旬平均(hpa)` = 气压[旬=="上"] %>% mean,
    `气压日平均值中旬平均(hpa)` = 气压[旬=="中"] %>% mean,
    `气压日平均值下旬平均(hpa)` = 气压[旬=="下"] %>% mean,
    `气压日平均值月平均(hpa)` = by(气压,day,mean) %>% mean,
    `气压日最高值月平均(hpa)` = by(气压,day,max) %>% mean,
    `气压日最低值月平均(hpa)` = by(气压,day,min) %>% mean,
    `气压月极大值(hpa)` = 气压 %>% max,
    `气压月极大值出现日期` = datetime[气压==max(气压)][1] %>% as.Date() %>% as.character(),
    `气压月极小值(hpa)` = 气压 %>% min,
    `气压月极小值出现日期` = datetime[气压==min(气压)][1] %>% as.Date() %>% as.character(),
    `水汽压日平均值上旬平均(hpa)` = 水汽压[旬=="上"] %>% mean,
    `水汽压日平均值中旬平均(hpa)` = 水汽压[旬=="中"] %>% mean,
    `水汽压日平均值下旬平均(hpa)` = 水汽压[旬=="下"] %>% mean,
    `水汽压日平均值月平均(hpa)` = by(水汽压,day,mean) %>% mean,
    `水汽压月极大值(hpa)` = 水汽压 %>% max,
    `水汽压月极大值出现日期` = datetime[水汽压==max(水汽压)][1] %>% as.Date() %>% as.character(),
    `水汽压月极小值(hpa)` = 水汽压 %>% min,
    `水汽压月极小值出现日期` = datetime[水汽压==min(水汽压)][1] %>% as.Date() %>% as.character(),
    `海平面气压日平均值上旬平均(hpa)` = 海平面气压[旬=="上"] %>% mean,
    `海平面气压日平均值中旬平均(hpa)` = 海平面气压[旬=="中"] %>% mean,
    `海平面气压日平均值下旬平均(hpa)` = 海平面气压[旬=="下"] %>% mean,
    `海平面气压日平均值月平均(hpa)` = by(海平面气压,day,mean) %>% mean,
    `海平面气压日最高值月平均(hpa)` = by(海平面气压,day,max) %>% mean,
    `海平面气压日最低值月平均(hpa)` = by(海平面气压,day,min) %>% mean,
    `海平面气压月极大值(hpa)` = 海平面气压 %>% max,
    `海平面气压月极大值出现日期` = datetime[海平面气压==max(海平面气压)][1] %>% as.Date() %>% as.character(),
    `海平面气压月极小值(hpa)` = 海平面气压 %>% min,
    `海平面气压月极小值出现日期` = datetime[海平面气压==min(海平面气压)][1] %>% as.Date() %>% as.character(),
    `气温日平均值上旬平均(℃)` = 气温[旬=="上"] %>% mean,
    `气温日平均值中旬平均(℃)` = 气温[旬=="中"] %>% mean,
    `气温日平均值下旬平均(℃)` = 气温[旬=="下"] %>% mean,
    `气温日平均值月平均(℃)` = by(气温,day,mean) %>% mean,
    `气温日最高值月平均(℃)` = by(最高气温,day,max) %>% mean,
    `气温日最低值月平均(℃)` = by(最低气温,day,min) %>% mean,
    `气温月极大值(℃)` = 最高气温 %>% max,
    `气温月极大值出现日期` = datetime[最高气温==max(最高气温)][1] %>% as.Date() %>% as.character(),
    `气温月极小值(℃)` = 最低气温 %>% min,
    `气温月极小值出现日期` = datetime[最低气温==min(最低气温)][1] %>% as.Date() %>% as.character(),
    `露点温度日平均值上旬平均(℃)` = 露点温度[旬=="上"] %>% mean,
    `露点温度日平均值中旬平均(℃)` = 露点温度[旬=="中"] %>% mean,
    `露点温度日平均值下旬平均(℃)` = 露点温度[旬=="下"] %>% mean,
    `露点温度日平均值月平均(℃)` = by(露点温度,day,mean) %>% mean,
    `露点温度日最高值月平均(℃)` = by(露点温度,day,max) %>% mean,
    `露点温度日最低值月平均(℃)` = by(露点温度,day,min) %>% mean,
    `露点温度月极大值(℃)` = 露点温度 %>% max,
    `露点温度月极大值出现日期` = datetime[露点温度==max(露点温度)][1] %>% as.Date() %>% as.character(),
    `露点温度月极小值(℃)` = 露点温度 %>% min,
    `露点温度月极小值出现日期` = datetime[露点温度==min(露点温度)][1] %>% as.Date() %>% as.character(),
    `相对湿度日平均值上旬平均(%)` = 相对湿度[旬=="上"] %>% mean,
    `相对湿度日平均值中旬平均(%)` = 相对湿度[旬=="中"] %>% mean,
    `相对湿度日平均值下旬平均(%)` = 相对湿度[旬=="下"] %>% mean,
    `相对湿度日平均值月平均(%)` = by(相对湿度,day,mean) %>% mean,
    `相对湿度日最低值月平均(%)` = by(相对湿度,day,min) %>% mean,
    `相对湿度月极小值(%)` = 相对湿度 %>% min,
    `相对湿度月极小值出现日期` = datetime[相对湿度==min(相对湿度)][1] %>% as.Date() %>% as.character(),
    `降水上旬合计值(mm)` = 一小时雨量[旬=="上"] %>% sum,
    `降水中旬合计值(mm)` = 一小时雨量[旬=="中"] %>% sum,
    `降水下旬合计值(mm)` = 一小时雨量[旬=="下"] %>% sum,
    `降水量月合计值(mm)` = 一小时雨量 %>% sum,
    `降水量月极大值(mm/h)` = 一小时雨量 %>% max,
    `降水量月极大值出现日期` = datetime[一小时雨量==max(一小时雨量)][1] %>% as.Date() %>% as.character(),
    `降水量感雨器月合计值(mm)`= "",
    `降水量感雨器感应月最长时间` = "",
    `2分钟平均风速月平均(m/s)` = ws2 %>% mean,
    `2分钟平均风速月最多风向`  = wd2 %>% Mode,  
    `2分钟平均风速月极大值(m/s)`= ws2 %>% max,
    `2分钟平均风速月极大值风向` = wd2[ws2==max(ws2)][1],
    `2分钟平均风速月极大值出现日期`=datetime[ws2==max(ws2)][1]%>% as.Date() %>% as.character(),
    `10分钟平均风速月平均(m/s)` = ws10 %>% mean,
    `10分钟平均风速月最多风向`  = wd10 %>% Mode,  
    `10分钟平均风速月极大值(m/s)`= by(ws10,day,mean) %>% max,
    `10分钟平均风速月极大值风向` = wd10[ws10==max(ws10)][1],
    `10分钟平均年风速月极大值出现日期`=datetime[ws10==max(ws10)][1]%>% as.Date() %>% as.character(),
    `10分钟最大风速月极大值(m/s)` = max(最大风速),
    `10分钟最大风速月极大值风向` = 最大风向[最大风速==max(最大风速)][1],
    `10分钟最大风速月极大值出现日期` = datetime[最大风速==max(最大风速)][1]%>% as.Date() %>% as.character(),
    `60分钟最大风速月极大值(m/s)` = "", 
    `60分钟最大风速月极大值风向` = "",   
    `60分钟最大风速月极大值出现日期`  = "",  
    `60分钟最大风速月极大值出现次数` = "",   
    `备注`  = ""
  ) %>% 
  ungroup() %>% 
  select(col_names)-> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

# target_table %>% names() -> a
# col_names[1:length(a)] -> b
# setequal(a,b)
# 
# a %>% setdiff(b)
# b %>% setdiff(a)
# 
# col_names %>% str_subset("10分钟") %>% setdiff(names(temp_table))
# raw_dt

###########################################################################################################
table_name = "QX5"
template_info %>% 
  filter(sheet_name == table_name) %>% pull(cn) %>% 
  str_split(";",simplify = T) %>% 
  as.vector() -> col_names
raw_dt %>% 
  mutate(水汽压 = 6.11*10^(7.5*温度/(237+温度))*相对湿度)%>% 
  mutate(海平面气压 = 气压* 10^(6/18400*(1+温度/273)))  -> raw_dt2
raw_dt2 %>%
  group_by(生态站代码=station,年=year) %>% 
  summarise(
    "气压年极大值(hpa)" = max(气压),
    "气压年极大值出现日期" = datetime[气压==max(气压)][1] %>% as.Date() %>% as.character(),
    "气压年极小值(hpa)" = min(气压),            
    "气压年极小值出现日期" = datetime[气压==min(气压)][1] %>% as.Date() %>% as.character(),
    "气压日平均值年平均(hpa)" = by(气压,day,mean) %>% mean,
    "气压日最高值年平均(hpa)" = by(气压,day,max) %>% mean,  
    "气压日最低值年平均(hpa)" = by(气压,day,min) %>% mean,
    "水汽压年极大值(hpa)" = max(水汽压),       
    "水汽压年极大值出现日期" = datetime[水汽压==max(水汽压)][1] %>% as.Date() %>% as.character(),     
    "水汽压年极小值(hpa)" = min(水汽压),       
    "水汽压年极小值出现日期" = datetime[水汽压==min(水汽压)][1] %>% as.Date() %>% as.character(),   
    "水汽压日平均值年平均(hpa)" = by(水汽压,day,mean) %>% mean, 
    "海平面气压年极大值(hpa)" = max(海平面气压),       
    "海平面气压年极大值出现日期" = datetime[海平面气压==max(海平面气压)][1] %>% as.Date() %>% as.character(),     
    "海平面气压年极小值(hpa)" = min(海平面气压),       
    "海平面气压年极小值出现日期" = datetime[海平面气压==min(海平面气压)][1] %>% as.Date() %>% as.character(),   
    "海平面气压日平均值年平均(hpa)" = by(海平面气压,day,mean) %>% mean, 
    "海平面气压日最高值年平均(hpa)" = by(海平面气压,day,max) %>% mean, 
    "海平面气压日最低值年平均(hpa)" = by(海平面气压,day,min) %>% mean, 
    "气温年极大值(℃)" = max(最高气温),       
    "气温年极大值出现日期" = datetime[最高气温==max(最高气温)][1] %>% as.Date() %>% as.character(),     
    "气温年极小值(℃)" = min(最低气温),       
    "气温年极小值出现日期" = datetime[最低气温==min(最低气温)][1] %>% as.Date() %>% as.character(),   
    "气温日平均值年平均(℃)" = by(温度,day,mean) %>% mean, 
    "气温日最高值年平均(℃)" = by(最高气温,day,max) %>% mean, 
    "气温日最低值年平均(℃)" = by(最低气温,day,min) %>% mean, 
    "露点温度年极大值(℃)" = max(露点),       
    "露点温度年极大值出现日期" = datetime[露点==max(露点)][1] %>% as.Date() %>% as.character(),     
    "露点温度年极小值(℃)" = min(露点),       
    "露点温度年极小值出现日期" = datetime[露点==min(露点)][1] %>% as.Date() %>% as.character(),   
    "露点温度日平均值年平均(℃)" = by(露点,day,mean) %>% mean, 
    "露点温度日最高值年平均(℃)" = by(露点,day,max) %>% mean, 
    "露点温度日最低值年平均(℃)" = by(露点,day,min) %>% mean, 
    "相对湿度年极小值(%)" = min(相对湿度),  
    "相对湿度年极小值出现日期" = datetime[相对湿度==min(相对湿度)][1] %>% as.Date() %>% as.character(),    
    "相对湿度日平均值年平均(%)" = by(相对湿度,day,mean) %>% mean, 
    "相对湿度日最低值年平均(%)" = by(相对湿度,day,min) %>% mean,
    "降水量年极大值(mm/h)" = max(一小时雨量),  
    "降水量年极大值出现日期" = datetime[一小时雨量==max(一小时雨量)][1] %>% as.Date() %>% as.character(),  
    "降水量年合计值(mm)" = sum(一小时雨量),
    "降水量感雨器感应年最长时间(mm)"="",
    "降水量感雨器年合计值"="",
    "2分钟平均风速年极大值(m/s)" = max(`2分钟风速`),  
    "2分钟平均风速年极大值风向" = `2分钟风向`[`2分钟风速`==max(`2分钟风速`)][1],   
    "2分钟平均风速年极大值出现日期" = datetime[`2分钟风速`==max(`2分钟风速`)][1] %>% as.Date() %>% as.character(),
    `10分钟平均风速年极大值(m/s)`= by(`10分钟风速`,day,mean) %>% max,
    `10分钟平均风速年极大值风向` = `10分钟风向`[`10分钟风速`==max(`10分钟风速`)][1],
    `10分钟平均风速年极大值出现日期`=datetime[`10分钟风速`==max(`10分钟风速`)][1]%>% as.Date() %>% as.character(),
    `10分钟最大风速年极大值(m/s)` = max(最大风速),
    `10分钟最大风速年极大值风向` = 最大风向[最大风速==max(最大风速)][1],
    `10分钟最大风速年极大值出现日期` = datetime[最大风速==max(最大风速)][1]%>% as.Date() %>% as.character(),
    "60分钟最大风速年极大值(m/s)" = "",
    "60分钟最大风速年极大值风向" = "",
    "60分钟最大风速年极大值出现日期" = "",
    "60分钟最大风速年极大值出现次数" = "",
    "备注"  = ""
  ) -> target_table
writeData(wb,table_name,target_table,startRow = 4,startCol = 1,colNames = F)

# target_table %>% names() -> a
# col_names[1:length(a)] -> b
# setequal(a,b)
# col_names %>% setdiff(a)
# 
# template_info %>% 
#   filter(str_detect(title,"自动站气象观测")) %>% 
#   #select(sheet_name)
#   #filter(sheet_name == "W60W1") %>% 
#   select(title) %>% 
#   print(n = Inf)

write_rds(wb,path = "temp_wb.rds")
saveWorkbook(wb,"temp_wb.xlsx",overwrite = T)


