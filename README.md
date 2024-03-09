# spark-study
- 這邊所有內容是從 `Spark 學習手冊` 的範例用 python 抄或是改寫出來的
  - 天瓏書局 (1st edition): [中文版](https://www.tenlong.com.tw/products/9789864760466)
  - Amazon
    - (1st edition): [英文版](https://www.amazon.com/Learning-Spark-Lightning-Fast-Data-Analysis/dp/1449358624) 
    - (2nd edition): [英文版](https://www.amazon.com/-/zh_TW/Jules-Damji-dp-1492050040/dp/1492050040/ref=dp_ob_title_bk)

# P.S
- 下面是我的 `.bash_profile` 的內容, 會讓 Spark 使用 python 3.11 (我的 spark 裝在 `/opt/spark/latest/`)

```
export SPARK_HOME=/home/sary357/spark/latest/
export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11

export PATH="$SPARK_HOME/bin:$PATH"
```
