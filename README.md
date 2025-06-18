
# 📘 作业 3 - MovieLens 100k 数据分析（Spark + Cassandra）

> STQD6324 数据管理 · 2024/2025 第二学期  
> 学生：XXX  
> 日期：2025 年 6 月

---

## 📁 数据来源

使用 [MovieLens 100k 数据集](https://grouplens.org/datasets/movielens/100k/)，包括以下三个文件：

- `u.user`：用户基本信息  
- `u.item`：电影信息  
- `u.data`：评分记录

数据已上传至 HDFS 路径：

```bash
/tmp/ml-100k/
```

---

## 🧱 技术栈

- Apache Spark 2 (PySpark)
- Apache Cassandra
- Zeppelin Notebook
- HDFS + Ambari

---

## ✅ 实现步骤

| 步骤 | 内容 |
|------|------|
| 1 | 导入库，配置 Zeppelin Spark2 与 Cassandra 连接 |
| 2 | 上传数据至 HDFS（通过 Ambari 文件管理界面） |
| 3 | 使用 PySpark 读取文件为 RDD 并结构化为 DataFrame |
| 4 | 写入结构化数据到 Cassandra（Keyspace: `movielens`） |
| 5 | 通过 DataFrame 查询或 CQL 查询进行分析 |

---

## 📊 作业题目回答

### i）计算每部电影的平均评分

- 通过 `groupBy(item_id)` 计算 `avg(rating)`  
- 与 `u.item` join 显示电影标题

```python
avg_ratings_df = rating_df.groupBy("item_id").agg(avg("rating").alias("avg_rating"))
joined_df = avg_ratings_df.join(item_df, on="item_id")
```

---

### ii）找出评分平均分最高的电影（前 10 名）

- 使用 `.orderBy("avg_rating", ascending=False).limit(10)`

---

### iii）找出评分总数 ≥ 50 的用户，并找出他们最喜欢的电影类型

- `groupBy(user_id)` + `count(*)` 过滤出活跃用户  
- 与 `u.item` 展开后的 genre 信息 join  
- 用 `Window` 分区取出每位用户评分最多的类型

---

### iv）找出年龄在 30～40 岁的科学家

```python
user_df.filter((user_df.occupation == "scientist") & (user_df.age >= 30) & (user_df.age <= 40))
```

---

## 📦 附加内容

### 📂 HDFS 路径结构：
```
/tmp/ml-100k/
├── u.user
├── u.item
└── u.data
```

### 🧾 Cassandra 表结构（Keyspace: `movielens`）

```cql
CREATE TABLE users (
  user_id int PRIMARY KEY,
  age int,
  gender text,
  occupation text,
  zip text
);
```

---

## 💡 运行说明

```python
# 读取用户数据
user_df = parse_user_file("hdfs:///tmp/ml-100k/u.user")

# 写入 Cassandra
user_df.write   .format("org.apache.spark.sql.cassandra")   .mode("append")   .option("keyspace", "movielens")   .option("table", "users")   .option("spark.cassandra.connection.host", "127.0.0.1")   .save()
```

---

## ✅ 总结

- 所有任务均已在 Zeppelin 中完成并验证  
- 数据成功导入 Cassandra 并通过 Spark 查询  
- 使用 `RDD` → `DataFrame` 的转换，理解了 Spark 数据处理流程  
- 查询与分析逻辑使用 SQL-style 语法实现，结果可视化清晰
