# hive2ch配置文档

本脚本由python自动生成，使用时请配置好config文件。	——v0.1

## 1 mysql2hive

### 1.1 新增mysql2hive conf

```sql
{
    "ol_mysql_jobs_headhunting_consultant_job_candidate_act_log": {
        "update_field": "uptime",
        "is_increment": true,
        "interval": [
            "da"
        ],
        "db": "jobs",
        "host": "companydbs2",
        "condition": "1 = 1",
        "table": "headhunting_consultant_job_candidate_act_log",
        "overwrite": false,
        "schema": "id, uid, job_req_id, candidate_uid, act_name, act_uid, extra, crtime, uptime"
    }
}
```

### 1.2 新建history表的同步sql

```sql

    INSERT OVERWRITE TABLE ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history PARTITION (p_date = '{target_date}')
    SELECT
        id, uid, job_req_id, candidate_uid, act_name, act_uid, extra, crtime, uptime
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY uptime DESC) AS PK_RANK_SEQ_
        FROM
        (
            SELECT * FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history WHERE p_date = DATE_SUB('{target_date}', 1)
            UNION ALL
            SELECT * FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_update WHERE p_date = '{target_date}'
        ) T1
    ) T2
    WHERE T2.PK_RANK_SEQ_ = 1;
        
```

### 1.3 重跑update任务

此时mairflow平台上mysql2hive Tag下会新增两个任务update任务和history任务，我们重跑update任务以首次全量同步mysql数据至hive update表的对应分区。

### 1.4 检验mysql update表同步

```sql

        -- 验证表结构
        DESC ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_update;

        -- 检查分区是否正常
        SHOW PARTITIONS ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_update;

        -- 检查数据是否存在空值异常值
        SELECT *
        FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_update
        WHERE p_date=2024-09-29
        LIMIT 100;

        -- 检查数据是否存在重复值
        SELECT COUNT(id),
               COUNT(DISTINCT id)
        FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_update
        WHERE p_date=2024-09-29;
```

### 1.5 创建history表

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history LIKE ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_update STORED AS ORC;
```

### 1.6 重跑history任务

重跑history任务以同步update表数据至history表。

```sql

        -- 验证表结构
        DESC ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history;

        -- 检查分区是否正常
        SHOW PARTITIONS ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history;

        -- 检查数据是否存在空值异常值
        SELECT *
        FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history
        WHERE p_date=2024-09-29
        LIMIT 100;

        -- 检查数据是否存在重复值
        SELECT COUNT(id),
               COUNT(DISTINCT id)
        FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history
        WHERE p_date=2024-09-29;
```

### 1.7 将mysql2hive conf改为增量同步

```sql
{
    "ol_mysql_jobs_headhunting_consultant_job_candidate_act_log": {
        "update_field": "uptime",
        "is_increment": true,
        "interval": [
            "da"
        ],
        "db": "jobs",
        "host": "companydbs2",
        "condition": "uptime >= '{st}'",
        "table": "headhunting_consultant_job_candidate_act_log",
        "overwrite": false,
        "schema": "id, uid, job_req_id, candidate_uid, act_name, act_uid, extra, crtime, uptime"
    }
}
```

## 2 hive_dw_daily

### 2.1 创建hive_dw表

```sql
CREATE EXTERNAL TABLE `dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da`(
  `log_id` bigint COMMENT '主键id',
  `uid` bigint COMMENT 'uid',
  `job_req_id` bigint COMMENT 'ai_ht_job_requirements的id',
  `candidate_uid` bigint COMMENT '候选人的uid',
  `act_name` string COMMENT '行为名称',
  `act_uid` bigint COMMENT '执行行为的uid',
  `extra` string COMMENT '其他信息',
  `create_ts` timestamp COMMENT '创建时间',
  `update_ts` timestamp COMMENT '更新时间'
) COMMENT 'AI猎头-候选人行为记录当日明细表'
PARTITIONED BY ( `p_date` string COMMENT '日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
TBLPROPERTIES ('orc.compression'='snappy');
```

### 2.2 新增hive_dw同步sql

```sql
INSERT OVERWRITE TABLE dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da PARTITION (p_date = '{target_date}')
SELECT id
     , uid
     , job_req_id
     , candidate_uid
     , act_name
     , act_uid
     , extra
     , crtime
     , uptime
FROM ods.ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history
WHERE p_date = '{target_date}'
```

### 2.3 新增hive_dw同步conf

```sql
{
    "ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history": {
        "external_dag_id": "mysql2hive_daily",
        "external_task_id": "ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history",
        "task_type": "external",
        "delta_minutes": 30
    },
    "dwd_dw_jobs_headhunting_consultant_job_candidate_act_log_da": {
        "task_type": "hive",
        "dependencies": [
            "ol_mysql_jobs_headhunting_consultant_job_candidate_act_log_history"
        ],
        "hive_table": "dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da"
    }
}
```

### 2.4 重跑dw同步任务

重跑dw以同步ods update表和history表数据至hive dw层。

### 2.5 数据验证

```sql

        -- 验证表结构
        DESC dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da;

        -- 检查分区是否正常
        SHOW PARTITIONS dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da;

        -- 检查数据是否存在空值异常值
        SELECT *
        FROM dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da
        WHERE p_date=2024-09-29
        LIMIT 100;

        -- 检查数据是否存在重复值
        SELECT COUNT(id),
               COUNT(DISTINCT id)
        FROM dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da
        WHERE p_date=2024-09-29;
```

## 3 hive2ch

### 3.1 新增hive2ch任务配置conf

```sql
{
    "wait_dwd_dw_jobs_headhunting_consultant_job_candidate_act_log_da": {
        "external_dag_id": "hive_dw_daily",
        "external_task_id": "dwd_dw_jobs_headhunting_consultant_job_candidate_act_log_da",
        "task_type": "external",
        "delta_minutes": 0
    },
    "dwd_dw_jobs_headhunting_consultant_job_candidate_act_log_da": {
        "task_type": "hive2ch",
        "dependencies": [
            "wait_dwd_dw_jobs_headhunting_consultant_job_candidate_act_log_da"
        ],
        "hive_table": "dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da",
        "shard_table": "dwd_shard.dw_jobs_headhunting_consultant_job_candidate_act_log_da",
        "partition_field": "d",
        "drop_partitions": [
            "tuple(toDate('{target_date}'))"
        ],
        "pk_fields": "log_id",
        "overwrite": false,
        "table": "dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da",
        "check_enable": true
    }
}
```

### 3.2 新建hive2ch 建表sql

```sql
create table dwd_shard.dw_jobs_headhunting_consultant_job_candidate_act_log_da on CLUSTER ch_cluster1
(
    `d` Date comment '分区',
    log_id bigint comment '主键id',
    uid bigint comment 'uid',
    job_req_id bigint comment 'ai_ht_job_requirements的id',
    candidate_uid bigint comment '候选人的uid',
    act_name string comment '行为名称',
    act_uid bigint comment '执行行为的uid',
    extra string comment '其他信息',
    create_ts timestamp comment '创建时间',
    update_ts timestamp comment '更新时间'
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/dwd_shard/dwd_shard.dw_jobs_headhunting_consultant_job_candidate_act_log_da/{layer}', '{replica}')
PARTITION BY d
ORDER BY log_id
TTL d + toIntervalDay(2)
SETTINGS index_granularity = 8192;  # 定义了每个索引块中行的数量
```

```sql
create table dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da on cluster ch_cluster1
(
    `d` Date comment '分区',
    log_id bigint comment '主键id',
    uid bigint comment 'uid',
    job_req_id bigint comment 'ai_ht_job_requirements的id',
    candidate_uid bigint comment '候选人的uid',
    act_name string comment '行为名称',
    act_uid bigint comment '执行行为的uid',
    extra string comment '其他信息',
    create_ts timestamp comment '创建时间',
    update_ts timestamp comment '更新时间'
) ENGINE = Distributed('ch_cluster1', 'dwd_shard', 'dwd_shard.dw_jobs_headhunting_consultant_job_candidate_act_log_da', rand());
```

### 3.3 hive2ch insert sql

```sql

SELECT
    p_date as d,
    log_id,
    uid,
    job_req_id,
    candidate_uid,
    act_name,
    act_uid,
    extra,
    create_ts,
    update_ts
FROM dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da
WHERE p_date = '{target_date}';
        
```

