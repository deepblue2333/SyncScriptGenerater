# 2024/09/26 Chengyu zhouchengyuhun@outklook.com
# 该程序用于自动化生成脉脉mysql2ch的表同步脚本

import sqlparse
from sqlglot import condition

from sqlparse.tokens import _TokenType
# 1 mysql2hive脚本生成
# 2 格式化sql

import json

class ScriptGenerator:
    def __init__(self, config):
        self.config = config

    def generate_dw_create_sql(self):
        config = self.config
        table_name = f"{config['layer']}.{config['Prefix']}_{config['Database']}_{config['TableName']}_{config['SyncType']}"
        comment = config["Comment"]

        # 生成字段部分
        fields = config["Fields"]
        field_sql = []

        for field, field_info in fields.items():
            field_name = field_info.get("as", field)
            field_type = mysql2hive_type_mapping.get(field_info["type"])
            field_comment = field_info["comment"]
            field_sql.append(f"  `{field_name}` {field_type} COMMENT '{field_comment}'")

        # 拼接最终的SQL语句
        sql = f"CREATE EXTERNAL TABLE `{table_name}`(\n"
        sql += ",\n".join(field_sql)
        sql += f"\n) COMMENT '{comment}'\n"
        sql += "PARTITIONED BY ( `p_date` string COMMENT '日期')\n"
        sql += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'\n"
        sql += "STORED AS ORC\n"
        sql += "TBLPROPERTIES ('orc.compression'='snappy');"

        return sql


# 数据类型转换映射
mysql2hive_type_mapping = {
    "bigint": "bigint",
    "bigint unsigned": "bigint",
    "varchar": "string",
    "text": "string",
    "timestamp": "timestamp"
}




# 数据类型转换映射
Sync_type_mapping = {
    "da": "daily"
}


# 生成 conf 的函数
def generate_mysql2hive_conf(config, first=True):
    table_name = config["TableName"]
    conf_key = f"ol_mysql_{config['Database']}_{table_name}"

    # 获取字段名
    fields = config["Fields"]
    field_names = [name for name in fields.keys()]

    if first:
        condition = "1 = 1"
    else:
        condition = "uptime >= '{st}'"

    # 生成 conf 结构
    conf = {
        conf_key: {
            "update_field": "uptime",
            "is_increment": True,
            "interval": Sync_type_mapping[config["SyncType"]],
            "db": config["Database"],
            "host": config["Host"],
            "condition": condition,
            "table": table_name,
            "overwrite": False,
            "schema": ", ".join(field_names)
        }
    }

    conf_result = json.dumps(conf, ensure_ascii=False, indent=4)

    return conf_result


# 生成 CREATE TABLE 语句
def generate_create_ods_table_sql(config):
    table_name = config["TableName"]
    create_table_sql = f'''CREATE EXTERNAL TABLE IF NOT EXISTS ods.ol_mysql_{config["Database"]}_{table_name}_history LIKE ods.ol_mysql_{config["Database"]}_{table_name}_update STORED AS ORC;'''
    return create_table_sql

def generate_insert_ods_table_sql(config):
    # 提取表名和字段
    table_name = config['TableName']
    fields = config['Fields']
    database = config["Database"]
    primary = config["PrimaryKey"]
    field_names = []
    for field, details in fields.items():
        field_names.append(field)

    # 生成 SQL 语句
    insert_sql = f"""
INSERT OVERWRITE TABLE ods.ol_mysql_{database}_{table_name}_history PARTITION (p_date = '{{target_date}}')
SELECT
    {', '.join(field_names)}
FROM
(
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {primary} ORDER BY uptime DESC) AS PK_RANK_SEQ_
    FROM
    (
        SELECT * FROM ods.ol_mysql_{database}_{table_name}_history WHERE p_date = DATE_SUB('{{target_date}}', 1)
        UNION ALL
        SELECT * FROM ods.ol_mysql_{database}_{table_name}_update WHERE p_date = '{{target_date}}'
    ) T1
) T2
WHERE T2.PK_RANK_SEQ_ = 1;
    """

    return insert_sql

def generate_insert_dw_table_sql(config):
    # 提取表名和字段信息
    table_name = config["TableName"]
    target_date = '{target_date}'  # 这里可以替换为实际日期
    fields = config["Fields"]
    database = config["Database"]

    # 生成 SQL 语句
    insert_sql = f"INSERT OVERWRITE TABLE {config['layer']}.{config['Prefix']}_{database}_{table_name}_{config['SyncType']} PARTITION (p_date = '{target_date}')\n"
    insert_sql += "SELECT "

    # 构建字段列表
    field_list = []
    for field_name, field_info in fields.items():
        field_list.append(f"{field_name}")

    insert_sql += "\n     , ".join(field_list)  # 将字段连接成字符串
    insert_sql += f"\nFROM ods.ol_mysql_{config['Database']}_{table_name}_history\n"
    insert_sql += f"WHERE p_date = '{target_date}'"

    return insert_sql


# 生成任务配置
def generate_dw_task_conf(config):
    table_name = config['TableName']
    layer = config['layer']
    sync_type = config['SyncType']

    task_config = {
        f"ol_mysql_{config['Database']}_{table_name}_history": {
            "external_dag_id": "mysql2hive_daily",
            "external_task_id": f"ol_mysql_{config['Database']}_{table_name}_history",
            "task_type": "external",
            "delta_minutes": 30
        },
        f"{layer}_{config['Prefix']}_{config['Database']}_{table_name}_{sync_type}": {
            "task_type": "hive",
            "dependencies": [
                f"ol_mysql_{config['Database']}_{table_name}_history"
            ],
            "hive_table": f"{layer}.{config['Prefix']}_{config['Database']}_{table_name}_{sync_type}"
        }
    }

    conf_result = json.dumps(task_config, ensure_ascii=False, indent=4)

    return conf_result

# 生成表的SQL
def generate_ch_create_sql(config):
    # 生成字段定义部分的SQL
    fields = config["Fields"]
    field_sqls = []
    for field, properties in fields.items():
        field_name = properties.get("as", field)
        field_type = properties["type"].replace("bigint unsigned", "UInt64").replace("bigint", "UInt64").replace("varchar", "String").replace("text", "String").replace("timestamp", "DateTime")
        comment = properties["comment"]
        field_sqls.append(f"{field_name} {field_type} comment '{comment}'")
    fields_sql = ",\n    ".join(field_sqls)

    table_name_shard = f"dwd_shard.{config['Prefix']}_{config['Database']}_{config['TableName']}_{config['SyncType']}"
    table_name_distributed = f"dwd.{config['Prefix']}_{config['Database']}_{config['TableName']}_{config['SyncType']}"

    shard_table_sql = f'''create table {table_name_shard} on CLUSTER ch_cluster1
(
    `d` Date comment '分区',
    {fields_sql}
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/dwd_shard/{table_name_shard}/{{layer}}', '{{replica}}')
PARTITION BY d
ORDER BY log_id
TTL d + toIntervalDay(2)
SETTINGS index_granularity = 8192;  # 定义了每个索引块中行的数量'''

    distributed_table_sql = f'''create table {table_name_distributed} on cluster ch_cluster1
(
    `d` Date comment '分区',
    {fields_sql}
) ENGINE = Distributed('ch_cluster1', 'dwd_shard', '{table_name_shard}', rand());'''

    return shard_table_sql, distributed_table_sql


if __name__ == "__main__":

    # 读取 JSON 配置文件
    with open('config.json', 'r') as f:
        config = json.load(f)

    # 生成 SQL
    # sql_statement = generate_dw_create_sql(config)

    # 输出 SQL
    # print(sql_statement)

    sql_statement = generate_mysql2hive_conf(config)
    print(sql_statement)

    # 生成 SQL
    create_table_sql = generate_create_ods_table_sql(config)

    # 打印结果
    print(create_table_sql)

    insert_sql = generate_insert_ods_table_sql(config)
    print(insert_sql)

    insert_sql = generate_insert_dw_table_sql(config)
    print(insert_sql)

    conf_result = generate_dw_task_conf(config)
    print(conf_result)

    shard_table_sql, distributed_table_sql = generate_ch_create_sql(config)
    print(shard_table_sql)
    print(distributed_table_sql)
