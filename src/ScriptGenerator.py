# 2024/09/26 Chengyu zhouchengyuhun@outklook.com
# 该程序用于自动化生成脉脉mysql2ch的表同步脚本

from sqlparse.tokens import _TokenType
# 1 mysql2hive脚本生成
# 2 格式化sql

import json
from datetime import datetime, timedelta

class ScriptGenerator:
    def __init__(self, config):
        self.config = config

        self.mysql_hive_type_mapping = {
                "bigint": "bigint",
                "bigint unsigned": "bigint",
                "varchar": "string",
                "text": "string",
                "timestamp": "timestamp"
            }

        self.mysql_ch_type_mapping = {
            "bigint": "UInt64",
            "bigint unsigned": "UInt64",
            "varchar": "String",
            "text": "String",
            "timestamp": "DateTime"
        }


    # 生成 conf 的函数
    def generate_mysql2hive_conf(self, first=True):
        config = self.config
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
                "interval": [config["SyncType"]],
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

    def generate_dw_create_sql(self):
        config = self.config
        table_name = f"{config['layer']}.{config['Prefix']}_{config['Database']}_{config['TableName']}_{config['SyncType']}"
        comment = config["Comment"]

        # 生成字段部分
        fields = config["Fields"]
        field_sql = []

        for field, field_info in fields.items():
            field_name = field_info.get("as", field)
            field_type = self.mysql_hive_type_mapping.get(field_info["type"])
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

    def generate_insert_ods_table_sql(self):
        config = self.config
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

    # 生成 CREATE TABLE 语句
    def generate_create_ods_history_table_sql(self):
        config = self.config
        table_name = config["TableName"]
        create_table_sql = f'''CREATE EXTERNAL TABLE IF NOT EXISTS ods.ol_mysql_{config["Database"]}_{table_name}_history LIKE ods.ol_mysql_{config["Database"]}_{table_name}_update STORED AS ORC;'''
        return create_table_sql

    def generate_insert_dw_table_sql(self):
        config = self.config
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
    def generate_dw_task_conf(self):
        config = self.config
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

    # 生成任务配置
    def generate_hive2ch_task_conf(self):
        config = self.config
        table_name = config["TableName"]
        layer = config["layer"]

        fields = config["Fields"]
        pk_fields = ''
        for field_name, field_info in fields.items():
            if field_name == config["PrimaryKey"]:
                pk_fields = field_info.get("as")



        conf = {
            f"wait_{layer}_{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}": {
                "external_dag_id": "hive_dw_daily",
                "external_task_id": f"{layer}_{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}",
                "task_type": "external",
                "delta_minutes": 0
            },
            f"{layer}_{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}": {
                "task_type": "hive2ch",
                "dependencies": [
                    f"wait_{layer}_{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}"
                ],
                "hive_table": f"{layer}.{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}",
                "shard_table": f"{layer}_shard.{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}",
                "partition_field": "d",
                "drop_partitions": [
                    "tuple(toDate('{target_date}'))"
                ],
                "pk_fields": f"{pk_fields}",
                "overwrite": False,
                "table": f"{layer}.{config['Prefix']}_{config['Database']}_{table_name}_{config['SyncType']}",
                "check_enable": True
            }
        }

        conf_result = json.dumps(conf, ensure_ascii=False, indent=4)

        return conf_result

    # 生成表的SQL
    def generate_ch_create_sql(self):
        config = self.config
        # 生成字段定义部分的SQL
        fields = config["Fields"]

        pk_fields = ''
        for field_name, field_info in fields.items():
            if field_name == config["PrimaryKey"]:
                pk_fields = field_info.get("as")

        field_sqls = []
        for field, properties in fields.items():
            field_name = properties.get("as", field)
            field_type = self.mysql_hive_type_mapping.get(properties["type"])
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
ORDER BY {pk_fields}
TTL d + toIntervalDay(2)
SETTINGS index_granularity = 8192;  # 定义了每个索引块中行的数量'''

        distributed_table_sql = f'''create table {table_name_distributed} on cluster ch_cluster1
(
    `d` Date comment '分区',
    {fields_sql}
) ENGINE = Distributed('ch_cluster1', 'dwd_shard', '{table_name_shard}', rand());'''

        return shard_table_sql, distributed_table_sql

    def generate_ch_insert_sql(self):
        config = self.config

        # 获取表名、字段信息
        table_name = config["TableName"]
        fields = config["Fields"]
        layer = config["layer"]
        sync_type = config["SyncType"]

        # SQL 语句构建
        select_fields = ["p_date as d"]
        for field_name, field_info in fields.items():
            alias = field_info.get("as", field_name)
            select_fields.append(alias)

        fields_sql = ",\n    ".join(select_fields)

        # 构建查询语句
        sql_query = f'''
SELECT
    {fields_sql}
FROM {layer}.{config['Prefix']}_{config['Database']}_{table_name}_{sync_type}
WHERE p_date = '{{target_date}}';
        '''

        return sql_query

    def generate_check_update_ods_table_sql(self):
        config = self.config
        table_name = config['TableName']
        database = config["Database"]
        table = f"ods.ol_mysql_{database}_{table_name}_update"
        sql = self.generate_checking_sql(table)
        return sql

    def generate_check_history_ods_table_sql(self):
        config = self.config
        table_name = config['TableName']
        database = config["Database"]
        table = f"ods.ol_mysql_{database}_{table_name}_history"
        sql = self.generate_checking_sql(table)
        return sql

    def generate_check_dw_table_sql(self):
        config = self.config
        table = f"{config['layer']}.{config['Prefix']}_{config['Database']}_{config['TableName']}_{config['SyncType']}"
        sql = self.generate_checking_sql(table)
        return sql


    def generate_checking_sql(self, table_name):
        config = self.config

        today = datetime.now()
        yesterday = today - timedelta(days=1)
        target_date = yesterday.strftime('%Y-%m-%d')

        check_sql = f'''
        -- 验证表结构
        DESC {table_name};

        -- 检查分区是否正常
        SHOW PARTITIONS {table_name};

        -- 检查数据是否存在空值异常值
        SELECT *
        FROM {table_name}
        WHERE p_date={target_date}
        LIMIT 100;

        -- 检查数据是否存在重复值
        SELECT COUNT({config['PrimaryKey']}),
               COUNT(DISTINCT {config['PrimaryKey']})
        FROM {table_name}
        WHERE p_date={target_date};'''

        return check_sql















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
