import unittest

from src.main import *

class TestGenerateSQL(unittest.TestCase):

    def setUp(self):
        # 设置初始的 JSON 配置作为测试数据
        self.maxDiff = None  # 允许显示更长的差异
        self.config_json = '''
        {
            "TableName": "headhunting_consultant_job_candidate_act_log",
            "Comment": "AI猎头-候选人行为记录当日明细表",
            "DbType": "mysql",
            "Database": "jobs",
            "Fields": {
                "id": {
                    "type": "bigint unsigned",
                    "comment": "主键id",
                    "as": "log_id"
                },
                "uid": {
                    "type": "bigint",
                    "comment": "uid"
                },
                "job_req_id": {
                    "type": "bigint",
                    "comment": "ai_ht_job_requirements的id"
                },
                "candidate_uid": {
                    "type": "bigint",
                    "comment": "候选人的uid"
                },
                "act_name": {
                    "type": "varchar",
                    "comment": "行为名称"
                },
                "act_uid": {
                    "type": "bigint",
                    "comment": "执行行为的uid"
                },
                "extra": {
                    "type": "text",
                    "comment": "其他信息"
                },
                "crtime": {
                    "type": "timestamp",
                    "comment": "创建时间",
                    "as": "create_ts"
                },
                "uptime": {
                    "type": "timestamp",
                    "comment": "更新时间",
                    "as": "update_ts"
                }
            },
            "layer": "dwd",
            "Prefix": "dw",
            "SyncType": "da",
            "Host": "companydbs2"
        }'''

        self.dw_create_expected_sql = (
            '''CREATE EXTERNAL TABLE `dwd.dw_jobs_headhunting_consultant_job_candidate_act_log_da`(
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
TBLPROPERTIES ('orc.compression'='snappy');'''
        )

        self.mysql2hive_expected_conf = (
            '''"ol_mysql_jobs_headhunting_consultant_job_candidate_act_log": {
    "update_field": "uptime",
    "is_increment": true,
    "interval": "daily",
    "db": "jobs",
    "host": "companydbs2",
    "condition": "1 = 1",
    "table": "headhunting_consultant_job_candidate_act_log",
    "overwrite": false,
    "schema": "id, uid, job_req_id, candidate_uid, act_name, act_uid, extra, crtime, uptime"
}'''
        )

    def test_generate_dw_create_sql(self):
        # 解析 JSON 配置
        config = json.loads(self.config_json)
        # 调用 generate_sql 函数生成 SQL 语句
        generated_sql = generate_dw_create_sql(config)

        expected_sql = self.dw_create_expected_sql.replace('\t', '\\t').strip()
        # 验证生成的 SQL 是否与预期相同
        self.assertEqual(generated_sql.strip(), expected_sql)


    def test_generate_mysql2hive_conf(self):
        # 解析 JSON 配置
        config = json.loads(self.config_json)
        # 调用 generate_sql 函数生成 SQL 语句
        generated_sql = generate_mysql2hive_conf(config)

        expected_sql = self.mysql2hive_expected_conf.replace('\t', '\\t').strip()
        # 验证生成的 SQL 是否与预期相同
        self.assertEqual(generated_sql.strip(), expected_sql)




# 运行测试
if __name__ == '__main__':
    unittest.main()