import json

from MarkdownGenerator import MarkdownGenerator
from ScriptGenerator import ScriptGenerator

def main():
    md_gen = MarkdownGenerator()

    # 读取 JSON 配置文件
    with open('src/config.json', 'r') as f:
        config = json.load(f)

    script_gen = ScriptGenerator(config)

    # 添加标题
    md_gen.add_heading("hive2ch配置文档", 1)
    md_gen.add_paragraph("本脚本由python自动生成，使用时请配置好config文件。\t——v0.1")

    md_gen.add_heading("1 mysql2hive", 2)

    md_gen.add_heading("1.1 新增mysql2hive conf", 3)
    mysql2hive_conf = script_gen.generate_mysql2hive_conf(first=True)
    md_gen.add_code_block(mysql2hive_conf, "sql")
    md_gen.add_heading("1.2 新建history表的同步sql", 3)
    history_sql = script_gen.generate_insert_ods_table_sql()
    md_gen.add_code_block(history_sql, "sql")
    md_gen.add_heading("1.3 重跑update任务", 3)
    md_gen.add_paragraph("此时mairflow平台上mysql2hive Tag下会新增两个任务update任务和history任务，我们重跑upda"
                         "te任务以首次全量同步mysql数据至hive update表的对应分区。")
    md_gen.add_heading("1.4 检验mysql update表同步", 3)
    update_check_sql = script_gen.generate_check_update_ods_table_sql()
    md_gen.add_code_block(update_check_sql, "sql")
    md_gen.add_heading("1.5 创建history表", 3)
    history_create_table_sql = script_gen.generate_create_ods_history_table_sql()
    md_gen.add_code_block(history_create_table_sql, "sql")
    md_gen.add_heading("1.6 重跑history任务", 3)
    md_gen.add_paragraph("重跑history任务以同步update表数据至history表。")
    history_check_sql = script_gen.generate_check_history_ods_table_sql()
    md_gen.add_code_block(history_check_sql, "sql")
    md_gen.add_heading("1.7 将mysql2hive conf改为增量同步", 3)
    mysql2hive_conf = script_gen.generate_mysql2hive_conf(first=False)
    md_gen.add_code_block(mysql2hive_conf, "sql")

    md_gen.add_heading("2 hive_dw_daily", 2)
    md_gen.add_heading("2.1 创建hive_dw表", 3)
    dw_create_table_sql = script_gen.generate_dw_create_sql()
    md_gen.add_code_block(dw_create_table_sql, "sql")
    md_gen.add_heading("2.2 新增hive_dw同步sql", 3)
    hive_dw_insert_sql = script_gen.generate_insert_dw_table_sql()
    md_gen.add_code_block(hive_dw_insert_sql, "sql")
    md_gen.add_heading("2.3 新增hive_dw同步conf", 3)
    hive_dw_task_conf = script_gen.generate_dw_task_conf()
    md_gen.add_code_block(hive_dw_task_conf, "sql")
    md_gen.add_heading("2.4 重跑dw同步任务", 3)
    md_gen.add_paragraph("重跑dw以同步ods update表和history表数据至hive dw层。")
    md_gen.add_heading("2.5 数据验证", 3)
    de_check_sql = script_gen.generate_check_dw_table_sql()
    md_gen.add_code_block(de_check_sql, "sql")

    md_gen.add_heading("3 hive2ch", 2)

    md_gen.add_heading("3.1 新增hive2ch任务配置conf", 3)
    hive2ch_conf = script_gen.generate_hive2ch_task_conf()
    md_gen.add_code_block(hive2ch_conf, "sql")
    md_gen.add_heading("3.2 新建hive2ch 建表sql", 3)
    shard_table_sql, distributed_table_sql = script_gen.generate_ch_create_sql()
    md_gen.add_code_block(shard_table_sql, "sql")
    md_gen.add_code_block(distributed_table_sql, "sql")
    md_gen.add_heading("3.3 hive2ch insert sql", 3)
    hive2ch_insert_sql = script_gen.generate_ch_insert_sql()
    md_gen.add_code_block(hive2ch_insert_sql, "sql")











    # 添加段落
    # md_gen.add_paragraph("这是一个简单的 Python 脚本，用于生成 Markdown 格式的文件。")

    # 添加列表
    # md_gen.add_list(["项目 1", "项目 2", "项目 3"])

    # 添加有序列表
    # md_gen.add_list(["步骤 1", "步骤 2", "步骤 3"], ordered=True)

    # 添加代码块
    # code = """
    #     def hello_world():
    #         print("Hello, world!")
    #     """

    # 添加链接
    # md_gen.add_link("点击这里了解更多", "https://www.example.com")

    # 添加图片
    # md_gen.add_image("示例图片", "https://www.example.com/image.jpg")

    # 保存为文件
    md_gen.save_to_file("target/mysql2ch同步脚本.md")


if __name__ == '__main__':
    main()
