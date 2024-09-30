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
    md_gen.add_heading("hive_dw建表语句", 2)

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

    dw_create_sql = script_gen.generate_dw_create_sql()
    md_gen.add_code_block(dw_create_sql, "sql")

    # 添加链接
    # md_gen.add_link("点击这里了解更多", "https://www.example.com")

    # 添加图片
    # md_gen.add_image("示例图片", "https://www.example.com/image.jpg")

    # 保存为文件
    md_gen.save_to_file("同步脚本.md")


if __name__ == '__main__':
    main()
