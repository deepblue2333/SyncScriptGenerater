class MarkdownGenerator:
    def __init__(self):
        self.content = ""

    def add_heading(self, text, level=1):
        self.content += f"{'#' * level} {text}\n\n"

    def add_paragraph(self, text):
        self.content += f"{text}\n\n"

    def add_list(self, items, ordered=False):
        for i, item in enumerate(items):
            if ordered:
                self.content += f"{i + 1}. {item}\n"
            else:
                self.content += f"- {item}\n"
        self.content += "\n"

    def add_code_block(self, code, language=""):
        self.content += f"```{language}\n{code}\n```\n\n"

    def add_link(self, text, url):
        self.content += f"[{text}]({url})\n\n"

    def add_image(self, alt_text, url):
        self.content += f"![{alt_text}]({url})\n\n"

    def save_to_file(self, filename):
        with open(filename, 'w') as f:
            f.write(self.content)
        print(f"Markdown file '{filename}' has been generated.")


# 示例用法
if __name__ == "__main__":
    md_gen = MarkdownGenerator()

    # 添加标题
    md_gen.add_heading("Markdown 生成器", 1)
    md_gen.add_heading("介绍", 2)

    # 添加段落
    md_gen.add_paragraph("这是一个简单的 Python 脚本，用于生成 Markdown 格式的文件。")

    # 添加列表
    md_gen.add_list(["项目 1", "项目 2", "项目 3"])

    # 添加有序列表
    md_gen.add_list(["步骤 1", "步骤 2", "步骤 3"], ordered=True)

    # 添加代码块
    code = """
    def hello_world():
        print("Hello, world!")
    """
    md_gen.add_code_block(code, "python")

    # 添加链接
    md_gen.add_link("点击这里了解更多", "https://www.example.com")

    # 添加图片
    md_gen.add_image("示例图片", "https://www.example.com/image.jpg")

    # 保存为文件
    md_gen.save_to_file("README.md")
