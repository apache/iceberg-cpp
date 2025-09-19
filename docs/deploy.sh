#!/bin/bash

# 部署文档到 GitHub Pages 的脚本

echo "🚀 开始构建和部署文档..."

# 检查是否安装了 mkdocs
if ! command -v mkdocs &> /dev/null; then
    echo "❌ 未找到 mkdocs，请先安装："
    echo "   brew install mkdocs"
    echo "   pip3 install mkdocs-material"
    exit 1
fi

# 构建文档
echo "📚 构建文档..."
mkdocs build --clean

if [ $? -eq 0 ]; then
    echo "✅ 文档构建成功！"
    echo "📁 构建的文件在 site/ 目录中"
    echo ""
    echo "🌐 本地预览："
    echo "   mkdocs serve"
    echo ""
    echo "🚀 部署到 GitHub Pages："
    echo "   1. 提交所有更改："
    echo "      git add ."
    echo "      git commit -m 'Update documentation'"
    echo "      git push"
    echo ""
    echo "   2. GitHub Actions 会自动部署到 GitHub Pages"
    echo "   3. 访问：https://your-username.github.io/iceberg-cpp"
else
    echo "❌ 文档构建失败！"
    exit 1
fi
