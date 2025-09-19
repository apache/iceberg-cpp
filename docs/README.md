# Iceberg C++ 文档

这是 Iceberg C++ 项目的文档，使用 MkDocs 和 Material 主题构建。

## 本地开发

### 安装依赖

```bash
# 安装 MkDocs
brew install mkdocs

# 安装 Material 主题
pip3 install mkdocs-material
```

### 本地预览

```bash
# 启动本地服务器
mkdocs serve

# 在浏览器中打开 http://localhost:8000
```

### 构建文档

```bash
# 构建静态文件
mkdocs build

# 构建的文件会在 site/ 目录中
```

## 部署

### 自动部署

文档会自动部署到 GitHub Pages：

1. 推送代码到 `main` 分支
2. GitHub Actions 会自动构建和部署文档
3. 访问：https://your-username.github.io/iceberg-cpp

### 手动部署

```bash
# 使用部署脚本
./docs/deploy.sh
```

## 文档结构

```
docs/
├── index.md                    # 首页
├── configuration.md            # 配置指南
├── examples.md                 # 示例代码
├── getting-started/
│   ├── installation.md         # 安装指南
│   └── quickstart.md          # 快速开始
└── README.md                  # 本文档
```

## 配置

主要配置文件是 `mkdocs.yml`，包含：

- 站点信息
- 主题配置
- 导航结构
- Markdown 扩展

## 贡献

1. 编辑 `docs/` 目录中的 Markdown 文件
2. 本地预览更改：`mkdocs serve`
3. 提交并推送更改
4. 文档会自动更新

## 参考

- [MkDocs 文档](https://www.mkdocs.org/)
- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
- [PyIceberg 文档](https://py.iceberg.apache.org/) (设计参考)
