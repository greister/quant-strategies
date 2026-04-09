#!/usr/bin/env python3
"""
将策略文档同步到思源笔记 (SiYuan Note)
通过思源笔记 HTTP API (默认端口 6806)

用法:
  python3 sync_to_siyuan.py              # 批量同步所有文档
  python3 sync_to_siyuan.py --rebuild    # 重建文档结构（删除后重新创建）

文档结构:
  00-📋-量化策略文档中心    # 主索引/导航
  01-📊-策略仓库总览        # 项目总览
  02-🤖-Agent开发指南      # AI协作规范
  11-📖-策略概述           # 独立强度因子-策略说明
  12-🔄-多因子组合说明     # 独立强度因子-组合配置
  13-📘-开发手册          # 独立强度因子-开发手册
  14-📝-Claude专用指南    # 独立强度因子-AI协作
"""

import sys
import json
import requests
import argparse
from pathlib import Path
from datetime import datetime


SIYUAN_API = "http://127.0.0.1:6806"
NOTEBOOK_ID = "20260205163314-5bk48fr"  # 量化策略笔记本


def create_doc_with_md(notebook, path, markdown):
    """创建 Markdown 文档"""
    payload = {
        "notebook": notebook,
        "path": path,
        "markdown": markdown
    }
    try:
        resp = requests.post(
            f"{SIYUAN_API}/api/filetree/createDocWithMd",
            json=payload,
            timeout=15
        )
        return resp.json()
    except Exception as e:
        return {"code": -1, "msg": str(e)}


def rename_doc_title(notebook, path, title):
    """重命名文档标题"""
    payload = {
        "notebook": notebook,
        "path": path,
        "title": title
    }
    try:
        requests.post(
            f"{SIYUAN_API}/api/filetree/renameDoc",
            json=payload,
            timeout=5
        )
    except:
        pass


def remove_doc(notebook, path):
    """删除文档"""
    try:
        for ext in ["", ".sy"]:
            full_path = path if path.endswith(".sy") else path + ext
            requests.post(
                f"{SIYUAN_API}/api/filetree/removeDoc",
                json={"notebook": notebook, "path": full_path},
                timeout=5
            )
    except:
        pass


def check_api():
    """检查思源笔记 API 是否可用"""
    try:
        resp = requests.get(f"{SIYUAN_API}/api/system/version", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("code") == 0
    except:
        pass
    return False


def generate_index_content():
    """生成索引文档内容"""
    return """# 📊 量化策略开发文档中心

> 🎯 本文档是量化交易策略的知识库，沉淀策略设计、开发规范与实践经验

---

## 📚 文档目录

### 总览
- [[01-📊-策略仓库总览]] - 项目整体架构与快速开始
- [[02-🤖-Agent开发指南]] - AI Agent 协作开发规范

### 独立强度因子策略
- [[11-📖-策略概述]] - 策略原理与计分规则
- [[12-🔄-多因子组合说明]] - 多因子组合配置
- [[13-📘-开发手册]] - 数据库配置与代码规范
- [[14-📝-Claude专用指南]] - Claude Code 使用指南

---

## 🚀 快速开始

```bash
# 执行所有策略
./scripts/run_all_strategies.sh [YYYY-MM-DD]

# 生成报告
python3 ./scripts/generate_report.py [YYYY-MM-DD]
```

---

*本文档由 sync_to_siyuan.py 自动生成*
*生成时间: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """*
"""


def sync_docs(rebuild=False):
    """
    同步策略文档到思源笔记
    
    Args:
        rebuild: 如果为 True，则删除现有文档后重新创建
    """
    if not check_api():
        print("❌ 无法连接到思源笔记 API")
        print("   请确保思源笔记正在运行且已开启 HTTP API (端口 6806)")
        return False
    
    # 确定基础路径
    script_dir = Path(__file__).parent.resolve()
    if "01.independence-score" in str(script_dir):
        base_dir = script_dir.parent.parent
    else:
        base_dir = script_dir.parent
    
    print(f"📁 项目目录: {base_dir}")
    print(f"📓 目标笔记本: {NOTEBOOK_ID}\n")
    
    # 文档配置: (路径, 标题, 本地文件或内容)
    docs = [
        {
            "path": "/00-📋-量化策略文档中心",
            "title": "📋 量化策略文档中心",
            "content": generate_index_content(),
            "is_generated": True
        },
        {
            "path": "/01-📊-策略仓库总览",
            "title": "📊 策略仓库总览",
            "file": base_dir / "README.md"
        },
        {
            "path": "/02-🤖-Agent开发指南",
            "title": "🤖 Agent 开发指南",
            "file": base_dir / "AGENTS.md"
        },
        {
            "path": "/11-📖-策略概述",
            "title": "📖 策略概述",
            "file": base_dir / "01.independence-score" / "README.md"
        },
        {
            "path": "/12-🔄-多因子组合说明",
            "title": "🔄 多因子组合说明",
            "file": base_dir / "01.independence-score" / "docs" / "多因子组合说明.md"
        },
        {
            "path": "/13-📘-开发手册",
            "title": "📘 开发手册",
            "file": base_dir / "01.independence-score" / "AGENTS.md"
        },
        {
            "path": "/14-📝-Claude专用指南",
            "title": "📝 Claude 专用指南",
            "file": base_dir / "01.independence-score" / "CLAUDE.md"
        },
    ]
    
    # 如需重建，先删除旧文档
    if rebuild:
        print("🗑️  清理旧文档...\n")
        for doc in docs:
            remove_doc(NOTEBOOK_ID, doc["path"])
            print(f"  🗑️  {doc['title']}")
        print()
    
    # 创建/更新文档
    print("📝 同步文档...\n")
    success_count = 0
    
    for doc in docs:
        # 获取内容
        if doc.get("is_generated"):
            content = doc["content"]
        elif doc.get("file") and doc["file"].exists():
            content = doc["file"].read_text(encoding='utf-8')
        else:
            print(f"⚠️  {doc['title']}: 文件不存在")
            continue
        
        # 创建文档
        result = create_doc_with_md(NOTEBOOK_ID, doc["path"], content)
        
        if result.get("code") == 0:
            # 设置标题
            rename_doc_title(NOTEBOOK_ID, doc["path"] + ".sy", doc["title"])
            print(f"✅ {doc['title']}")
            success_count += 1
        else:
            print(f"❌ {doc['title']}: {result.get('msg', '未知错误')}")
    
    print(f"\n{'='*60}")
    print(f"✅ 同步完成: {success_count}/{len(docs)} 个文档")
    print(f"📖 入口文档: 00-📋-量化策略文档中心")
    print(f"{'='*60}")
    
    return success_count == len(docs)


def main():
    parser = argparse.ArgumentParser(description='同步策略文档到思源笔记')
    parser.add_argument('--rebuild', action='store_true', help='重建文档结构（删除后重新创建）')
    
    args = parser.parse_args()
    
    success = sync_docs(rebuild=args.rebuild)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
