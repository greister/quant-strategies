#!/usr/bin/env python3
"""
将策略文档同步到思源笔记 (SiYuan Note)
通过思源笔记 HTTP API (默认端口 6806)

用法:
  python3 sync_to_siyuan.py <文档路径> [文档标题]
  python3 sync_to_siyuan.py README.md "独立强度因子策略说明"
"""

import sys
import json
import re
import requests
from pathlib import Path
from datetime import datetime


SIYUAN_API = "http://127.0.0.1:6806"
STRATEGY_BOX_ID = "20260205163314-5bk48fr"  # 策略笔记本 ID


def markdown_to_sy(md_content: str, title: str) -> dict:
    """
    将 Markdown 转换为思源笔记格式 (KMDF - Kernel Markdown Format)
    简化版：将 Markdown 按段落/标题分割为 blocks
    """
    blocks = []
    lines = md_content.split('\n')
    current_block = {"type": "paragraph", "content": ""}
    
    for line in lines:
        # 标题
        if line.startswith('# '):
            if current_block["content"]:
                blocks.append(current_block)
            blocks.append({
                "type": "h1",
                "content": line[2:].strip()
            })
            current_block = {"type": "paragraph", "content": ""}
        elif line.startswith('## '):
            if current_block["content"]:
                blocks.append(current_block)
            blocks.append({
                "type": "h2", 
                "content": line[3:].strip()
            })
            current_block = {"type": "paragraph", "content": ""}
        elif line.startswith('### '):
            if current_block["content"]:
                blocks.append(current_block)
            blocks.append({
                "type": "h3",
                "content": line[4:].strip()
            })
            current_block = {"type": "paragraph", "content": ""}
        # 代码块
        elif line.startswith('```'):
            if current_block["content"]:
                blocks.append(current_block)
            # 简化处理，实际应该收集到结束标记
            blocks.append({
                "type": "code",
                "content": line
            })
            current_block = {"type": "paragraph", "content": ""}
        # 列表
        elif line.strip().startswith('- ') or line.strip().startswith('* '):
            if current_block["content"]:
                blocks.append(current_block)
            blocks.append({
                "type": "list",
                "content": line.strip()[2:]
            })
            current_block = {"type": "paragraph", "content": ""}
        # 表格分隔线跳过
        elif line.strip().startswith('|---'):
            continue
        # 普通段落
        else:
            if line.strip():
                current_block["content"] += line + "\n"
    
    # 添加最后一个 block
    if current_block["content"]:
        blocks.append(current_block)
    
    return {
        "title": title,
        "blocks": blocks
    }


def create_siyuan_doc(box_id: str, path: str, title: str, md_content: str) -> bool:
    """
    通过思源笔记 API 创建文档
    使用 createDocWithMd API 直接传入 Markdown 内容
    """
    try:
        # 使用 createDocWithMd API
        create_payload = {
            "notebook": box_id,
            "path": path,
            "markdown": md_content  # 直接传入 Markdown 内容
        }
        
        resp = requests.post(
            f"{SIYUAN_API}/api/filetree/createDocWithMd",
            json=create_payload,
            timeout=10
        )
        
        if resp.status_code == 200:
            result = resp.json()
            if result.get("code") == 0:
                doc_id = result["data"]
                # 重命名文档（设置标题）
                if doc_id:
                    rename_payload = {
                        "notebook": box_id,
                        "path": path + ".sy",
                        "title": title
                    }
                    requests.post(
                        f"{SIYUAN_API}/api/filetree/renameDoc",
                        json=rename_payload,
                        timeout=5
                    )
                print(f"✅ 文档已创建: {title}")
                return True
            else:
                print(f"⚠️ API 返回错误: {result.get('msg')}")
                return False
        else:
            print(f"❌ HTTP 错误: {resp.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"❌ 无法连接到思源笔记 API (http://127.0.0.1:6806)")
        print("   请确保思源笔记正在运行且已开启 HTTP API")
        return False
    except Exception as e:
        print(f"❌ 错误: {e}")
        return False


def sync_docs_to_siyuan():
    """
    同步策略开发文档到思源笔记
    """
    # 当前在 worktree 中，根据当前位置确定基础路径
    script_dir = Path(__file__).parent.resolve()
    
    # 如果在 01.independence-score/scripts/ 下
    if "01.independence-score" in str(script_dir):
        base_dir = script_dir.parent.parent  # worktree 根目录
        strategy_dir = script_dir.parent  # 01.independence-score
    else:
        base_dir = script_dir.parent
        strategy_dir = base_dir / "01.independence-score"
    
    print(f"📁 基础目录: {base_dir}")
    print(f"📁 策略目录: {strategy_dir}")
    print()
    
    # 要同步的文档列表
    docs_to_sync = [
        # 根级别文档 (worktree 根)
        (base_dir / "README.md", "📊 策略仓库总览"),
        (base_dir / "AGENTS.md", "🤖 Agent 开发指南"),
        
        # 独立强度因子策略
        (strategy_dir / "README.md", "📈 独立强度因子策略"),
        (strategy_dir / "CLAUDE.md", "📝 Claude 专用指南"),
        (strategy_dir / "AGENTS.md", "🔧 独立强度因子-开发指南"),
        (strategy_dir / "docs" / "多因子组合说明.md", "🔄 多因子组合说明"),
    ]
    
    print("=" * 60)
    print("🔄 同步策略文档到思源笔记")
    print(f"📁 目标笔记本: {STRATEGY_BOX_ID}")
    print("=" * 60)
    print()
    
    success_count = 0
    for full_path, title in docs_to_sync:
        if full_path.exists():
            print(f"📄 同步: {title}")
            md_content = full_path.read_text(encoding='utf-8')
            
            # 构建思源笔记路径
            relative_path = full_path.relative_to(base_dir)
            siyuan_path = "/" + str(relative_path).replace(".md", "").replace("/", "-")
            
            if create_siyuan_doc(STRATEGY_BOX_ID, siyuan_path, title, md_content):
                success_count += 1
            print()
        else:
            print(f"⚠️ 文件不存在: {full_path}")
            print()
    
    print("=" * 60)
    print(f"✅ 同步完成: {success_count}/{len(docs_to_sync)} 个文档")
    print("=" * 60)
    return success_count > 0


def main():
    if len(sys.argv) > 1:
        # 单文件模式
        doc_path = sys.argv[1]
        title = sys.argv[2] if len(sys.argv) > 2 else Path(doc_path).stem
        
        full_path = Path(doc_path)
        if not full_path.exists():
            print(f"❌ 文件不存在: {doc_path}")
            sys.exit(1)
        
        md_content = full_path.read_text(encoding='utf-8')
        siyuan_path = "/" + title.replace(" ", "-").replace("/", "-")
        
        success = create_siyuan_doc(STRATEGY_BOX_ID, siyuan_path, title, md_content)
        sys.exit(0 if success else 1)
    else:
        # 批量同步模式
        success = sync_docs_to_siyuan()
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
