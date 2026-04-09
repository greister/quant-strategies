#!/usr/bin/env python3
"""
将报告同步到 Obsidian Vault
用法: python3 sync_report_to_vault.py <报告文件路径> [子目录]
"""

import sys
from pathlib import Path
from datetime import datetime
import shutil


def sync_to_vault(report_path: str, subdir: str = ""):
    """
    将报告同步到 Obsidian Vault
    
    Args:
        report_path: 报告文件路径
        subdir: Vault 中的子目录（可选）
    """
    report_file = Path(report_path)
    if not report_file.exists():
        print(f"❌ 报告文件不存在: {report_path}")
        return False
    
    # Obsidian Vault 路径
    vault_dir = Path('/mnt/d/obsidian/OrbitOS-vault')
    if not vault_dir.exists():
        print(f"⚠️ Obsidian Vault 路径不存在: {vault_dir}")
        return False
    
    # 目标目录
    target_dir = vault_dir / '30_Research' / '量化分析' / '策略执行结果' / '01-独立强度因子'
    if subdir:
        target_dir = target_dir / subdir
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # 复制文件
    target_file = target_dir / report_file.name
    shutil.copy2(report_file, target_file)
    
    print(f"✅ 报告已同步到 Obsidian Vault: {target_file}")
    return True


def main():
    if len(sys.argv) < 2:
        print("用法: python3 sync_report_to_vault.py <报告文件路径> [子目录]")
        print("示例:")
        print("  python3 sync_report_to_vault.py results/backtest_report_20250409.md")
        print("  python3 sync_report_to_vault.py results/analysis.md 回测分析")
        sys.exit(1)
    
    report_path = sys.argv[1]
    subdir = sys.argv[2] if len(sys.argv) > 2 else ""
    
    success = sync_to_vault(report_path, subdir)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
