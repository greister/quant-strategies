# Git Worktree 工作流指南

> 为每个策略创建独立工作目录，实现并行开发

---

## 📁 Worktree 结构

```
~/scripts/
├── 40.strategies/                    # 主仓库 (master)
│   ├── 00.shared/
│   ├── 01.independence-score/
│   ├── 02.momentum-factor/
│   ├── 03.low-beta-hybrid/
│   └── 04.combined-factor/
│
└── 40.strategies-worktrees/          # Worktree 目录
    ├── independence-score/           # feature/independence-score 分支
    ├── momentum-factor/              # feature/momentum-factor 分支
    ├── low-beta-hybrid/              # feature/low-beta-hybrid 分支
    └── combined-factor/              # feature/combined-factor 分支
```

---

## 🚀 快速开始

### 1. 切换到策略工作目录

```bash
# 独立强度策略
cd ~/scripts/40.strategies-worktrees/independence-score

# 动量因子策略
cd ~/scripts/40.strategies-worktrees/momentum-factor

# 低贝塔混合策略
cd ~/scripts/40.strategies-worktrees/low-beta-hybrid

# 综合因子策略
cd ~/scripts/40.strategies-worktrees/combined-factor
```

### 2. 查看当前分支

```bash
git branch
# 输出: * feature/independence-score
```

---

## 📝 工作流程

### 开发新功能

```bash
# 1. 进入对应策略的 worktree
cd ~/scripts/40.strategies-worktrees/independence-score

# 2. 创建功能分支（基于策略分支）
git checkout -b feature/independence-score-backtest

# 3. 开发... 修改文件

# 4. 提交到当前 worktree
git add .
git commit -m "feat: add backtest for independence score"

# 5. 推送到远程
git push origin feature/independence-score-backtest
```

### 同步主仓库更新

```bash
# 在主仓库获取更新
cd ~/scripts/40.strategies
git pull origin master

# 在各个 worktree 同步
cd ~/scripts/40.strategies-worktrees/independence-score
git rebase master
```

---

## 🔧 常用命令

```bash
# 查看所有 worktree
git worktree list

# 创建新的 worktree
git worktree add ../40.strategies-worktrees/new-strategy feature/new-strategy

# 删除 worktree
git worktree remove ../40.strategies-worktrees/old-strategy

# 清理已删除的 worktree 引用
git worktree prune
```

---

## ⚠️ 注意事项

1. **不要在多个 worktree 同时修改同一文件** - 会导致冲突
2. **共享的 `00.shared/` 修改建议在主仓库进行**
3. **各 worktree 是同一个 Git 仓库的不同视图** - 提交会共享
4. **Worktree 不能嵌套** - 必须在主仓库之外

---

## 🎯 推荐工作模式

| 场景 | 推荐工作目录 |
|------|-------------|
| 修改全局配置/共享脚本 | `40.strategies/` (master) |
| 开发独立强度策略 | `40.strategies-worktrees/independence-score/` |
| 开发动量因子策略 | `40.strategies-worktrees/momentum-factor/` |
| 同时开发多个策略 | 多个终端，各用一个 worktree |

---

## 📚 参考

- [Git Worktree 文档](https://git-scm.com/docs/git-worktree)
- 主项目文档: [README.md](./README.md)
