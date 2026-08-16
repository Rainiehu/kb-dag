# kb-dag

以 KB 目录树为 DAG 的机械调度框架（最小 demo）。

## 模型

| 概念 | 载体 |
|---|---|
| 叶子节点 | md 文件，正文即任务描述（prompt） |
| 子图（复合节点） | 文件夹，可无限嵌套 |
| 边 | frontmatter `deps: [...]`，**只能指向同层兄弟**（文件名可省略 `.md`） |
| 文件夹自身的 deps | 文件夹内可选的 `index.md` 的 frontmatter（`index.md` 不是执行节点） |
| 本层拓扑图 | 每层自动生成的 `dag.md`（mermaid），每次运行时刷新，不是执行节点 |

**黑盒递归**：依赖一个文件夹 = 等它内部整个子图跑完。orchestrator 递归调度子图。

**source/sink 对称数据流**：

- 节点执行 = `executor(prompt)`，prompt = 各 dep 的产出 + 自身正文
- 子图入参：注入给内部无 deps 的节点（source）
- 子图返回值：内部无下游的节点（sink）产出的合并

**状态**：KB 本身不可变。产出写入 `runs/<id>/` 下的镜像路径。

## 调度

- ready-set 并发（asyncio）：每层子图中所有就绪节点同时执行
- fail-fast：任一节点失败即停止调度新节点，等在飞节点结束后报错退出
- 断点续跑：同一 `--run` id 重跑时，已有产出的节点直接跳过
- 加载时做环检测与悬空依赖检测

## 用法

```sh
python3 orchestrator.py example-kb --run demo          # stub 执行器（零成本验证调度）
python3 orchestrator.py example-kb --run demo          # 同 id 重跑 = 断点续跑
python3 orchestrator.py example-kb --run r1 --real     # 真实调用 claude -p
```

## 示例 KB 结构

```
example-kb/
  brief.md                      source
  research/                     子图, deps: [brief]（写在其 index.md）
    market.md                   source ─┐ 与 users 并发
    users.md                    source ─┘
    competitors/                嵌套子图, deps: [market]
      list.md                   source
      analyze.md                deps: [list] → sink
    synthesis.md                deps: [market, users, competitors] → sink
  report.md                     deps: [research, brief] → 根图 sink
```
