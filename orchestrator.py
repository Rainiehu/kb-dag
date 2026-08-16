#!/usr/bin/env python3
"""kb-dag: 以 KB 目录树为 DAG 的机械调度 orchestrator。

模型:
- md 文件 = 叶子节点, frontmatter `deps:` 声明对同层兄弟的依赖
- 文件夹 = 子图(黑盒复合节点), 可无限嵌套; 其同层依赖写在内部 index.md 的 frontmatter
- 依赖只能指向同层兄弟; 依赖 folder = 等其内部子图全部跑完
- 子图入参注入内部 source 节点(无 deps), 返回值 = 内部 sink 节点(无下游)产出合并
- 节点执行 = executor(prompt), prompt = 上游产出 + 自身正文; executor 可插拔
- KB 不可变, 产出写入 runs/<id>/ 镜像路径; 同 id 重跑时已有产出的节点跳过(断点续跑)
- 调度: ready-set 并发(asyncio); 失败 fail-fast, 等在飞节点结束后退出
"""

import argparse
import asyncio
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

INDEX = "index.md"
DAG_MD = "dag.md"  # 每层自动生成的拓扑图, 不是执行节点

# ---------- 解析 ----------

def parse_frontmatter_deps(text: str) -> tuple[list[str], str]:
    """极简 frontmatter 解析: 只识别 deps 的行内数组或块列表, 返回 (deps, 正文)。"""
    if not text.startswith("---"):
        return [], text
    end = text.find("\n---", 3)
    if end == -1:
        return [], text
    fm, body = text[3:end], text[end + 4:]
    deps: list[str] = []
    lines = fm.strip().splitlines()
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        m = re.match(r"deps:\s*\[(.*)\]\s*$", stripped)
        if m:
            deps = [d.strip().strip("'\"") for d in m.group(1).split(",") if d.strip()]
            break
        if stripped == "deps:":
            i += 1
            while i < len(lines) and (m2 := re.match(r"\s*-\s*(.+)", lines[i])):
                deps.append(m2.group(1).strip().strip("'\""))
                i += 1
            break
        i += 1
    return deps, body.lstrip("\n")


@dataclass
class Node:
    name: str                 # 兄弟间引用名: 文件为去 .md 的 stem, 文件夹为目录名
    path: Path
    deps: list[str]
    body: str = ""                                  # 文件节点正文
    children: Optional[dict[str, "Node"]] = None    # 文件夹节点的子节点表

    @property
    def is_graph(self) -> bool:
        return self.children is not None


def load_graph(dirpath: Path) -> Node:
    children: dict[str, Node] = {}
    folder_deps: list[str] = []
    for p in sorted(dirpath.iterdir()):
        if p.name.startswith((".", "_")) or p.name == DAG_MD:
            continue
        if p.name == INDEX:
            folder_deps, _ = parse_frontmatter_deps(p.read_text(encoding="utf-8"))
            continue
        if p.is_dir():
            node = load_graph(p)
        elif p.suffix == ".md":
            deps, body = parse_frontmatter_deps(p.read_text(encoding="utf-8"))
            node = Node(p.stem, p, deps, body=body)
        else:
            continue
        if node.name in children:
            sys.exit(f"错误: {dirpath} 下 '{node.name}' 同名冲突(文件与文件夹)")
        children[node.name] = node
    return Node(dirpath.name, dirpath, folder_deps, children=children)


def validate(graph: Node, prefix: str = "") -> None:
    """归一化 dep 名(去 .md), 检查悬空引用与环, 递归子图。"""
    kids = graph.children or {}
    for n in kids.values():
        n.deps = [d[:-3] if d.endswith(".md") else d for d in n.deps]
        for d in n.deps:
            if d not in kids:
                sys.exit(f"错误: {prefix}{n.name} 依赖了不存在的兄弟节点 '{d}'")
    color: dict[str, int] = {}

    def dfs(name: str, stack: list[str]) -> None:
        color[name] = 1
        for d in kids[name].deps:
            if color.get(d) == 1:
                sys.exit(f"错误: 依赖环 {prefix}: {' -> '.join(stack + [d])}")
            if color.get(d) != 2:
                dfs(d, stack + [d])
        color[name] = 2

    for name in kids:
        if color.get(name) != 2:
            dfs(name, [name])
    for n in kids.values():
        if n.is_graph:
            validate(n, prefix + n.name + "/")

# ---------- 拓扑图 ----------

def mermaid(graph: Node) -> str:
    """本层子图的 mermaid flowchart: 文件节点方框, 文件夹节点双框。"""
    kids = graph.children or {}
    lines = ["flowchart LR"]
    for n in kids.values():
        label = f'{n.name}[["{n.name}/"]]' if n.is_graph else f'{n.name}["{n.name}.md"]'
        lines.append(f"    {label}")
    for n in kids.values():
        for d in n.deps:
            lines.append(f"    {d} --> {n.name}")
    return "\n".join(lines)


def write_dag_md(graph: Node, kb_root: Path) -> None:
    """在每层文件夹里生成/刷新 dag.md, 递归子图。"""
    rel = graph.path.relative_to(kb_root)
    title = "." if str(rel) == "." else f"{rel}/"
    content = f"# {title} 层拓扑\n\n(由 orchestrator 自动生成, 请勿手改)\n\n```mermaid\n{mermaid(graph)}\n```\n"
    (graph.path / DAG_MD).write_text(content, encoding="utf-8")
    for n in (graph.children or {}).values():
        if n.is_graph:
            write_dag_md(n, kb_root)

# ---------- 执行 ----------

Inputs = list[tuple[str, str]]  # [(来源标签, 产出文本)]


@dataclass
class Ctx:
    kb_root: Path
    run_dir: Path
    executor: Callable  # async (prompt, rel_path) -> str


def log(depth: int, msg: str) -> None:
    print("  " * depth + msg, flush=True)


def build_prompt(body: str, inputs: Inputs) -> str:
    parts = []
    if inputs:
        sections = "\n\n".join(f"## {label}\n\n{text}" for label, text in inputs)
        parts.append(f"# 上游输入\n\n{sections}")
    parts.append(f"# 任务\n\n{body.strip()}")
    return "\n\n".join(parts)


def merge(pairs: Inputs) -> str:
    if len(pairs) == 1:
        return pairs[0][1]
    return "\n\n".join(f"## 来自 {name}\n\n{text}" for name, text in pairs)


async def run_node(node: Node, inputs: Inputs, ctx: Ctx, depth: int) -> str:
    rel = node.path.relative_to(ctx.kb_root)
    if node.is_graph:
        log(depth, f"▸ 进入子图 {rel}/")
        result = await run_graph(node, inputs, ctx, depth + 1)
        log(depth, f"◂ 子图完成 {rel}/")
        return result
    out_path = ctx.run_dir / rel
    if out_path.exists():
        log(depth, f"⏭ 跳过 {rel} (已有产出)")
        return out_path.read_text(encoding="utf-8")
    log(depth, f"▶ 执行 {rel}")
    output = await ctx.executor(build_prompt(node.body, inputs), str(rel))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(output, encoding="utf-8")
    log(depth, f"✓ 完成 {rel}")
    return output


async def run_graph(graph: Node, inputs: Inputs, ctx: Ctx, depth: int) -> str:
    """机械调度一层子图: ready-set 并发, fail-fast, 返回 sink 产出合并。"""
    kids = graph.children or {}
    has_dependent = {d for n in kids.values() for d in n.deps}
    outputs: dict[str, str] = {}
    pending = set(kids)
    running: dict[asyncio.Task, str] = {}

    while pending or running:
        ready = [n for n in pending if all(d in outputs for d in kids[n].deps)]
        for name in ready:
            pending.discard(name)
            node = kids[name]
            node_inputs = [(d, outputs[d]) for d in node.deps] if node.deps else inputs
            task = asyncio.create_task(run_node(node, node_inputs, ctx, depth))
            running[task] = name
        if not running:
            break
        done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            name = running.pop(task)
            try:
                outputs[name] = task.result()
            except Exception:
                if running:  # fail-fast: 不再调度新节点, 等在飞任务结束后上抛
                    await asyncio.wait(running)
                raise
    sinks = [(n, outputs[n]) for n in kids if n not in has_dependent]
    return merge(sinks) if sinks else ""

# ---------- executor ----------

async def stub_executor(prompt: str, rel: str) -> str:
    await asyncio.sleep(0.3)  # 模拟耗时, 便于观察并发
    return f"[stub] {rel} 的模拟产出 (prompt 共 {len(prompt)} 字符)"


async def claude_executor(prompt: str, rel: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate(prompt.encode("utf-8"))
    if proc.returncode != 0:
        raise RuntimeError(f"claude 执行 {rel} 失败: {err.decode('utf-8', 'replace')[:500]}")
    return out.decode("utf-8")

# ---------- 入口 ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="kb-dag orchestrator")
    ap.add_argument("kb", type=Path, help="KB 根目录")
    ap.add_argument("--run", help="run id; 复用同一 id 可断点续跑 (默认: 时间戳)")
    ap.add_argument("--real", action="store_true", help="真实调用 claude -p (默认 stub)")
    args = ap.parse_args()

    run_id = args.run or datetime.now().strftime("%Y%m%d-%H%M%S")
    kb_root = args.kb.resolve()
    if not kb_root.is_dir():
        sys.exit(f"错误: {kb_root} 不是目录")
    root = load_graph(kb_root)
    root.deps = []  # 根目录自身无同层依赖
    validate(root)
    write_dag_md(root, kb_root)

    ctx = Ctx(
        kb_root=kb_root,
        run_dir=Path("runs") / run_id,
        executor=claude_executor if args.real else stub_executor,
    )
    print(f"kb: {kb_root}\nrun: {ctx.run_dir}  executor: {'claude -p' if args.real else 'stub'}\n")
    result = asyncio.run(run_graph(root, [], ctx, 0))
    print(f"\n=== 最终产出 (根图 sink) ===\n\n{result}")


if __name__ == "__main__":
    main()
