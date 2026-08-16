# . 层拓扑

(由 orchestrator 自动生成, 请勿手改)

```mermaid
flowchart LR
    brief["brief.md"]
    report["report.md"]
    research[["research/"]]
    research --> report
    brief --> report
    brief --> research
```
