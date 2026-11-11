# hangar_ingest 数据接入工作台

保留现有领域校验、持久化与操作入口。Python 3.11 标准库和 SQLite；运行时无需下载依赖。测试覆盖已有领域行为、错误路径和恢复。本地联调材料不会自动采集到业务库。

## 本地运行

    python3 -m compileall -q hangar_ingest
    python3 -m unittest discover -s tests -v
    python3 -m hangar_ingest --db :memory: demo --fixture fixtures/demo.json
    python3 -m synthetic_exchange.smoke

## 容器运行

    docker build -t local/hangar_ingest .
    docker run --rm --network none local/hangar_ingest

构建时准备基础镜像，正式运行不下载。run_checks.sh 是完整的测试、编译检查及实际产品演示入口，正常在 90 秒内结束。每次演示使用临时数据或内存库，不改写源样本。

## 异步供应商采集

`hangar_ingest.collections` 把申请、排队、分片拉取、核对、导入和水位推进连成一次可恢复的采集任务（`collection.*` 操作，CLI 子命令 `collect`）。

* 任务身份是 `(source, UTC 窗口, 配置版本)`，重试复用同一任务和供应商幂等键；只记得受理号时可用 `export_id` 找回供应商已受理但响应丢失的原任务。
* 清单按 revision 与每片长度、SHA256、记录数核对：同 revision 仅链接可换签（refresh），字节/摘要变化或出现新版清单都会阻断，任务失败且水位不前进。
* 缺片、摘要或记录数不符同样阻断水位；记录级质量问题走既有隔离流程，只计入完成说明。
* 暂停/重启保留已验分片字节，不重复下载；取消后迟到响应无法完成旧窗口，同身份也不能重新申请。
* `collection.status` 用 `stage` 区分卡在申请、排队、下载、校验还是导入，并展示剩余尝试与截止时间；总尝试数 `max_attempts` 与 `deadline` 同时生效。

## 采集联调

POST /exports 使用 Idempotency-Key: local-window，返回同一 exp1。GET /exports/exp1 先排队再返回清单。分片 b 第一次返回链接过期，POST /exports/exp1/refresh 获取相同内容的新交接信息。

    python3 -m synthetic_exchange.server --port 8765
    python3 -m hangar_ingest --db /tmp/hangar.sqlite3 collect col-demo \
        --source feed1 --start 2026-11-11T00:00:00Z --end 2026-11-12T00:00:00Z \
        --config-revision 1 --supplier-url http://127.0.0.1:8765 \
        --idempotency-key local-window --max-attempts 20 \
        --deadline 2026-11-13T00:00:00Z

原有操作参数可通过 python3 -m hangar_ingest --help 查询。
