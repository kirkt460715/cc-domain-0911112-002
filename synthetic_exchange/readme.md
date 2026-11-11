# 本地协议服务

POST /exports 使用 Idempotency-Key: local-window，返回同一 exp1。GET /exports/exp1 先排队再返回清单。分片 b 第一次返回链接过期，POST /exports/exp1/refresh 获取相同内容的新交接信息。

场景是响应脚本，不包含采集客户端。可以复制场景文件并改变响应次序、状态码或样本内容来联调。服务只监听 loopback，数据均为合成。

    python3 -m synthetic_exchange.server --port 8765
    python3 -m synthetic_exchange.smoke
