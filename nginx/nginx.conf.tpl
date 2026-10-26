# LLM Gateway 的 nginx 负载均衡配置模板
#
# 由 scripts/nginx.sh 渲染成 $NGINX_HOME/conf/nginx.conf 后启动（替换逻辑见该脚本的
# render_config：监听端口、上游 server 列表、日志目录、pid 与临时目录等占位符）。
#
# 每一条关键指令旁边的注释就是「为什么这么写」，可直接抄到别的项目：
#   1. upstream least_conn：LLM 请求耗时长（几秒~几十秒），按「当前连接数最少」分发
#      比轮询更均衡，避免慢请求全压在一个实例上
#   2. max_fails / fail_timeout：被动健康检查，实例挂掉后自动摘除，恢复后自动加回
#   3. proxy_buffering off：SSE 流式必须关！开启时 nginx 会攒够缓冲才下发，
#      用户看到的「逐字输出」会变成「等几秒后一次性吐出」
#   4. proxy_http_version 1.1 + Connection ""：与上游复用长连接（配合 upstream keepalive）
#   5. proxy_read_timeout 300s：LLM 生成慢，别用默认 60s 把长回答掐断
#   6. proxy_connect_timeout 2s：实例已死时快速失败，把请求交给下一个上游

worker_processes auto;                      # 负载均衡本身不是瓶颈，按 CPU 核数开
error_log  __LOG_DIR__/nginx-error.log warn;
pid        __PID_FILE__;

events {
    worker_connections 1024;
}

http {
    # 免 root 解包的 nginx deb 里没有 conf/mime.types，所以不 include，
    # 只登记真正会用到的类型；上游返回的 Content-Type 会原样透传，缺省时才用 default_type。
    default_type application/json;
    types {
        text/html         html;
        text/plain        txt;
        application/json  json;
        text/event-stream sse;
    }

    # 临时文件全部落在 $NGINX_HOME 下，非 root 也能跑
    client_body_temp_path __TMP_DIR__/client_body;
    proxy_temp_path       __TMP_DIR__/proxy;
    fastcgi_temp_path     __TMP_DIR__/fastcgi;
    uwsgi_temp_path       __TMP_DIR__/uwsgi;
    scgi_temp_path        __TMP_DIR__/scgi;

    # 访问日志：JSON 格式，关键是记录 upstream_addr / upstream_http_x_gateway_instance，
    # 一眼就能看出「哪个请求被分到了哪个实例」，排障与验证负载均衡都靠它
    log_format gw escape=json
        '{'
        '"time":"$time_iso8601",'
        '"remote_addr":"$remote_addr",'
        '"method":"$request_method",'
        '"uri":"$uri",'
        '"status":$status,'
        '"request_time":$request_time,'
        '"upstream_addr":"$upstream_addr",'
        '"upstream_status":"$upstream_status",'
        '"upstream_time":"$upstream_response_time",'
        '"instance":"$upstream_http_x_gateway_instance",'
        '"content_type":"$sent_http_content_type",'
        '"api_key":"$http_x_api_key"'
        '}';

    upstream llm_gateway {
        zone llm_gateway 64k;               # 多 worker 共享健康状态与连接数（least_conn 依赖它）
        least_conn;                         # 按当前连接数最少分发：LLM 单请求耗时长，
                                            # 轮询容易把新请求丢给「已经压着几个慢请求」的实例
        __UPSTREAM_SERVERS__
        keepalive 32;                       # 与上游保持 32 个空闲长连接，省掉握手开销
    }

    server {
        listen      __LB_PORT__;
        server_name _;

        access_log  __LOG_DIR__/nginx-access.log gw;

        # 长上下文请求体可能超过默认的 1m
        client_max_body_size 8m;

        # LB 自身健康检查（不转发给网关，nginx 活着就能回 200）
        location = /lb-health {
            access_log off;
            default_type text/plain;
            return 200 "nginx lb ok\n";
        }

        # /metrics 必须「直连每个实例」抓取：走 LB 的话 Prometheus 拿不到是哪个实例的指标，
        # instance 标签会全部变成 LB 的地址，多实例曲线就分不开了。这里显式拒绝以免误用。
        location = /metrics {
            default_type text/plain;
            return 404 "metrics must be scraped per-instance directly (see GATEWAY_TARGETS), not via the load balancer\n";
        }

        location / {
            proxy_pass http://llm_gateway;

            # 与上游复用长连接（需配合 upstream keepalive；清空 Connection 头才能复用）
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host              $host;
            proxy_set_header X-Real-IP         $remote_addr;
            proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # —— 流式（SSE）三件套：不缓冲、不缓存、不攒请求体 ——
            proxy_buffering off;
            proxy_cache off;
            proxy_request_buffering off;

            # —— 超时：连接要快失败，读取要能等 ——
            proxy_connect_timeout 2s;
            proxy_send_timeout    300s;
            proxy_read_timeout    300s;
            send_timeout          300s;

            # 被动健康检查的「换机」条件：连接错误 / 超时。
            # POST 是非幂等请求，nginx 默认不会把「已发送出去」的请求重试到下一个上游，
            # 因此不会出现「一次提问被两个实例各生成一遍」；确实是连接阶段失败才会换机。
            # （若业务可接受重复，可加 non_idempotent，但 LLM 生成场景不建议。）
            proxy_next_upstream error timeout;
            proxy_next_upstream_tries 2;
            proxy_next_upstream_timeout 0;
        }
    }
}
