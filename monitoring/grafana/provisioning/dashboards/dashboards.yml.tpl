# Grafana 仪表盘自动加载配置（模板）
#
# Grafana 的 provisioning YAML 不支持环境变量展开，所以 options.path 必须是绝对路径。
# scripts/monitoring.sh 启动时会用项目里 dashboards 目录的真实绝对路径替换
# __DASHBOARDS_PATH__，渲染到 $MON_HOME/grafana/provisioning/dashboards/dashboards.yml。
#
# 注意：folderUid 必须与仪表盘自身的 uid 不同！
# 若两者相同（例如都用 llm-gateway），仪表盘的 meta.folderUid 会指向自己，
# Grafana 前端解析面包屑/权限时无限递归，页面报
# "RangeError: Maximum call stack size exceeded"。
apiVersion: 1

providers:
  - name: llm-gateway
    orgId: 1
    folder: LLM Gateway
    folderUid: llm-gateway-folder
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: __DASHBOARDS_PATH__
      foldersFromFilesStructure: false