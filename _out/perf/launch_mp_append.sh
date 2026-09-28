#!/bin/bash
# 多节点 append 压测启动器：在 stg-121 上运行，向 3 节点各起 P 个 driver 进程，
# 每个进程打本机 Server（16 线程），run-id 互异（fact key 已 run 隔离，无跨进程碰撞）。
#
# 关键：远端派发必须用 `ssh -f`（ssh 自身在认证后 fork 到后台、远端命令在远端前台跑），
# 不能用 `ssh -n HOST "cmd &"`——sshd 会等 channel EOF 而不返回，循环随即挂死。
# 本机（121）用 setsid 脱离会话后台起。6 进程近乎同时 dispatch（实测收尾相差 ≤3s），
# 才是有效的“同时并发”测量；若逐个等待则进程错峰、聚合无意义（见 §10.9-C Point A 教训）。
#
# 用法： launch_mp_append.sh <duration_sec> <run_prefix> <procs_per_node>
set -u
D=${1:-180}
PFX=${2:-mpB}
P=${3:-2}

PY121=/home/data/nl2sql/miniconda3/bin/python3.11
PY122=/home/data/user-defined-nl2sql/miniconda3/bin/python3.11
PY123=/home/data/nl2sql/miniconda3/bin/python3.11

run_cmd() {  # $1=py $2=rid $3=base  -> 打印一条前台运行的 driver 命令
  echo "cd /tmp && $1 cv_perf_driver.py --base-url $3 --out-dir /tmp/results-mp \
append --threads 16 --duration $D --run-id $2 --tag gate4-$2 \
</dev/null >/tmp/mp-$2.log 2>&1"
}

for i in $(seq 0 $((P - 1))); do
  # node 121 (local)：setsid 后台起，不阻塞循环
  RID="${PFX}-n121-p${i}"
  setsid bash -c "$(run_cmd "$PY121" "$RID" "http://10.129.33.121:8080/graphspaces/DEFAULT/graphs/hugegraph")" &
  echo "launched $RID (local)"
  # node 122 (remote)：ssh -f，远端前台跑、本地立即返回
  RID="${PFX}-n122-p${i}"
  ssh -f 10.129.33.122 "$(run_cmd "$PY122" "$RID" "http://10.129.33.122:8080/graphspaces/DEFAULT/graphs/hugegraph")"
  echo "launched $RID (122)"
  # node 123 (remote)
  RID="${PFX}-n123-p${i}"
  ssh -f 10.129.33.123 "$(run_cmd "$PY123" "$RID" "http://10.129.33.123:8080/graphspaces/DEFAULT/graphs/hugegraph")"
  echo "launched $RID (123)"
done
echo "ALL_LAUNCHED PFX=$PFX P=$P D=${D}s total_procs=$((3 * P)) concurrency=$((3 * P * 16))"
