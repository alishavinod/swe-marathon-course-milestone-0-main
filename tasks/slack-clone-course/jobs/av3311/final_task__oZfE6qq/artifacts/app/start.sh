#!/bin/sh
set -u
mkdir -p /app/data
start_node() {
  python3 /app/server.py "$1" "$2" >/tmp/huddle-node-"$2".log 2>&1 &
  eval "p$2=$!"
}
start_irc() {
  python3 /app/irc.py >/tmp/huddle-irc.log 2>&1 &
  pirc=$!
}
start_irc
start_node 8000 0
start_node 8001 1
start_node 8002 2
trap 'kill "$p0" "$p1" "$p2" "$pirc" 2>/dev/null || true; exit 0' INT TERM EXIT
while :; do
  for n in 0 1 2; do
    eval "pid=\$p$n"
    if ! kill -0 "$pid" 2>/dev/null; then
      case "$n" in
        0) start_node 8000 0 ;;
        1) start_node 8001 1 ;;
        2) start_node 8002 2 ;;
      esac
    fi
  done
  if ! kill -0 "$pirc" 2>/dev/null; then
    start_irc
  fi
  sleep 1
done
