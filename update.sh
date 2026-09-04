#!/bin/zsh
# Keeps the index level with the chain head. Intended for cron.
#
# Refuses to start if a run is already going. The stages inside `update` are
# serial by design because concurrent passes share one rate budget and both
# get slower; two overlapping cron runs would do the same thing, and a nightly
# job that occasionally takes longer than a day is not unusual here.
#
#   crontab -e
#   0 4 * * *  /Users/gentblaku/RobinhoodChain/update.sh
#
# The endpoint comes from .env (gitignored, holds an API key). Never pass it
# on the command line -- argv is visible in ps and lands in shell history.

cd /Users/gentblaku/RobinhoodChain || exit 1
LOCK=out/update.lock
LOG=out/update.log

mkdir -p out
if [ -e "$LOCK" ] && kill -0 "$(cat $LOCK 2>/dev/null)" 2>/dev/null; then
  echo "[$(date -u +%FT%TZ)] already running as pid $(cat $LOCK), skipping" >> $LOG
  exit 0
fi
echo $$ > $LOCK
trap 'rm -f $LOCK' EXIT INT TERM

echo "[$(date -u +%FT%TZ)] === update start ===" >> $LOG
.venv/bin/python scan.py update --min-lag 1000 >> $LOG 2>&1
rc=$?
echo "[$(date -u +%FT%TZ)] === update exit $rc ===" >> $LOG
exit $rc
