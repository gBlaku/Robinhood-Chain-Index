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

# Retry across process death. Every stage resumes from its own cursor, so a
# restart costs one chunk, not the run. A public endpoint drops connections
# and its upstream nodes return EOF; one such error killed a run 16.9M blocks
# in, which is a wrapper problem as much as a client one.
echo "[$(date -u +%FT%TZ)] === update start ===" >> $LOG
rc=1
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  .venv/bin/python scan.py update --min-lag 1000 >> $LOG 2>&1
  rc=$?
  [ $rc -eq 0 ] && break
  echo "[$(date -u +%FT%TZ)] attempt $attempt exited $rc, resuming in 60s" >> $LOG
  sleep 60
done
echo "[$(date -u +%FT%TZ)] === update exit $rc after $attempt attempt(s) ===" >> $LOG
exit $rc
