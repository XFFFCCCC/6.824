#!/usr/bin/env bash

# Only Crash Test for MIT 6.5840 MapReduce

# Optional: Enable Go race detector
# RACE=-race

TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1; then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1; then
    TIMEOUT=gtimeout
  else
    TIMEOUT=""
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi

if [ "$TIMEOUT" != "" ]; then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# clean up
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# build only what we need
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
echo '***' Starting crash test.

# generate the correct output with nocrash
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(( $TIMEOUT2 ../mrcoordinator ../pg*txt ); touch mr-done ) &
sleep 1

# start one initial worker
$TIMEOUT2 ../mrworker ../../mrapps/crash.so &

# coordinatorSock path
SOCKNAME=/var/tmp/5840-mr-`id -u`

# keep spawning workers until coordinator finishes
( while [ -e $SOCKNAME -a ! -f mr-done ]; do
    $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]; do
    $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]; do
  $TIMEOUT2 ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait

rm -f $SOCKNAME
sort mr-out* | grep . > mr-crash-all

if cmp mr-crash-all mr-correct-crash.txt; then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
  echo '***' PASSED CRASH TEST
else
  echo '***' FAILED CRASH TEST
  exit 1
fi
