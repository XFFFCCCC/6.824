#!/bin/bash

# 设置测试执行次数，默认执行10次，可以通过参数控制次数：./run_tests.sh 100
COUNT=${1:-10}

# 计数
PASSED=0
FAILED=0

echo "Running tests $COUNT times..."

for ((i = 1; i <= COUNT; i++)); do
    echo -n "[$i/$COUNT] Running test... "
    
    # 捕获测试输出，但不立刻显示
    OUTPUT=$(go test -v 2>&1)
    RESULT=$?

    if [ $RESULT -eq 0 ]; then
        ((PASSED++))
        echo "PASS"
    else
        ((FAILED++))
        echo "FAIL ❌"
        echo "------ Error Output (Run $i) ------"
        echo "$OUTPUT"
        echo "-----------------------------------"
    fi
done

echo
echo "✅ Passed: $PASSED"
echo "❌ Failed: $FAILED"
