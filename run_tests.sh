#!/bin/bash

echo "==================================="
echo "ORC Performance Testing"
echo "==================================="
echo ""

mvn clean compile test

if [ $? -ne 0 ]; then
    echo "Ошибка при выполнении тестов"
    exit 1
fi

echo ""
echo "==================================="
echo "Тесты завершены!"
echo "==================================="

