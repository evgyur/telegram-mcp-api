#!/bin/bash
# Скрипт для генерации Telegram session string

cd "$(dirname "$0")"
source venv/bin/activate

echo "=== Генерация Telegram Session String ==="
echo ""
echo "Вам нужно будет ввести:"
echo "1. Номер телефона (с кодом страны, например +79991234567)"
echo "2. Код подтверждения из Telegram"
echo "3. Пароль 2FA (если включен)"
echo ""

python session_string_generator.py

echo ""
echo "=== Готово! ==="
echo "Если session string был сгенерирован, он уже добавлен в .env"
echo "Проверьте файл .env:"
echo "  cat .env | grep TELEGRAM_SESSION_STRING"
