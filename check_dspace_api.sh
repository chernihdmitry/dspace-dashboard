#!/bin/bash

# Скрипт для проверки DSpace API и групп администратора

echo "==============================================="
echo "DSpace API Administrator Check"
echo "==============================================="
echo ""

# Ввод параметров
read -p "DSpace URL (например https://dspace.nuft.edu.ua): " DSPACE_URL
read -p "Email пользователя: " EMAIL
read -sp "Пароль: " PASSWORD
echo ""
echo ""

# Проверка обязательных параметров
if [ -z "$DSPACE_URL" ] || [ -z "$EMAIL" ] || [ -z "$PASSWORD" ]; then
    echo "ERROR: Не все параметры указаны"
    exit 1
fi

# Нормализация URL (убираем trailing slash)
DSPACE_URL="${DSPACE_URL%/}"

echo "[1/3] Поиск правильного API-endpoint'а..."
echo "      URL: $DSPACE_URL"
echo "      Email: $EMAIL"
echo ""

# Пробуем разные варианты базового URL
# НОВЫЙ ПОРЯДОК: сначала /server/api как приоритет для новых версий DSpace
API_CANDIDATES=(
    "$DSPACE_URL/server/api"
    "$DSPACE_URL/api"
    "$DSPACE_URL/rest/api"
)

API_BASE=""
LOGIN_RESPONSE=""
TOKEN=""

for candidate in "${API_CANDIDATES[@]}"; do
    echo "    Пробуем $candidate..."
    
    # Сначала пробуем обычный POST
    LOGIN_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$candidate/authn/login" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      -d "user=$EMAIL&password=$PASSWORD" \
      -H "Accept: application/json" 2>/dev/null)
    
    HTTP_CODE=$(echo "$LOGIN_RESPONSE" | tail -n1)
    BODY=$(echo "$LOGIN_RESPONSE" | head -n-1)
    
    echo "      HTTP Code: $HTTP_CODE"
    
    # Проверяем ответ
    if [ "$HTTP_CODE" = "200" ]; then
        TOKEN=$(echo "$BODY" | jq -r '.token // .access_token // .Authorization // empty' 2>/dev/null)
        if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
            API_BASE="$candidate"
            echo "    ✓ Найден! Token получен"
            break
        fi
    fi
    
    # Если не сработало - показываем часть ответа для отладки
    if echo "$BODY" | head -c 200 | grep -q "html\|HTML\|error\|Error"; then
        echo "      ⚠ Ответ выглядит как HTML ошибка, пробуем дальше..."
    fi
done

if [ -z "$API_BASE" ] || [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo ""
    echo "❌ ERROR: Не удалось авторизоваться"
    echo ""
    echo "HTTP Code последней попытки: $HTTP_CODE"
    echo ""
    echo "Ответ сервера:"
    echo "$BODY" | jq . 2>/dev/null || (echo "$BODY" | head -c 500)
    echo ""
    echo ""
    echo "Проверьте:"
    echo "  1. Правильность email и пароля (попробуйте войти в сам DSpace через браузер)"
    echo "  2. URL DSpace (попробуйте перейти на $DSPACE_URL в браузер)"
    echo "  3. Доступность /server/api (попробуйте $DSPACE_URL/server/api/ в браузер)"
    exit 1
fi

echo ""
echo "✓ Авторизация успешна"
echo "  API Base: $API_BASE"
echo "  Token: ${TOKEN:0:20}...${TOKEN: -20}"
echo ""

echo "[2/4] Проверка /authn/specialgroups..."
echo ""

SPECIAL_GROUPS=$(curl -s -w "\n%{http_code}" "$API_BASE/authn/specialgroups" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/json" 2>/dev/null)

SPECIAL_GROUPS_CODE=$(echo "$SPECIAL_GROUPS" | tail -n1)
SPECIAL_GROUPS=$(echo "$SPECIAL_GROUPS" | head -n-1)

echo "$SPECIAL_GROUPS" | jq . 2>/dev/null || echo "$SPECIAL_GROUPS"
echo ""

# Проверяем наличие Administrator в результатах
if echo "$SPECIAL_GROUPS" | jq -e '._embedded.groups[] | select(.name == "Administrator")' > /dev/null 2>/dev/null; then
    echo "✓ Найдена группа 'Administrator' в specialgroups"
else
    echo "❌ Группа 'Administrator' НЕ найдена в specialgroups"
fi

echo ""
echo "[3/4] Проверка /authn/status..."
echo ""

STATUS=$(curl -s "$API_BASE/authn/status" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/json" 2>/dev/null)

echo "$STATUS" | jq . 2>/dev/null || echo "$STATUS"
echo ""

# Дополнительная проверка через eperson groups
EPERSON_ID=$(echo "$STATUS" | jq -r '._links.eperson.href // empty' 2>/dev/null | grep -o '[a-f0-9\-]*$')

if [ -n "$EPERSON_ID" ]; then
    echo ""
    echo "[4/4] Проверка групп через /eperson/epersons/{id}/groups..."
    echo "      EPERSON_ID: $EPERSON_ID"
    echo ""
    
    EPERSON_GROUPS=$(curl -s "$API_BASE/eperson/epersons/$EPERSON_ID/groups" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Accept: application/json" 2>/dev/null)
    
    echo "$EPERSON_GROUPS" | jq . 2>/dev/null || echo "$EPERSON_GROUPS"
    
    # Проверяем наличие Administrator
    if echo "$EPERSON_GROUPS" | jq -e '._embedded.groups[] | select(.name == "Administrator")' > /dev/null 2>/dev/null; then
        echo ""
        echo "✓ Найдена группа 'Administrator' в eperson groups"
    else
        echo ""
        echo "❌ Группа 'Administrator' НЕ найдена в eperson groups"
    fi
fi

echo ""
echo "==============================================="
echo "Проверка завершена"
echo "==============================================="
