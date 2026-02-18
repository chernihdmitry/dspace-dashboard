# Отладка проблемы "Доступ тільки для адміністраторів"

Если вы видите ошибку `reason=not_admin` при попытке входа администратора в систему, это значит, что:
1. Учётные данные верны
2. Пользователь аутентифицирован в DSpace
3. Но система не может найти группу администратора для этого пользователя

## Быстрое решение

### Вариант 1: Использовать ADMIN_EMAILS (быстро)

Если нужно срочно дать доступ пользователю, используйте переменную `ADMIN_EMAILS`:

```bash
# Отредактируйте /etc/default/dspace-dashboard и добавьте:
ADMIN_EMAILS="i.putilina@ukr.net,other-admin@example.com"

# Перезагрузите приложение:
sudo systemctl restart dspace-dashboard
```

Это работает как fallback и не зависит от групп в DSpace.

### Вариант 2: Включить DEBUG логирование для диагностики

Чтобы понять, почему система не находит группу администратора:

```bash
# Отредактируйте /etc/default/dspace-dashboard и добавьте:
LOG_LEVEL=DEBUG

# Перезагрузите приложение:
sudo systemctl restart dspace-dashboard

# Попросите пользователя попытаться войти ещё раз
# Посмотрите логи:
tail -f /opt/dspace-dashboard/logs/errors.log | grep admin_check_failed
```

В логе вы увидите что-то вроде:

```
2026-02-18 11:45:30 [auth_dspace] WARNING admin_check_failed email=i.putilina@ukr.net ip=95.47.116.27 groups_debug={'authenticated': True, 'groups_via_eperson': [], 'groups_via_special': ['Administrators'], 'groups_toplevel': [], 'groups_via_embedded': [], 'email': 'i.putilina@ukr.net', 'errors': []}
```

## Интерпретация отладочной информации

В `groups_debug` показаны все найденные группы по разным каналам:

- **`groups_via_eperson`**: группы через `/api/eperson/{id}/groups` endpoint
- **`groups_via_special`**: группы через `/api/authn/specialgroups` endpoint  
- **`groups_via_embedded`**: группы в `_embedded.specialGroups` ответа `/api/authn/status`
- **`groups_toplevel`**: группы в корневом ответе `/api/authn/status`
- **`email`**: email пользователя (для проверки ADMIN_EMAILS)
- **`authenticated`**: успешна ли аутентификация (должна быть `true`)

### Пример 1: Группа "Administrators" (с 's')

Если видите:
```
'groups_via_special': ['Administrators']
```

Но система всё равно отказывает доступ, это может быть потому, что:
- Алгоритм проверки ищет "administrator" в названии групп (case-insensitive)
- **"Administrators" содержит "administrator"**, поэтому должна работать

Если это не срабатывает, есть bug в коде или проблема с API.

### Пример 2: Группа "Repository Administrators" или другое имя

Если видите:
```
'groups_via_special': ['Repository Administrators', 'Users']
```

Если группа содержит слово "administrator" (даже как часть более длинного названия), это должно работать.

## Что делать в разных ситуациях

### Ситуация 1: Группы вообще не возвращаются (все пусто)

```
'groups_via_eperson': []
'groups_via_special': []
'groups_via_embedded': []
'groups_toplevel': []
```

**Причины:**
1. В DSpace для пользователя не настроены никакие группы
2. API не возвращает информацию о группах (проблема версии или конфигурации DSpace)
3. Нет прав доступа к endpoint'ам групп

**Решение:**
- Проверьте в DSpace, что пользователь действительно входит в группу администраторов
- Используйте `ADMIN_EMAILS` как workaround
- Проверьте логи DSpace на ошибки авторизации

### Ситуация 2: Группы возвращаются, но не содержат "administrator"

```
'groups_via_special': ['Users', 'Staff', 'Contributors']
```

**Причины:**
- Пользователь не входит в группу администраторов в DSpace
- Группа администраторов названа по-другому (напр., "Admins" вместо "Administrator")

**Решение:**
1. Проверьте в DSpace в разделе группы, как именно названа группа администраторов
2. Либо добавьте пользователя в правильную группу администраторов
3. Либо используйте `ADMIN_EMAILS` для быстрого доступа

### Ситуация 3: Группа администраторов есть в одном канале, но не используется

```
'groups_via_special': ['Administrator']
'groups_via_eperson': []
```

Это нормально - система проверяет все каналы и использует первый найденный.

## Проверка конфигурации DSpace

Используйте curl для прямой проверки DSpace API:

```bash
# Замените <email> и <password> на реальные учётные данные
# и <dspace-url> на URL вашего DSpace сервера

# 1. Получить CSRF токен:
curl -v "https://<dspace-url>/api/authn/status"

# 2. Авторизоваться:
curl -X POST "https://<dspace-url>/api/authn/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "user=<email>&password=<password>"

# 3. Проверить статус (с полученным токеном):
curl "https://<dspace-url>/api/authn/status" \
  -H "Authorization: Bearer <your-jwt-token>"

# 4. Проверить группы специальные:
curl "https://<dspace-url>/api/authn/specialgroups" \
  -H "Authorization: Bearer <your-jwt-token>"
```

## Проверка переменных окружения

```bash
# Посмотреть текущие переменные окружения:
cat /etc/default/dspace-dashboard

# Проверить, что LOG_LEVEL правильно установлен:
grep LOG_LEVEL /etc/default/dspace-dashboard

# Проверить ADMIN_EMAILS:
grep ADMIN_EMAILS /etc/default/dspace-dashboard
```

## Просмотр логов

```bash
# Последние логи входа (с причинами):
tail -100 /opt/dspace-dashboard/logs/dspace-dashboard.log

# Последние ошибки (включая отладку групп):
tail -100 /opt/dspace-dashboard/logs/errors.log

# Следить за логами в реальном времени:
tail -f /opt/dspace-dashboard/logs/errors.log

# Фильтровать только ошибки с администратором:
grep "admin_check_failed" /opt/dspace-dashboard/logs/errors.log
```

## Контакты для поддержки

Если после всех проверок проблема остаётся, соберите следующую информацию:

1. Полный вывод отладки из логов (с `LOG_LEVEL=DEBUG`)
2. Результат проверки DSpace API (п. 2-4 выше)
3. Версию DSpace (`dspaceVersion` из `/api/`)
4. Версию приложения dashboard (`VERSION` файл)
5. Образец из `dspace.server.url` в конфигурации

