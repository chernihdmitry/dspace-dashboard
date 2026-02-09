import os
import requests
import sys
from typing import Optional, Dict, Any

DSPACE_API_ROOT = os.getenv("DSPACE_API_ROOT", "/server/api")
APP_BASE_URL = os.getenv("APP_BASE_URL", "").rstrip("/")

# Формируем полный URL к API
if DSPACE_API_ROOT.startswith("http://") or DSPACE_API_ROOT.startswith("https://"):
    API_BASE = DSPACE_API_ROOT.rstrip("/")
else:
    if not APP_BASE_URL:
        raise RuntimeError("APP_BASE_URL must be set for authentication")
    API_BASE = f"{APP_BASE_URL}{DSPACE_API_ROOT}".rstrip("/")

print(f"[AUTH DEBUG] API_BASE configured as: {API_BASE}", file=sys.stderr, flush=True)


def authenticate(email: str, password: str) -> Optional[str]:
    """
    Авторизация пользователя через DSpace REST API.
    Возвращает JWT токен если успешно, None если нет.
    """
    url = f"{API_BASE}/authn/login"
    
    print(f"[AUTH DEBUG] Attempting login to: {url}", file=sys.stderr, flush=True)
    print(f"[AUTH DEBUG] Email: {email}", file=sys.stderr, flush=True)
    
    try:
        # Шаг 1: Получаем CSRF токен
        print(f"[AUTH DEBUG] Step 1: Getting CSRF token...", file=sys.stderr, flush=True)
        csrf_response = requests.get(f"{API_BASE}/authn/status", timeout=10)
        
        csrf_token = csrf_response.headers.get("DSPACE-XSRF-TOKEN")
        csrf_cookie = csrf_response.cookies.get("DSPACE-XSRF-COOKIE")
        
        print(f"[AUTH DEBUG] CSRF token from header: {csrf_token}", file=sys.stderr, flush=True)
        print(f"[AUTH DEBUG] CSRF cookie: {csrf_cookie}", file=sys.stderr, flush=True)
        
        if not csrf_token:
            print(f"[AUTH ERROR] Failed to get CSRF token", file=sys.stderr, flush=True)
            return None
        
        # Шаг 2: Отправляем запрос на логин с CSRF токеном
        print(f"[AUTH DEBUG] Step 2: Sending login request with CSRF token...", file=sys.stderr, flush=True)
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-XSRF-TOKEN": csrf_token
        }
        
        cookies = {}
        if csrf_cookie:
            cookies["DSPACE-XSRF-COOKIE"] = csrf_cookie
        
        response = requests.post(
            url,
            data={"user": email, "password": password},
            headers=headers,
            cookies=cookies,
            timeout=10,
            allow_redirects=False
        )
        
        print(f"[AUTH DEBUG] Response status: {response.status_code}", file=sys.stderr, flush=True)
        print(f"[AUTH DEBUG] Response headers: {dict(response.headers)}", file=sys.stderr, flush=True)
        print(f"[AUTH DEBUG] Response cookies: {dict(response.cookies)}", file=sys.stderr, flush=True)
        print(f"[AUTH DEBUG] Response body: {response.text[:500]}", file=sys.stderr, flush=True)
        
        # DSpace возвращает 200 при успешной авторизации
        if response.status_code == 200:
            # Токен приходит в заголовке Authorization
            auth_header = response.headers.get("Authorization")
            if auth_header:
                print(f"[AUTH DEBUG] Found Authorization header: {auth_header[:50]}...", file=sys.stderr, flush=True)
                if auth_header.startswith("Bearer "):
                    return auth_header.replace("Bearer ", "")
                return auth_header
            
            # Также проверяем обновленный CSRF токен в куках
            new_csrf_token = response.cookies.get("DSPACE-XSRF-TOKEN") or response.cookies.get("DSPACE-XSRF-COOKIE")
            if new_csrf_token:
                print(f"[AUTH DEBUG] Found CSRF token in cookies", file=sys.stderr, flush=True)
                return new_csrf_token
            
            # Проверяем тело ответа
            try:
                data = response.json()
                print(f"[AUTH DEBUG] JSON response: {data}", file=sys.stderr, flush=True)
                if data.get("authenticated"):
                    # Если authenticated=true, возвращаем хотя бы что-то
                    return new_csrf_token or csrf_token or "authenticated"
            except:
                pass
        
        print(f"[AUTH DEBUG] Authentication failed with status {response.status_code}", file=sys.stderr, flush=True)
        return None
    except Exception as e:
        print(f"[AUTH ERROR] Authentication error: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return None


def check_user_status(token: str) -> Optional[Dict[str, Any]]:
    """
    Проверяет статус пользователя по токену.
    Возвращает информацию о пользователе если токен валиден.
    """
    url = f"{API_BASE}/authn/status"
    
    print(f"[AUTH DEBUG] Checking user status...", file=sys.stderr, flush=True)
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers, timeout=10)
        
        print(f"[AUTH DEBUG] Status check response: {response.status_code}", file=sys.stderr, flush=True)
        
        if response.status_code == 200:
            data = response.json()
            print(f"[AUTH DEBUG] User data: {data}", file=sys.stderr, flush=True)
            return data
        return None
    except Exception as e:
        print(f"[AUTH ERROR] Status check error: {e}", file=sys.stderr, flush=True)
        return None


def is_administrator(token: str, user_data: Optional[Dict] = None) -> bool:
    """
    Проверяет, является ли пользователь администратором.
    """
    print(f"[AUTH DEBUG] Checking administrator rights...", file=sys.stderr, flush=True)
    
    if not user_data:
        user_data = check_user_status(token)
    
    if not user_data or not user_data.get("authenticated"):
        print(f"[AUTH DEBUG] User not authenticated", file=sys.stderr, flush=True)
        return False
    
    print(f"[AUTH DEBUG] User authenticated, checking groups...", file=sys.stderr, flush=True)
    
    # Проверяем группы пользователя
    # В DSpace 7+ информация о группах может быть в разных местах
    
    # Вариант 1: прямо в user_data
    groups = user_data.get("groups", [])
    print(f"[AUTH DEBUG] Groups in user_data: {groups}", file=sys.stderr, flush=True)
    
    for group in groups:
        group_name = group.get("name", "").lower()
        print(f"[AUTH DEBUG] Checking group: {group_name}", file=sys.stderr, flush=True)
        if "administrator" in group_name:
            print(f"[AUTH DEBUG] ✓ User is administrator (group: {group_name})", file=sys.stderr, flush=True)
            return True
    
    # Вариант 2: через _links.specialGroups или _embedded
    if "_embedded" in user_data:
        embedded_groups = user_data["_embedded"].get("specialGroups", [])
        print(f"[AUTH DEBUG] Embedded groups: {embedded_groups}", file=sys.stderr, flush=True)
        for group in embedded_groups:
            group_name = group.get("name", "").lower()
            if "administrator" in group_name:
                print(f"[AUTH DEBUG] ✓ User is administrator (embedded group: {group_name})", file=sys.stderr, flush=True)
                return True
    
    # Вариант 3: через authorizations endpoint
    user_uuid = user_data.get("uuid") or user_data.get("id")
    print(f"[AUTH DEBUG] User UUID: {user_uuid}", file=sys.stderr, flush=True)
    
    if user_uuid:
        try:
            # Проверяем права через authorizations
            url = f"{API_BASE}/authz/authorizations/search/object"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"uri": f"{API_BASE}/eperson/{user_uuid}"}
            
            print(f"[AUTH DEBUG] Checking authorizations at: {url}", file=sys.stderr, flush=True)
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            print(f"[AUTH DEBUG] Authorizations response: {response.status_code}", file=sys.stderr, flush=True)
            
            if response.status_code == 200:
                data = response.json()
                print(f"[AUTH DEBUG] Authorizations data: {data}", file=sys.stderr, flush=True)
                
                # Ищем права администратора
                authorizations = data.get("_embedded", {}).get("authorizations", [])
                print(f"[AUTH DEBUG] Found {len(authorizations)} authorizations", file=sys.stderr, flush=True)
                
                for auth in authorizations:
                    feature = auth.get("feature", "")
                    print(f"[AUTH DEBUG] Authorization feature: {feature}", file=sys.stderr, flush=True)
                    if "administrator" in feature.lower() or "admin" in feature.lower():
                        print(f"[AUTH DEBUG] ✓ User is administrator (feature: {feature})", file=sys.stderr, flush=True)
                        return True
        except Exception as e:
            print(f"[AUTH ERROR] Authorization check error: {e}", file=sys.stderr, flush=True)
    
    # Вариант 4: email администратора (для совместимости)
    email = user_data.get("email", "").lower()
    print(f"[AUTH DEBUG] User email: {email}", file=sys.stderr, flush=True)
    
    if email in ["admin@dspace.org", "admin@localhost", "test@test.edu"]:
        print(f"[AUTH DEBUG] ✓ User is administrator (default admin email)", file=sys.stderr, flush=True)
        return True
    
    # Для отладки - если пользователь вообще авторизован и других способов нет
    # Можно добавить переменную окружения ADMIN_EMAILS
    admin_emails = os.getenv("ADMIN_EMAILS", "").lower().split(",")
    admin_emails = [e.strip() for e in admin_emails if e.strip()]
    print(f"[AUTH DEBUG] Admin emails from config: {admin_emails}", file=sys.stderr, flush=True)
    
    if email in admin_emails:
        print(f"[AUTH DEBUG] ✓ User is administrator (in ADMIN_EMAILS)", file=sys.stderr, flush=True)
        return True
    
    print(f"[AUTH DEBUG] ✗ User is NOT administrator", file=sys.stderr, flush=True)
    return False


def logout(token: str) -> bool:
    """
    Выход пользователя из системы.
    """
    url = f"{API_BASE}/authn/logout"
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, headers=headers, timeout=10)
        return response.status_code in [200, 204]
    except Exception:
        return False
