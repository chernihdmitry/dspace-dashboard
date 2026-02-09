import os
import logging
import requests
from typing import Optional, Dict, Any

DSPACE_API_ROOT = os.getenv("DSPACE_API_ROOT", "/server/api")
REST_BASE_URL = os.getenv("REST_BASE_URL", "").rstrip("/")

logger = logging.getLogger("auth_dspace")

# Формируем полный URL к API
if DSPACE_API_ROOT.startswith("http://") or DSPACE_API_ROOT.startswith("https://"):
    API_BASE = DSPACE_API_ROOT.rstrip("/")
else:
    if not REST_BASE_URL:
        raise RuntimeError("REST_BASE_URL must be set for authentication")
    API_BASE = f"{REST_BASE_URL}{DSPACE_API_ROOT}".rstrip("/")


def authenticate(email: str, password: str) -> Optional[str]:
    """
    Авторизация пользователя через DSpace REST API.
    Возвращает JWT токен если успешно, None если нет.
    """
    url = f"{API_BASE}/authn/login"
    
    try:
        # Шаг 1: Получаем CSRF токен
        csrf_response = requests.get(f"{API_BASE}/authn/status", timeout=10)
        if csrf_response.status_code != 200:
            logger.warning("authn/status returned %s", csrf_response.status_code)
        
        csrf_token = csrf_response.headers.get("DSPACE-XSRF-TOKEN")
        csrf_cookie = csrf_response.cookies.get("DSPACE-XSRF-COOKIE")
        if not csrf_token and csrf_cookie:
            csrf_token = csrf_cookie
        
        if not csrf_token:
            logger.warning("authn/status missing CSRF token; headers=%s cookies=%s", dict(csrf_response.headers), dict(csrf_response.cookies))
            return None
        
        # Шаг 2: Отправляем запрос на логин с CSRF токеном
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

        if response.status_code != 200:
            logger.warning("authn/login returned %s", response.status_code)
        
        # DSpace возвращает 200 при успешной авторизации
        if response.status_code == 200:
            # Токен приходит в заголовке Authorization
            auth_header = response.headers.get("Authorization")
            if auth_header:
                if auth_header.startswith("Bearer "):
                    return auth_header.replace("Bearer ", "")
                return auth_header
            
            # Также проверяем обновленный CSRF токен в куках
            new_csrf_token = response.cookies.get("DSPACE-XSRF-TOKEN") or response.cookies.get("DSPACE-XSRF-COOKIE")
            if new_csrf_token:
                return new_csrf_token
            
            # Проверяем тело ответа
            try:
                data = response.json()
                if data.get("authenticated"):
                    return new_csrf_token or csrf_token or "authenticated"
            except:
                pass
        
        return None
    except Exception:
        logger.exception("Authentication error")
        return None


def check_user_status(token: str) -> Optional[Dict[str, Any]]:
    """
    Проверяет статус пользователя по токену.
    Возвращает информацию о пользователе если токен валиден.
    """
    url = f"{API_BASE}/authn/status"
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning("authn/status (with token) returned %s", response.status_code)
        
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        logger.exception("Status check error")
        return None


def is_administrator(token: str, user_data: Optional[Dict] = None) -> bool:
    """
    Проверяет, является ли пользователь администратором.
    """
    if not user_data:
        user_data = check_user_status(token)
    
    if not user_data or not user_data.get("authenticated"):
        return False
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Получаем данные пользователя по ссылке _links.eperson
    eperson_link = user_data.get("_links", {}).get("eperson", {}).get("href")
    if eperson_link:
        try:
            response = requests.get(eperson_link, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning("eperson link returned %s", response.status_code)
            if response.status_code == 200:
                eperson_data = response.json()
                
                # Проверяем группы пользователя через _links.groups
                groups_link = eperson_data.get("_links", {}).get("groups", {}).get("href")
                if groups_link:
                    try:
                        groups_response = requests.get(groups_link, headers=headers, timeout=10)
                        if groups_response.status_code != 200:
                            logger.warning("eperson groups returned %s", groups_response.status_code)
                        
                        if groups_response.status_code == 200:
                            groups_data = groups_response.json()
                            
                            # Проверяем _embedded.groups
                            user_groups = groups_data.get("_embedded", {}).get("groups", [])
                            
                            for group in user_groups:
                                group_name = group.get("name", "")
                                
                                if "administrator" in group_name.lower():
                                    return True
                    except Exception:
                        logger.exception("Failed to fetch user groups")
                
                # Проверяем группы в eperson данных
                groups = eperson_data.get("groups", [])
                if groups:
                    for group in groups:
                        group_name = group.get("name", "").lower()
                        if "administrator" in group_name:
                            return True
        except Exception:
            logger.exception("Failed to fetch eperson data")
    
    # Проверяем specialGroups по ссылке
    special_groups_link = user_data.get("_links", {}).get("specialGroups", {}).get("href")
    if special_groups_link:
        try:
            response = requests.get(special_groups_link, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning("specialGroups returned %s", response.status_code)
            
            if response.status_code == 200:
                groups_data = response.json()
                groups = groups_data.get("_embedded", {}).get("groups", [])
                
                for group in groups:
                    group_name = group.get("name", "")
                    
                    if "administrator" in group_name.lower():
                        return True
        except Exception:
            logger.exception("Failed to fetch special groups")
    
    # Проверяем группы пользователя в user_data (если есть)
    groups = user_data.get("groups", [])
    
    for group in groups:
        group_name = group.get("name", "").lower()
        if "administrator" in group_name:
            return True
    
    # Вариант через _embedded
    if "_embedded" in user_data:
        embedded_groups = user_data["_embedded"].get("specialGroups", [])
        for group in embedded_groups:
            group_name = group.get("name", "").lower()
            if "administrator" in group_name:
                return True
    
    # Fallback: ADMIN_EMAILS
    if eperson_link:
        try:
            response = requests.get(eperson_link, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning("eperson link (for admin emails) returned %s", response.status_code)
            if response.status_code == 200:
                email = response.json().get("email", "").lower()
                
                # Проверяем ADMIN_EMAILS
                admin_emails = os.getenv("ADMIN_EMAILS", "").lower().split(",")
                admin_emails = [e.strip() for e in admin_emails if e.strip()]
                
                if email in admin_emails:
                    return True
        except Exception:
            logger.exception("Failed to fetch eperson email for ADMIN_EMAILS")
    
    return False


def logout(token: str) -> bool:
    """
    Выход пользователя из системы.
    """
    url = f"{API_BASE}/authn/logout"
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, headers=headers, timeout=10)
        if response.status_code not in [200, 204]:
            logger.warning("authn/logout returned %s", response.status_code)
        return response.status_code in [200, 204]
    except Exception:
        logger.exception("Logout error")
        return False
