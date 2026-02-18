import os
import logging
import requests
from typing import Optional, Dict, Any
from dspace_config import get_config_value, get_config_path

logger = logging.getLogger(__name__)


def _build_api_base(server_url: str) -> str:
    if not server_url:
        return ""
    base = server_url.rstrip("/")
    if base.endswith("/api"):
        return base
    return f"{base}/api"


def _get_api_base() -> str:
    server_url = get_config_value(
        "dspace.server.url",
        os.getenv("REST_BASE_URL", ""),
    ).rstrip("/")
    return _build_api_base(server_url).rstrip("/")


def authenticate(email: str, password: str) -> Optional[str]:
    """
    Авторизация пользователя через DSpace REST API.
    Возвращает JWT токен если успешно, None если нет.
    """
    api_base = _get_api_base()
    if not api_base:
        config_path = get_config_path()
        raise RuntimeError(
            f"dspace.server.url is not set in {config_path} and REST_BASE_URL is empty."
        )

    url = f"{api_base}/authn/login"

    def extract_csrf(resp: requests.Response) -> tuple[Optional[str], Optional[str], Optional[str]]:
        token = resp.headers.get("DSPACE-XSRF-TOKEN")
        cookie_name = None
        cookie_val = (
            resp.cookies.get("DSPACE-XSRF-COOKIE")
            or resp.cookies.get("DSPACE-XSRF-TOKEN")
        )
        if resp.cookies.get("DSPACE-XSRF-COOKIE"):
            cookie_name = "DSPACE-XSRF-COOKIE"
        elif resp.cookies.get("DSPACE-XSRF-TOKEN"):
            cookie_name = "DSPACE-XSRF-TOKEN"

        if not token and cookie_val:
            token = cookie_val
        return token, cookie_name, cookie_val

    try:
        session = requests.Session()

        # Шаг 1: Получаем CSRF токен (если сервер его выдает)
        csrf_token = None
        csrf_cookie_name = None
        csrf_cookie_val = None

        try:
            csrf_response = session.get(
                f"{api_base}/authn/status",
                headers={"Accept": "application/json"},
                timeout=10,
                allow_redirects=True,
            )
            csrf_token, csrf_cookie_name, csrf_cookie_val = extract_csrf(csrf_response)
        except Exception:
            # Если не удалось получить CSRF, попробуем логин без него
            pass

        if csrf_cookie_name and csrf_cookie_val:
            session.cookies.set(csrf_cookie_name, csrf_cookie_val)

        # Шаг 2: Отправляем запрос на логин (с CSRF токеном если он есть)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        if csrf_token:
            headers["X-XSRF-TOKEN"] = csrf_token

        response = session.post(
            url,
            data={"user": email, "password": password},
            headers=headers,
            timeout=10,
            allow_redirects=False,
        )

        # Если сервер требует CSRF и отдал токен только при попытке логина
        if response.status_code in (401, 403) and not csrf_token:
            retry_token, retry_cookie_name, retry_cookie_val = extract_csrf(response)
            if retry_cookie_name and retry_cookie_val:
                session.cookies.set(retry_cookie_name, retry_cookie_val)
            if retry_token:
                headers["X-XSRF-TOKEN"] = retry_token
                response = session.post(
                    url,
                    data={"user": email, "password": password},
                    headers=headers,
                    timeout=10,
                    allow_redirects=False,
                )

        # DSpace возвращает 200 при успешной авторизации
        if response.status_code == 200:
            # Токен приходит в заголовке Authorization
            auth_header = response.headers.get("Authorization")
            if auth_header:
                if auth_header.startswith("Bearer "):
                    return auth_header.replace("Bearer ", "")
                return auth_header
            
            # Также проверяем обновленный CSRF токен в куках
            new_csrf_token = (
                response.cookies.get("DSPACE-XSRF-TOKEN")
                or response.cookies.get("DSPACE-XSRF-COOKIE")
            )
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
        return None


def check_user_status(token: str) -> Optional[Dict[str, Any]]:
    """
    Проверяет статус пользователя по токену.
    Возвращает информацию о пользователе если токен валиден.
    """
    api_base = _get_api_base()
    if not api_base:
        return None

    url = f"{api_base}/authn/status"
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def is_administrator(token: str, user_data: Optional[Dict] = None) -> bool:
    """
    Проверяет, является ли пользователь администратором.
    """
    if not user_data:
        user_data = check_user_status(token)
    
    if not user_data or not user_data.get("authenticated"):
        logger.warning("is_administrator: user_data not authenticated")
        return False
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Получаем данные пользователя по ссылке _links.eperson
    eperson_link = user_data.get("_links", {}).get("eperson", {}).get("href")
    logger.debug("is_administrator: eperson_link=%s", eperson_link)
    if eperson_link:
        try:
            response = requests.get(eperson_link, headers=headers, timeout=10)
            if response.status_code == 200:
                eperson_data = response.json()
                
                # Проверяем группы пользователя через _links.groups
                groups_link = eperson_data.get("_links", {}).get("groups", {}).get("href")
                logger.debug("is_administrator: groups_link=%s", groups_link)
                if groups_link:
                    try:
                        groups_response = requests.get(groups_link, headers=headers, timeout=10)
                        if groups_response.status_code == 200:
                            groups_data = groups_response.json()
                            
                            # Проверяем _embedded.groups
                            user_groups = groups_data.get("_embedded", {}).get("groups", [])
                            logger.debug("is_administrator: found %d groups via _embedded", len(user_groups))
                            
                            for group in user_groups:
                                group_name = group.get("name", "")
                                logger.debug("is_administrator: checking group=%s", group_name)
                                
                                if "administrator" in group_name.lower():
                                    logger.info("is_administrator: FOUND admin via _embedded.groups")
                                    return True
                    except Exception as e:
                        logger.warning("is_administrator: failed to get groups via _links.groups: %s", str(e))
                
                # Проверяем группы в eperson данных
                groups = eperson_data.get("groups", [])
                logger.debug("is_administrator: eperson_data has %d groups directly", len(groups) if groups else 0)
                if groups:
                    for group in groups:
                        group_name = group.get("name", "").lower()
                        logger.debug("is_administrator: checking direct group=%s", group_name)
                        if "administrator" in group_name:
                            logger.info("is_administrator: FOUND admin via eperson.groups")
                            return True
        except Exception as e:
            logger.warning("is_administrator: failed to get eperson_link data: %s", str(e))
    
    # Проверяем specialGroups по ссылке
    special_groups_link = user_data.get("_links", {}).get("specialGroups", {}).get("href")
    logger.debug("is_administrator: special_groups_link=%s", special_groups_link)
    if special_groups_link:
        try:
            response = requests.get(special_groups_link, headers=headers, timeout=10)
            if response.status_code == 200:
                groups_data = response.json()
                groups = groups_data.get("_embedded", {}).get("groups", [])
                logger.debug("is_administrator: found %d groups via specialGroups", len(groups))
                
                for group in groups:
                    group_name = group.get("name", "")
                    logger.debug("is_administrator: checking special group=%s", group_name)
                    
                    if "administrator" in group_name.lower():
                        logger.info("is_administrator: FOUND admin via specialGroups")
                        return True
        except Exception as e:
            logger.warning("is_administrator: failed to get specialGroups: %s", str(e))
    
    # Проверяем группы пользователя в user_data (если есть)
    groups = user_data.get("groups", [])
    logger.debug("is_administrator: user_data has %d groups directly", len(groups) if groups else 0)
    
    for group in groups:
        group_name = group.get("name", "").lower()
        logger.debug("is_administrator: checking top-level group=%s", group_name)
        if "administrator" in group_name:
            logger.info("is_administrator: FOUND admin via user_data.groups")
            return True
    
    # Вариант через _embedded
    if "_embedded" in user_data:
        embedded_groups = user_data["_embedded"].get("specialGroups", [])
        logger.debug("is_administrator: found %d groups via _embedded.specialGroups", len(embedded_groups))
        for group in embedded_groups:
            group_name = group.get("name", "").lower()
            logger.debug("is_administrator: checking embedded group=%s", group_name)
            if "administrator" in group_name:
                logger.info("is_administrator: FOUND admin via _embedded.specialGroups")
                return True
    
    # Fallback: ADMIN_EMAILS
    if eperson_link:
        try:
            response = requests.get(eperson_link, headers=headers, timeout=10)
            if response.status_code == 200:
                email = response.json().get("email", "").lower()
                logger.debug("is_administrator: fallback check for email=%s", email)
                
                # Проверяем ADMIN_EMAILS
                admin_emails = os.getenv("ADMIN_EMAILS", "").lower().split(",")
                admin_emails = [e.strip() for e in admin_emails if e.strip()]
                logger.debug("is_administrator: ADMIN_EMAILS configured=%d", len(admin_emails))
                
                if email in admin_emails:
                    logger.info("is_administrator: FOUND admin via ADMIN_EMAILS fallback")
                    return True
        except Exception as e:
            logger.warning("is_administrator: failed fallback ADMIN_EMAILS check: %s", str(e))
    
    logger.warning("is_administrator: NOT FOUND - no admin groups detected")
    return False


def _get_user_groups_debug(token: str, user_data: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Вспомогательная функция для отладки - возвращает все найденные группы пользователя.
    """
    if not user_data:
        user_data = check_user_status(token)
    
    result = {
        "authenticated": bool(user_data and user_data.get("authenticated")),
        "groups_via_embedded": [],
        "groups_via_eperson": [],
        "groups_via_special": [],
        "groups_toplevel": [],
        "email": None,
        "errors": []
    }
    
    if not result["authenticated"]:
        return result
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        # Пробуем получить email для логирования
        eperson_link = user_data.get("_links", {}).get("eperson", {}).get("href")
        if eperson_link:
            resp = requests.get(eperson_link, headers=headers, timeout=10)
            if resp.status_code == 200:
                result["email"] = resp.json().get("email")
        
        # Группы через _embedded.specialGroups
        if "_embedded" in user_data and "specialGroups" in user_data["_embedded"]:
            for group in user_data["_embedded"].get("specialGroups", []):
                group_name = group.get("name", "")
                result["groups_via_embedded"].append(group_name)
        
        # Группы через eperson link
        if eperson_link:
            resp = requests.get(eperson_link, headers=headers, timeout=10)
            if resp.status_code == 200:
                eperson_data = resp.json()
                
                # Прямые группы в eperson
                for group in eperson_data.get("groups", []):
                    group_name = group.get("name", "")
                    result["groups_via_eperson"].append(group_name)
                
                # Группы через groups link
                groups_link = eperson_data.get("_links", {}).get("groups", {}).get("href")
                if groups_link:
                    resp = requests.get(groups_link, headers=headers, timeout=10)
                    if resp.status_code == 200:
                        groups_data = resp.json()
                        for group in groups_data.get("_embedded", {}).get("groups", []):
                            group_name = group.get("name", "")
                            result["groups_via_eperson"].append(group_name)
        
        # SpecialGroups через link
        special_link = user_data.get("_links", {}).get("specialGroups", {}).get("href")
        if special_link:
            resp = requests.get(special_link, headers=headers, timeout=10)
            if resp.status_code == 200:
                groups_data = resp.json()
                for group in groups_data.get("_embedded", {}).get("groups", []):
                    group_name = group.get("name", "")
                    result["groups_via_special"].append(group_name)
        
        # Топлевель группы
        for group in user_data.get("groups", []):
            group_name = group.get("name", "")
            result["groups_toplevel"].append(group_name)
    
    except Exception as e:
        result["errors"].append(str(e))
    
    return result


def logout(token: str) -> bool:
    """
    Выход пользователя из системы.
    """
    api_base = _get_api_base()
    if not api_base:
        return False

    url = f"{api_base}/authn/logout"
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, headers=headers, timeout=10)
        return response.status_code in [200, 204]
    except Exception:
        return False
