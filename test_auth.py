#!/usr/bin/env python3
"""
Тестовый скрипт для проверки DSpace REST API авторизации
"""
import os
import sys
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения
if os.path.exists("/etc/default/dspace-dashboard"):
    load_dotenv("/etc/default/dspace-dashboard")
load_dotenv()

DSPACE_API_ROOT = os.getenv("DSPACE_API_ROOT", "/server/api")
APP_BASE_URL = os.getenv("APP_BASE_URL", "").rstrip("/")

# Формируем полный URL к API
if DSPACE_API_ROOT.startswith("http://") or DSPACE_API_ROOT.startswith("https://"):
    API_BASE = DSPACE_API_ROOT.rstrip("/")
else:
    if not APP_BASE_URL:
        print("ERROR: APP_BASE_URL must be set")
        sys.exit(1)
    API_BASE = f"{APP_BASE_URL}{DSPACE_API_ROOT}".rstrip("/")

print(f"API Base URL: {API_BASE}")
print()

# Проверяем доступность API
print("1. Checking API availability...")
try:
    response = requests.get(f"{API_BASE}", timeout=10)
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.text[:200]}")
except Exception as e:
    print(f"   ERROR: {e}")

print()

# Проверяем эндпоинт /authn/status (без авторизации)
print("2. Checking /authn/status endpoint...")
try:
    response = requests.get(f"{API_BASE}/authn/status", timeout=10)
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.text[:200]}")
except Exception as e:
    print(f"   ERROR: {e}")

print()

# Пробуем авторизоваться
if len(sys.argv) >= 3:
    email = sys.argv[1]
    password = sys.argv[2]
    
    print(f"3. Testing login with email: {email}")
    url = f"{API_BASE}/authn/login"
    print(f"   URL: {url}")
    
    print()
    print("   Attempt 1: Using form data (DSpace 7+ format)")
    try:
        response = requests.post(
            url,
            data={"user": email, "password": password},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            },
            timeout=10,
            allow_redirects=False
        )
        
        print(f"   Status: {response.status_code}")
        print(f"   Headers: {dict(response.headers)}")
        print(f"   Cookies: {dict(response.cookies)}")
        print(f"   Response: {response.text[:500]}")
        
        # Пробуем разные способы получения токена
        print()
        print("   Token extraction attempts:")
        
        # 1. Authorization header
        auth_header = response.headers.get("Authorization")
        if auth_header:
            print(f"   - Authorization header: {auth_header}")
        
        # 2. JSON response
        try:
            data = response.json()
            print(f"   - JSON response keys: {list(data.keys())}")
            if "token" in data:
                print(f"   - Token from JSON: {data['token'][:50]}...")
            if "authenticated" in data:
                print(f"   - Authenticated: {data['authenticated']}")
        except:
            print(f"   - Not a JSON response")
        
        # 3. Cookies
        for cookie_name in response.cookies:
            print(f"   - Cookie {cookie_name}: {response.cookies[cookie_name][:50]}...")
            
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    print()
    print("   Attempt 2: Using JSON format")
    try:
        response = requests.post(
            url,
            json={"email": email, "password": password},
            headers={"Content-Type": "application/json"},
            timeout=10,
            allow_redirects=False
        )
        
        print(f"   Status: {response.status_code}")
        print(f"   Headers: {dict(response.headers)}")
        print(f"   Cookies: {dict(response.cookies)}")
        print(f"   Response: {response.text[:500]}")
        
        # Пробуем разные способы получения токена
        print()
        print("   Token extraction attempts:")
        
        # 1. Authorization header
        auth_header = response.headers.get("Authorization")
        if auth_header:
            print(f"   - Authorization header: {auth_header}")
        
        # 2. JSON response
        try:
            data = response.json()
            print(f"   - JSON response keys: {list(data.keys())}")
            if "token" in data:
                print(f"   - Token from JSON: {data['token'][:50]}...")
            if "authenticated" in data:
                print(f"   - Authenticated: {data['authenticated']}")
        except:
            print(f"   - Not a JSON response")
        
        # 3. Cookies
        for cookie_name in response.cookies:
            print(f"   - Cookie {cookie_name}: {response.cookies[cookie_name][:50]}...")
            
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
else:
    print("3. To test login, run:")
    print(f"   python3 {sys.argv[0]} email@example.com password")
