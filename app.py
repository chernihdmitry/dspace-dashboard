import os
from datetime import date
from flask import Flask, render_template, redirect, url_for, jsonify, request, flash, session
from flask_caching import Cache
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from pathlib import Path
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

# Загрузка переменных окружения из файлов
# Приоритет: .env в текущей директории, затем /etc/default/dspace-dashboard
if os.path.exists("/etc/default/dspace-dashboard"):
    load_dotenv("/etc/default/dspace-dashboard")
load_dotenv()  # загружает .env если есть

import solr_client as solr
import matomo_client as matomo
import auth_dspace

APP_TITLE = os.getenv("APP_TITLE", "DSpace Live Dashboard")
REPO_NAME = os.getenv("REPO_NAME", "iRDPU")

START_YEAR = int(os.getenv("START_YEAR", "2025"))
START_MONTH = int(os.getenv("START_MONTH", "1"))

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))

MONTH_NAMES_UA = {
    1: "Січень", 2: "Лютий", 3: "Березень", 4: "Квітень",
    5: "Травень", 6: "Червень", 7: "Липень", 8: "Серпень",
    9: "Вересень", 10: "Жовтень", 11: "Листопад", 12: "Грудень",
}

def read_version() -> str:
    try:
        return Path(__file__).with_name("VERSION").read_text(encoding="utf-8").strip()
    except Exception:
        return "dev"

APP_VERSION = os.getenv("APP_VERSION") or read_version()


# ---- User Model for Flask-Login ----
class User(UserMixin):
    def __init__(self, user_id: str, email: str, token: str):
        self.id = user_id
        self.email = email
        self.token = token


def create_app():
    app = Flask(__name__)
    
    # Secret key for sessions
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", os.urandom(24).hex())
    
    # чтобы Flask/werkzeug учитывал заголовки от прокси (proto/host/prefix)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1, x_prefix=1)

    # ---- Flask-Login Setup ----
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "login"
    login_manager.login_message = "Необхідна авторизація"
    login_manager.login_message_category = "warning"

    @login_manager.user_loader
    def load_user(user_id):
        # Загружаем пользователя из сессии
        token = session.get("token")
        email = session.get("email")
        if token and email:
            # Проверяем валидность токена
            user_data = auth_dspace.check_user_status(token)
            if user_data and user_data.get("authenticated"):
                return User(user_id, email, token)
        return None


    # ---- Cache ----
    app.config["CACHE_TYPE"] = os.getenv("CACHE_TYPE", "SimpleCache")
    app.config["CACHE_DEFAULT_TIMEOUT"] = CACHE_TTL_SECONDS
    cache = Cache(app)

    # ---- Jinja filters ----
    @app.template_filter("fmt")
    def fmt_int(v):
        try:
            return f"{int(v):,}".replace(",", " ")
        except Exception:
            return v

    # ---- Globals ----
    @app.context_processor
    def inject_globals():
        return {
            "APP_TITLE": APP_TITLE,
            "REPO_NAME": REPO_NAME,
            "APP_VERSION": APP_VERSION,
            "today_str": date.today().strftime("%d.%m.%Y"),
            "matomo_configured": matomo.is_configured(),
        }

    # ----------------------------
    # Authentication Routes
    # ----------------------------

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for("index"))
        
        if request.method == "POST":
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "")
            
            if not email or not password:
                flash("Введіть email та пароль", "danger")
                return render_template("login.html")
            
            # Авторизация через DSpace API
            token = auth_dspace.authenticate(email, password)
            
            if not token:
                flash("Невірний email або пароль", "danger")
                return render_template("login.html")
            
            # Проверяем статус пользователя
            user_data = auth_dspace.check_user_status(token)
            
            if not user_data or not user_data.get("authenticated"):
                flash("Помилка авторизації", "danger")
                return render_template("login.html")
            
            # Проверяем права администратора
            if not auth_dspace.is_administrator(token, user_data):
                flash("Доступ тільки для адміністраторів", "danger")
                return render_template("login.html")
            
            # Создаем пользователя и логиним
            user_id = user_data.get("uuid") or user_data.get("id") or email
            user = User(user_id, email, token)
            login_user(user, remember=True)
            
            # Сохраняем в сессию
            session["token"] = token
            session["email"] = email
            
            flash(f"Вітаємо, {email}!", "success")
            
            # Редирект на запрошенную страницу или на главную
            next_page = request.args.get("next")
            if next_page and next_page.startswith("/"):
                return redirect(next_page)
            return redirect(url_for("index"))
        
        return render_template("login.html")

    @app.route("/logout")
    @login_required
    def logout():
        token = session.get("token")
        if token:
            auth_dspace.logout(token)
        
        session.clear()
        logout_user()
        flash("Ви вийшли з системи", "info")
        return redirect(url_for("login"))

    # ----------------------------
    # Routes
    # ----------------------------

    @app.get("/")
    @login_required
    def index():
        return redirect(url_for("info"))

    @app.get("/info")
    @login_required
    @cache.cached(timeout=CACHE_TTL_SECONDS, key_prefix="info_page_v1")
    def info():
        # --- Solr totals ---
        data = solr.repo_totals()

        # --- Sparkline stats ---
        new_7d = solr.submitted_last_days(7)
        spark_labels, spark_values = solr.submitted_sparkline(30)

        spark_sum = sum(spark_values)
        spark_avg = round(spark_sum / max(1, len(spark_values)), 1)
        today_cnt = int(spark_values[-1]) if spark_values else 0
        yesterday_cnt = int(spark_values[-2]) if len(spark_values) >= 2 else 0
        spark_max = max(spark_values) if spark_values else 0

        total_docs = max(1, int(data.get("total_docs", 0)))

        # ---- Languages: all values + percent ----
        langs_all = sorted((data.get("langs", {}) or {}).items(), key=lambda x: x[1], reverse=True)
        langs_ui = [{"key": k, "count": v, "pct": (v / total_docs) * 100} for k, v in langs_all]

        # ---- Types: all values + percent ----
        types_all = sorted((data.get("types", {}) or {}).items(), key=lambda x: x[1], reverse=True)
        types_ui = [{"key": k, "count": v, "pct": (v / total_docs) * 100} for k, v in types_all]

        # ---- DSpace server info (REST root) ----
        ds_error = None
        ui_url = None
        server_url = None
        oai_url = None
        dspace_version = None
        dspace_name = None

        try:
            ds = solr.dspace_root_info()
            ui_url = ds.get("dspaceUI")
            server_url = ds.get("dspaceServer")
            dspace_version = ds.get("dspaceVersion")
            dspace_name = ds.get("dspaceName")
            if server_url:
                oai_url = f"{server_url}/oai/request?verb=Identify"
        except Exception as e:
            app.logger.exception("Failed to read DSpace root info")
            ds_error = str(e)

        return render_template(
            "info.html",
            data=data,
            new_7d=new_7d,
            spark_labels=spark_labels,
            spark_values=spark_values,
            langs_ui=langs_ui,
            types_ui=types_ui,
            spark_sum=spark_sum,
            spark_avg=spark_avg,
            today_cnt=today_cnt,
            yesterday_cnt=yesterday_cnt,
            spark_max=spark_max,
            # dspace
            ui_url=ui_url,
            server_url=server_url,
            oai_url=oai_url,
            dspace_version=dspace_version,
            dspace_name=dspace_name,
            ds_error=ds_error,
        )

    @app.get("/statistics")
    @login_required
    def statistics():
        today = date.today()
        return redirect(url_for("statistics_period", year=today.year, month=0))

    @app.get("/statistics/<int:year>/<int:month>")
    @login_required
    def statistics_period(year: int, month: int):
        # Месяц 0 означает "весь год"
        if month == 0:
            return statistics_year_view(year)
        
        if month < 1 or month > 12:
            return redirect(url_for("statistics"))

        today = date.today()
        years = list(range(START_YEAR, today.year + 1))
        months = list(range(1, 13))

        key = f"stats_{year}_{month}_v1"
        stats = cache.get(key)

        error = None
        if stats is None:
            try:
                stats = solr.stats_for_month(year, month)
                cache.set(key, stats, timeout=CACHE_TTL_SECONDS)
            except Exception as e:
                app.logger.exception("Statistics failed")
                stats = {"submitted": 0, "views": 0, "downloads": 0}
                error = str(e)

        month_name = MONTH_NAMES_UA.get(month, str(month))

        return render_template(
            "statistics.html",
            year=year,
            month=month,
            month_name=month_name,
            stats=stats,
            error=error,
            years=years,
            months=months,
            selected_year=year,
            selected_month=month,
            month_names=MONTH_NAMES_UA,
            today_year=today.year,
            today_month=today.month,
        )

    def statistics_year_view(year: int):
        """Внутренняя функция для отображения статистики за весь год"""
        today = date.today()
        years = list(range(START_YEAR, today.year + 1))
        months = list(range(1, 13))

        key = f"stats_{year}_by_months_v1"
        months_data = cache.get(key)

        error = None
        if months_data is None:
            try:
                months_data = solr.stats_year_by_months(year)
                cache.set(key, months_data, timeout=CACHE_TTL_SECONDS)
            except Exception as e:
                app.logger.exception("Statistics for year failed")
                months_data = []
                error = str(e)

        return render_template(
            "statistics.html",
            year=year,
            month=0,
            month_name=f"Усі місяці {year} року",
            months_data=months_data,
            error=error,
            years=years,
            months=months,
            selected_year=year,
            selected_month=0,
            month_names=MONTH_NAMES_UA,
            today_year=today.year,
            today_month=today.month,
        )

    @app.get("/statistics/dynamics")
    @login_required
    def statistics_dynamics_redirect():
        today = date.today()
        return redirect(url_for("statistics_dynamics", year=today.year))

    @app.get("/statistics/dynamics/<int:year>")
    @login_required
    def statistics_dynamics(year: int = None):
        """Страница с графиком динамики за год"""
        today = date.today()
        
        if year is None:
            year = today.year
        
        if year < START_YEAR or year > today.year:
            year = today.year
        
        years = list(range(START_YEAR, today.year + 1))
        
        key = f"stats_dynamics_{year}_v1"
        dynamics_data = cache.get(key)
        
        error = None
        if dynamics_data is None:
            try:
                dynamics_data = solr.stats_dynamics_for_year(year)
                cache.set(key, dynamics_data, timeout=CACHE_TTL_SECONDS)
            except Exception as e:
                app.logger.exception("Dynamics data failed")
                dynamics_data = []
                error = str(e)
        
        return render_template(
            "statistics_dynamics.html",
            dynamics_data=dynamics_data,
            error=error,
            year=year,
            years=years,
            selected_year=year,
            month_names=MONTH_NAMES_UA,
        )

    @app.get("/statistics/<int:year>/<int:month>/details")
    @login_required
    def month_details(year: int, month: int):
        if month < 1 or month > 12:
            return redirect(url_for("statistics"))

        key = f"month_daily_{year}_{month}_v3"
        daily = cache.get(key)
        if daily is None:
            daily = solr.month_daily_stats(year, month)
            cache.set(key, daily, timeout=CACHE_TTL_SECONDS)

        month_name = MONTH_NAMES_UA.get(month, str(month))
        return render_template(
            "month.html",
            year=year,
            month=month,
            month_name=month_name,
            daily=daily,
        )

    @app.get("/submitters")
    @login_required
    def submitters():
        today = date.today()
        return redirect(url_for("submitters_month", year=today.year, month=0))

    @app.get("/submitters/<int:year>/<int:month>")
    @login_required
    def submitters_month(year: int, month: int):
        # Месяц 0 означает "весь год"
        if month == 0:
            return submitters_year_view(year)
        
        if month < 1 or month > 12:
            return redirect(url_for("submitters"))

        today = date.today()
        years = list(range(START_YEAR, today.year + 1))
        months = list(range(1, 13))

        key = f"submitters_{year}_{month}_v3"
        rows = cache.get(key)

        error = None
        if rows is None:
            try:
                rows = solr.submitters_for_month(year, month, limit=300)
                cache.set(key, rows, timeout=CACHE_TTL_SECONDS)
            except Exception as e:
                app.logger.exception("Submitters failed")
                rows = []
                error = str(e)

        month_name = MONTH_NAMES_UA.get(month, str(month))

        return render_template(
            "submitters.html",
            year=year,
            month=month,
            month_name=month_name,
            rows=rows,
            error=error,
            years=years,
            months=months,
            selected_year=year,
            selected_month=month,
            month_names=MONTH_NAMES_UA,
            today_year=today.year,
            today_month=today.month,
        )

    def submitters_year_view(year: int):
        """Внутренняя функция для отображения данных за весь год"""
        today = date.today()
        years = list(range(START_YEAR, today.year + 1))
        months = list(range(1, 13))

        key = f"submitters_{year}_all_v1"
        rows = cache.get(key)

        error = None
        if rows is None:
            try:
                rows = solr.submitters_for_year(year, limit=300)
                cache.set(key, rows, timeout=CACHE_TTL_SECONDS)
            except Exception as e:
                app.logger.exception("Submitters for year failed")
                rows = []
                error = str(e)

        return render_template(
            "submitters.html",
            year=year,
            month=0,  # 0 = весь год
            month_name=f"Усі місяці {year} року",
            rows=rows,
            error=error,
            years=years,
            months=months,
            selected_year=year,
            selected_month=0,
            month_names=MONTH_NAMES_UA,
            today_year=today.year,
            today_month=today.month,
        )

    @app.get("/submitters/heatmap")
    @app.get("/submitters/heatmap/<int:year>")
    @login_required
    def submitters_heatmap(year: int = None):
        """Страница с тепловой картой отправителей по месяцам"""
        today = date.today()
        
        # Если год не указан, используем текущий
        if year is None:
            year = today.year
        
        # Проверка диапазона года
        if year < START_YEAR or year > today.year:
            year = today.year
        
        # Формируем список доступных годов
        years = list(range(START_YEAR, today.year + 1))
        
        key = f"submitters_heatmap_{year}_v2"
        heatmap_data = cache.get(key)
        
        error = None
        if heatmap_data is None:
            try:
                heatmap_data = solr.submitters_heatmap_data(year, limit=30)
                cache.set(key, heatmap_data, timeout=CACHE_TTL_SECONDS)
            except Exception as e:
                app.logger.exception("Heatmap data failed")
                heatmap_data = {"months": [], "submitters": [], "data": []}
                error = str(e)
        
        return render_template(
            "submitters_heatmap.html",
            heatmap_data=heatmap_data,
            error=error,
            year=year,
            years=years,
            selected_year=year,
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # ----------------------------
    # Matomo Analytics
    # ----------------------------

    @app.get("/matomo")
    @login_required
    def matomo_page():
        """Страница Matomo Analytics Dashboard"""
        if not matomo.is_configured():
            return render_template(
                "matomo.html",
                error="Matomo не настроен. Проверьте переменные окружения: MATOMO_BASE_URL, MATOMO_SITE_ID, MATOMO_TOKEN_AUTH",
                configured=False
            )
        
        # Передаем URL и Site ID для ссылки на Matomo
        matomo_url = os.getenv("MATOMO_BASE_URL", "").rstrip("/")
        matomo_site_id = os.getenv("MATOMO_SITE_ID", "")
        
        return render_template(
            "matomo.html", 
            configured=True,
            matomo_url=matomo_url,
            matomo_site_id=matomo_site_id
        )

    @app.get("/api/matomo/summary")
    @login_required
    def matomo_summary_api():
        """
        API endpoint для получения агрегированных данных Matomo
        
        Query params:
            period: 'day' или 'range' (по умолчанию 'day')
            date: 'yesterday', 'last7', 'last30' (по умолчанию 'yesterday')
        """
        if not matomo.is_configured():
            return jsonify({
                "success": False,
                "error": "Matomo не настроен"
            }), 503
        
        date_param = request.args.get("date", "yesterday")
        exclude_technical = request.args.get("exclude_technical", "0") == "1"
        
        # Валідація параметрів
        valid_dates = ["yesterday", "today", "last7", "last30", "last365"]
        
        # Перевіряємо чи це діапазон дат (формат: YYYY-MM-DD,YYYY-MM-DD)
        is_date_range = ',' in date_param and len(date_param.split(',')) == 2
        
        # Якщо не валідний готовий період і не діапазон - використовуємо yesterday
        if date_param not in valid_dates and not is_date_range:
            app.logger.warning(f"Invalid date parameter: {date_param}, using yesterday")
            date_param = "yesterday"
        
        try:
            data = matomo.get_summary_data(date_param, exclude_technical=exclude_technical)
            return jsonify(data)
        except Exception as e:
            app.logger.exception("Matomo API failed")
            return jsonify({
                "success": False,
                "error": str(e)
            }), 500

    return app


app = create_app()
