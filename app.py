import os
from datetime import date
from flask import Flask, render_template, redirect, url_for
from flask_caching import Cache
from pathlib import Path

import solr_client as solr

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

def create_app():
    app = Flask(__name__)

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
        }

    # ----------------------------
    # Routes
    # ----------------------------

    @app.get("/")
    def index():
        return redirect(url_for("info"))

    @app.get("/info")
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
    def statistics():
        key = f"monthly_stats_{START_YEAR}_{START_MONTH}_v2"
        rows = cache.get(key)
        if rows is None:
            rows = solr.monthly_stats(START_YEAR, START_MONTH)
            cache.set(key, rows, timeout=CACHE_TTL_SECONDS)

        for r in rows:
            r["month_name"] = MONTH_NAMES_UA.get(int(r["month"]), str(r["month"]))

        return render_template(
            "statistics.html",
            rows=rows,
            start_year=START_YEAR,
            start_month=START_MONTH,
        )

    @app.get("/statistics/<int:year>/<int:month>")
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
    def submitters():
        today = date.today()
        return redirect(url_for("submitters_month", year=today.year, month=today.month))

    @app.get("/submitters/<int:year>/<int:month>")
    def submitters_month(year: int, month: int):
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

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()
