from flask import Flask, render_template, request, redirect, url_for, abort, jsonify
from dashboard.api import api_bp
from dashboard.db_util import (
    _strip_timezone,
    db_session,
    get_daily_health_summary,
    get_metric_history,
    get_patient_profile,
    get_recent_active_alert_count,
    get_recent_alerts,
    get_vitals,
    list_patient_profiles,
    update_patient_profile,
)
from datetime import datetime, timedelta
import threading
import time
from typing import Optional


app = Flask(__name__)
app.register_blueprint(api_bp)


def _metric_risk(value: float, *, min_ok: float, max_ok: float) -> float:
    mid = (min_ok + max_ok) / 2.0
    half = max(1e-9, (max_ok - min_ok) / 2.0)

    if min_ok <= value <= max_ok:
        base = min(1.0, abs(value - mid) / half)
        return 0.2 * base

    if value < min_ok:
        out = (min_ok - value) / half
    else:
        out = (value - max_ok) / half
    return min(1.0, 0.2 + 0.8 * min(1.0, out))


def compute_risk_score_from_vitals(
    vitals,
    *,
    now_utc: Optional[datetime] = None,
    active_alert_count: int = 0,
) -> Optional[float]:
    now_utc = now_utc or datetime.now()
    cutoff = now_utc - timedelta(minutes=30)

    metric_config = {
        "heart_rate": {"min": 60.0, "max": 100.0, "weight": 0.20},
        "bp_systolic": {"min": 110.0, "max": 130.0, "weight": 0.15},
        "bp_diastolic": {"min": 70.0, "max": 85.0, "weight": 0.10},
        "spo2": {"min": 95.0, "max": 100.0, "weight": 0.25},
        "temperature": {"min": 36.5, "max": 37.5, "weight": 0.15},
        "respiratory_rate": {"min": 12.0, "max": 18.0, "weight": 0.15},
    }

    latest: dict = {}
    for row in vitals or []:
        ts = _strip_timezone(getattr(row, "timestamp", None))
        if ts is None or ts < cutoff:
            continue
        metric = getattr(row, "metric_name", None)
        if metric in metric_config and metric not in latest:
            try:
                latest[metric] = float(getattr(row, "value", None))
            except (TypeError, ValueError):
                continue
        if len(latest) == len(metric_config):
            break

    if not latest and not active_alert_count:
        return None

    score = 0.0
    wsum = 0.0
    for metric, cfg in metric_config.items():
        if metric not in latest:
            continue
        w = float(cfg["weight"])
        s = _metric_risk(latest[metric], min_ok=float(cfg["min"]), max_ok=float(cfg["max"]))
        score += w * s
        wsum += w

    if wsum > 0:
        score = score / wsum

    if active_alert_count > 0:
        score = min(1.0, score + min(0.30, 0.10 * float(active_alert_count)))

    return max(0.0, min(1.0, float(score)))




def recalculate_all_patient_risk_scores() -> int:
    updated = 0
    with db_session() as db:
        patient_ids = [p["patient_id"] for p in db.mongo_db.patient_profiles.find({}, {"patient_id": 1})]
        now_utc = datetime.now()
        for patient_id in patient_ids:
            vitals = get_vitals(db.cassandra_session, patient_id, limit=200)
            try:
                active_alerts = get_recent_active_alert_count(db.cassandra_session, patient_id, now_utc=now_utc)
            except Exception:
                active_alerts = 0
            score = compute_risk_score_from_vitals(
                vitals,
                now_utc=now_utc,
                active_alert_count=active_alerts,
            )
            if score is None:
                continue

            existing = db.mongo_db.patient_profiles.find_one({"patient_id": patient_id}, {"risk_score": 1}) or {}
            prev = existing.get("risk_score")
            if isinstance(prev, (int, float)) and abs(float(prev) - float(score)) < 0.001:
                continue

            update_patient_profile(
                db.mongo_db,
                patient_id,
                {
                    "risk_score": float(score),
                    "risk_score_updated_at": now_utc,
                    "risk_score_source": "auto_vitals_v1",
                },
            )
            updated += 1

    return updated


def _risk_score_loop():
    while True:
        try:
            recalculate_all_patient_risk_scores()
        except Exception:
            app.logger.exception("Risk score updater failed")
        time.sleep(60)


def start_risk_score_updater():
    t = threading.Thread(target=_risk_score_loop, name="risk-score-updater", daemon=True)
    t.start()

@app.route("/")
def index():
    with db_session() as db:
        patients = list_patient_profiles(db.mongo_db)
        alerts = get_recent_alerts(db.cassandra_session, limit=5, since_minutes=30)
        return render_template("index.html", patients=patients, alerts=alerts)


@app.route("/alerts")
def alerts_page():
    with db_session() as db:
        alerts = get_recent_alerts(db.cassandra_session, limit=200, since_minutes=24 * 60)
        return render_template("alerts.html", alerts=alerts)


@app.route("/daily-stats")
def daily_stats_page():
    with db_session() as db:
        date_str = (request.args.get("date") or "").strip()
        if date_str:
            try:
                stat_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                stat_date = datetime.now().date()
        else:
            stat_date = datetime.now().date()

        rows = get_daily_health_summary(db.cassandra_session, stat_date)
        return render_template(
            "daily_stats.html",
            stat_date=stat_date,
            stats=rows,
        )


@app.route("/api/patients/risks")
def api_patients_risks():
    with db_session() as db:
        docs = list(
            db.mongo_db.patient_profiles.find(
                {},
                {
                    "_id": 0,
                    "patient_id": 1,
                    "risk_score": 1,
                    "risk_score_updated_at": 1,
                },
            ).sort("patient_id", 1)
        )

        for d in docs:
            ts = d.get("risk_score_updated_at")
            if hasattr(ts, "isoformat"):
                d["risk_score_updated_at"] = ts.isoformat(sep=' ', timespec='seconds')
        return jsonify(docs)


@app.route("/patient/<patient_id>")
def patient_detail(patient_id):
    with db_session() as db:
        profile = get_patient_profile(db.mongo_db, patient_id)
        vitals = get_vitals(db.cassandra_session, patient_id, limit=200)
        return render_template("detail.html", profile=profile, vitals=vitals)


@app.route("/patient/<patient_id>/edit", methods=["GET", "POST"])
def edit_patient(patient_id):
    with db_session() as db:
        profile = get_patient_profile(db.mongo_db, patient_id)
        if not profile:
            abort(404)

        if request.method == "GET":
            return render_template("edit_profile.html", profile=profile)

        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        medical_notes = (request.form.get("medical_notes") or "").strip()

        if not name:
            return render_template(
                "edit_profile.html",
                profile=profile,
                error="Name is required.",
            )

        update_doc = {
            "name": name,
            "medical_notes": medical_notes,
            "last_updated": datetime.now(),
            "contact_info": {
                "email": email,
                "phone": phone,
            },
        }

        update_patient_profile(db.mongo_db, patient_id, update_doc)
        return redirect(url_for("patient_detail", patient_id=patient_id))


@app.route("/patient/<patient_id>/metric/<metric_name>")
def patient_metric(patient_id, metric_name):
    with db_session() as db:
        profile = get_patient_profile(db.mongo_db, patient_id)
        history = get_metric_history(db.cassandra_session, patient_id, metric_name, limit=20)
        return render_template(
            "metric_detail.html",
            profile=profile,
            metric_name=metric_name,
            history=history,
        )

@app.route("/base")
def base():
    return render_template("base.html")

if __name__ == "__main__":
    start_risk_score_updater()
    app.run(debug=True, port=5000, use_reloader=False)