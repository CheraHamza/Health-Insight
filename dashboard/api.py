from datetime import datetime
from flask import Blueprint, jsonify, request

from dashboard.db_util import (
    db_session,
    get_daily_health_summary,
    get_metric_history,
    get_patient_alerts,
    get_patient_profile,
    get_recent_alerts,
    get_vitals,
    list_patient_profiles,
    serialize_vitals,
)

api_bp = Blueprint("api", __name__)


@api_bp.route('/api/patient/<patient_id>/vitals')
def api_patient_vitals(patient_id):
	limit = request.args.get('limit', 200)
	with db_session() as db:
		vitals = get_vitals(db.cassandra_session, patient_id, limit=limit)
		return jsonify(serialize_vitals(vitals))


@api_bp.route('/api/patient/<patient_id>/metric/<metric_name>')
def api_patient_metric_history(patient_id, metric_name):
	limit = request.args.get('limit', 20)
	with db_session() as db:
		history = get_metric_history(db.cassandra_session, patient_id, metric_name, limit=limit)
		return jsonify(serialize_vitals(history))


@api_bp.route('/api/alerts/recent')
def api_recent_alerts():
	limit = request.args.get('limit', 10)
	since_minutes = request.args.get('since_minutes', 30)
	with db_session() as db:
		alerts = get_recent_alerts(db.cassandra_session, limit=limit, since_minutes=since_minutes)
		return jsonify(alerts)


@api_bp.route('/api/patient/<patient_id>/alerts')
def api_patient_alerts(patient_id):
	limit = request.args.get('limit', 50)
	since_minutes = request.args.get('since_minutes', 60)
	with db_session() as db:
		alerts = get_patient_alerts(db.cassandra_session, patient_id, limit=limit, since_minutes=since_minutes)
		return jsonify(alerts)


@api_bp.route('/api/patients')
def api_patients():
	with db_session() as db:
		patients = list_patient_profiles(db.mongo_db, projection={"_id": 0})
		return jsonify(patients)


@api_bp.route('/api/patient/<patient_id>')
def api_patient(patient_id):
	with db_session() as db:
		profile = get_patient_profile(db.mongo_db, patient_id, projection={"_id": 0})
		if not profile:
			return jsonify({"error": "not_found"}), 404
		return jsonify(profile)


@api_bp.route('/api/daily-stats')
def api_daily_stats():
	date_str = (request.args.get('date') or '').strip()
	if date_str:
		try:
			stat_date = datetime.strptime(date_str, "%Y-%m-%d").date()
		except ValueError:
			stat_date = datetime.now().date()
	else:
		stat_date = datetime.now().date()

	with db_session() as db:
		rows = get_daily_health_summary(db.cassandra_session, stat_date)
		payload = [
			{
				"stat_date": getattr(r, "stat_date", stat_date).isoformat(),
				"metric_name": getattr(r, "metric_name", None),
				"avg_value": getattr(r, "avg_value", None),
				"min_value": getattr(r, "min_value", None),
				"max_value": getattr(r, "max_value", None),
				"patient_count": getattr(r, "patient_count", None),
			}
			for r in rows
		]
		return jsonify(payload)
