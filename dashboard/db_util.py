from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Iterable, List, Optional, Sequence

import pymongo
from cassandra.cluster import Cluster

MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
CASSANDRA_HOST: Sequence[str] = ["127.0.0.1"]
CASSANDRA_PORT: int = 9042
KEYSPACE = "health_insight"


@dataclass
class DBHandles:
    mongo_client: pymongo.MongoClient
    mongo_db: pymongo.database.Database
    cassandra_cluster: Cluster
    cassandra_session: object

    def close(self) -> None:
        try:
            self.mongo_client.close()
        finally:
            try:
                self.cassandra_cluster.shutdown()
            except Exception:
                # Cassandra shutdown can throw if cluster already closed; keep quiet.
                pass


def get_mongo_client(uri: str = MONGO_URI) -> pymongo.MongoClient:
    return pymongo.MongoClient(uri, readPreference="primaryPreferred")


def get_cassandra_session(
    hosts: Sequence[str] = CASSANDRA_HOST,
    port: int = CASSANDRA_PORT,
    keyspace: str = KEYSPACE,
):
    cluster = Cluster(hosts, port=port)
    session = cluster.connect(keyspace)
    return cluster, session


def get_db_handles() -> DBHandles:
    m_client = get_mongo_client()
    m_db = m_client.health_insight
    c_cluster, c_session = get_cassandra_session()
    return DBHandles(m_client, m_db, c_cluster, c_session)


@contextmanager
def db_session():
    handles = get_db_handles()
    try:
        yield handles
    finally:
        handles.close()


def _strip_timezone(dt: datetime) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def serialize_vitals(rows: Iterable) -> List[dict]:
    return [
        {
            "metric_name": r.metric_name,
            "value": float(r.value),
            "unit": r.unit,
            "timestamp": r.timestamp.isoformat(sep=" ", timespec="seconds")
            if hasattr(r.timestamp, "isoformat")
            else str(r.timestamp),
        }
        for r in rows
    ]


def serialize_alerts(rows: Iterable) -> List[dict]:
    payload = []
    for r in rows:
        ts = _strip_timezone(getattr(r, "timestamp", None))
        payload.append(
            {
                "alert_id": str(getattr(r, "alert_id", "")),
                "patient_id": getattr(r, "patient_id", None),
                "metric_name": getattr(r, "metric_name", None),
                "observed_value": float(getattr(r, "observed_value", 0.0) or 0.0),
                "alert_type": getattr(r, "alert_type", None),
                "timestamp": ts.isoformat(sep=" ", timespec="seconds") if ts else None,
                "status": (getattr(r, "status", "") or "").upper(),
            }
        )
    return payload


def get_vitals(session, patient_id: str, limit: int = 200):
    limit = max(1, min(int(limit), 500))
    query = (
        f"SELECT metric_name, value, unit, timestamp "
        f"FROM patient_vitals WHERE patient_id=%s LIMIT {limit}"
    )
    rows = session.execute(query, (patient_id,))
    return sorted(list(rows), key=lambda x: x.timestamp, reverse=True)


def get_metric_history(session, patient_id: str, metric_name: str, limit: int = 200):
    limit = max(1, min(int(limit), 500))
    query = (
        f"SELECT metric_name, value, unit, timestamp FROM patient_vitals "
        f"WHERE patient_id=%s AND metric_name=%s LIMIT {limit} ALLOW FILTERING"
    )
    rows = session.execute(query, (patient_id, metric_name))
    return sorted(list(rows), key=lambda x: x.timestamp, reverse=True)


def get_recent_alerts(session, *, limit: int = 20, since_minutes: int = 30, scan_limit: int = 500):
    limit = max(1, min(int(limit), 100))
    scan_limit = max(limit, min(int(scan_limit), 2000))
    since_minutes = max(1, min(int(since_minutes), 24 * 60))
    cutoff = datetime.now() - timedelta(minutes=since_minutes)

    rows = None
    try:
        day = datetime.now().date()
        query = (
            "SELECT alert_id, patient_id, metric_name, observed_value, alert_type, timestamp, status "
            "FROM health_alerts_by_day WHERE day=%s ORDER BY timestamp DESC LIMIT %s"
        )
        rows = session.execute(query, (day, scan_limit))
    except Exception:
        query = (
            "SELECT alert_id, patient_id, metric_name, observed_value, alert_type, timestamp, status "
            f"FROM health_alerts LIMIT {scan_limit}"
        )
        rows = session.execute(query)

    items = []
    for r in rows:
        ts = _strip_timezone(getattr(r, "timestamp", None))
        if ts is None or ts < cutoff:
            continue
        status = (getattr(r, "status", "") or "").upper()
        if status != "ACTIVE":
            continue
        items.append(
            {
                "alert_id": str(getattr(r, "alert_id", "")),
                "patient_id": getattr(r, "patient_id", None),
                "metric_name": getattr(r, "metric_name", None),
                "observed_value": float(getattr(r, "observed_value", 0.0) or 0.0),
                "alert_type": getattr(r, "alert_type", None),
                "timestamp": ts.isoformat(sep=" ", timespec="seconds"),
                "status": status,
            }
        )

    items.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    return items[:limit]


def get_patient_alerts(
    session,
    patient_id: str,
    *,
    limit: int = 50,
    since_minutes: int = 60,
):
    """Return recent alerts for a single patient using the by-patient table."""
    limit = max(1, min(int(limit), 200))
    since_minutes = max(1, min(int(since_minutes), 24 * 60))
    cutoff = datetime.now() - timedelta(minutes=since_minutes)

    query = (
        f"SELECT alert_id, patient_id, metric_name, observed_value, alert_type, timestamp, status "
        f"FROM health_alerts_by_patient WHERE patient_id=%s LIMIT {limit}"
    )
    rows = session.execute(query, (patient_id,))

    items = []
    for r in rows:
        ts = _strip_timezone(getattr(r, "timestamp", None))
        if ts is None or ts < cutoff:
            continue
        status = (getattr(r, "status", "") or "").upper()
        items.append(
            {
                "alert_id": str(getattr(r, "alert_id", "")),
                "patient_id": getattr(r, "patient_id", None),
                "metric_name": getattr(r, "metric_name", None),
                "observed_value": float(getattr(r, "observed_value", 0.0) or 0.0),
                "alert_type": getattr(r, "alert_type", None),
                "timestamp": ts.isoformat(sep=" ", timespec="seconds") if ts else None,
                "status": status,
            }
        )

    items.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    return items


def get_recent_active_alert_count(session, patient_id: str, *, now_utc: datetime) -> int:
    cutoff = now_utc - timedelta(minutes=30)
    query = (
        "SELECT timestamp, status FROM health_alerts_by_patient WHERE patient_id=%s LIMIT 50"
    )
    try:
        rows = session.execute(query, (patient_id,))
    except Exception:
        fallback = (
            "SELECT timestamp, status FROM health_alerts WHERE patient_id=%s "
            "ALLOW FILTERING LIMIT 50"
        )
        rows = session.execute(fallback, (patient_id,))

    count = 0
    for r in rows:
        ts = _strip_timezone(getattr(r, "timestamp", None))
        if ts is None or ts < cutoff:
            continue
        if (getattr(r, "status", "") or "").upper() == "ACTIVE":
            count += 1
    return count


def get_daily_health_summary(session, stat_date: date):
    query = (
        "SELECT stat_date, metric_name, avg_value, min_value, max_value, patient_count "
        "FROM daily_health_summary WHERE stat_date=%s"
    )
    rows = session.execute(query, (stat_date,))
    items = list(rows)
    items.sort(key=lambda r: getattr(r, "metric_name", "") or "")
    return items


def list_patient_profiles(m_db, *, projection=None):
    projection = projection or {"_id": 0}
    return list(m_db.patient_profiles.find({}, projection).sort("patient_id", 1))


def get_patient_profile(m_db, patient_id: str, *, projection=None):
    projection = projection or {"_id": 0}
    return m_db.patient_profiles.find_one({"patient_id": patient_id}, projection)


def update_patient_profile(m_db, patient_id: str, update_doc: dict) -> None:
    profiles_collection = m_db.get_collection(
        "patient_profiles",
        write_concern=pymongo.WriteConcern(w="majority", j=True),
    )
    profiles_collection.update_one({"patient_id": patient_id}, {"$set": update_doc})

