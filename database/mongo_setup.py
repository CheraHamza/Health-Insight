from pymongo import MongoClient, WriteConcern
from datetime import datetime

def init_mongo():
    # Connect to MongoDB replica set
    client = MongoClient(
        'mongodb://mongodb-1:27017,mongodb-2:27017,mongodb-3:27017/',
        replicaSet='rs0',
        readPreference='primaryPreferred'
    )
    db = client.health_insight
    
    # Patient Profiles with Quorum Consistency Requirement
    profiles = db.get_collection(
        "patient_profiles", 
        write_concern=WriteConcern(w="majority", j=True)
    )

    # clear old data
    print("🧹 Clearing old patient profiles...")
    profiles.delete_many({})
    
    # Seed data

    sample_patients = [
        {
            "patient_id": "P-001",
            "name": "Hamza Chera",
            "contact_info": {"email": "hamza.chera@example.com", "phone": "0123456789"},
            "medical_notes": "Asthma (mild, well-controlled). Uses rescue inhaler as needed.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 11, 5, 9, 30),
        },
        {
            "patient_id": "P-002",
            "name": "Mehdi Chera",
            "contact_info": {"email": "mehdi.chera@example.com", "phone": "0123456789"},
            "medical_notes": "Type 2 diabetes; on metformin. Last A1C: 7.1%.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 10, 21, 14, 5),
        },
        {
            "patient_id": "P-003",
            "name": "Farah fulla",
            "contact_info": {"email": "farah.fulla@example.com", "phone": "0123456789"},
            "medical_notes": "No chronic conditions. Family history of hypertension.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 9, 12, 11, 45),
        },
        {
            "patient_id": "P-004",
            "name": "Aya zine",
            "contact_info": {"email": "aya.zine@example.com", "phone": "0123456789"},
            "medical_notes": "Post-op knee replacement (2024-07). Undergoing PT.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 12, 2, 16, 10),
        },
        {
            "patient_id": "P-005",
            "name": "fateh allal",
            "contact_info": {"email": "fateh.allal@example.com", "phone": "0123456789"},
            "medical_notes": "Seasonal allergies. Carries epinephrine auto-injector.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 8, 30, 10, 20),
        },
        {
            "patient_id": "P-006",
            "name": "marwa benz",
            "contact_info": {"email": "marwa.benz@example.com", "phone": "0123456789"},
            "medical_notes": "Hypertension; on ACE inhibitor. Home BP avg 128/82.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 12, 10, 8, 15),
        },
        {
            "patient_id": "P-007",
            "name": "maya shorouq",
            "contact_info": {"email": "maya.shorouq@example.com", "phone": "0123456789"},
            "medical_notes": "Migraines 2–3/mo. Uses triptan; advised hydration and sleep hygiene.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 11, 18, 13, 0),
        },
        {
            "patient_id": "P-008",
            "name": "Nasrlah Ali",
            "contact_info": {"email": "nasrlah.ali@example.com", "phone": "0123456789"},
            "medical_notes": "Hyperlipidemia; on statin. LDL 98 mg/dL (last check).",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 7, 22, 9, 50),
        },
        {
            "patient_id": "P-009",
            "name": "Meriem khaled",
            "contact_info": {"email": "meriem.khaled@example.com", "phone": "0123456789"},
            "medical_notes": "No known chronic conditions. Training for half marathon.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 6, 14, 15, 40),
        },
        {
            "patient_id": "P-010",
            "name": "mohamed amine",
            "contact_info": {"email": "mohamed.amine@example.com", "phone": "0123456789"},
            "medical_notes": "GERD managed with PPI. Avoids late meals and caffeine.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 12, 1, 12, 5),
        },
        {
            "patient_id": "P-011",
            "name": "Imene Hani",
            "contact_info": {"email": "imene.hani@example.com", "phone": "0123456789"},
            "medical_notes": "Pregnancy (24 weeks). Regular prenatal care; no complications.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 11, 25, 10, 10),
        },
        {
            "patient_id": "P-012",
            "name": "Alex jones",
            "contact_info": {"email": "alex.jones@example.com", "phone": "0123456789"},
            "medical_notes": "Chronic low back pain. In PT; uses NSAIDs sparingly.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 10, 3, 17, 25),
        },
        {
            "patient_id": "P-013",
            "name": "Joe Rogan",
            "contact_info": {"email": "joe.rogan@example.com", "phone": "0123456789"},
            "medical_notes": "Hashimoto's thyroiditis; stable on levothyroxine.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 9, 29, 8, 40),
        },
        {
            "patient_id": "P-014",
            "name": "Elijah Rivera",
            "contact_info": {"email": "elijah.rivera@example.com", "phone": "0123456789"},
            "medical_notes": "Obstructive sleep apnea; compliant with CPAP (6.5 hrs/night).",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 12, 14, 22, 15),
        },
        {
            "patient_id": "P-015",
            "name": "Evelyn Scott",
            "contact_info": {"email": "evelyn.scott@example.com", "phone": "0123456789"},
            "medical_notes": "Depression in remission; on SSRI. Attends therapy monthly.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 8, 18, 14, 55),
        },
        {
            "patient_id": "P-016",
            "name": "William Perez",
            "contact_info": {"email": "william.perez@example.com", "phone": "0123456789"},
            "medical_notes": "Coronary artery disease; stent placed 2023. On dual antiplatelet therapy.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 12, 20, 9, 5),
        },
        {
            "patient_id": "P-017",
            "name": "Abigail Moore",
            "contact_info": {"email": "abigail.moore@example.com", "phone": "0123456789"},
            "medical_notes": "Iron-deficiency anemia; on oral iron. Recheck CBC in 6 weeks.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 7, 11, 11, 30),
        },
        {
            "patient_id": "P-018",
            "name": "Henry Turner",
            "contact_info": {"email": "henry.turner@example.com", "phone": "0123456789"},
            "medical_notes": "COPD (moderate). Uses LAMA/LABA inhaler; former smoker.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 11, 2, 7, 50),
        },
        {
            "patient_id": "P-019",
            "name": "Emily Walker",
            "contact_info": {"email": "emily.walker@example.com", "phone": "0123456789"},
            "medical_notes": "Celiac disease; adheres to gluten-free diet. No recent flares.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 10, 8, 15, 5),
        },
        {
            "patient_id": "P-020",
            "name": "Oliver Adams",
            "contact_info": {"email": "oliver.adams@example.com", "phone": "0123456789"},
            "medical_notes": "Chronic kidney disease (Stage 3a). eGFR 52 mL/min; monitored quarterly.",
            "risk_score": 0.0,
            "last_updated": datetime(2024, 12, 5, 18, 35),
        },
    ]
    profiles.insert_many(sample_patients)
    print(f"Successfully seeded {len(sample_patients)} patient profiles into MongoDB.")

if __name__ == "__main__":
    init_mongo()