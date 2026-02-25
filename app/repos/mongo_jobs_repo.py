# app/repos/mongo_jobs_repo.py

# from __future__ import annotations
# from datetime import datetime
# from typing import Optional
# from pymongo.collection import Collection
#
#
# class MongoJobsRepository:
#     def __init__(self, col: Collection):
#         self.col = col
#         self.col.create_index("job_id", unique=True)
#         self.col.create_index("uuid")
#
#     def create(self, *, job_id: str, uuid: str, payload: dict) -> None:
#         now = datetime.utcnow()
#         self.col.update_one(
#             {"job_id": job_id},
#             {"$setOnInsert": {"job_id": job_id, "uuid": uuid, "payload": payload, "status": "queued", "created_at": now,
#                              "updated_at": now, "error": None, "attempt": 0}},
#             upsert=True,
#         )
#
#     def set_status(self, job_id: str, *, status: str, attempt: int, error: Optional[str] = None) -> None:
#         now = datetime.utcnow()
#         self.col.update_one(
#             {"job_id": job_id},
#             {"$set": {"status": status, "attempt": attempt, "error": error, "updated_at": now}},
#         )
#
#     def get(self, job_id: str) -> Optional[dict]:
#         return self.col.find_one({"job_id": job_id}, {"_id": 0})


from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional
from pymongo.collection import Collection


class MongoJobsRepository:
    def __init__(self, col: Collection, *, ttl_seconds: int):
        self.col = col
        self.ttl_seconds = int(ttl_seconds)

        self.col.create_index("job_id", unique=True)
        self.col.create_index("uuid")

        # TTL index: Mongo удалит документ, когда expires_at <= now
        # expireAfterSeconds=0 => удаляем сразу по наступлению expires_at
        self.col.create_index("expires_at", expireAfterSeconds=0)

    def create(self, *, job_id: str, uuid: str, payload: dict) -> None:
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=self.ttl_seconds)

        self.col.update_one(
            {"job_id": job_id},
            {"$setOnInsert": {
                "job_id": job_id,
                "uuid": uuid,
                "payload": payload,
                "status": "queued",
                "created_at": now,
                "updated_at": now,
                "expires_at": expires_at,   # <-- важно для TTL
                "error": None,
                "attempt": 0,
            }},
            upsert=True,
        )

    def set_status(self, job_id: str, *, status: str, attempt: int, error: Optional[str] = None) -> None:
        now = datetime.utcnow()
        self.col.update_one(
            {"job_id": job_id},
            {"$set": {"status": status, "attempt": attempt, "error": error, "updated_at": now}},
        )

    def get(self, job_id: str) -> Optional[dict]:
        return self.col.find_one({"job_id": job_id}, {"_id": 0})

    def count(self) -> int:
        return int(self.col.count_documents({}))

    def count_by_status(self) -> dict:
        # простая агрегация по status
        pipeline = [
            {"$group": {"_id": "$status", "n": {"$sum": 1}}},
        ]
        res = {"queued": 0, "running": 0, "done": 0, "failed": 0}
        for row in self.col.aggregate(pipeline):
            res[str(row["_id"])] = int(row["n"])
        return res

    def find_by_uuid(self, uuid: str, *, limit: int = 50) -> list[dict]:
        cur = self.col.find({"uuid": uuid}, {"_id": 0}).sort("created_at", -1).limit(int(limit))
        return list(cur)