import json, os, random
from pathlib import Path

users = [f"user_{i:06d}" for i in range(10000)]
actions = ["login", "view", "click", "purchase", "logout"]

Path("logs").mkdir(exist_ok=True)

for fileno in range(100):
    with open(f"logs/part-{fileno:04d}.jsonl", "w", encoding="utf-8") as f:
        for _ in range(100_000):
            log = {
                "timestamp": "2025-11-27T12:34:56",
                "user_id": random.choice(users),
                "action": random.choice(actions),
                "session_id": random.randint(1, 999999)
            }
            f.write(json.dumps(log, ensure_ascii=False) + "\n")
print("Готово! 10 000 файлів створено в папці logs/")