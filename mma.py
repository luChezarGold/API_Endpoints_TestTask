import random
import time
import asyncio
import requests
from datetime import datetime
from typing import Dict
from threading import Thread

from fastapi import FastAPI, Depends
from sqlalchemy import Column, Integer, String, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

import uvicorn

DATABASE_URL = "sqlite:///./tasks.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Task(Base):
    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True, index=True)
    status = Column(String, default="In Queue")
    create_time = Column(Float, default=lambda: time.time())
    start_time = Column(Float, nullable=True)
    exec_time = Column(Float, nullable=True)


Base.metadata.create_all(bind=engine)

task_queue = []
active_tasks = 0
MAX_CONCURRENT_TASKS = 2
lock = asyncio.Lock()

app = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/add_task")
async def add_task(db: Session = Depends(get_db)):
    new_task = Task()
    db.add(new_task)
    db.commit()
    db.refresh(new_task)

    task_queue.append(new_task.id)
    asyncio.create_task(process_queue())

    return {"task_id": new_task.id}


@app.get("/task_status/{task_id}")
def get_task_status(task_id: int, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        return {"error": "Task not found"}

    return {
        "status": task.status,
        "create_time": datetime.fromtimestamp(task.create_time).isoformat() if task.create_time else None,
        "start_time": datetime.fromtimestamp(task.start_time).isoformat() if task.start_time else None,
        "time_to_execute": task.exec_time,
    }


async def process_queue():
    global active_tasks

    async with lock:
        while task_queue and active_tasks < MAX_CONCURRENT_TASKS:
            task_id = task_queue.pop(0)

            db = SessionLocal()
            try:
                task = db.query(Task).filter(Task.id == task_id).first()
                if task:
                    task.status = "Run"
                    task.start_time = time.time()
                    db.commit()
                    active_tasks += 1
                    asyncio.create_task(execute_task(task_id))
            finally:
                db.close()


@app.get("/task_status")
def get_all_tasks(db: Session = Depends(get_db)):
    tasks = db.query(Task).all()
    return [{"id": task.id, "status": task.status, "create_time": task.create_time,
             "start_time": task.start_time, "time_to_execute": task.exec_time} for task in tasks]


async def execute_task(task_id: int):
    global active_tasks
    exec_time = random.randint(1, 10)
    await asyncio.sleep(exec_time)

    db = SessionLocal()
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        if task:
            task.status = "Completed"
            task.exec_time = exec_time
            db.commit()
    finally:
        db.close()

    async with lock:
        active_tasks -= 1
        asyncio.create_task(process_queue())


def tst():
    base_url = "http://127.0.0.1:8000"
    num_tasks = 10
    task_ids = []

    print("=== Тест 1: Создание множества задач ===")
    for _ in range(num_tasks):
        response = requests.post(f"{base_url}/add_task")
        if response.status_code == 200:
            task_id = response.json().get("task_id")
            task_ids.append(task_id)
            print(f"Задача создана, ID: {task_id}")
        else:
            print(f"Ошибка создания задачи: {response.text}")

    print("\n=== Тест 2: Отслеживание статусов задач ===")
    completed_tasks = set()

    while len(completed_tasks) < num_tasks:
        for task_id in task_ids:
            if task_id in completed_tasks:
                continue

            response = requests.get(f"{base_url}/task_status/{task_id}")
            if response.status_code == 200:
                status = response.json().get("status")
                if status == "Completed":
                    completed_tasks.add(task_id)
                    print(f"✅ Задача {task_id} завершена!")
                else:
                    print(f"Задача {task_id} в статусе {status}...")
            else:
                print(f"Ошибка получения статуса задачи {task_id}: {response.text}")

        time.sleep(2)

    print("\n=== Статусы всех задач в таблице ===")
    response = requests.get(f"{base_url}/task_status")
    if response.status_code == 200:
        tasks = response.json()
        for task in tasks:
            task_id = task['id']
            status = task['status']
            create_time = task['create_time']
            start_time = task['start_time']
            exec_time = task['time_to_execute']
            print(f"Задача ID: {task_id}, Статус: {status}, Время создания: {create_time}, "
                  f"Время старта: {start_time}, Время выполнения: {exec_time}")
    else:
        print("Ошибка получения всех задач:", response.text)

    print("\nВсе задачи выполнены!")


if __name__ == "__main__":
    server_thread = Thread(target=lambda: uvicorn.run(app, host="127.0.0.1", port=8000))
    server_thread.daemon = True
    server_thread.start()

    time.sleep(1)

    tst()
