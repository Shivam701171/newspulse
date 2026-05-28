from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional, List
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os, json, httpx, hashlib
from groq import Groq
from dotenv import load_dotenv
from pywebpush import webpush, WebPushException
load_dotenv()

# ─── Config ──────────────────────────────────────────────────────────────────
NEWS_API_KEY  = os.getenv("NEWS_API_KEY", "")
GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
DATABASE_URL  = os.getenv("DATABASE_URL", "sqlite:///./newspulse.db")
FETCH_INTERVAL_MINUTES = int(os.getenv("FETCH_INTERVAL", "30"))
VAPID_PUBLIC  = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_EMAIL   = os.getenv("VAPID_EMAIL", "mailto:test@test.com")

# ─── Database ─────────────────────────────────────────────────────────────────
engine       = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base         = declarative_base()

# ─── Models ──────────────────────────────────────────────────────────────────
class TopicDB(Base):
    __tablename__ = "topics"
    id          = Column(Integer, primary_key=True, index=True)
    device_id   = Column(String, index=True, nullable=False)
    name        = Column(String, nullable=False)
    query       = Column(String, nullable=False)
    emoji       = Column(String, default="📰")
    created_at  = Column(DateTime, default=datetime.utcnow)
    last_fetched = Column(DateTime, nullable=True)
    is_active   = Column(Boolean, default=True)

class UpdateDB(Base):
    __tablename__ = "updates"
    id           = Column(Integer, primary_key=True, index=True)
    topic_id     = Column(Integer, nullable=False)
    device_id    = Column(String, nullable=False)
    title        = Column(String, nullable=False)
    summary      = Column(Text, nullable=False)
    source       = Column(String, nullable=True)
    url          = Column(String, nullable=True)
    published_at = Column(DateTime, nullable=True)
    fetched_at   = Column(DateTime, default=datetime.utcnow)
    is_read      = Column(Boolean, default=False)
    sentiment    = Column(String, default="neutral")  # positive/negative/neutral
    article_hash = Column(String, nullable=True)      # prevent duplicates

Base.metadata.create_all(bind=engine)

# Ensure new tables are created if they don't exist
from sqlalchemy import inspect
inspector = inspect(engine)
class PushSubscription(Base):
    __tablename__ = "push_subscriptions"
    id         = Column(Integer, primary_key=True)
    device_id  = Column(String, index=True)
    endpoint   = Column(String, unique=True)
    p256dh     = Column(String)
    auth       = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

if 'push_subscriptions' not in inspector.get_table_names():
    PushSubscription.__table__.create(bind=engine)

# ─── Schemas ──────────────────────────────────────────────────────────────────
class TopicCreate(BaseModel):
    device_id: str
    name:      str
    query:     str
    emoji:     Optional[str] = "📰"

class TopicOut(BaseModel):
    id:           int
    device_id:    str
    name:         str
    query:        str
    emoji:        str
    created_at:   datetime
    last_fetched: Optional[datetime]
    is_active:    bool
    class Config: from_attributes = True

class UpdateOut(BaseModel):
    id:           int
    topic_id:     int
    title:        str
    summary:      str
    source:       Optional[str]
    url:          Optional[str]
    published_at: Optional[datetime]
    fetched_at:   datetime
    is_read:      bool
    sentiment:    str
    class Config: from_attributes = True

class SubRequest(BaseModel):
    device_id: str
    endpoint:  str
    p256dh:    str
    auth:      str

class MarkReadRequest(BaseModel):
    device_id: str

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="NewsPulse API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:   yield db
    finally: db.close()

def send_push(device_id: str, topic_name: str, count: int):
    db2 = SessionLocal()
    try:
        subs = db2.query(PushSubscription).filter(PushSubscription.device_id == device_id).all()
        for sub in subs:
            try:
                webpush(
                    subscription_info={
                        "endpoint": sub.endpoint,
                        "keys": {"p256dh": sub.p256dh, "auth": sub.auth}
                    },
                    data=json.dumps({
                        "title": f"NewsPulse · {topic_name}",
                        "body":  f"{count} new update{'s' if count > 1 else ''}"
                    }),
                    vapid_private_key=VAPID_PRIVATE,
                    vapid_claims={"sub": VAPID_EMAIL}
                )
            except Exception as e:
                print(f"Push error: {e}")
    finally:
        db2.close()

# ─── News Fetching ────────────────────────────────────────────────────────────
def fetch_news_for_topic(topic_id: int):
    db = SessionLocal()
    try:
        topic = db.query(TopicDB).filter(TopicDB.id == topic_id).first()
        if not topic or not topic.is_active:
            return

        articles = []

        # Try NewsAPI first
        if NEWS_API_KEY:
            try:
                url = "https://newsapi.org/v2/everything"
                params = {
                    "q":        topic.query,
                    "sortBy":   "publishedAt",
                    "pageSize": 10,
                    "language": "en",
                    "apiKey":   NEWS_API_KEY,
                }
                if topic.last_fetched:
                    params["from"] = topic.last_fetched.strftime("%Y-%m-%dT%H:%M:%S")

                resp = httpx.get(url, params=params, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    articles = data.get("articles", [])
            except Exception as e:
                print(f"NewsAPI error: {e}")

        # Fallback: GNews (free, no key needed for basic)
        if not articles:
            try:
                url = f"https://gnews.io/api/v4/search"
                params = {
                    "q":       topic.query,
                    "lang":    "en",
                    "max":     10,
                    "apikey":  os.getenv("GNEWS_API_KEY", ""),
                }
                resp = httpx.get(url, params=params, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    raw = data.get("articles", [])
                    articles = [{
                        "title":       a.get("title"),
                        "description": a.get("description"),
                        "content":     a.get("content"),
                        "url":         a.get("url"),
                        "publishedAt": a.get("publishedAt"),
                        "source":      {"name": a.get("source", {}).get("name")},
                    } for a in raw]
            except Exception as e:
                print(f"GNews error: {e}")

        if not articles:
            topic.last_fetched = datetime.utcnow()
            db.commit()
            return

        # Process and summarize articles
        new_articles = []
        for article in articles:
            title = article.get("title", "")
            if not title or title == "[Removed]":
                continue

            content = article.get("content") or article.get("description") or ""
            art_hash = hashlib.md5((title + content[:100]).encode()).hexdigest()

            # Skip duplicates
            existing = db.query(UpdateDB).filter(UpdateDB.article_hash == art_hash).first()
            if existing:
                continue

            new_articles.append({
                "title":   title,
                "content": content,
                "url":     article.get("url", ""),
                "source":  article.get("source", {}).get("name", "Unknown"),
                "published_at": article.get("publishedAt"),
                "hash":    art_hash,
            })

        if not new_articles or not GROQ_API_KEY:
            topic.last_fetched = datetime.utcnow()
            db.commit()
            return

        # AI summarize batch
        client = Groq(api_key=GROQ_API_KEY)
        for art in new_articles[:5]:  # limit to 5 per run
            try:
                prompt = f"""Topic being tracked: "{topic.name}"
Article title: {art['title']}
Article content: {art['content'][:800]}

Write a sharp 2-3 sentence summary of what's happening and why it matters for someone tracking "{topic.name}".
Then on a new line write exactly one word: POSITIVE, NEGATIVE, or NEUTRAL to indicate sentiment.
Format:
SUMMARY: <your summary>
SENTIMENT: <POSITIVE/NEGATIVE/NEUTRAL>"""

                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                    temperature=0.4,
                )
                text = response.choices[0].message.content.strip()

                summary   = text.split("SENTIMENT:")[0].replace("SUMMARY:", "").strip()
                sentiment = "neutral"
                if "SENTIMENT:" in text:
                    s = text.split("SENTIMENT:")[-1].strip().upper()
                    if "POSITIVE" in s:   sentiment = "positive"
                    elif "NEGATIVE" in s: sentiment = "negative"

                pub_dt = None
                if art["published_at"]:
                    try:
                        pub_dt = datetime.fromisoformat(art["published_at"].replace("Z", "+00:00")).replace(tzinfo=None)
                    except:
                        pass

                update = UpdateDB(
                    topic_id     = topic.id,
                    device_id    = topic.device_id,
                    title        = art["title"],
                    summary      = summary,
                    source       = art["source"],
                    url          = art["url"],
                    published_at = pub_dt,
                    sentiment    = sentiment,
                    article_hash = art["hash"],
                )
                db.add(update)

            except Exception as e:
                print(f"AI summary error: {e}")

        topic.last_fetched = datetime.utcnow()
        db.commit()
        print(f"✓ Fetched news for topic: {topic.name}")
        if new_articles:
            send_push(topic.device_id, topic.name, len(new_articles))

        print(f"✓ Fetched news for topic: {topic.name}")
    except Exception as e:
        print(f"Fetch error for topic {topic_id}: {e}")
    finally:
        db.close()

def fetch_all_active_topics():
    db = SessionLocal()
    try:
        topics = db.query(TopicDB).filter(TopicDB.is_active == True).all()
        topic_ids = [t.id for t in topics]
    finally:
        db.close()

    for tid in topic_ids:
        fetch_news_for_topic(tid)

# ─── Scheduler ────────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_all_active_topics,
    trigger=IntervalTrigger(minutes=FETCH_INTERVAL_MINUTES),
    id="news_fetcher",
    replace_existing=True,
)
scheduler.start()

class PushSubscription(Base):
    __tablename__ = "push_subscriptions"
    id         = Column(Integer, primary_key=True)
    device_id  = Column(String, index=True)
    endpoint   = Column(String, unique=True)
    p256dh     = Column(String)
    auth       = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class SubRequest(BaseModel):
    device_id: str
    endpoint:  str
    p256dh:    str
    auth:      str

@app.post("/api/push/subscribe")
def subscribe(req: SubRequest):
    db = SessionLocal()
    try:
        existing = db.query(PushSubscription).filter(PushSubscription.endpoint == req.endpoint).first()
        if not existing:
            db.add(PushSubscription(**req.dict()))
            db.commit()
        return {"message": "Subscribed"}
    finally:
        db.close()

@app.get("/api/push/vapid-public-key")
def get_vapid_key():
    return {"key": VAPID_PUBLIC}
# ─── Topic Routes ─────────────────────────────────────────────────────────────
@app.get("/api/topics/{device_id}", response_model=List[TopicOut])
def get_topics(device_id: str):
    db = SessionLocal()
    try:
        return db.query(TopicDB).filter(TopicDB.device_id == device_id, TopicDB.is_active == True).order_by(TopicDB.created_at.desc()).all()
    finally:
        db.close()

@app.post("/api/topics", response_model=TopicOut)
def create_topic(topic: TopicCreate, background_tasks: BackgroundTasks):
    db = SessionLocal()
    try:
        # Limit 20 topics per device
        count = db.query(TopicDB).filter(TopicDB.device_id == topic.device_id, TopicDB.is_active == True).count()
        if count >= 20:
            raise HTTPException(status_code=400, detail="Maximum 20 topics allowed")

        db_topic = TopicDB(
            device_id = topic.device_id,
            name      = topic.name.strip(),
            query     = topic.query.strip(),
            emoji     = topic.emoji or "📰",
        )
        db.add(db_topic)
        db.commit()
        db.refresh(db_topic)
        topic_id = db_topic.id
    finally:
        db.close()

    # Fetch immediately in background
    background_tasks.add_task(fetch_news_for_topic, topic_id)
    db2 = SessionLocal()
    try:
        return db2.query(TopicDB).filter(TopicDB.id == topic_id).first()
    finally:
        db2.close()

@app.delete("/api/topics/{topic_id}")
def delete_topic(topic_id: int, device_id: str):
    db = SessionLocal()
    try:
        topic = db.query(TopicDB).filter(TopicDB.id == topic_id, TopicDB.device_id == device_id).first()
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        topic.is_active = False
        db.commit()
        return {"message": "Topic removed"}
    finally:
        db.close()

@app.post("/api/topics/{topic_id}/refresh")
def refresh_topic(topic_id: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(fetch_news_for_topic, topic_id)
    return {"message": "Refresh started"}

# ─── Updates Routes ───────────────────────────────────────────────────────────
@app.get("/api/updates/{device_id}", response_model=List[UpdateOut])
def get_updates(device_id: str, topic_id: Optional[int] = None, limit: int = 50):
    db = SessionLocal()
    try:
        query = db.query(UpdateDB).filter(UpdateDB.device_id == device_id)
        if topic_id:
            query = query.filter(UpdateDB.topic_id == topic_id)
        return query.order_by(UpdateDB.fetched_at.desc()).limit(limit).all()
    finally:
        db.close()

@app.get("/api/updates/{device_id}/unread-count")
def unread_count(device_id: str):
    db = SessionLocal()
    try:
        count = db.query(UpdateDB).filter(UpdateDB.device_id == device_id, UpdateDB.is_read == False).count()
        return {"count": count}
    finally:
        db.close()

@app.put("/api/updates/{update_id}/read")
def mark_read(update_id: int, req: MarkReadRequest):
    db = SessionLocal()
    try:
        update = db.query(UpdateDB).filter(UpdateDB.id == update_id, UpdateDB.device_id == req.device_id).first()
        if update:
            update.is_read = True
            db.commit()
        return {"message": "Marked as read"}
    finally:
        db.close()

@app.put("/api/updates/{device_id}/read-all")
def mark_all_read(device_id: str, topic_id: Optional[int] = None):
    db = SessionLocal()
    try:
        query = db.query(UpdateDB).filter(UpdateDB.device_id == device_id, UpdateDB.is_read == False)
        if topic_id:
            query = query.filter(UpdateDB.topic_id == topic_id)
        query.update({"is_read": True})
        db.commit()
        return {"message": "All marked as read"}
    finally:
        db.close()

# ─── AI Ask ───────────────────────────────────────────────────────────────────
class AskRequest(BaseModel):
    device_id: str
    topic_id:  Optional[int] = None
    question:  str

@app.post("/api/ask")
def ask_ai(req: AskRequest):
    if not GROQ_API_KEY:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY not set")

    db = SessionLocal()
    try:
        query = db.query(UpdateDB).filter(UpdateDB.device_id == req.device_id)
        if req.topic_id:
            query = query.filter(UpdateDB.topic_id == req.topic_id)
        updates = query.order_by(UpdateDB.fetched_at.desc()).limit(30).all()

        if not updates:
            return {"reply": "No news updates found yet. Add a topic and wait for news to be fetched."}

        news_context = "\n\n".join([
            f"• {u.title} ({u.source}, {u.published_at.strftime('%b %d') if u.published_at else 'recent'})\n  {u.summary}"
            for u in updates
        ])

        topic_name = "your tracked topics"
        if req.topic_id:
            topic = db.query(TopicDB).filter(TopicDB.id == req.topic_id).first()
            if topic:
                topic_name = topic.name

        system = f"""You are a sharp news analyst with deep knowledge of {topic_name}.
You have access to the latest news updates the user is tracking.
Answer questions clearly, cite specific developments, and give context.
Be direct and insightful — not just a summary repeater.

Latest news updates:
{news_context}"""

        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": req.question},
            ],
            max_tokens=500,
            temperature=0.6,
        )
        return {"reply": response.choices[0].message.content}
    finally:
        db.close()

# ─── Health ───────────────────────────────────────────────────────────────────
@app.api_route("/", methods=["GET", "HEAD"])
@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok", "service": "NewsPulse"}