"""
Telegram Sticker / Emoji Pack Bot  +  Номерные знаки
Токен берётся из переменной окружения BOT_TOKEN.
"""

import io
import json
import logging
import os
import random
import re
import string
import gzip

from PIL import Image, ImageDraw, ImageFont

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    InputSticker,
    LabeledPrice,
    Update,
)
from telegram.constants import StickerFormat, StickerType
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

BOT_TOKEN   = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ── PostgreSQL хранилище паков ────────────────────────────────────────────────

class DB:
    """Простое хранилище паков в PostgreSQL.
    Таблица: user_packs(user_id BIGINT PRIMARY KEY, packs JSONB)
    """
    _conn = None

    @classmethod
    def _get_conn(cls):
        import psycopg2
        if cls._conn is None or cls._conn.closed:
            cls._conn = psycopg2.connect(DATABASE_URL, sslmode="require")
            cls._conn.autocommit = True
        return cls._conn

    @classmethod
    def init(cls):
        if not DATABASE_URL:
            logger.info("DATABASE_URL не задан — паки хранятся только в памяти")
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_packs (
                        user_id BIGINT PRIMARY KEY,
                        packs   JSONB NOT NULL DEFAULT '[]'
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_profiles (
                        user_id       BIGINT PRIMARY KEY,
                        registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        purchases     INTEGER NOT NULL DEFAULT 0,
                        stars_spent   INTEGER NOT NULL DEFAULT 0
                    )
                """)
            logger.info("DB инициализирована ✅")
            cls.init_admin_tables()
        except Exception as e:
            logger.error("DB init error: %s", e)

    @classmethod
    def load_packs(cls, user_id: int) -> list:
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT packs FROM user_packs WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                return row[0] if row else []
        except Exception as e:
            logger.error("DB load_packs error: %s", e)
            return []

    @classmethod
    def save_packs(cls, user_id: int, packs: list):
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_packs (user_id, packs)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET packs = EXCLUDED.packs
                """, (user_id, json.dumps(packs)))
        except Exception as e:
            logger.error("DB save_packs error: %s", e)

    @classmethod
    def init_profile(cls, user_id: int):
        """Создаёт запись профиля если не существует."""
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_profiles (
                        user_id       BIGINT PRIMARY KEY,
                        registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        purchases     INTEGER NOT NULL DEFAULT 0,
                        stars_spent   INTEGER NOT NULL DEFAULT 0
                    )
                """)
                cur.execute("""
                    INSERT INTO user_profiles (user_id)
                    VALUES (%s)
                    ON CONFLICT (user_id) DO NOTHING
                """, (user_id,))
        except Exception as e:
            logger.error("DB init_profile error: %s", e)

    @classmethod
    def get_profile(cls, user_id: int) -> dict:
        """Возвращает профиль пользователя."""
        if not DATABASE_URL:
            return {"registered_at": None, "purchases": 0, "stars_spent": 0}
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT registered_at, purchases, stars_spent FROM user_profiles WHERE user_id = %s",
                    (user_id,)
                )
                row = cur.fetchone()
                if row:
                    return {"registered_at": row[0], "purchases": row[1], "stars_spent": row[2]}
                return {"registered_at": None, "purchases": 0, "stars_spent": 0}
        except Exception as e:
            logger.error("DB get_profile error: %s", e)
            return {"registered_at": None, "purchases": 0, "stars_spent": 0}

    @classmethod
    def record_purchase(cls, user_id: int, stars: int = 1):
        """Увеличивает счётчик покупок и потраченных звёзд."""
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE user_profiles
                    SET purchases   = purchases   + 1,
                        stars_spent = stars_spent + %s
                    WHERE user_id = %s
                """, (stars, user_id))
        except Exception as e:
            logger.error("DB record_purchase error: %s", e)

    @classmethod
    def init_admin_tables(cls):
        """Создаёт таблицы для статистики и настроек."""
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_stats (
                        stat_key   TEXT PRIMARY KEY,
                        stat_value BIGINT NOT NULL DEFAULT 0
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_users (
                        day       DATE NOT NULL,
                        user_id   BIGINT NOT NULL,
                        PRIMARY KEY (day, user_id)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        day        DATE NOT NULL,
                        stat_key   TEXT NOT NULL,
                        stat_value BIGINT NOT NULL DEFAULT 0,
                        PRIMARY KEY (day, stat_key)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_settings (
                        key   TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                        id          SERIAL PRIMARY KEY,
                        send_at     TIMESTAMP NOT NULL,
                        message_text TEXT,
                        photo_file_id TEXT,
                        caption     TEXT,
                        done        BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
                # Дефолтные настройки цен
                for key, val in [
                    ("price_plate_sticker", "1"),
                    ("price_plate_emoji",   "1"),
                    ("maintenance_mode",    "0"),
                ]:
                    cur.execute("""
                        INSERT INTO bot_settings (key, value)
                        VALUES (%s, %s)
                        ON CONFLICT (key) DO NOTHING
                    """, (key, val))
                # Дефолтные счётчики статистики
                for key in ["sticker_packs_created", "emoji_packs_created",
                            "plates_created", "stickers_added", "emoji_added"]:
                    cur.execute("""
                        INSERT INTO bot_stats (stat_key, stat_value)
                        VALUES (%s, 0)
                        ON CONFLICT (stat_key) DO NOTHING
                    """, (key,))
        except Exception as e:
            logger.error("DB init_admin_tables error: %s", e)

    @classmethod
    def increment_stat(cls, key: str, amount: int = 1):
        if not DATABASE_URL:
            return
        import datetime
        today = datetime.date.today().isoformat()
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_stats (stat_key, stat_value) VALUES (%s, %s)
                    ON CONFLICT (stat_key) DO UPDATE
                    SET stat_value = bot_stats.stat_value + EXCLUDED.stat_value
                """, (key, amount))
                cur.execute("""
                    INSERT INTO daily_stats (day, stat_key, stat_value) VALUES (%s, %s, %s)
                    ON CONFLICT (day, stat_key) DO UPDATE
                    SET stat_value = daily_stats.stat_value + EXCLUDED.stat_value
                """, (today, key, amount))
        except Exception as e:
            logger.error("DB increment_stat error: %s", e)

    @classmethod
    def get_stats(cls) -> dict:
        if not DATABASE_URL:
            return {}
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT stat_key, stat_value FROM bot_stats")
                return {row[0]: row[1] for row in cur.fetchall()}
        except Exception as e:
            logger.error("DB get_stats error: %s", e)
            return {}

    @classmethod
    def get_stats_days(cls) -> list:
        """Список дат у которых есть daily_stats, DESC."""
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT day FROM daily_stats ORDER BY day DESC")
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error("DB get_stats_days error: %s", e)
            return []

    @classmethod
    def get_stats_for_day(cls, day) -> dict:
        """Статистика за конкретный день."""
        if not DATABASE_URL:
            return {}
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT stat_key, stat_value FROM daily_stats WHERE day = %s", (day,)
                )
                return {row[0]: row[1] for row in cur.fetchall()}
        except Exception as e:
            logger.error("DB get_stats_for_day error: %s", e)
            return {}

    @classmethod
    def get_daily_users_count(cls, day) -> int:
        """Количество активных пользователей за день."""
        if not DATABASE_URL:
            return 0
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM daily_users WHERE day = %s", (day,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error("DB get_daily_users_count error: %s", e)
            return 0

    @classmethod
    def get_all_users(cls) -> list:
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT up.user_id, up.registered_at, up.purchases, up.stars_spent,
                           (SELECT COUNT(*) FROM user_packs upk WHERE upk.user_id = up.user_id) as has_packs
                    FROM user_profiles up
                    ORDER BY up.registered_at DESC
                """)
                return cur.fetchall()
        except Exception as e:
            logger.error("DB get_all_users error: %s", e)
            return []

    @classmethod
    def get_daily_new_users(cls, day: str) -> int:
        """day — строка 'YYYY-MM-DD'."""
        if not DATABASE_URL:
            return 0
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM daily_users WHERE day = %s", (day,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error("DB get_daily_new_users error: %s", e)
            return 0

    @classmethod
    def get_all_daily_stats(cls) -> list:
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT day, COUNT(*) as cnt
                    FROM daily_users
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT 30
                """)
                return cur.fetchall()
        except Exception as e:
            logger.error("DB get_all_daily_stats error: %s", e)
            return []

    @classmethod
    def get_daily_stats_full(cls, limit: int = 14) -> list:
        """Возвращает список (day, active_users, new_users) за последние N дней."""
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        du.day,
                        COUNT(du.user_id) AS active_users,
                        COUNT(up.user_id) AS new_users
                    FROM daily_users du
                    LEFT JOIN user_profiles up
                        ON up.user_id = du.user_id
                        AND DATE(up.registered_at) = du.day
                    GROUP BY du.day
                    ORDER BY du.day DESC
                    LIMIT %s
                """, (limit,))
                return cur.fetchall()
        except Exception as e:
            logger.error("DB get_daily_stats_full error: %s", e)
            return []

    @classmethod
    def record_daily_user(cls, user_id: int):
        if not DATABASE_URL:
            return
        import datetime
        today = datetime.date.today().isoformat()
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_users (day, user_id) VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (today, user_id))
        except Exception as e:
            logger.error("DB record_daily_user error: %s", e)

    @classmethod
    def get_setting(cls, key: str, default: str = "") -> str:
        if not DATABASE_URL:
            return default
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM bot_settings WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
        except Exception as e:
            logger.error("DB get_setting error: %s", e)
            return default

    @classmethod
    def set_setting(cls, key: str, value: str):
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (key, value))
        except Exception as e:
            logger.error("DB set_setting error: %s", e)

    @classmethod
    def get_all_user_ids(cls) -> list:
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM user_profiles")
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error("DB get_all_user_ids error: %s", e)
            return []

    @classmethod
    def save_scheduled_broadcast(cls, send_at, message_text=None, photo_file_id=None, caption=None):
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scheduled_broadcasts (send_at, message_text, photo_file_id, caption)
                    VALUES (%s, %s, %s, %s)
                """, (send_at, message_text, photo_file_id, caption))
        except Exception as e:
            logger.error("DB save_scheduled_broadcast error: %s", e)

    @classmethod
    def get_pending_broadcasts(cls):
        if not DATABASE_URL:
            return []
        import datetime
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, send_at, message_text, photo_file_id, caption
                    FROM scheduled_broadcasts
                    WHERE done = FALSE AND send_at <= %s
                """, (datetime.datetime.utcnow(),))
                return cur.fetchall()
        except Exception as e:
            logger.error("DB get_pending_broadcasts error: %s", e)
            return []

    @classmethod
    def mark_broadcast_done(cls, broadcast_id: int):
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("UPDATE scheduled_broadcasts SET done = TRUE WHERE id = %s", (broadcast_id,))
        except Exception as e:
            logger.error("DB mark_broadcast_done error: %s", e)

    @classmethod
    def add_maintenance_waiter(cls, user_id: int):
        """Запоминает пользователя, писавшего во время тех. перерыва."""
        if not DATABASE_URL:
            return
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS maintenance_waiters (
                        user_id BIGINT PRIMARY KEY
                    )
                """)
                cur.execute("""
                    INSERT INTO maintenance_waiters (user_id)
                    VALUES (%s) ON CONFLICT DO NOTHING
                """, (user_id,))
        except Exception as e:
            logger.error("DB add_maintenance_waiter error: %s", e)

    @classmethod
    def pop_maintenance_waiters(cls) -> list:
        """Возвращает и очищает список ждущих пользователей."""
        if not DATABASE_URL:
            return []
        try:
            conn = cls._get_conn()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS maintenance_waiters (
                        user_id BIGINT PRIMARY KEY
                    )
                """)
                cur.execute("SELECT user_id FROM maintenance_waiters")
                users = [row[0] for row in cur.fetchall()]
                cur.execute("DELETE FROM maintenance_waiters")
                return users
        except Exception as e:
            logger.error("DB pop_maintenance_waiters error: %s", e)
            return []


# ── Константы админа ──────────────────────────────────────────────────────────

ADMIN_USERNAME = "tntks"

def is_admin(update: Update) -> bool:
    user = upd
