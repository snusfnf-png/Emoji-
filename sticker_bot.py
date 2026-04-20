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
import colorsys
import gzip
import numpy as np

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
    user = update.effective_user
    return user is not None and user.username == ADMIN_USERNAME

def is_maintenance() -> bool:
    return DB.get_setting("maintenance_mode", "0") == "1"


(
    MAIN_MENU,        # 0
    CHOOSE_TYPE,      # 1
    PACK_NAME,        # 2
    PACK_LINK,        # 3
    ADDING_STICKER,   # 4
    WAITING_EMOJI,    # 5
    PACK_SELECTED,    # 6
    ADD_STICKER_FILE, # 7
    ADD_STICKER_EMOJI,# 8
    DELETE_STICKER,   # 9
    CHANGE_ICON,      # 10
    PLATE_COUNTRY,    # 11
    PLATE_REGION,     # 12
    PLATE_INPUT,      # 13
    ADAPTIVE_CONFIRM, # 14  — подтверждение адаптивного эмодзи-пака
    RENAMING_PACK,    # 15  — переименование пака
    PLATE_PACK_NAME,  # 16  — ввод названия пака для номера
    PLATE_PACK_LINK,  # 17  — ввод ссылки пака для номера
    # ── Копирование пака ──
    COPY_PACK_LINK,       # 18 — ожидание ссылки на пак для копирования
    COPY_PACK_TYPE,       # 19 — выбор типа нового пака (стикер/эмодзи/добавить в)
    COPY_PACK_SELECT,     # 20 — выбор существующего пака для добавления
    # ── Админ ──
    ADMIN_MENU,           # 21
    ADMIN_BROADCAST,      # 22
    ADMIN_SCHEDULED_TIME, # 23
    ADMIN_SCHEDULED_MSG,  # 24
    ADMIN_SET_PRICE,      # 25
    ADMIN_DAILY_DATE,     # 26
    # ── Покраска стикеров ──
    PAINT_SCOPE,          # 27 — один стикер или весь пак
    PAINT_COLOR,          # 28 — выбор цвета
    PAINT_HEX,            # 29 — ввод HEX
) = range(30)

MAX_STICKER_BYTES = 500 * 1024
MAX_INPUT_BYTES   = 10 * 1024 * 1024

# ── Шрифты: автоматический поиск / загрузка ──────────────────────────────────

def _find_or_download_fonts():
    import urllib.request

    candidates_bold = [
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    candidates_reg = [
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arial.ttf",
    ]

    bold = next((p for p in candidates_bold if os.path.exists(p)), None)
    reg  = next((p for p in candidates_reg  if os.path.exists(p)), None)
    if bold and reg:
        return bold, reg

    font_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")
    os.makedirs(font_dir, exist_ok=True)
    dl_bold = os.path.join(font_dir, "Roboto-Bold.ttf")
    dl_reg  = os.path.join(font_dir, "Roboto-Regular.ttf")

    for path, url in [
        (dl_bold, "https://github.com/googlefonts/roboto/raw/main/src/hinted/Roboto-Bold.ttf"),
        (dl_reg,  "https://github.com/googlefonts/roboto/raw/main/src/hinted/Roboto-Regular.ttf"),
    ]:
        if not os.path.exists(path):
            try:
                logger.info("Скачиваю шрифт: %s", path)
                urllib.request.urlretrieve(url, path)
            except Exception as e:
                logger.warning("Не удалось скачать шрифт: %s", e)

    return (bold or dl_bold), (reg or dl_reg)


FONT_BOLD, FONT_REG = _find_or_download_fonts()


def _find_or_download_kz_flag() -> str | None:
    """Скачивает PNG флага Казахстана и возвращает путь к файлу."""
    import urllib.request

    font_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")
    os.makedirs(font_dir, exist_ok=True)
    flag_path = os.path.join(font_dir, "kz_flag.png")
    if os.path.exists(flag_path) and os.path.getsize(flag_path) > 1000:
        return flag_path

    urls = [
        "https://flagcdn.com/w160/kz.png",
        "https://raw.githubusercontent.com/hampusborgos/country-flags/main/png250px/kz.png",
        "https://raw.githubusercontent.com/twitter/twemoji/master/assets/72x72/1f1f0-1f1ff.png",
    ]
    for url in urls:
        try:
            logger.info("Скачиваю флаг КЗ из %s", url)
            urllib.request.urlretrieve(url, flag_path)
            logger.info("Флаг КЗ успешно скачан: %s", flag_path)
            return flag_path
        except Exception as e:
            logger.warning("Не удалось скачать флаг КЗ (%s): %s", url, e)
    return None


KZ_FLAG_PATH = _find_or_download_kz_flag()
# ── Данные регионов ───────────────────────────────────────────────────────────

REGIONS = {
    "RU": {
        "01":"Респ. Адыгея","101":"Респ. Адыгея",
        "02":"Респ. Башкортостан","102":"Респ. Башкортостан","702":"Респ. Башкортостан",
        "03":"Респ. Бурятия","103":"Респ. Бурятия",
        "04":"Респ. Алтай",
        "05":"Респ. Дагестан","105":"Респ. Дагестан",
        "06":"Респ. Ингушетия",
        "07":"Кабардино-Балкарская Респ.",
        "08":"Респ. Калмыкия",
        "09":"Карачаево-Черкесская Респ.",
        "10":"Респ. Карелия",
        "11":"Респ. Коми",
        "12":"Респ. Марий Эл",
        "13":"Респ. Мордовия","113":"Респ. Мордовия",
        "14":"Респ. Саха (Якутия)",
        "15":"Респ. Северная Осетия",
        "16":"Респ. Татарстан","116":"Респ. Татарстан","716":"Респ. Татарстан",
        "17":"Респ. Тыва",
        "18":"Удмуртская Респ.",
        "19":"Респ. Хакасия",
        "21":"Чувашская Респ.","121":"Чувашская Респ.",
        "22":"Алтайский край","122":"Алтайский край",
        "23":"Краснодарский край","93":"Краснодарский край","123":"Краснодарский край","193":"Краснодарский край",
        "24":"Красноярский край","84":"Красноярский край","88":"Красноярский край","124":"Красноярский край",
        "25":"Приморский край","125":"Приморский край",
        "26":"Ставропольский край","126":"Ставропольский край",
        "27":"Хабаровский край",
        "28":"Амурская обл.",
        "29":"Архангельская обл.",
        "30":"Астраханская обл.",
        "31":"Белгородская обл.",
        "32":"Брянская обл.",
        "33":"Владимирская обл.",
        "34":"Волгоградская обл.","134":"Волгоградская обл.",
        "35":"Вологодская обл.",
        "36":"Воронежская обл.","136":"Воронежская обл.",
        "37":"Ивановская обл.",
        "38":"Иркутская обл.","138":"Иркутская обл.",
        "39":"Калининградская обл.","91":"Калининградская обл.",
        "40":"Калужская обл.",
        "41":"Камчатский край",
        "42":"Кемеровская обл.","142":"Кемеровская обл.",
        "43":"Кировская обл.",
        "44":"Костромская обл.",
        "45":"Курганская обл.",
        "46":"Курская обл.",
        "47":"Ленинградская обл.","147":"Ленинградская обл.",
        "48":"Липецкая обл.",
        "49":"Магаданская обл.",
        "50":"Московская обл.","90":"Московская обл.","150":"Московская обл.","190":"Московская обл.",
        "250":"Московская обл.","550":"Московская обл.","750":"Московская обл.","790":"Московская обл.",
        "51":"Мурманская обл.",
        "52":"Нижегородская обл.","152":"Нижегородская обл.","252":"Нижегородская обл.",
        "53":"Новгородская обл.",
        "54":"Новосибирская обл.","154":"Новосибирская обл.",
        "55":"Омская обл.","155":"Омская обл.",
        "56":"Оренбургская обл.","156":"Оренбургская обл.",
        "57":"Орловская обл.",
        "58":"Пензенская обл.",
        "59":"Пермский край","81":"Пермский край","159":"Пермский край",
        "60":"Псковская обл.",
        "61":"Ростовская обл.","161":"Ростовская обл.","761":"Ростовская обл.",
        "62":"Рязанская обл.",
        "63":"Самарская обл.","163":"Самарская обл.","763":"Самарская обл.",
        "64":"Саратовская обл.","164":"Саратовская обл.",
        "65":"Сахалинская обл.",
        "66":"Свердловская обл.","96":"Свердловская обл.","196":"Свердловская обл.",
        "67":"Смоленская обл.",
        "68":"Тамбовская обл.",
        "69":"Тверская обл.",
        "70":"Томская обл.",
        "71":"Тульская обл.",
        "72":"Тюменская обл.","172":"Тюменская обл.",
        "73":"Ульяновская обл.",
        "74":"Челябинская обл.","174":"Челябинская обл.","774":"Челябинская обл.",
        "75":"Забайкальский край","80":"Забайкальский край",
        "76":"Ярославская обл.",
        "77":"г. Москва","97":"г. Москва","99":"г. Москва","177":"г. Москва",
        "197":"г. Москва","199":"г. Москва","777":"г. Москва","797":"г. Москва",
        "799":"г. Москва","977":"г. Москва",
        "78":"г. Санкт-Петербург","98":"г. Санкт-Петербург","178":"г. Санкт-Петербург","198":"г. Санкт-Петербург",
        "79":"Еврейская авт. обл.",
        "82":"Респ. Крым",
        "83":"Ненецкий авт. окр.",
        "85":"Запорожская обл.","185":"Запорожская обл.",
        "86":"Ханты-Мансийский авт. окр.","186":"Ханты-Мансийский авт. окр.",
        "87":"Чукотский авт. окр.",
        "89":"Ямало-Ненецкий авт. окр.",
        "92":"г. Севастополь",
        "95":"Чеченская Респ.",
        "180":"Донецкая Нар. Респ.",
        "181":"Луганская Нар. Респ.",
        "184":"Херсонская обл.",
    },
    "UA": {
        "AA":"г. Киев","КА":"г. Киев",
        "AI":"Киевская обл.","КI":"Киевская обл.",
        "BC":"Львовская обл.","КС":"Львовская обл.",
        "AC":"Волынская обл. (Луцк)",
        "AO":"Закарпатская обл. (Ужгород)","КО":"Закарпатская обл. (Ужгород)",
        "AT":"Ивано-Франковская обл.","КТ":"Ивано-Франковская обл.",
        "AM":"Житомирская обл.","КМ":"Житомирская обл.",
        "BK":"Ровненская обл.","НК":"Ровненская обл.",
        "BO":"Тернопольская обл.","НО":"Тернопольская обл.",
        "BX":"Хмельницкая обл.","НХ":"Хмельницкая обл.",
        "CE":"Черновицкая обл.","ІЕ":"Черновицкая обл.",
        "BA":"Кировоградская обл. (Кропивницкий)","НА":"Кировоградская обл. (Кропивницкий)",
        "BI":"Полтавская обл.","НI":"Полтавская обл.",
        "CA":"Черкасская обл.","IA":"Черкасская обл.",
        "CB":"Черниговская обл.","IB":"Черниговская обл.",
        "BM":"Сумская обл.","НМ":"Сумская обл.",
        "AB":"Винницкая обл.","КВ":"Винницкая обл.",
        "AX":"Харьковская обл.","КХ":"Харьковская обл.",
        "AE":"Днепропетровская обл.","КЕ":"Днепропетровская обл.",
        "AH":"Донецкая обл.","КН":"Донецкая обл.",
        "BB":"Луганская обл.","НВ":"Луганская обл.",
        "BH":"Николаевская обл.","НН":"Николаевская обл.",
        "BT":"Херсонская обл.","НТ":"Херсонская обл.",
        "BВ":"Запорожская обл.","НЗ":"Запорожская обл.",
        "BI":"Полтавская обл.","НI":"Полтавская обл.",
        "ВА":"Одесская обл.","ОА":"Одесская обл.",
    },
    "BY": {
        "1":"Брестская область",
        "2":"Витебская область",
        "3":"Гомельская область",
        "4":"Гродненская область",
        "5":"Минская область",
        "6":"Могилёвская область",
        "7":"г. Минск",
    },
    "KZ": {
        "01":"г. Астана","02":"г. Алматы",
        "03":"Акмолинская обл.","04":"Актюбинская обл.",
        "05":"Алматинская обл.","06":"Атырауская обл.",
        "07":"Западно-Казахстанская обл.","08":"Жамбылская обл.",
        "09":"Карагандинская обл.","10":"Костанайская обл.",
        "11":"Кызылординская обл.","12":"Мангистауская обл.",
        "13":"Туркестанская обл.","14":"Павлодарская обл.",
        "15":"Северо-Казахстанская обл.","16":"Восточно-Казахстанская обл.",
        "17":"г. Шымкент","18":"Абайская обл.",
        "19":"Жетысуская обл.","20":"Улытауская обл.",
    },
}

COUNTRY_NAMES = {"RU": "🇷🇺 Россия", "UA": "🇺🇦 Украина", "BY": "🇧🇾 Беларусь", "KZ": "🇰🇿 Казахстан"}

PLATE_SCHEMES = {
    "RU": (
        "Схема: <b>Л ЦЦЦ ЛЛ</b>\n"
        "Л = буква: А В Е К М Н О Р С Т У Х\n"
        "Ц = цифра: 0–9\n\n"
        "Пример: <code>В 131 ОР</code>"
    ),
    "UA": (
        "Схема: <b>ЦЦЦЦ ЛЛ</b>\n"
        "Ц = цифра: 0–9\n"
        "Л = буква: А–Я (кириллица)\n\n"
        "Пример: <code>1234 АВ</code>"
    ),
    "BY": (
        "Схема: <b>ЦЦЦЦ ЛЛ</b>\n"
        "Ц = цифра: 0–9\n"
        "Л = буква: А–Я (кириллица)\n\n"
        "Пример: <code>1234 АВ</code>"
    ),
    "KZ": (
        "Схема: <b>ЦЦЦ ЛЛЛ</b>\n"
        "Ц = цифра: 0–9\n"
        "Л = латинская буква: A–Z\n\n"
        "Пример: <code>001 ABС</code>"
    ),
}

REGIONS_PER_PAGE = 24   # 6 рядов × 4 кнопки

# ── Генерация изображения номерного знака ─────────────────────────────────────

def _dot_grid(draw, w, h):
    for x in range(0, w + 1, 20):
        for y in range(0, h + 1, 20):
            draw.ellipse([x - 1, y - 1, x + 1, y + 1], fill="#d0d0d0")


def _ru_flag(draw, fx, fy, fw=32, fh=22):
    """Рисует флаг РФ в позиции fx, fy."""
    th = fh // 3
    draw.rectangle([fx, fy, fx + fw, fy + th], fill="white", outline="#cccccc", width=1)
    draw.rectangle([fx, fy + th, fx + fw, fy + th * 2], fill="#003DA5")
    draw.rectangle([fx, fy + th * 2, fx + fw, fy + fh], fill="#CC0000")


def _ua_flag(draw, fx, fy, fw=32, fh=22):
    hh = fh // 2
    draw.rectangle([fx, fy, fx + fw, fy + hh], fill="#005BBB")
    draw.rectangle([fx, fy + hh, fx + fw, fy + fh], fill="#FFD500")


def _by_flag(draw, fx, fy, fw=32, fh=22):
    """Флаг Беларуси: красная полоса (2/3) + зелёная (1/3) + белая орнаментальная полоска слева."""
    red_h = round(fh * 2 / 3)
    grn_h = fh - red_h
    draw.rectangle([fx, fy,           fx + fw, fy + red_h], fill="#CF101A")
    draw.rectangle([fx, fy + red_h,   fx + fw, fy + fh],    fill="#007828")
    # Белая полоска с орнаментом (≈1/9 ширины)
    orn_w = max(4, fw // 9)
    draw.rectangle([fx, fy, fx + orn_w, fy + fh], fill="white")
    # Ромбы — традиционный орнамент
    d  = max(3, orn_w - 1)
    cx = fx + orn_w // 2
    for yi in range(fy, fy + fh, d):
        half = d // 2
        col  = "#CF101A" if (yi + half) < (fy + red_h) else "#007828"
        pts  = [(cx, yi), (fx + orn_w - 1, yi + half), (cx, min(yi + d, fy + fh)), (fx + 1, yi + half)]
        draw.polygon(pts, fill=col)


def _kz_flag(draw, fx, fy, fw=32, fh=22):
    draw.rectangle([fx, fy, fx + fw, fy + fh], fill="#00AFCA")
    fnt = ImageFont.truetype(FONT_BOLD, 10)
    draw.text((fx + fw // 2, fy + fh // 2), "☀", fill="#FFD700", font=fnt, anchor="mm")


def generate_plate_image(country: str, chars: str, region: str, region_name: str) -> bytes:
    W, H = 580, 290
    img  = Image.new("RGB", (W, H), "#efefef")
    draw = ImageDraw.Draw(img)
    _dot_grid(draw, W, H)

    fnt_hdr = ImageFont.truetype(FONT_REG, 13)
    fnt_sub = ImageFont.truetype(FONT_REG, 11)
    fnt_ftr = ImageFont.truetype(FONT_REG, 12)
    draw.text((W // 2, 15), "HOMEPA  —  CARDROP",  fill="#aaaaaa", font=fnt_hdr, anchor="mm")
    draw.text((W // 2, 29), "@cardrop_game_bot",   fill="#aaaaaa", font=fnt_sub, anchor="mm")

    cx, cy = W // 2, H // 2

    if country == "RU":
        pw, ph = 490, 118
        px, py = cx - pw // 2, cy - ph // 2
        # Shadow
        draw.rounded_rectangle([px + 5, py + 5, px + pw + 5, py + ph + 5], radius=10, fill="#b8b8b8")
        # Plate body
        draw.rounded_rectangle([px, py, px + pw, py + ph], radius=10, fill="white",
                                outline="#111111", width=4)
        # Vertical divider
        right_w = 110
        rdx = px + pw - right_w
        draw.line([(rdx, py + 6), (rdx, py + ph - 6)], fill="#111111", width=3)
        # Main characters (left side)
        fnt_pl = ImageFont.truetype(FONT_BOLD, 72)
        draw.text((px + (pw - right_w) // 2, py + ph // 2), chars,
                  fill="#111111", font=fnt_pl, anchor="mm")
        # Right panel center x
        rcx = rdx + right_w // 2
        rpt = py + 6
        rpb = py + ph - 6
        rph = rpb - rpt
        # Region code — centered in top 65% of right panel
        region_cy = rpt + int(rph * 0.42)
        fnt_r = ImageFont.truetype(FONT_BOLD, 48)
        draw.text((rcx, region_cy), region, fill="#111111", font=fnt_r, anchor="mm")
        # RUS + flag — bottom portion
        fw, fh = 22, 15
        fnt_rus = ImageFont.truetype(FONT_BOLD, 13)
        rus_w = int(fnt_rus.getlength("RUS"))
        gap = 3
        total_w = rus_w + gap + fw
        rus_cy = rpt + int(rph * 0.82)
        tx = rcx - total_w // 2
        fx = tx + rus_w + gap
        fy = rus_cy - fh // 2
        draw.text((tx, rus_cy), "RUS", fill="#111111", font=fnt_rus, anchor="lm")
        _ru_flag(draw, fx, fy, fw=fw, fh=fh)

    elif country == "UA":
        pw, ph = 490, 118
        px, py = cx - pw // 2, cy - ph // 2
        draw.rounded_rectangle([px+4, py+4, px+pw+4, py+ph+4], radius=8, fill="#b0b0b0")
        draw.rounded_rectangle([px, py, px+pw, py+ph], radius=8, fill="white",
                                outline="#111111", width=4)
        strip_w = 62
        draw.rounded_rectangle([px+2, py+2, px+strip_w, py+ph-2], radius=7, fill="#003DA5")
        draw.line([(px+strip_w, py+4), (px+strip_w, py+ph-4)], fill="#111111", width=3)
        ffw, ffh = 38, 26
        ffx = px + (strip_w - ffw) // 2
        ffy = py + 16
        draw.rectangle([ffx, ffy,        ffx+ffw, ffy+ffh//2], fill="#005BBB")
        draw.rectangle([ffx, ffy+ffh//2, ffx+ffw, ffy+ffh],   fill="#FFD500")
        fnt_ua = ImageFont.truetype(FONT_BOLD, 17)
        draw.text((px + strip_w//2, py + ph - 14), "UA", fill="white", font=fnt_ua, anchor="mm")
        c = chars.strip().upper().replace(" ", "")
        if len(c) == 6:
            body = f"{region} {c[:4]} {c[4:]}" if region else f"{c[:4]} {c[4:]}"
        elif len(c) == 4:
            body = f"{region} {c}" if region else c
        else:
            body = f"{region} {c}" if region else c
        fnt_pl = ImageFont.truetype(FONT_BOLD, 66)
        content_cx = px + strip_w + (pw - strip_w) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_pl, anchor="mm")

    elif country == "BY":
        pw, ph = 490, 118
        px, py = cx - pw // 2, cy - ph // 2
        draw.rounded_rectangle([px+4, py+4, px+pw+4, py+ph+4], radius=8, fill="#b0b0b0")
        draw.rounded_rectangle([px, py, px+pw, py+ph], radius=8, fill="white",
                                outline="#111111", width=5)

        zone_w = 90
        fl_w   = 72
        fl_h   = 48
        fnt_by2 = ImageFont.truetype(FONT_BOLD, 16)
        by_bbox = fnt_by2.getbbox("BY")
        by_h    = by_bbox[3] - by_bbox[1]
        total_h = fl_h + 4 + by_h
        fl_x = px + (zone_w - fl_w) // 2
        fl_y = py + (ph - total_h) // 2
        red_h = round(fl_h * 2 / 3)

        draw.rectangle([fl_x, fl_y, fl_x+fl_w, fl_y+red_h],       fill="#CF101A")
        draw.rectangle([fl_x, fl_y+red_h, fl_x+fl_w, fl_y+fl_h],  fill="#007828")

        orn_w = max(7, fl_w // 9)
        draw.rectangle([fl_x, fl_y, fl_x+orn_w, fl_y+fl_h], fill="white")

        step = max(5, orn_w + 1)
        ocx  = fl_x + orn_w // 2
        for yi in range(fl_y, fl_y + fl_h, step):
            y_top  = yi
            y_mid  = yi + step // 2
            y_bot  = min(yi + step, fl_y + fl_h)
            col    = "#CF101A" if y_mid < (fl_y + red_h) else "#007828"
            draw.polygon([
                (ocx,            y_top),
                (fl_x + orn_w-1, y_mid),
                (ocx,            y_bot),
                (fl_x + 1,       y_mid),
            ], fill=col)
            if y_mid < fl_y + fl_h - 2:
                mini = step // 4
                draw.polygon([
                    (ocx,          y_mid - mini),
                    (ocx + mini,   y_mid),
                    (ocx,          y_mid + mini),
                    (ocx - mini,   y_mid),
                ], fill="white")

        draw.text((fl_x + fl_w//2, fl_y + fl_h + 4), "BY", fill="#111111",
                  font=fnt_by2, anchor="mt")

        text_x = px + zone_w + 6
        c = chars.replace(" ", "").upper()
        if len(c) >= 6:
            body = f"{c[:4]} {c[4:6]}-{region}"
        elif len(c) >= 4:
            body = f"{c[:4]} {c[4:]}-{region}"
        else:
            body = f"{chars}-{region}"
        fnt_pl     = ImageFont.truetype(FONT_BOLD, 68)
        content_cx = text_x + (px + pw - 8 - text_x) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_pl, anchor="mm")

    elif country == "KZ":
        pw, ph = 490, 118
        px, py = cx - pw // 2, cy - ph // 2
        draw.rounded_rectangle([px+4, py+6, px+pw+4, py+ph+6], radius=10, fill="#aaaaaa")
        draw.rounded_rectangle([px, py, px+pw, py+ph], radius=10, fill="white",
                                outline="#1a1a1a", width=5)

        SW = 90; RW = 64
        col_cx = px + SW // 2

        target_flag_h = 52

        fnt_kz_label = ImageFont.truetype(FONT_BOLD, 17)
        kz_bbox = fnt_kz_label.getbbox("KZ")
        kz_h = kz_bbox[3] - kz_bbox[1]

        flag_drawn = False
        if KZ_FLAG_PATH and os.path.exists(KZ_FLAG_PATH):
            try:
                flag_img = Image.open(KZ_FLAG_PATH).convert("RGBA")
                max_flag_w = SW - 16
                ratio_h = target_flag_h / flag_img.height
                new_w   = int(flag_img.width * ratio_h)
                if new_w > max_flag_w:
                    ratio_h = max_flag_w / flag_img.width
                new_w = max(1, int(flag_img.width  * ratio_h))
                new_h = max(1, int(flag_img.height * ratio_h))
                resized  = flag_img.resize((new_w, new_h), Image.LANCZOS)
                total_h  = resized.height + 3 + kz_h
                flag_y   = py + (ph - total_h) // 2
                flag_x   = col_cx - resized.width // 2
                img.paste(resized, (flag_x, flag_y), resized)
                flag_bottom = flag_y + resized.height
                flag_drawn = True
            except Exception:
                pass

        if not flag_drawn:
            fl_w, fl_h = 56, 38
            total_h = fl_h + 3 + kz_h
            flag_y  = py + (ph - total_h) // 2
            fl_x    = col_cx - fl_w // 2
            draw.rectangle([fl_x, flag_y, fl_x+fl_w, flag_y+fl_h], fill="#00AFCA")
            flag_bottom = flag_y + fl_h

        draw.text((col_cx, flag_bottom + 3), "KZ",
                  fill="#111111", font=fnt_kz_label, anchor="mt")

        rdx = px + pw - RW
        draw.line([(rdx, py+10), (rdx, py+ph-10)], fill="#bbbbbb", width=2)
        fnt_reg = ImageFont.truetype(FONT_BOLD, 40)
        draw.text((rdx + RW // 2, cy), region, fill="#111111", font=fnt_reg, anchor="mm")

        c    = chars.replace(" ", "")
        body = f"{c[:3]} {c[3:]}" if len(c) == 6 else chars
        num_cx = px + SW + (rdx - px - SW) // 2
        fnt_pl = ImageFont.truetype(FONT_BOLD, 72)
        draw.text((num_cx, cy + 2), body, fill="#111111", font=fnt_pl, anchor="mm")

    draw.text((W // 2, H - 18), "@crreate_st_em_bot", fill="#aaaaaa", font=fnt_ftr, anchor="mm")

    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


# ── Утилиты (стикеры) ─────────────────────────────────────────────────────────

def random_suffix(length: int = 10) -> str:
    first = random.choice(string.ascii_lowercase)
    rest  = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


def sanitize_suffix(raw: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]", "", raw.lower())
    if not cleaned or not cleaned[0].isalpha():
        cleaned = "s" + cleaned
    cleaned = cleaned[:64]
    if len(cleaned) < 5:
        cleaned = cleaned + random_suffix(5 - len(cleaned))
    return cleaned


def build_pack_name(bot_username: str, suffix: str) -> str:
    return f"{suffix}_by_{bot_username}"


def plural_sticker(n: int) -> str:
    if 11 <= n % 100 <= 19:
        return "стикеров"
    r = n % 10
    if r == 1:      return "стикер"
    if 2 <= r <= 4: return "стикера"
    return "стикеров"


def plural_nomer(n: int) -> str:
    if 11 <= n % 100 <= 19:
        return "номеров"
    r = n % 10
    if r == 1:      return "номер"
    if 2 <= r <= 4: return "номера"
    return "номеров"


def pack_url(pack_name: str) -> str:
    return f"https://t.me/addstickers/{pack_name}"


# ══════════════════════════════════════════════════════════════════════════════
# ПОКРАСКА СТИКЕРОВ — движок и хендлеры
# ══════════════════════════════════════════════════════════════════════════════

PAINT_PALETTE: list[tuple[str, str]] = [
    ("🔴 Красный",    "#FF3333"),
    ("🟠 Оранжевый",  "#FF8C00"),
    ("🟡 Жёлтый",     "#FFD700"),
    ("🟢 Зелёный",    "#00C853"),
    ("🔵 Синий",      "#1565C0"),
    ("🟣 Фиолетовый", "#7B1FA2"),
    ("🩷 Розовый",    "#FF4081"),
    ("🩵 Голубой",    "#00B4D8"),
    ("⚪ Белый",      "#FFFFFF"),
    ("⚫ Чёрный",     "#1A1A1A"),
    ("🟤 Коричневый", "#795548"),
    ("🩶 Серый",      "#9E9E9E"),
]


def _paint_valid_hex(s: str) -> bool:
    return bool(re.fullmatch(r"#?[0-9A-Fa-f]{6}", s.strip()))


def _paint_norm_hex(s: str) -> str:
    return "#" + s.strip().lstrip("#").upper()


def _paint_hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def recolor_webp(raw: bytes, hex_color: str) -> bytes:
    """
    Полная перекраска: каждый видимый пиксель заливается выбранным цветом.
    Яркость оригинала умножается на целевой цвет.
    Для белого (#FFFFFF) итог = полный яркостный диапазон (белый/серый/чёрный).
    Альфа-канал сохраняется без изменений.
    """
    h = hex_color.lstrip("#")
    tr = int(h[0:2], 16) / 255.0
    tg = int(h[2:4], 16) / 255.0
    tb = int(h[4:6], 16) / 255.0

    img = Image.open(io.BytesIO(raw)).convert("RGBA")
    arr = np.array(img, dtype=np.float32)

    alpha = arr[:, :, 3]

    # Воспринимаемая яркость пикселя (0..1)
    lum = (0.299 * arr[:, :, 0] + 0.587 * arr[:, :, 1] + 0.114 * arr[:, :, 2]) / 255.0

    # Gamma 0.6 поднимает средние тона — светлые пиксели становятся ярче,
    # тёмные детали лучше видны, белый реально белый а не серый
    lum_bright = np.power(np.maximum(lum, 0), 0.6)

    # Полная заливка цветом с сохранением яркостной структуры
    final_r = np.clip(tr * lum_bright * 255, 0, 255).astype(np.uint8)
    final_g = np.clip(tg * lum_bright * 255, 0, 255).astype(np.uint8)
    final_b = np.clip(tb * lum_bright * 255, 0, 255).astype(np.uint8)

    result = np.stack([final_r, final_g, final_b, alpha.astype(np.uint8)], axis=2)

    out = Image.fromarray(result, "RGBA")
    buf = io.BytesIO()
    out.save(buf, "WebP", quality=90)
    return buf.getvalue()


def _lottie_is_color(v) -> bool:
    return (
        isinstance(v, list)
        and 3 <= len(v) <= 4
        and all(isinstance(x, (int, float)) and -0.01 <= x <= 1.01 for x in v[:3])
    )


def _lottie_map_color(arr: list, tr: float, tg: float, tb: float) -> list:
    # Яркость исходного цвета (0..1)
    L = 0.299 * arr[0] + 0.587 * arr[1] + 0.114 * arr[2]
    # Усиливаем яркость чтобы цвета были насыщенными, а не блёклыми
    # gamma = 0.6 поднимает тёмные детали (иначе белый даёт серые пятна вместо белых)
    L_bright = L ** 0.6
    result = [
        round(min(tr * L_bright, 1.0), 6),
        round(min(tg * L_bright, 1.0), 6),
        round(min(tb * L_bright, 1.0), 6),
    ]
    if len(arr) >= 4:
        result.append(arr[3])
    return result


def _lottie_patch_val(obj: dict, tr: float, tg: float, tb: float) -> None:
    k = obj.get("k")
    if _lottie_is_color(k):
        obj["k"] = _lottie_map_color(k, tr, tg, tb)
    elif isinstance(k, list):
        for kf in k:
            if isinstance(kf, dict):
                for p in ("s", "e"):
                    if p in kf and _lottie_is_color(kf[p]):
                        kf[p] = _lottie_map_color(kf[p], tr, tg, tb)


def _lottie_walk(node, tr: float, tg: float, tb: float) -> None:
    if isinstance(node, dict):
        ty = node.get("ty")
        for key, val in node.items():
            if key in ("c", "sc", "fc") and isinstance(val, dict) and "k" in val:
                _lottie_patch_val(val, tr, tg, tb)
            elif key == "g" and ty in ("gf", "gs") and isinstance(val, dict):
                _lottie_patch_gradient(val, tr, tg, tb)
            else:
                _lottie_walk(val, tr, tg, tb)
    elif isinstance(node, list):
        for item in node:
            _lottie_walk(item, tr, tg, tb)


def _lottie_patch_gradient(obj: dict, tr: float, tg: float, tb: float) -> None:
    k = obj.get("k")
    if isinstance(k, list) and k and isinstance(k[0], (int, float)):
        new_k = []
        i = 0
        while i < len(k):
            if i + 3 < len(k):
                pos = k[i]
                r2, g2, b2 = float(k[i+1]), float(k[i+2]), float(k[i+3])
                L = (0.299 * r2 + 0.587 * g2 + 0.114 * b2) ** 0.6
                new_k.extend([pos, round(min(tr*L,1.0), 6), round(min(tg*L,1.0), 6), round(min(tb*L,1.0), 6)])
                i += 4
            else:
                new_k.append(k[i]); i += 1
        obj["k"] = new_k
    elif isinstance(k, list):
        for kf in k:
            if isinstance(kf, dict):
                for p in ("s", "e"):
                    if p in kf and isinstance(kf[p], list):
                        arr2 = kf[p]
                        new_arr = []
                        i = 0
                        while i < len(arr2):
                            if i + 3 < len(arr2) and all(isinstance(arr2[i+j], (int, float)) for j in range(4)):
                                pos = arr2[i]
                                r2, g2, b2 = float(arr2[i+1]), float(arr2[i+2]), float(arr2[i+3])
                                L = (0.299*r2 + 0.587*g2 + 0.114*b2) ** 0.6
                                new_arr.extend([pos, round(min(tr*L,1.0), 6), round(min(tg*L,1.0), 6), round(min(tb*L,1.0), 6)])
                                i += 4
                            else:
                                new_arr.append(arr2[i]); i += 1
                        kf[p] = new_arr


def recolor_tgs(raw: bytes, hex_color: str) -> bytes:
    """Перекрашивает TGS-стикер (gzip Lottie JSON)."""
    r, g, b = _paint_hex_to_rgb(hex_color)
    tr, tg, tb = r / 255.0, g / 255.0, b / 255.0
    lottie = json.loads(gzip.decompress(raw))
    _lottie_walk(lottie, tr, tg, tb)
    return gzip.compress(
        json.dumps(lottie, separators=(",", ":")).encode("utf-8"),
        compresslevel=9,
    )


def recolor_webm(raw: bytes, hex_color: str) -> bytes | None:
    """
    Перекрашивает WEBM видео-стикер через ffmpeg.
    Шаги: grayscale → умножить каналы на целевой цвет.
    Работает корректно для белого (#FFFFFF) и всех других цветов.
    """
    import subprocess, tempfile, os
    r, g, b = _paint_hex_to_rgb(hex_color)
    ffmpeg = _get_ffmpeg_path()
    with tempfile.TemporaryDirectory() as tmpdir:
        in_path  = os.path.join(tmpdir, "input.webm")
        out_path = os.path.join(tmpdir, "output.webm")
        with open(in_path, "wb") as f:
            f.write(raw)
        # 1) Перевести в grayscale (сохраняя яркость)
        # 2) Умножить каждый канал на нормированный целевой цвет
        # colorchannelmixer: rr/rg/rb задают откуда берётся R-канал
        # Чтобы получить grayscale*color используем формулу:
        # new_R = (0.299*R + 0.587*G + 0.114*B) * (target_R/255)
        rn, gn, bn = r/255, g/255, b/255
        color_filter = (
            f"colorchannelmixer="
            f"rr={0.299*rn:.4f}:rg={0.587*rn:.4f}:rb={0.114*rn:.4f}:"
            f"gr={0.299*gn:.4f}:gg={0.587*gn:.4f}:gb={0.114*gn:.4f}:"
            f"br={0.299*bn:.4f}:bg={0.587*bn:.4f}:bb={0.114*bn:.4f}"
        )
        cmd = [
            ffmpeg, "-y", "-i", in_path,
            "-vf", color_filter,
            "-c:v", "libvpx-vp9",
            "-b:v", "0", "-crf", "30",
            "-deadline", "good", "-cpu-used", "2",
            "-an", out_path,
        ]
        try:
            r2 = subprocess.run(cmd, capture_output=True, timeout=60)
            if r2.returncode == 0 and os.path.exists(out_path):
                result = open(out_path, "rb").read()
                if result:
                    return result
        except Exception as e:
            logger.warning("recolor_webm failed: %s", e)
        return None


def _paint_kb_colors(last: str | None = None) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for name, hx in PAINT_PALETTE:
        mark = "✓ " if hx == last else ""
        row.append(InlineKeyboardButton(f"{mark}{name}", callback_data=f"paint_clr:{hx}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🎨 Свой HEX цвет", callback_data="paint_clr:custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="begin")])
    return InlineKeyboardMarkup(rows)


def _paint_kb_scope() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🖼 Один стикер", callback_data="paint_scope:single"),
            InlineKeyboardButton("📦 Весь пак",    callback_data="paint_scope:pack"),
        ],
        [InlineKeyboardButton("◀️ Назад", callback_data="begin")],
    ])


def _paint_make_pack_name(bot_username: str, uid: int, pack_name: str, hex_color: str) -> str:
    color_suffix = hex_color.lstrip("#").lower()
    base = re.sub(r"[^a-z0-9]", "_", pack_name.lower())[:20].strip("_")
    uid_short = str(uid)[-6:]
    new_name = f"{base}_{color_suffix}_{uid_short}_by_{bot_username}"
    return new_name[:64]


# ── Вход в режим покраски ────────────────────────────────────────────────────

async def paint_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Кнопка '🎨 Покрасить стикер' в главном меню."""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("paint_sticker", None)
    await query.edit_message_text(
        "🎨 <b>Покраска стикера / эмодзи</b>\n\n"
        "Отправь мне:\n"
        "• Стикер напрямую (статичный, анимированный или видео)\n"
        "• Ссылку на стикер-пак: <code>t.me/addstickers/ИМЯ</code>\n"
        "• Ссылку на эмодзи-пак: <code>t.me/addemoji/ИМЯ</code>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="begin")]]),
    )
    return PAINT_SCOPE


async def paint_from_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Кнопка 'Покрасить стикер/эмодзи' в управлении конкретным паком — сразу к выбору цвета."""
    query = update.callback_query
    await query.answer()
    pack_name = query.data.split(":", 1)[1]
    context.user_data["paint_sticker"] = {
        "file_id":     None,
        "is_animated": False,
        "pack_name":   pack_name,
        "emoji":       "⭐",
        "mode":        "pack",
    }
    last = context.user_data.get("paint_last_color")
    await query.edit_message_text(
        f"🎨 <b>Покраска пака <code>{pack_name}</code></b>\n\nВыбери цвет:",
        parse_mode="HTML",
        reply_markup=_paint_kb_colors(last),
    )
    return PAINT_COLOR


async def paint_receive_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь прислал стикер — сначала проверяем, можно ли красить, потом предлагаем меню."""
    s = update.message.sticker

    # Определяем тип стикера
    sticker_type_tg = getattr(s, "type", None)
    is_custom_emoji = (
        str(sticker_type_tg) == "custom_emoji"
        or sticker_type_tg == StickerType.CUSTOM_EMOJI
    )

    # Кастомные эмодзи — сразу отказ, покраска одного не работает
    if is_custom_emoji:
        pack_hint = (
            f"\n\nДля покраски всего пака отправь ссылку: <code>t.me/addemoji/{s.set_name}</code>"
            if s.set_name else ""
        )
        await update.message.reply_text(
            f"❌ <b>Этот стикер нельзя покрасить.</b>\n"
            f"<i>Кастомные эмодзи не поддерживаются для покраски одиночного стикера.{pack_hint}</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
        )
        return PAINT_SCOPE

    if s.is_video:
        kind = "видео WEBM"
    elif s.is_animated:
        kind = "анимированный TGS"
    else:
        kind = "статичный WebP"

    context.user_data["paint_sticker"] = {
        "file_id":         s.file_id,
        "is_animated":     s.is_animated,
        "is_video":        s.is_video,
        "pack_name":       s.set_name or "",
        "emoji":           s.emoji or "⭐",
        "is_custom_emoji": False,
    }
    last = context.user_data.get("paint_last_color")

    if s.set_name:
        await update.message.reply_text(
            f"🖌 <b>Стикер получен</b> <i>({kind})</i>\n\n"
            f"📦 Пак: <code>{s.set_name}</code>\n\n"
            f"Красить только этот стикер или весь пак?",
            parse_mode="HTML",
            reply_markup=_paint_kb_scope(),
        )
        return PAINT_SCOPE
    else:
        context.user_data["paint_sticker"]["mode"] = "single"
        await update.message.reply_text(
            f"🖌 <b>Стикер получен</b> <i>({kind})</i>\n\nВыбери цвет:",
            parse_mode="HTML",
            reply_markup=_paint_kb_colors(last),
        )
        return PAINT_COLOR


async def paint_receive_custom_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь прислал сообщение с премиум-эмодзи (custom_emoji entity).
    Одиночный кастомный эмодзи красить нельзя — предлагаем только весь пак.
    """
    msg = update.message
    entities = msg.entities or []
    custom_emoji_id = None
    for ent in entities:
        if ent.type == "custom_emoji" and ent.custom_emoji_id:
            custom_emoji_id = ent.custom_emoji_id
            break

    if not custom_emoji_id:
        await msg.reply_text(
            "❌ Не удалось извлечь эмодзи. Отправь ссылку вида:\n"
            "<code>t.me/addemoji/ИМЯ</code>",
            parse_mode="HTML",
        )
        return PAINT_SCOPE

    status = await msg.reply_text("🔍 Ищу информацию об эмодзи...")
    try:
        stickers = await context.bot.get_custom_emoji_stickers([custom_emoji_id])
        if not stickers:
            raise ValueError("Пусто")
        s = stickers[0]
        pack_name = getattr(s, "set_name", None)
    except Exception as e:
        await status.edit_text(
            f"❌ Не удалось получить информацию об эмодзи: <code>{e}</code>\n"
            "Отправь ссылку на пак: <code>t.me/addemoji/ИМЯ</code>",
            parse_mode="HTML",
        )
        return PAINT_SCOPE

    await status.delete()

    if pack_name:
        # Есть пак — сразу к выбору цвета для всего пака (одиночный кастомный эмодзи не поддерживается)
        context.user_data["paint_sticker"] = {
            "file_id":         None,
            "is_animated":     False,
            "pack_name":       pack_name,
            "emoji":           s.emoji or "⭐",
            "is_custom_emoji": True,
            "mode":            "pack",
        }
        last = context.user_data.get("paint_last_color")
        await msg.reply_text(
            f"🖌 <b>Эмодзи из пака <code>{pack_name}</code></b>\n\n"
            f"<i>Одиночный кастомный эмодзи красить нельзя — будет перекрашен весь пак.</i>\n\n"
            f"Выбери цвет:",
            parse_mode="HTML",
            reply_markup=_paint_kb_colors(last),
        )
        return PAINT_COLOR
    else:
        await msg.reply_text(
            "❌ <b>Этот эмодзи нельзя покрасить.</b>\n\n"
            "<i>Кастомные эмодзи без пака не поддерживаются.</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
        )
        return PAINT_SCOPE


async def paint_receive_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь прислал ссылку на пак или имя пака."""
    text = (update.message.text or "").strip()
    # Поддерживаем addstickers и addemoji
    m = re.search(r"t\.me/addemoji/([A-Za-z0-9_]+)", text)
    m2 = re.search(r"t\.me/addstickers/([A-Za-z0-9_]+)", text)
    pack_name = (m or m2).group(1) if (m or m2) else (
        text if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{4,63}", text) else None
    )

    if not pack_name:
        await update.message.reply_text(
            "❌ Не распознал. Отправь стикер или ссылку вида:\n"
            "<code>t.me/addstickers/ИМЯ</code> или <code>t.me/addemoji/ИМЯ</code>",
            parse_mode="HTML",
        )
        return PAINT_SCOPE

    context.user_data["paint_sticker"] = {
        "file_id":     None,
        "is_animated": False,
        "pack_name":   pack_name,
        "emoji":       "⭐",
        "mode":        "pack",
    }
    last = context.user_data.get("paint_last_color")
    await update.message.reply_text(
        f"📦 <b>Пак: <code>{pack_name}</code></b>\n\nВыбери цвет для покраски всего пака:",
        parse_mode="HTML",
        reply_markup=_paint_kb_colors(last),
    )
    return PAINT_COLOR


async def paint_scope_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Callback: paint_scope:single или paint_scope:pack."""
    query = update.callback_query
    await query.answer()
    mode = query.data.split(":", 1)[1]
    context.user_data.setdefault("paint_sticker", {})["mode"] = mode
    last = context.user_data.get("paint_last_color")
    await query.edit_message_text(
        "🖌 <b>Выбери цвет:</b>",
        parse_mode="HTML",
        reply_markup=_paint_kb_colors(last),
    )
    return PAINT_COLOR


async def paint_color_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Callback: paint_clr:HEX или paint_clr:custom."""
    query = update.callback_query
    val = query.data.split(":", 1)[1]

    if val == "custom":
        await query.answer()
        await query.edit_message_text(
            "🖌 <b>Введи HEX цвет</b>\n\nПример: <code>#FF3333</code> или <code>1565C0</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="begin")]]),
        )
        return PAINT_HEX

    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass
    await _paint_execute(update, context, val)
    return CHOOSE_TYPE


async def paint_hex_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь ввёл произвольный HEX."""
    val = (update.message.text or "").strip()
    if not _paint_valid_hex(val):
        await update.message.reply_text(
            "❌ Неверный формат. Пример: <code>#FF3333</code> или <code>FF3333</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="begin")]]),
        )
        return PAINT_HEX
    await _paint_execute(update, context, _paint_norm_hex(val))
    return CHOOSE_TYPE


async def _paint_execute(update: Update, context: ContextTypes.DEFAULT_TYPE, hex_color: str):
    """Выполняет покраску — одного стикера или целого пака (включая эмодзи-паки)."""
    import asyncio
    data = context.user_data.get("paint_sticker", {})
    mode = data.get("mode", "single")
    bot  = context.bot
    msg  = update.effective_message
    uid  = update.effective_user.id

    status = await msg.reply_text("🔄 <b>Перекрашиваю...</b>", parse_mode="HTML")

    async def dl(file_id: str) -> bytes:
        f = await bot.get_file(file_id)
        buf = io.BytesIO()
        await f.download_to_memory(buf)
        buf.seek(0)
        return buf.read()

    async def upload_and_make_sticker(raw: bytes, fmt: StickerFormat, emoji: str) -> InputSticker:
        """Загружает файл через upload_sticker_file и возвращает InputSticker."""
        if fmt == StickerFormat.ANIMATED:
            fname = "sticker.tgs"
        elif fmt == StickerFormat.VIDEO:
            fname = "sticker.webm"
        else:
            fname = "sticker.webp"
        uploaded = await bot.upload_sticker_file(
            user_id=uid,
            sticker=InputFile(io.BytesIO(raw), filename=fname),
            sticker_format=fmt,
        )
        fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)
        # Пробуем оригинальный emoji, при ошибке валидации используем ⭐
        for emoji_try in [emoji, "⭐", "😊"]:
            try:
                try:
                    return InputSticker(sticker=uploaded.file_id, emoji_list=[emoji_try], format=fmt_str)
                except TypeError:
                    return InputSticker(sticker=uploaded.file_id, emoji_list=[emoji_try])
            except Exception as e:
                if "emoji" in str(e).lower() or "STICKER_EMOJI_INVALID" in str(e):
                    logger.warning("emoji '%s' rejected, trying fallback: %s", emoji_try, e)
                    continue
                raise
        raise ValueError(f"Не удалось подобрать валидный эмодзи для стикера")

    async def create_set(new_name: str, title: str, sticker: InputSticker,
                         fmt: StickerFormat, sticker_type: StickerType):
        kwargs = dict(
            user_id=uid,
            name=new_name,
            title=title,
            stickers=[sticker],
            sticker_type=sticker_type,
            sticker_format=fmt,
        )
        try:
            await bot.create_new_sticker_set(**kwargs)
        except TypeError:
            kwargs.pop("sticker_format", None)
            await bot.create_new_sticker_set(**kwargs)

    try:
        if mode == "single":
            is_custom = data.get("is_custom_emoji", False)

            # Скачиваем файл с fallback для премиум-эмодзи
            raw = None
            try:
                raw = await dl(data["file_id"])
            except Exception as dl_err:
                dl_err_s = str(dl_err).lower()
                # Пробуем скачать через прямой URL
                try:
                    tg_file = await bot.get_file(data["file_id"])
                    if tg_file.file_path:
                        import urllib.request as _ur
                        with _ur.urlopen(tg_file.file_path) as resp:
                            raw = resp.read()
                except Exception:
                    pass
                if raw is None:
                    if is_custom:
                        try:
                            await status.delete()
                        except Exception:
                            pass
                        await msg.reply_text(
                            "❌ <b>Этот стикер нельзя покрасить.</b>\n\n"
                            "<i>Кастомные эмодзи не поддерживаются для покраски одиночного стикера. "
                            "Попробуй отправить ссылку на весь пак: <code>t.me/addemoji/ИМЯ</code></i>",
                            parse_mode="HTML",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
                        )
                        context.user_data.pop("paint_sticker", None)
                        return
                    raise Exception(f"Не удалось скачать файл: {dl_err}")

            is_video   = data.get("is_video", False)
            is_animated = data.get("is_animated", False)
            if is_animated:
                fmt    = StickerFormat.ANIMATED
                result = recolor_tgs(raw, hex_color)
                fname  = "sticker.tgs"
            elif is_video:
                fmt    = StickerFormat.VIDEO
                result = recolor_webm(raw, hex_color) or raw
                fname  = "sticker.webm"
            else:
                fmt    = StickerFormat.STATIC
                result = recolor_webp(raw, hex_color)
                fname  = "sticker.webp"

            # upload_sticker_file — пробуем с sticker_format, потом без
            try:
                uploaded = await bot.upload_sticker_file(
                    user_id=uid,
                    sticker=InputFile(io.BytesIO(result), filename=fname),
                    sticker_format=fmt,
                )
            except TypeError:
                uploaded = await bot.upload_sticker_file(
                    user_id=uid,
                    sticker=InputFile(io.BytesIO(result), filename=fname),
                )

            if is_custom:
                # Кастомные эмодзи нельзя покрасить — Telegram не позволяет ботам
                # создавать паки типа CUSTOM_EMOJI без специальных прав.
                try:
                    await status.delete()
                except Exception:
                    pass
                await msg.reply_text(
                    "❌ <b>Этот стикер нельзя покрасить.</b>\n\n"
                    "<i>Кастомные эмодзи не поддерживаются для покраски одиночного стикера. "
                    "Попробуй отправить ссылку на весь пак: <code>t.me/addemoji/ИМЯ</code></i>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
                )
                context.user_data.pop("paint_sticker", None)
                return

            # Для обычных стикеров — создаём временный пак и отправляем стикер
            import time as _time
            bot_me = await bot.get_me()
            color_suffix = hex_color.lstrip("#").lower()
            uid_short = str(uid)[-6:]
            ts = str(int(_time.time()))[-4:]
            tmp_name = f"tmp{uid_short}{ts}{color_suffix}_by_{bot_me.username}"[:64]
            stype = StickerType.REGULAR
            fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)

            # Перебираем эмодзи-кандидаты пока Telegram не примет
            _emoji_candidates = [data.get("emoji") or "⭐", "⭐", "😊", "🌟", "✨"]
            _emoji_candidates = list(dict.fromkeys(e for e in _emoji_candidates if e))  # уникальные

            inp = None
            last_err = None
            for _ec in _emoji_candidates:
                try:
                    try:
                        _inp_try = InputSticker(sticker=uploaded.file_id, emoji_list=[_ec], format=fmt_str)
                    except TypeError:
                        _inp_try = InputSticker(sticker=uploaded.file_id, emoji_list=[_ec])

                    try:
                        await create_set(tmp_name, f"Recolor [{hex_color}]", _inp_try, fmt, stype)
                    except Exception as e:
                        es = str(e).lower()
                        if "already" in es or "occupied" in es:
                            await bot.add_sticker_to_set(user_id=uid, name=tmp_name, sticker=_inp_try)
                        elif "emoji" in es or "sticker_emoji_invalid" in es:
                            last_err = e
                            logger.warning("create_set emoji '%s' rejected: %s", _ec, e)
                            continue
                        else:
                            raise
                    inp = _inp_try
                    break
                except Exception as e:
                    es = str(e).lower()
                    if "emoji" in es or "sticker_emoji_invalid" in es:
                        last_err = e
                        logger.warning("InputSticker emoji '%s' rejected: %s", _ec, e)
                        continue
                    raise

            if inp is None:
                raise Exception(f"Не удалось подобрать эмодзи для стикера: {last_err}")

            try:
                await status.delete()
            except Exception:
                pass

            tmp_set = await bot.get_sticker_set(tmp_name)
            await bot.send_sticker(msg.chat_id, tmp_set.stickers[0].file_id)

        else:  # pack
            pack_name = data["pack_name"]
            try:
                pack = await bot.get_sticker_set(pack_name)
            except Exception as e:
                await status.edit_text(
                    f"❌ Не удалось загрузить пак <code>{pack_name}</code>: <code>{e}</code>",
                    parse_mode="HTML",
                )
                context.user_data.pop("paint_sticker", None)
                return

            stickers   = pack.stickers
            total      = len(stickers)
            # Определяем тип пака: эмодзи или обычный
            is_emoji_pack = (pack.sticker_type == StickerType.CUSTOM_EMOJI)
            sticker_type  = StickerType.CUSTOM_EMOJI if is_emoji_pack else StickerType.REGULAR
            # Определяем формат пака по первому стикеру (включая видео)
            first = stickers[0] if stickers else None
            if first is None:
                await status.edit_text("❌ Пак пуст.", parse_mode="HTML")
                context.user_data.pop("paint_sticker", None)
                return
            if first.is_video:
                pack_fmt = StickerFormat.VIDEO
            elif first.is_animated:
                pack_fmt = StickerFormat.ANIMATED
            else:
                pack_fmt = StickerFormat.STATIC

            await status.edit_text(
                f"🔄 <b>Перекрашиваю {total} {'эмодзи' if is_emoji_pack else 'стикеров'}...</b>",
                parse_mode="HTML",
            )

            bot_me       = await bot.get_me()
            new_pack_name  = _paint_make_pack_name(bot_me.username, uid, pack_name, hex_color)
            new_pack_title = f"{pack.title} [{hex_color}]"[:64]

            ok            = 0
            pack_created  = False

            for i, s in enumerate(stickers):
                try:
                    # Пробуем скачать файл; для премиум-эмодзи это может не работать
                    raw = None
                    try:
                        raw = await dl(s.file_id)
                    except Exception as dl_err:
                        dl_err_s = str(dl_err).lower()
                        # Премиум-эмодзи часто недоступны для скачивания —
                        # пробуем через file_path напрямую
                        if "file is too big" in dl_err_s or "inaccessible" in dl_err_s or "invalid" in dl_err_s or "wrong" in dl_err_s:
                            try:
                                tg_file = await bot.get_file(s.file_id)
                                if tg_file.file_path:
                                    import urllib.request
                                    with urllib.request.urlopen(tg_file.file_path) as resp:
                                        raw = resp.read()
                            except Exception:
                                pass
                        if raw is None:
                            logger.warning("paint sticker %d/%d: cannot download, skipping: %s", i+1, total, dl_err)
                            continue

                    if s.is_animated:
                        fmt = StickerFormat.ANIMATED
                        out = recolor_tgs(raw, hex_color)
                    elif s.is_video:
                        fmt = StickerFormat.VIDEO
                        out = recolor_webm(raw, hex_color) or raw
                    else:
                        fmt = StickerFormat.STATIC
                        out = recolor_webp(raw, hex_color)
                        # webp → png
                        img2 = Image.open(io.BytesIO(out)).convert("RGBA")
                        # для эмодзи 100×100, для стикеров 512×512
                        size = 100 if is_emoji_pack else 512
                        img2 = img2.resize((size, size), Image.LANCZOS)
                        b2   = io.BytesIO()
                        img2.save(b2, "PNG")
                        out = b2.getvalue()

                    emoji_char = s.emoji or "⭐"
                    sticker_obj = await upload_and_make_sticker(out, fmt, emoji_char)

                    if not pack_created:
                        await create_set(new_pack_name, new_pack_title, sticker_obj, fmt, sticker_type)
                        pack_created = True
                    else:
                        await bot.add_sticker_to_set(user_id=uid, name=new_pack_name, sticker=sticker_obj)
                    ok += 1

                except Exception as e:
                    logger.warning("paint sticker %d/%d failed: %s", i+1, total, e)

                if (i + 1) % 10 == 0:
                    try:
                        await status.edit_text(
                            f"🔄 <b>{i+1}/{total}</b> обработано...", parse_mode="HTML"
                        )
                    except Exception:
                        pass
                await asyncio.sleep(0.35)

            try:
                await status.delete()
            except Exception:
                pass

            if pack_created:
                prefix   = "addemoji" if is_emoji_pack else "addstickers"
                pack_link = f"https://t.me/{prefix}/{new_pack_name}"
                # Сохраняем данные о покрашенном паке — нужны для кнопки "Сохранить"
                context.user_data["paint_result_pack"] = {
                    "name":  new_pack_name,
                    "title": new_pack_title,
                    "type":  "emoji" if is_emoji_pack else "sticker",
                    "count": ok,
                }
                await msg.reply_text(
                    f"✅ <b>Готово!</b> {ok}/{total} {'эмодзи' if is_emoji_pack else 'стикеров'} перекрашено\n"
                    f"🖌 Цвет: <code>{hex_color}</code>\n\n"
                    f"📦 <b>Ваш пак:</b> <a href=\"{pack_link}\">{new_pack_title}</a>",
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💾 Сохранить в мои паки", callback_data="paint_save_pack")],
                        [InlineKeyboardButton("🏠 В меню", callback_data="begin")],
                    ]),
                )
            else:
                await msg.reply_text(
                    f"❌ Не удалось перекрасить ни одного {'эмодзи' if is_emoji_pack else 'стикера'}.\n"
                    f"<i>Проверь что пак существует и содержит статичные/анимированные стикеры.</i>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
                )
            context.user_data.pop("paint_sticker", None)
            return

        context.user_data["paint_last_color"] = hex_color
        context.user_data.pop("paint_sticker", None)
        await msg.reply_text(
            f"✅ Готово! Цвет: <code>{hex_color}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
        )

    except Exception as exc:
        logger.exception("_paint_execute error: %s", exc)
        err_text = f"❌ Ошибка: <code>{_friendly_tg_error(exc)}</code>"
        sent = False
        try:
            await status.edit_text(err_text, parse_mode="HTML")
            sent = True
        except Exception:
            pass
        if not sent:
            try:
                await msg.reply_text(err_text, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]))
            except Exception:
                pass
        context.user_data.pop("paint_sticker", None)




async def paint_save_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет покрашенный пак в 'Мои паки' и переходит к управлению им."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    pack_info = context.user_data.get("paint_result_pack")
    if not pack_info:
        await query.answer("⚠️ Данные о паке не найдены. Покрась пак заново.", show_alert=True)
        return PAINT_SCOPE

    packs = get_packs(context, user_id)

    # Проверяем — вдруг уже сохранён
    existing_idx = next(
        (i for i, p in enumerate(packs) if p.get("name") == pack_info["name"]),
        None,
    )

    if existing_idx is None:
        # Добавляем новый пак в список
        packs.append({
            "title":    pack_info["title"],
            "name":     pack_info["name"],
            "suffix":   pack_info["name"].split("_by_")[0],  # часть до _by_botname
            "type":     pack_info["type"],
            "count":    pack_info["count"],
            "adaptive": False,
        })
        save_packs(context, user_id)
        existing_idx = len(packs) - 1

    # Очищаем временные данные
    context.user_data.pop("paint_result_pack", None)

    # Переходим сразу к управлению этим паком
    context.user_data["selected_pack_index"] = existing_idx
    pack  = packs[existing_idx]
    is_emoji = pack["type"] in ("emoji", "emoji_adaptive")
    icon  = "✨" if is_emoji else "🖼"
    n     = pack.get("count", 0)
    url   = pack_url(pack["name"])
    item_word = "эмодзи" if is_emoji else "стикеров"
    paint_label = "🖼️ Покрасить эмодзи" if is_emoji else "🖼️ Покрасить стикер"
    kb = [
        [InlineKeyboardButton("🔗 Открыть пак",           url=url)],
        [InlineKeyboardButton("✏️ Переименовать пак",       callback_data="mgmt_rename")],
        [InlineKeyboardButton("➕ Добавить " + ("эмодзи" if is_emoji else "стикер"), callback_data="mgmt_add")],
        [InlineKeyboardButton("🗑 Удалить " + ("эмодзи" if is_emoji else "стикер"),  callback_data="mgmt_delete")],
        [InlineKeyboardButton("🖼 Сменить иконку пака",    callback_data="mgmt_icon")],
        [InlineKeyboardButton(paint_label, callback_data=f"paint_from_pack:{pack['name']}")],
        [InlineKeyboardButton("❌ Удалить весь пак",        callback_data="mgmt_delete_all")],
        [InlineKeyboardButton("◀️ К списку паков",          callback_data="list_packs")],
    ]
    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Сохранено!", callback_data="noop_ps")],
            [InlineKeyboardButton("📋 Перейти в мои паки", callback_data="list_packs")],
            [InlineKeyboardButton("🏠 В меню", callback_data="begin")],
        ])
    )
    # Отправляем новое сообщение с управлением паком
    await update.effective_message.reply_text(
        f"{icon} <b>{pack['title']}</b>\n{item_word.capitalize()}: {n}\n\n"
        f"✅ <i>Пак сохранён в «Мои паки»</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED



def get_packs(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> list:
    """Возвращает список паков пользователя.
    Если DATABASE_URL задан — из PostgreSQL, иначе из bot_data (память).
    """
    if DATABASE_URL:
        # Кэшируем в bot_data чтобы не дёргать БД на каждый запрос
        cache_key = f"packs_{user_id}"
        if cache_key not in context.bot_data:
            context.bot_data[cache_key] = DB.load_packs(user_id)
        return context.bot_data[cache_key]
    return context.bot_data.setdefault(str(user_id), {}).setdefault("packs", [])


def save_packs(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Сохраняет паки пользователя в БД."""
    if DATABASE_URL:
        cache_key = f"packs_{user_id}"
        packs = context.bot_data.get(cache_key, [])
        DB.save_packs(user_id, packs)


def back_btn(label: str = "◀️ Назад", data: str = "begin") -> list:
    return [InlineKeyboardButton(label, callback_data=data)]


def _friendly_tg_error(err: str) -> str:
    e = str(err)
    if "STICKERSET_INVALID" in e:
        return "Пак не найден или недопустимое имя. Попробуй другую ссылку."
    if "PEER_ID_INVALID" in e:
        return "Сначала напиши боту /start в личке Telegram."
    if "file is too big" in e.lower():
        return "Файл слишком большой даже после сжатия."
    if "STICKERS_TOO_MUCH" in e:
        return "В паке уже максимальное количество стикеров (120)."
    if "bot was blocked" in e.lower():
        return "Бот заблокирован. Разблокируй его и попробуй снова."
    if "STICKER_PNG_NOPNG" in e or "wrong file type" in e.lower():
        return "Неверный формат. Отправь фото, видео, стикер или гифку."
    if "STICKER_EMOJI_INVALID" in e:
        return "Неверный эмодзи. Используй стандартный смайлик, например: 😊"
    if "flood" in e.lower() or "retry" in e.lower():
        return "Telegram временно ограничил запросы. Подожди немного и попробуй снова."
    if "timeout" in e.lower():
        return "Telegram не ответил вовремя. Попробуй снова."
    if "name is already" in e.lower() or "already occupied" in e.lower():
        return "Эта ссылка уже занята. Попробуй другое название."
    return e


# ── Обработка изображений ─────────────────────────────────────────────────────

def process_image(raw_bytes: bytes, size: int, max_bytes: int = MAX_STICKER_BYTES) -> bytes:
    img = Image.open(io.BytesIO(raw_bytes))
    original_mode = img.mode
    # Если у оригинала нет прозрачности — используем белый фон
    has_alpha = original_mode in ("RGBA", "LA", "PA") or (
        original_mode == "P" and "transparency" in img.info
    )
    img = img.convert("RGBA")
    ratio = min(size / max(img.width, 1), size / max(img.height, 1))
    new_w = max(1, round(img.width * ratio))
    new_h = max(1, round(img.height * ratio))
    img_resized = img.resize((new_w, new_h), Image.LANCZOS)
    # Прозрачный фон только если у оригинала была прозрачность (стикеры, PNG с alpha)
    bg_color = (0, 0, 0, 0) if has_alpha else (255, 255, 255, 255)
    canvas = Image.new("RGBA", (size, size), bg_color)
    offset = ((size - new_w) // 2, (size - new_h) // 2)
    canvas.paste(img_resized, offset, mask=img_resized)
    # Сохраняем с минимальным сжатием (лучшее качество)
    for compress in range(1, 10):
        buf = io.BytesIO()
        canvas.save(buf, format="PNG", optimize=True, compress_level=compress)
        data = buf.getvalue()
        if len(data) <= max_bytes:
            return data
    # Только если не влезает — уменьшаем
    scale = 0.95
    while scale > 0.2:
        sw = max(1, round(new_w * scale))
        sh = max(1, round(new_h * scale))
        r2 = img.resize((sw, sh), Image.LANCZOS)
        c2 = Image.new("RGBA", (size, size), bg_color)
        c2.paste(r2, ((size - sw) // 2, (size - sh) // 2), mask=r2)
        buf = io.BytesIO()
        c2.save(buf, format="PNG", optimize=True, compress_level=9)
        data = buf.getvalue()
        if len(data) <= max_bytes:
            return data
        scale -= 0.05
    return data


def process_image_for_sticker(raw_bytes: bytes, pack_type: str = "sticker") -> bytes:
    # стикер: 512x512 макс 500кб | эмодзи: 100x100 макс 100кб
    if "emoji" in pack_type:
        return process_image(raw_bytes, 100, 100 * 1024)
    return process_image(raw_bytes, 512, MAX_STICKER_BYTES)


def process_image_for_thumbnail(raw_bytes: bytes) -> bytes:
    return process_image(raw_bytes, 100, max_bytes=32 * 1024)


def _get_ffmpeg_path() -> str:
    import shutil
    path = shutil.which("ffmpeg")
    if path:
        return path
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        pass
    return "ffmpeg"


def convert_to_webm(raw_bytes: bytes, pack_type: str = "sticker") -> bytes | None:
    """Конвертирует MP4/GIF в WEBM VP9 для Telegram.
    sticker: 512x512, макс 256кб, 3 сек
    emoji:   100x100, макс 100кб, 3 сек
    """
    import subprocess, tempfile, os
    ffmpeg = _get_ffmpeg_path()
    is_emoji = "emoji" in pack_type
    dim      = 100 if is_emoji else 512
    max_size = 90 * 1024 if is_emoji else 240 * 1024

    def _run(in_path, out_path, bitrate, crf):
        vf = (
            f"scale={dim}:{dim}:force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={dim}:{dim}:(ow-iw)/2:(oh-ih)/2:color=black@0,"
            f"fps=30"
        )
        pix = "yuva420p" if is_emoji else "yuv420p"
        cmd = [
            ffmpeg, "-y", "-i", in_path,
            "-t", "3",
            "-vf", vf,
            "-c:v", "libvpx-vp9",
            "-b:v", bitrate,
            "-crf", str(crf),
            "-deadline", "good",
            "-cpu-used", "2",
            "-pix_fmt", pix,
            "-an",
            out_path
        ]
        r = subprocess.run(cmd, capture_output=True, timeout=120)
        if r.returncode != 0:
            logger.warning("ffmpeg stderr: %s", r.stderr.decode(errors="replace")[-500:])
        return r.returncode == 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Не указываем расширение — ffmpeg сам определит формат
        in_path  = os.path.join(tmpdir, "input_raw")
        out_path = os.path.join(tmpdir, "output.webm")
        with open(in_path, "wb") as f:
            f.write(raw_bytes)
        try:
            # Попытка 1: хорошее качество
            ok = _run(in_path, out_path, "200k" if not is_emoji else "80k", 30)
            if ok and os.path.exists(out_path):
                data = open(out_path, "rb").read()
                logger.info("convert_to_webm pass1: %d bytes", len(data))
                if len(data) <= max_size:
                    return data
            # Попытка 2: меньше битрейт если файл большой
            ok = _run(in_path, out_path, "0", 45)
            if ok and os.path.exists(out_path):
                data = open(out_path, "rb").read()
                logger.info("convert_to_webm pass2: %d bytes", len(data))
                return data
            return None
        except FileNotFoundError:
            logger.warning("ffmpeg не найден")
            return None
        except Exception as e:
            logger.warning("convert_to_webm failed: %s", e)
            return None


async def get_sticker_data(bot, msg, pack_type: str = "sticker"):
    """Универсальная обработка любого медиа-файла для стикера/эмодзи.
    Поддерживает: фото, видео, GIF, стикеры (static/animated/video), документы.
    """
    async def dl(file_id: str) -> bytes:
        f   = await bot.get_file(file_id)
        buf = io.BytesIO()
        await f.download_to_memory(buf)
        buf.seek(0)
        return buf.read()

    def to_image(raw: bytes) -> bytes | None:
        """Конвертирует любой формат изображения в PNG для стикера."""
        try:
            img = Image.open(io.BytesIO(raw))
            if img.mode != "RGBA":
                img = img.convert("RGBA")
            buf = io.BytesIO()
            img.save(buf, "PNG")
            return process_image_for_sticker(buf.getvalue(), pack_type)
        except Exception as e:
            logger.warning("to_image failed: %s", e)
            return None

    def to_video(raw: bytes) -> bytes | None:
        """Конвертирует видео/GIF в WEBM для стикера."""
        return convert_to_webm(raw, pack_type)

    # ── Готовый стикер TG (включая премиум эмодзи) ───────────────────────────
    if msg.sticker:
        s = msg.sticker
        # Определяем формат
        if getattr(s, "is_animated", False):
            fmt = StickerFormat.ANIMATED
        elif getattr(s, "is_video", False):
            fmt = StickerFormat.VIDEO
        elif hasattr(s, "format") and s.format:
            fmt = s.format
        else:
            fmt = StickerFormat.STATIC

        # Премиум эмодзи (CUSTOM_EMOJI) — используем file_id напрямую без скачивания.
        # Telegram позволяет передавать file_id существующего стикера в InputSticker.
        sticker_type_tg = getattr(s, "type", None)
        is_custom_emoji = (str(sticker_type_tg) == "custom_emoji" or
                           sticker_type_tg == StickerType.CUSTOM_EMOJI)
        if is_custom_emoji:
            logger.info("Premium emoji sticker: using file_id directly, fmt=%s", fmt)
            # Возвращаем file_id + формат + флаг is_custom_emoji=True
            return s.file_id, fmt, True

        raw = await dl(s.file_id)
        if fmt == StickerFormat.STATIC:
            result = to_image(raw)
            return (result, StickerFormat.STATIC) if result else (None, None)
        if fmt == StickerFormat.VIDEO:
            result = to_video(raw)
            return (result, StickerFormat.VIDEO) if result else (raw, StickerFormat.VIDEO)
        if fmt == StickerFormat.ANIMATED:
            return raw, StickerFormat.ANIMATED
        return raw, fmt

    # ── Фото (сжатое Telegram) ────────────────────────────────────────────────
    if msg.photo:
        raw    = await dl(msg.photo[-1].file_id)
        result = to_image(raw)
        return (result, StickerFormat.STATIC) if result else (None, None)

    # ── Видео (mp4 из Telegram) ───────────────────────────────────────────────
    if msg.video:
        raw    = await dl(msg.video.file_id)
        result = to_video(raw)
        return (result, StickerFormat.VIDEO) if result else (None, None)

    # ── GIF-анимация ──────────────────────────────────────────────────────────
    if msg.animation:
        raw    = await dl(msg.animation.file_id)
        result = to_video(raw)
        return (result, StickerFormat.VIDEO) if result else (None, None)

    # ── Документ (файл отправлен без сжатия) ──────────────────────────────────
    if msg.document:
        d    = msg.document
        mime = (d.mime_type or "").lower()
        name = (d.file_name or "").lower()
        raw  = await dl(d.file_id)

        # TGS → анимированный стикер (без конвертации)
        if "tgs" in mime or name.endswith(".tgs"):
            return raw, StickerFormat.ANIMATED

        # WEBM → видео-стикер (уже готовый формат, только переразмерить)
        if "webm" in mime or name.endswith(".webm"):
            result = to_video(raw)
            return (result, StickerFormat.VIDEO) if result else (raw, StickerFormat.VIDEO)

        # GIF, MP4, MOV, AVI, видео → конвертируем в WEBM
        is_video = (
            "gif" in mime or "video" in mime
            or any(name.endswith(x) for x in (".gif", ".mp4", ".mov", ".avi", ".webm"))
        )
        if is_video:
            result = to_video(raw)
            return (result, StickerFormat.VIDEO) if result else (None, None)

        # Изображения (PNG, WEBP, JPG, BMP и т.д.)
        result = to_image(raw)
        if result:
            return result, StickerFormat.STATIC
        # Если PIL не смог — пробуем как видео
        result = to_video(raw)
        return (result, StickerFormat.VIDEO) if result else (None, None)

    return None, None


async def get_custom_emoji_sticker_data(bot, custom_emoji_id: str):
    """Получает данные стикера для премиум-эмодзи по его custom_emoji_id.
    Возвращает (file_id, fmt, True) или (None, None, False).
    """
    try:
        stickers = await bot.get_custom_emoji_stickers([custom_emoji_id])
        if not stickers:
            return None, None, False
        s = stickers[0]
        if getattr(s, "is_animated", False):
            fmt = StickerFormat.ANIMATED
        elif getattr(s, "is_video", False):
            fmt = StickerFormat.VIDEO
        elif hasattr(s, "format") and s.format:
            fmt = s.format
        else:
            fmt = StickerFormat.STATIC
        logger.info("get_custom_emoji_sticker_data: file_id=%s fmt=%s", s.file_id, fmt)
        return s.file_id, fmt, True
    except Exception as e:
        logger.warning("get_custom_emoji_sticker_data failed: %s", e)
        return None, None, False




async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    kb = [
        [InlineKeyboardButton("🖼  Создать стикер-пак",      callback_data="create_sticker")],
        [InlineKeyboardButton("✨  Создать эмодзи-пак",       callback_data="create_emoji")],
        [InlineKeyboardButton("🚗  Создать номерной знак",    callback_data="create_plate")],
        [InlineKeyboardButton("📋  Скопировать пак",          callback_data="copy_pack")],
        [InlineKeyboardButton("🎨  Покрасить стикер",         callback_data="paint_start")],
    ]
    text = (
        "👋 <b>Что хочешь создать?</b>\n\n"
        "• <b>Стикер-пак</b> — обычные стикеры\n"
        "• <b>Эмодзи-пак</b> — premium emoji\n"
        "• <b>Номерной знак</b> — номера стран СНГ\n"
        "• <b>Скопировать пак</b> — скопировать, конвертировать пак\n"
        "• <b>Покрасить стикер</b> — перекрасить стикер или пак в любой цвет"
    )
    markup = InlineKeyboardMarkup(kb)
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
    else:
        await update.effective_message.reply_text(text, parse_mode="HTML", reply_markup=markup)
    return CHOOSE_TYPE


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user    = update.effective_user
    user_id = user.id
    name    = f"@{user.username}" if user.username else user.first_name

    # Регистрируем профиль при первом входе
    DB.init_profile(user_id)
    DB.record_daily_user(user_id)
    profile = DB.get_profile(user_id)

    reg_date = profile["registered_at"]
    if reg_date:
        reg_str = reg_date.strftime("%Y-%m-%d")
    else:
        import datetime
        reg_str = datetime.date.today().strftime("%Y-%m-%d")

    purchases   = profile["purchases"]
    stars_spent = profile["stars_spent"]

    name = user.first_name or user.username or str(user_id)

    # Считаем стикеры/эмодзи/номера из паков
    packs = get_packs(context, user_id)
    sticker_count = sum(p.get("count", 0) for p in packs if not p.get("plate_pack") and p.get("type") == "sticker")
    emoji_count   = sum(p.get("count", 0) for p in packs if not p.get("plate_pack") and p.get("type") in ("emoji", "emoji_adaptive"))
    plate_count   = sum(p.get("count", 0) for p in packs if p.get("plate_pack"))

    text = (
        f"👤 <b>Профиль {name}</b>\n\n"
        f"<blockquote>"
        f"🖼 Стикеров: <b>{sticker_count}</b>\n"
        f"✨ Эмодзи: <b>{emoji_count}</b>\n"
        f"🚗 Номеров: <b>{plate_count}</b>\n"
        f"🛍️ Покупок: <b>{purchases}</b>\n"
        f"⭐ Потрачено: <b>{stars_spent}</b>\n"
        f"📅 Регистрация: <b>{reg_str}</b>"
        f"</blockquote>"
    )

    kb = [
        [InlineKeyboardButton("Меню 🏠", callback_data="start_begin"),
         InlineKeyboardButton("Мои паки 📁", callback_data="start_packs")],
        [InlineKeyboardButton("Поддержка ℹ️", url="https://t.me/tntks")],
    ]

    PROFILE_VIDEO = os.environ.get("PROFILE_VIDEO_FILE_ID", "").strip()

    sent = False
    if PROFILE_VIDEO:
        try:
            await update.message.reply_video(
                video=PROFILE_VIDEO,
                caption=text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            sent = True
        except Exception as e:
            logger.warning("reply_video failed (%s), falling back to text", e)

    if not sent:
        await update.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
    return MAIN_MENU


async def start_begin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Кнопка 'Начать' из профиля — отправляет новое сообщение с главным меню."""
    query = update.callback_query
    await query.answer()
    kb = [
        [InlineKeyboardButton("🖼  Создать стикер-пак",   callback_data="create_sticker")],
        [InlineKeyboardButton("✨  Создать эмодзи-пак",    callback_data="create_emoji")],
        [InlineKeyboardButton("🚗  Создать номерной знак", callback_data="create_plate")],
        [InlineKeyboardButton("📋  Скопировать пак",       callback_data="copy_pack")],
        [InlineKeyboardButton("🎨  Покрасить стикер",      callback_data="paint_start")],
    ]
    text = (
        "👋 <b>Что хочешь создать?</b>\n\n"
        "• <b>Стикер-пак</b> — обычные стикеры\n"
        "• <b>Эмодзи-пак</b> — premium emoji\n"
        "• <b>Номерной знак</b> — номера стран СНГ\n"
        "• <b>Скопировать пак</b> — скопировать, конвертировать пак\n"
        "• <b>Покрасить стикер</b> — перекрасить стикер или пак в любой цвет"
    )
    await query.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
    return CHOOSE_TYPE


async def start_packs_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Кнопка 'Мои паки' из профиля — отправляет новое сообщение со списком паков."""
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    packs   = get_packs(context, user_id)

    if not packs:
        await query.message.reply_text(
            "У тебя пока нет созданных паков.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="start_begin")]]),
        )
        return MAIN_MENU

    kb = []
    for i, p in enumerate(packs):
        if p.get("plate_pack"):
            icon = "🖼️" if p["type"] == "sticker" else "✨"
        elif p["type"] == "sticker":
            icon = "🖼"
        elif p.get("adaptive"):
            icon = "🖼️"
        else:
            icon = "✨"
        n = p.get("count", 0)
        count_label = plural_nomer(n) if p.get("plate_pack") else plural_sticker(n)
        kb.append([InlineKeyboardButton(
            f"{icon} {p['title']} ({n} {count_label})",
            callback_data=f"select_pack_{i}",
        )])
    kb.append(back_btn("🏠 В меню", "begin"))
    await query.message.reply_text(
        "🕐 Изменения в наборе отобразятся в течение часа (или перезайди в тг чтобы увидеть изменения)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED


async def packs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /packs — показывает список паков пользователя."""
    user_id = update.effective_user.id
    packs   = get_packs(context, user_id)

    if not packs:
        await update.message.reply_text(
            "У тебя пока нет созданных паков.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="begin")]]),
        )
        return MAIN_MENU

    kb = []
    for i, p in enumerate(packs):
        if p.get("plate_pack"):
            icon = "🖼️" if p["type"] == "sticker" else "✨"
        elif p["type"] == "sticker":
            icon = "🖼"
        elif p.get("adaptive"):
            icon = "🖼️"
        else:
            icon = "✨"
        n = p.get("count", 0)
        count_label = plural_nomer(n) if p.get("plate_pack") else plural_sticker(n)
        kb.append([InlineKeyboardButton(
            f"{icon} {p['title']} ({n} {count_label})",
            callback_data=f"select_pack_{i}",
        )])
    kb.append(back_btn("🏠 В меню", "begin"))
    await update.message.reply_text(
        "🕐 Изменения в наборе отобразятся в течение часа (или перезайди в тг чтобы увидеть изменения)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /menu — сбрасывает состояние и показывает главное меню."""
    for key in (
        "new_pack_title", "new_pack_suffix", "new_pack_stickers",
        "creating_type", "selected_pack_index", "pending_data", "pending_fmt",
        "plate_country", "plate_region", "file_queue", "file_queue_done",
        "fire_emoji_confirmed_new", "fire_btn_shown_new",
        "fire_emoji_confirmed_mgmt", "fire_btn_shown_mgmt",
    ):
        context.user_data.pop(key, None)
    return await send_main_menu(update, context)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Ключи активных процессов: создание пака, номера, копирование
    active_keys = (
        "creating_type", "new_pack_title",          # создание пака
        "plate_country",                             # создание номера
        "copy_pack_source_name", "_user_id_copy",   # копирование пака
        "adm_price_key", "adm_scheduled_msg",        # admin flows
    )
    is_active = any(k in context.user_data for k in active_keys)
    if not is_active:
        return CHOOSE_TYPE  # молча игнорируем — нет активного процесса

    # Очищаем все данные процессов
    for k in (
        "creating_type", "plate_country", "selected_pack_index",
        "new_pack_title", "adm_price_key", "adm_scheduled_msg",
        "copy_pack_source_name", "_user_id_copy",
        "copy_pack_source_type", "copy_pack_source_title", "copy_pack_sticker_count",
        "copy_target_type", "copy_result_pack_name", "copy_result_pack_title",
        "copy_result_pack_type", "copy_result_pack_count",
    ):
        context.user_data.pop(k, None)
    return await send_main_menu(update, context)


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN PANEL
# ══════════════════════════════════════════════════════════════════════════════

def _admin_main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Пользователи",       callback_data="adm_users")],
        [InlineKeyboardButton("📊 Статистика",          callback_data="adm_stats")],
        [InlineKeyboardButton("📣 Рассылка",            callback_data="adm_broadcast")],
        [InlineKeyboardButton("🕐 Отложенная рассылка", callback_data="adm_schedule")],
        [InlineKeyboardButton("💰 Цены",                callback_data="adm_prices")],
        [InlineKeyboardButton("🔧 Тех. перерыв",         callback_data="adm_maintenance")],
    ])

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return MAIN_MENU  # молча игнорируем
    await update.message.reply_text(
        "🛠 <b>Админ-панель</b>",
        parse_mode="HTML",
        reply_markup=_admin_main_kb(),
    )
    return ADMIN_MENU


async def adm_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "🛠 <b>Админ-панель</b>",
        parse_mode="HTML",
        reply_markup=_admin_main_kb(),
    )
    return ADMIN_MENU


# ── Пользователи ──────────────────────────────────────────────────────────────
async def adm_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    users = DB.get_all_users()
    if not users:
        text = "👥 <b>Пользователи</b>\n\nПока нет ни одного пользователя."
    else:
        lines = [f"👥 <b>Пользователи ({len(users)} чел.)</b>\n"]
        for uid, reg_at, purchases, stars, _ in users[:50]:
            reg = reg_at.strftime("%d.%m.%Y %H:%M") if reg_at else "—"
            lines.append(f"• <code>{uid}</code> | {reg} | 🛍{purchases} ⭐{stars}")
        if len(users) > 50:
            lines.append(f"\n<i>...и ещё {len(users) - 50} чел.</i>")
        text = "\n".join(lines)
    await q.edit_message_text(
        text, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm_back")]]),
    )
    return ADMIN_MENU


# ── Статистика ────────────────────────────────────────────────────────────────
def _stats_text_and_kb(stats: dict, day_users: int, day=None, page: int = 0, total_pages: int = 1) -> tuple:
    import datetime as _dt
    if day is None:
        header = "📊 <b>Статистика за все время</b>"
        date_line = ""
    else:
        try:
            d = _dt.date.fromisoformat(str(day))
            date_line = d.strftime("%d.%m.%Y")
        except Exception:
            date_line = str(day)
        header = f"📊 <b>Статистика за {date_line}</b>"

    sp = stats.get("sticker_packs_created", 0)
    ep = stats.get("emoji_packs_created", 0)
    pl = stats.get("plates_created", 0)
    sa = stats.get("stickers_added", 0)
    ea = stats.get("emoji_added", 0)

    text = (
        f"{header}\n"
        "━━━━━━━━━━━━━━━━\n"
        f"👤 Пользователей:     <b>{day_users}</b>\n"
        f"🖼 Стикер-паков:      <b>{sp}</b>\n"
        f"✨ Эмодзи-паков:      <b>{ep}</b>\n"
        f"🚗 Номерных знаков:   <b>{pl}</b>\n"
        f"➕ Стикеров в паках:  <b>{sa}</b>\n"
        f"➕ Эмодзи в паках:    <b>{ea}</b>\n"
        "━━━━━━━━━━━━━━━━"
    )
    if total_pages > 1:
        text += f"\n📄 Страница <b>{page + 1}</b> / <b>{total_pages}</b>"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Новее", callback_data=f"adm_stats_p_{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Старее ▶️", callback_data=f"adm_stats_p_{page + 1}"))

    kb = []
    if nav:
        kb.append(nav)
    if day is not None:
        kb.append([InlineKeyboardButton("📊 За все время", callback_data="adm_stats_p_0")])
    kb.append([InlineKeyboardButton("◀️ В меню", callback_data="adm_back")])

    return text, InlineKeyboardMarkup(kb)


async def adm_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    stats       = DB.get_stats()
    total_users = len(DB.get_all_users())
    days        = DB.get_stats_days()
    total_pages = len(days) + 1  # стр.0 = все время, стр.1..N = дни

    text, kb = _stats_text_and_kb(
        stats=stats, day_users=total_users,
        day=None, page=0, total_pages=total_pages,
    )
    await q.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
    return ADMIN_MENU


async def adm_stats_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    page  = int(q.data.replace("adm_stats_p_", ""))
    days  = DB.get_stats_days()
    total = len(days) + 1

    if page == 0 or not days:
        stats       = DB.get_stats()
        total_users = len(DB.get_all_users())
        text, kb = _stats_text_and_kb(
            stats=stats, day_users=total_users,
            day=None, page=0, total_pages=total,
        )
    else:
        idx = min(page - 1, len(days) - 1)
        day = days[idx]
        text, kb = _stats_text_and_kb(
            stats=DB.get_stats_for_day(day),
            day_users=DB.get_daily_users_count(day),
            day=day, page=page, total_pages=total,
        )

    await q.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
    return ADMIN_MENU


async def adm_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await adm_stats(update, context)


# ── Рассылка ──────────────────────────────────────────────────────────────────
async def adm_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    context.user_data.pop("adm_broadcast_photo", None)
    await q.edit_message_text(
        "📣 <b>Рассылка</b>\n\n"
        "Отправь сообщение для рассылки.\n"
        "Можно отправить текст или фото с подписью.\n\n"
        "Для отмены нажми кнопку ниже.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm_back")]]),
    )
    return ADMIN_BROADCAST


async def adm_broadcast_receive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return ADMIN_BROADCAST
    msg = update.message
    user_ids = DB.get_all_user_ids()
    ok = fail = 0
    if msg.photo:
        photo_id = msg.photo[-1].file_id
        caption  = msg.caption or ""
        for uid in user_ids:
            try:
                await context.bot.send_photo(chat_id=uid, photo=photo_id, caption=caption, parse_mode="HTML")
                ok += 1
            except Exception:
                fail += 1
    else:
        text = msg.text or ""
        for uid in user_ids:
            try:
                await context.bot.send_message(chat_id=uid, text=text, parse_mode="HTML")
                ok += 1
            except Exception:
                fail += 1
    await msg.reply_text(
        f"✅ Рассылка завершена.\nДоставлено: <b>{ok}</b>\nОшибок: <b>{fail}</b>",
        parse_mode="HTML",
        reply_markup=_admin_main_kb(),
    )
    return ADMIN_MENU


# ── Отложенная рассылка ───────────────────────────────────────────────────────
async def adm_schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    context.user_data.pop("adm_scheduled_msg",   None)
    context.user_data.pop("adm_scheduled_photo", None)
    context.user_data.pop("adm_scheduled_caption", None)
    await q.edit_message_text(
        "🕐 <b>Отложенная рассылка</b>\n\n"
        "Шаг 1: отправь сообщение или фото с подписью.\n"
        "Потом укажу время.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm_back")]]),
    )
    return ADMIN_SCHEDULED_MSG


async def adm_schedule_receive_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return ADMIN_SCHEDULED_MSG
    msg = update.message
    if msg.photo:
        context.user_data["adm_scheduled_photo"]   = msg.photo[-1].file_id
        context.user_data["adm_scheduled_caption"] = msg.caption or ""
        context.user_data["adm_scheduled_msg"]     = None
    else:
        context.user_data["adm_scheduled_msg"]     = msg.text or ""
        context.user_data["adm_scheduled_photo"]   = None
    await msg.reply_text(
        "✅ Сообщение сохранено.\n\n"
        "Шаг 2: введи дату и время отправки в формате:\n"
        "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n\nПример: <code>25.12.2025 14:00</code>\n\n"
        "🕐 Время указывается по <b>МСК (UTC+3)</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm_back")]]),
    )
    return ADMIN_SCHEDULED_TIME


async def adm_schedule_receive_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return ADMIN_SCHEDULED_TIME
    import datetime
    import zoneinfo
    text = update.message.text.strip()
    try:
        naive_dt = datetime.datetime.strptime(text, "%d.%m.%Y %H:%M")
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат. Введи дату и время как: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>",
            parse_mode="HTML",
        )
        return ADMIN_SCHEDULED_TIME
    # Интерпретируем введённое время как московское (UTC+3) и переводим в UTC
    msk_tz = zoneinfo.ZoneInfo("Europe/Moscow")
    msk_dt = naive_dt.replace(tzinfo=msk_tz)
    send_at = msk_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)  # naive UTC для хранения в БД
    DB.save_scheduled_broadcast(
        send_at=send_at,
        message_text=context.user_data.get("adm_scheduled_msg"),
        photo_file_id=context.user_data.get("adm_scheduled_photo"),
        caption=context.user_data.get("adm_scheduled_caption"),
    )
    await update.message.reply_text(
        f"✅ Рассылка запланирована на <b>{naive_dt.strftime('%d.%m.%Y %H:%M')} МСК</b>",
        parse_mode="HTML",
        reply_markup=_admin_main_kb(),
    )
    return ADMIN_MENU


# ── Цены ──────────────────────────────────────────────────────────────────────
PRICE_LABELS = {
    "price_plate_sticker": "🚗 Номер → стикер-пак (⭐)",
    "price_plate_emoji":   "🚗 Номер → эмодзи-пак (⭐)",
}

async def adm_prices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    lines = ["💰 <b>Текущие цены:</b>\n"]
    kb = []
    for key, label in PRICE_LABELS.items():
        val = DB.get_setting(key, "1")
        lines.append(f"• {label}: <b>{val} ⭐</b>")
        kb.append([InlineKeyboardButton(f"✏️ {label}", callback_data=f"adm_setprice_{key}")])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="adm_back")])
    await q.edit_message_text(
        "\n".join(lines), parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return ADMIN_MENU


async def adm_price_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    key = q.data.replace("adm_setprice_", "")
    context.user_data["adm_price_key"] = key
    label = PRICE_LABELS.get(key, key)
    cur   = DB.get_setting(key, "1")
    await q.edit_message_text(
        f"✏️ <b>Изменить цену</b>\n\n{label}\nТекущая цена: <b>{cur} ⭐</b>\n\nВведи новое значение (целое число):",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm_back")]]),
    )
    return ADMIN_SET_PRICE


async def adm_price_receive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update):
        return ADMIN_SET_PRICE
    text = update.message.text.strip()
    if not text.isdigit() or int(text) < 1:
        await update.message.reply_text("❌ Введи целое число больше 0.")
        return ADMIN_SET_PRICE
    key   = context.user_data.get("adm_price_key", "")
    label = PRICE_LABELS.get(key, key)
    DB.set_setting(key, text)
    await update.message.reply_text(
        f"✅ Цена «{label}» изменена на <b>{text} ⭐</b>",
        parse_mode="HTML",
        reply_markup=_admin_main_kb(),
    )
    return ADMIN_MENU


# ── Тех. перерыв ─────────────────────────────────────────────────────────────
_MAINT_EMOJI = '<tg-emoji emoji-id="5345906554510012647">🔧</tg-emoji>'

async def adm_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    current = DB.get_setting("maintenance_mode", "0")
    if current == "1":
        DB.set_setting("maintenance_mode", "0")
        status = "✅ Тех. перерыв <b>выключен</b>. Бот работает в штатном режиме."
        # Оповещаем пользователей, которые писали во время перерыва
        waiters = DB.pop_maintenance_waiters()
        if waiters:
            notify_text = (
                "✅ Технические работы завершены!\n"
                "Бот снова работает в штатном режиме. Можете продолжать."
            )
            sent = 0
            for uid in waiters:
                try:
                    await context.bot.send_message(chat_id=uid, text=notify_text)
                    sent += 1
                except Exception:
                    pass
            status += f"\n\nОповещено пользователей: <b>{sent}</b>"
    else:
        DB.set_setting("maintenance_mode", "1")
        status = f"{_MAINT_EMOJI} Тех. перерыв <b>включён</b>. Пользователи видят сообщение о работах."
    await q.edit_message_text(
        status, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="adm_back")]]),
    )
    return ADMIN_MENU


# ── Обработчик технических работ для обычных пользователей ───────────────────
async def maintenance_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отвечает на любую команду/сообщение во время тех. перерыва."""
    if is_admin(update):
        return  # admin не блокируется
    await update.effective_message.reply_text(
        f'<tg-emoji emoji-id="5345906554510012647">🔧</tg-emoji> '
        "В данный момент проводятся технические работы. Пожалуйста, попробуйте позже.",
        parse_mode="HTML",
    )


# ── Job: проверка отложенных рассылок ────────────────────────────────────────
async def check_scheduled_broadcasts(context: ContextTypes.DEFAULT_TYPE):
    rows = DB.get_pending_broadcasts()
    for row in rows:
        broadcast_id, send_at, message_text, photo_file_id, caption = row
        user_ids = DB.get_all_user_ids()
        ok = fail = 0
        for uid in user_ids:
            try:
                if photo_file_id:
                    await context.bot.send_photo(chat_id=uid, photo=photo_file_id,
                                                  caption=caption or "", parse_mode="HTML")
                else:
                    await context.bot.send_message(chat_id=uid, text=message_text or "", parse_mode="HTML")
                ok += 1
            except Exception:
                fail += 1
        DB.mark_broadcast_done(broadcast_id)
        logger.info("Отложенная рассылка #%d: доставлено %d, ошибок %d", broadcast_id, ok, fail)


# ══════════════════════════════════════════════════════════════════════════════




async def begin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await send_main_menu(update, context)


# ── Список паков ──────────────────────────────────────────────────────────────

async def list_packs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    packs   = get_packs(context, user_id)

    if not packs:
        await query.edit_message_text(
            "У тебя пока нет созданных паков.",
            reply_markup=InlineKeyboardMarkup([back_btn()]),
        )
        return CHOOSE_TYPE

    kb = []
    for i, p in enumerate(packs):
        if p.get("plate_pack"):
            icon = "🖼️" if p["type"] == "sticker" else "✨"
        elif p["type"] == "sticker":
            icon = "🖼"
        elif p.get("adaptive"):
            icon = "🖼️"
        else:
            icon = "✨"
        n    = p.get("count", 0)
        count_label = plural_nomer(n) if p.get("plate_pack") else plural_sticker(n)
        kb.append([InlineKeyboardButton(
            f"{icon} {p['title']} ({n} {count_label})",
            callback_data=f"select_pack_{i}",
        )])
    kb.append(back_btn())
    await query.edit_message_text(
        "📋 <b>Твои паки:</b>\n\nВыбери пак для управления.\n\n"
        "🕐 Изменения в наборе отобразятся в течение часа (или перезайди в тг чтобы увидеть изменения)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED


async def select_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx   = int(query.data.replace("select_pack_", ""))
    context.user_data["selected_pack_index"] = idx
    # Сбрасываем флаги "везде 🔥" при возврате к паку — следующий раз спросим снова
    context.user_data.pop("fire_emoji_confirmed_mgmt", None)
    context.user_data.pop("fire_btn_shown_mgmt", None)
    user_id = update.effective_user.id
    pack    = get_packs(context, user_id)[idx]
    is_emoji = pack["type"] in ("emoji", "emoji_adaptive")
    is_adaptive = pack.get("adaptive", False)
    if pack.get("plate_pack"):
        icon = "🖼️" if pack["type"] == "sticker" else "✨"
    elif pack["type"] == "sticker":
        icon = "🖼"
    elif is_adaptive:
        icon = "🖼️"
    else:
        icon = "✨"
    n   = pack.get("count", 0)
    url = pack_url(pack["name"])
    is_plate     = bool(pack.get("plate_pack"))
    add_label    = "➕ Добавить эмодзи"  if is_emoji else "➕ Добавить стикер"
    delete_label = "🗑 Удалить эмодзи"   if is_emoji else "🗑 Удалить стикер"
    item_word    = "номеров" if is_plate else ("эмодзи" if is_emoji else "стикеров")
    paint_label  = "🖼️ Покрасить эмодзи" if is_emoji else "🖼️ Покрасить стикер"
    kb = [
        [InlineKeyboardButton("🔗 Открыть пак",        url=url)],
        [InlineKeyboardButton("✏️ Переименовать пак",   callback_data="mgmt_rename")],
        *([[InlineKeyboardButton(add_label,              callback_data="mgmt_add")]] if not is_plate else []),
        [InlineKeyboardButton(delete_label,             callback_data="mgmt_delete")],
        *([[InlineKeyboardButton("🖼 Сменить иконку пака", callback_data="mgmt_icon")]] if not is_plate else []),
        *([[InlineKeyboardButton(paint_label, callback_data=f"paint_from_pack:{pack['name']}")]] if not is_plate else []),
        [InlineKeyboardButton("❌ Удалить весь пак",    callback_data="mgmt_delete_all")],
        back_btn("◀️ К списку паков", "list_packs"),
    ]
    await query.edit_message_text(
        f"{icon} <b>{pack['title']}</b>\n{item_word.capitalize()}: {n}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED


# ── Создание пака ─────────────────────────────────────────────────────────────

async def start_create(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query     = update.callback_query
    await query.answer()
    pack_type = "sticker" if query.data == "create_sticker" else "emoji"
    context.user_data["creating_type"]   = pack_type
    context.user_data["emoji_adaptive"]  = False  # сброс флага
    # Сбрасываем флаги "везде 🔥" — новый пак, новая сессия
    context.user_data.pop("fire_emoji_confirmed_new", None)
    context.user_data.pop("fire_btn_shown_new", None)
    if pack_type == "emoji":
        kb = [[InlineKeyboardButton("✔️ Сделать адаптивным", callback_data="set_adaptive")]]
        await query.edit_message_text(
            "✨ <b>Создание эмодзи-пака</b>\n\n"
            "Если вы хотите сделать пак адаптивным (эмодзи для фона профиля) — "
            "нажмите кнопку ниже.\n\n"
            "✍️ <b>Введите название</b> для нового набора:\n\n/cancel — отменить",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return PACK_NAME
    else:
        await query.edit_message_text(
            "📝 <b>Создание стикер-пака</b>\n\nНапиши <b>название</b> набора:\n\n/cancel — отменить",
            parse_mode="HTML",
        )
        return PACK_NAME


async def set_adaptive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь нажал 'Сделать адаптивным'."""
    query = update.callback_query
    await query.answer()
    context.user_data["emoji_adaptive"] = True
    await query.edit_message_text(
        "✨ <b>Создание адаптивного эмодзи-пака</b>\n\n"
        "⚠️ <b>Важно!</b> Принимаются только подходящие форматы:\n"
        "• <b>PNG / WEBP</b> — статичный эмодзи (прозрачный фон)\n"
        "• <b>WEBM</b> — анимированный эмодзи\n\n"
        "Изображения должны быть <b>одноцветными с прозрачностью</b> — "
        "иначе будут чёрные квадратики.\n\n"
        "✍️ <b>Введите название</b> для нового набора:\n\n/cancel — отменить",
        parse_mode="HTML",
    )
    return PACK_NAME


async def receive_pack_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    title = update.message.text.strip()
    if not title:
        await update.message.reply_text("Название не может быть пустым:")
        return PACK_NAME
    context.user_data["new_pack_title"]    = title
    context.user_data["new_pack_stickers"] = []
    kb = [[InlineKeyboardButton("🎲 Случайная ссылка", callback_data="random_link")]]
    await update.message.reply_text(
        "🔗 <b>Придумай короткую ссылку</b>\n\nТолько латиница, цифры и <code>_</code>.\n\n/cancel — отменить",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_LINK


async def _prompt_first_sticker(update: Update, suffix: str, context=None) -> int:
    is_adaptive = context.user_data.get("emoji_adaptive", False) if context else False
    if is_adaptive:
        formats_text = "<i>Принимаются только: <b>PNG/WEBP</b> (статичный) или <b>WEBM</b> (анимация).</i>"
    else:
        formats_text = "<i>Поддерживаются: фото/видео/стикер/гифка</i>"
    text = (
        f"✅ Ссылка: <code>{suffix}</code> сохранена\n\n"
        "Теперь отправь мне первый файл👇\n"
        f"{formats_text}\n\n/cancel - отменить"
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="HTML")
    else:
        await update.message.reply_text(text, parse_mode="HTML")
    return ADDING_STICKER


async def use_random_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    suffix = random_suffix()
    context.user_data["new_pack_suffix"] = suffix
    return await _prompt_first_sticker(update, suffix, context)


async def receive_pack_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw    = update.message.text.strip()
    suffix = sanitize_suffix(raw)
    context.user_data["new_pack_suffix"] = suffix
    if suffix != raw.lower():
        await update.message.reply_text(
            f"ℹ️ Ссылка скорректирована: <code>{suffix}</code>", parse_mode="HTML"
        )
    return await _prompt_first_sticker(update, suffix, context)


async def _apply_fire_to_queue(bot_obj, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Применяет 🔥 ко всем оставшимся файлам в очереди. Возвращает кол-во успешно добавленных."""
    queue        = context.user_data.get("file_queue", [])
    done         = context.user_data.get("file_queue_done", 0)
    user_id      = context.user_data.get("_user_id", 0)
    pack_type    = context.user_data.get("creating_type", "sticker")
    if pack_type == "emoji" and context.user_data.get("emoji_adaptive"):
        pack_type = "emoji_adaptive"
    stickers_buf = context.user_data.setdefault("new_pack_stickers", [])
    suffix       = context.user_data.get("new_pack_suffix") or random_suffix()
    context.user_data["new_pack_suffix"] = suffix
    bot_username = context.user_data.get("_bot_username", "")
    if not bot_username:
        try:
            me = await bot_obj.get_me()
            bot_username = me.username
            context.user_data["_bot_username"] = bot_username
        except Exception:
            pass
    pack_name = build_pack_name(bot_username, suffix)
    title     = context.user_data.get("new_pack_title", "My Pack")
    added     = 0
    while done < len(queue):
        file_info = queue[done]
        data      = file_info["data"]
        fmt       = file_info["fmt"]
        is_ce     = file_info.get("is_custom_emoji", False)
        err = await _push_sticker_to_tg(
            bot_obj, user_id, pack_name, pack_type,
            title, data, fmt, ["🔥"], stickers_buf,
            is_custom_emoji=is_ce
        )
        if not err:
            stickers_buf.append({"fmt": str(fmt)})
            context.user_data["new_pack_stickers"] = stickers_buf
            added += 1
        done += 1
    context.user_data["file_queue_done"]  = done
    context.user_data["file_queue_added"] = context.user_data.get("file_queue_added", 0) + added
    return added


async def _ask_emoji_for_next(update_or_msg, context: ContextTypes.DEFAULT_TYPE) -> int:
    queue = context.user_data.get("file_queue", [])
    done  = context.user_data.get("file_queue_done", 0)
    total = len(queue)
    msg   = update_or_msg.message if hasattr(update_or_msg, "message") else update_or_msg
    if done >= total:
        return await _finish_batch(update_or_msg, context)
    idx_label = f" {done + 1}/{total}" if total > 1 else ""
    # Показываем кнопку "везде 🔥" только если не была показана в этой сессии
    fire_btn_shown = context.user_data.get("fire_btn_shown_new", False)
    kb = None
    if not fire_btn_shown:
        context.user_data["fire_btn_shown_new"] = True
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("везде 🔥", callback_data="fire_all_confirm_new")]
        ])
    await msg.reply_text(
        f"😊 <b>Укажи эмодзи</b> для стикера{idx_label}\n"
        "Можно несколько через пробел: <code>😎 🔥</code>\n\n/cancel — отменить",
        parse_mode="HTML",
        reply_markup=kb,
    )
    return WAITING_EMOJI


async def _finish_batch(update_or_msg, context: ContextTypes.DEFAULT_TYPE) -> int:
    stickers_buf = context.user_data.get("new_pack_stickers", [])
    suffix       = context.user_data.get("new_pack_suffix") or random_suffix()
    context.user_data["new_pack_suffix"] = suffix
    bot_username = context.user_data.get("_bot_username", "")
    if not bot_username:
        try:
            bot_me = await context.bot.get_me()
            bot_username = bot_me.username
            context.user_data["_bot_username"] = bot_username
        except Exception:
            pass
    pack_name    = build_pack_name(bot_username, suffix)
    n            = len(stickers_buf)
    url          = pack_url(pack_name)
    title        = context.user_data.get("new_pack_title", "My Pack")

    # Определяем объект для отправки сообщений
    if hasattr(update_or_msg, "callback_query") and update_or_msg.callback_query:
        send_msg = update_or_msg.callback_query.message
    elif hasattr(update_or_msg, "message") and update_or_msg.message:
        send_msg = update_or_msg.message
    else:
        send_msg = update_or_msg

    if n == 0:
        await send_msg.reply_text(
            "⚠️ Не удалось добавить ни одного стикера.\n\n"
            "Попробуй снова — отправь файл 👇\n\n/cancel — отменить"
        )
        context.user_data["file_queue"]      = []
        context.user_data["file_queue_done"] = 0
        return ADDING_STICKER

    kb = [
        [InlineKeyboardButton("🔗 Открыть пак",   url=url)],
        [InlineKeyboardButton("💾 Сохранить пак", callback_data="save_pack")],
    ]
    added = context.user_data.pop("file_queue_added", 0)
    text = (
        f"🎉 <b>Пак создан!</b>\n<b>{title}</b>\n\nСтикеров: {n} {plural_sticker(n)}\n\nОтправь ещё файл или сохрани 👇"
        if added == n
        else f"✅ Добавлено {added} {plural_sticker(added)}!\nВсего: {n} {plural_sticker(n)}\n\nОтправь ещё файл или сохрани 👇"
    )
    await send_msg.reply_text(
        text, parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(kb),
    )
    context.user_data.pop("file_queue", None)
    context.user_data.pop("file_queue_done", None)
    return ADDING_STICKER


async def receive_sticker_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Принимает файл(ы) для стикера. Поддерживает одиночные файлы и альбомы."""
    import asyncio
    try:
        pack_type = context.user_data.get("creating_type", "sticker")
        msg       = update.message

        # ── Обработка текста с премиум-эмодзи entities ──
        # Пользователь отправил премиум-эмодзи прямо в чат (не как стикер)
        if msg.text and msg.entities:
            from telegram import MessageEntity
            custom_emoji_entities = [
                e for e in msg.entities
                if e.type == MessageEntity.CUSTOM_EMOJI and e.custom_emoji_id
            ]
            if custom_emoji_entities:
                # Берём первый custom_emoji и получаем его file_id
                entity = custom_emoji_entities[0]
                data, fmt, is_ce = await get_custom_emoji_sticker_data(
                    context.bot, entity.custom_emoji_id
                )
                if data is None:
                    await msg.reply_text(
                        "❌ Не удалось получить данные премиум-эмодзи. Попробуй другой или /cancel"
                    )
                    return ADDING_STICKER

                bot_me = await context.bot.get_me()
                context.user_data["_bot_username"] = bot_me.username
                context.user_data["_user_id"]      = update.effective_user.id

                logger.info("receive_sticker_file: premium emoji via entity id=%s fmt=%s",
                            entity.custom_emoji_id, fmt)

                queue = context.user_data.setdefault("file_queue", [])
                done  = context.user_data.get("file_queue_done", 0)
                if done >= len(queue):
                    context.user_data["file_queue"]       = []
                    context.user_data["file_queue_done"]  = 0
                    context.user_data["file_queue_added"] = 0
                    queue = context.user_data["file_queue"]
                queue.append({"data": data, "fmt": fmt, "is_custom_emoji": True})
                context.user_data["file_queue_done"]  = 0
                context.user_data["file_queue_added"] = 0
                # Если подтверждено "везде 🔥" — применяем автоматически
                if context.user_data.get("fire_emoji_confirmed_new"):
                    await _apply_fire_to_queue(context.bot, context)
                    return await _finish_batch(update, context)
                return await _ask_emoji_for_next(update, context)

        # ── Проверка размера ──
        file_size = None
        if msg.document:    file_size = msg.document.file_size
        elif msg.photo:     file_size = msg.photo[-1].file_size
        elif msg.video:     file_size = msg.video.file_size
        elif msg.animation: file_size = msg.animation.file_size
        if file_size and file_size > MAX_INPUT_BYTES:
            await msg.reply_text(
                f"❌ Файл слишком большой ({file_size/1024/1024:.1f} МБ). Максимум — 10 МБ.")
            return ADDING_STICKER

        bot_me = await context.bot.get_me()
        context.user_data["_bot_username"] = bot_me.username
        context.user_data["_user_id"]      = update.effective_user.id

        logger.info("receive_sticker_file: pack_type=%s media_group=%s", pack_type, msg.media_group_id)

        # ── Конвертируем файл ──
        sticker_result = await get_sticker_data(context.bot, msg, pack_type)
        # get_sticker_data возвращает (data, fmt) или (data, fmt, is_custom_emoji)
        if len(sticker_result) == 3:
            data, fmt, is_custom_emoji = sticker_result
        else:
            data, fmt = sticker_result
            is_custom_emoji = False

        if data is None:
            if msg.media_group_id:
                # В альбоме — молча пропускаем проблемный файл, не прерываем
                logger.warning("Album file skipped: could not process")
                return ADDING_STICKER
            await msg.reply_text(
                "❌ Не получилось обработать файл. Попробуй другой или /cancel",
                parse_mode="HTML",
            )
            return ADDING_STICKER

        # Для адаптивного пака — только STATIC или VIDEO (не TGS)
        is_adaptive_pack = context.user_data.get("emoji_adaptive", False)
        if is_adaptive_pack and fmt == StickerFormat.ANIMATED:
            await msg.reply_text(
                "❌ Для адаптивного пака не поддерживается TGS-анимация.\n"
                "Отправь <b>PNG/WEBP</b> (статичный) или <b>WEBM</b> (анимация).",
                parse_mode="HTML",
            )
            return ADDING_STICKER

        logger.info("receive_sticker_file: got data len=%s fmt=%s is_custom_emoji=%s",
                    len(data) if isinstance(data, bytes) else "id", fmt, is_custom_emoji)

        # ── Альбом (несколько файлов с одним media_group_id) ──
        if msg.media_group_id:
            group_id = msg.media_group_id

            # Если это новый альбом — сбрасываем очередь
            if context.user_data.get("_last_media_group") != group_id:
                context.user_data["_last_media_group"] = group_id
                context.user_data["file_queue"]        = []
                context.user_data["file_queue_done"]   = 0
                context.user_data["file_queue_added"]  = 0

            queue = context.user_data.setdefault("file_queue", [])
            queue.append({"data": data, "fmt": fmt, "is_custom_emoji": is_custom_emoji})
            context.user_data["file_queue"] = queue

            # Отменяем предыдущий таймер
            old_task = context.user_data.pop("_album_task", None)
            if old_task and not old_task.done():
                old_task.cancel()

            chat_id = msg.chat_id
            bot_ref = context.bot

            async def _album_flush(gid=group_id):
                await asyncio.sleep(2.0)  # ждём все файлы альбома
                if context.user_data.get("_last_media_group") != gid:
                    return
                context.user_data.pop("_last_media_group", None)
                q     = context.user_data.get("file_queue", [])
                done  = context.user_data.get("file_queue_done", 0)
                total = len(q)
                logger.info("Album flush: %d files, done=%d", total, done)
                if not q or done >= total:
                    return
                # Если подтверждено "везде 🔥" — применяем автоматически без вопроса
                if context.user_data.get("fire_emoji_confirmed_new"):
                    await _apply_fire_to_queue(bot_ref, context)
                    stickers_buf = context.user_data.get("new_pack_stickers", [])
                    n     = len(stickers_buf)
                    added = context.user_data.pop("file_queue_added", 0)
                    suffix      = context.user_data.get("new_pack_suffix") or random_suffix()
                    bot_username = context.user_data.get("_bot_username", "")
                    pack_name   = build_pack_name(bot_username, suffix)
                    url         = pack_url(pack_name)
                    title       = context.user_data.get("new_pack_title", "My Pack")
                    kb = [
                        [InlineKeyboardButton("🔗 Открыть пак",   url=url)],
                        [InlineKeyboardButton("💾 Сохранить пак", callback_data="save_pack")],
                    ]
                    text = (
                        f"🎉 <b>Пак создан!</b>\n<b>{title}</b>\n\nСтикеров: {n} {plural_sticker(n)}\n\nОтправь ещё файл или сохрани 👇"
                        if added == n
                        else f"✅ Добавлено {added} {plural_sticker(added)}!\nВсего: {n} {plural_sticker(n)}\n\nОтправь ещё файл или сохрани 👇"
                    )
                    context.user_data.pop("file_queue", None)
                    context.user_data.pop("file_queue_done", None)
                    await bot_ref.send_message(chat_id=chat_id, text=text, parse_mode="HTML",
                                               disable_web_page_preview=True,
                                               reply_markup=InlineKeyboardMarkup(kb))
                    return
                idx_label = f" {done+1}/{total}" if total > 1 else ""
                text_out = (
                    f"📥 Принято <b>{total}</b> файл(ов) из альбома.\n\n"
                    f"😊 <b>Укажи эмодзи</b> для файла{idx_label}\n"
                    "Можно несколько через пробел: <code>😎 🔥</code>\n\n/cancel — отменить"
                )
                context.user_data["_album_awaiting_emoji"] = True
                await bot_ref.send_message(chat_id=chat_id, text=text_out, parse_mode="HTML")

            task = asyncio.create_task(_album_flush())
            context.user_data["_album_task"] = task
            return ADDING_STICKER

        # ── Одиночный файл ──
        queue = context.user_data.setdefault("file_queue", [])
        # Если предыдущая очередь уже обработана — сбрасываем
        done = context.user_data.get("file_queue_done", 0)
        if done >= len(queue):
            context.user_data["file_queue"]       = []
            context.user_data["file_queue_done"]  = 0
            context.user_data["file_queue_added"] = 0
            queue = context.user_data["file_queue"]
        queue.append({"data": data, "fmt": fmt, "is_custom_emoji": is_custom_emoji})
        context.user_data["file_queue_done"]  = 0
        context.user_data["file_queue_added"] = 0
        # Если подтверждено "везде 🔥" — применяем автоматически без вопроса
        if context.user_data.get("fire_emoji_confirmed_new"):
            await _apply_fire_to_queue(context.bot, context)
            return await _finish_batch(update, context)
        return await _ask_emoji_for_next(update, context)

    except Exception as e:
        logger.exception("receive_sticker_file crashed: %s", e)
        try:
            await update.message.reply_text(f"❌ Ошибка при обработке файла: {e}\n\nПопробуй снова.")
        except Exception:
            pass
        return ADDING_STICKER




async def receive_album_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает текст-эмодзи в состоянии ADDING_STICKER только если ждём ответ на альбом."""
    if context.user_data.get("_album_awaiting_emoji"):
        context.user_data.pop("_album_awaiting_emoji", None)
        return await receive_emoji_for_new(update, context)
    # Иначе — молча игнорируем текст
    return ADDING_STICKER


async def receive_emoji_for_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает ввод эмодзи при создании нового пака."""
    try:
        text       = update.message.text.strip()
        emoji_list = _extract_emoji(text)
        if not emoji_list:
            await update.message.reply_text(
                "❌ Не нашёл эмодзи! Отправь только смайлики, например: 😎 🔥\n\n"
                "Попробуй ещё раз:"
            )
            return WAITING_EMOJI

        user_id      = update.effective_user.id
        pack_type    = context.user_data.get("creating_type", "sticker")
        if pack_type == "emoji" and context.user_data.get("emoji_adaptive"):
            pack_type = "emoji_adaptive"
        title        = context.user_data.get("new_pack_title", "My Pack")
        suffix       = context.user_data.get("new_pack_suffix") or random_suffix()
        context.user_data["new_pack_suffix"] = suffix
        bot_me       = await context.bot.get_me()
        bot_username = bot_me.username
        context.user_data["_bot_username"] = bot_username
        pack_name    = build_pack_name(bot_username, suffix)
        stickers_buf = context.user_data.setdefault("new_pack_stickers", [])
        queue        = context.user_data.get("file_queue", [])
        done         = context.user_data.get("file_queue_done", 0)

        if done >= len(queue):
            return await _finish_batch(update, context)

        item        = queue[done]
        data, fmt   = item["data"], item["fmt"]
        is_custom_emoji = item.get("is_custom_emoji", False)

        # Если fmt не задан — файл не был правильно обработан
        if data is None or fmt is None:
            logger.warning("receive_emoji_for_new: data=%s fmt=%s — skipping", data is None, fmt)
            queue.pop(done) if done < len(queue) else None
            context.user_data["file_queue"] = queue
            await update.message.reply_text(
                "❌ Файл не удалось обработать. Отправь другой файл или /cancel"
            )
            return ADDING_STICKER
        logger.info("receive_emoji_for_new: fmt=%s data_len=%s is_custom_emoji=%s",
                    fmt, len(data) if isinstance(data, bytes) else "str", is_custom_emoji)

        action_word = "эмодзи" if "emoji" in pack_type else "стикер"
        status_new  = await update.message.reply_text(f"⏳ Добавляю {action_word}...")

        err = await _push_sticker_to_tg(
            context.bot, user_id, pack_name, pack_type,
            title, data, fmt, emoji_list, stickers_buf,
            is_custom_emoji=is_custom_emoji
        )

        try:
            await status_new.delete()
        except Exception:
            pass

        if err:
            # Сбрасываем текущий файл из очереди — он проблемный
            queue.pop(done) if done < len(queue) else None
            context.user_data["file_queue"] = queue
            await update.message.reply_text(
                f"❌ {err}\n\nОтправь другой файл или /cancel",
            )
            return ADDING_STICKER

        stickers_buf.append({"fmt": str(fmt)})
        context.user_data["new_pack_stickers"] = stickers_buf
        context.user_data["file_queue_done"]   = done + 1
        context.user_data["file_queue_added"]  = context.user_data.get("file_queue_added", 0) + 1
        return await _ask_emoji_for_next(update, context)

    except Exception as e:
        logger.exception("receive_emoji_for_new crashed: %s", e)
        try:
            await update.message.reply_text(
                f"❌ Внутренняя ошибка: {e}\n\nПопробуй снова или /cancel"
            )
        except Exception:
            pass
        return WAITING_EMOJI

def _extract_emoji(text: str) -> list:
    """Извлекает ВСЕ эмодзи из текста — включая ↩️ ✔️ 🌍 🗳️ 🏬 🆕 и все прочие."""
    import unicodedata
    result = []
    i = 0
    while i < len(text):
        cp = ord(text[i])
        # Проверяем: это эмодзи-символ?
        is_emoji = (
            0x1F300 <= cp <= 0x1FAFF   # основные эмодзи-блоки
            or 0x2600 <= cp <= 0x27BF  # разные символы / dingbats
            or 0x2300 <= cp <= 0x23FF  # технические
            or 0x2B00 <= cp <= 0x2BFF  # прочие символы
            or 0x2190 <= cp <= 0x21FF  # стрелки (↩️ ↪️ и др.)
            or 0x2194 <= cp <= 0x2199
            or 0x21A9 <= cp <= 0x21AA
            or 0x25AA <= cp <= 0x25FE  # геометрия
            or 0x2614 <= cp <= 0x2615
            or 0x2648 <= cp <= 0x2653  # зодиак
            or 0x231A <= cp <= 0x231B  # часы
            or 0x23E9 <= cp <= 0x23F3
            or 0x24C2 == cp
            or 0x1F170 <= cp <= 0x1F171
            or 0x1F17E <= cp <= 0x1F17F
            or 0x1F18E == cp
            or 0x1F191 <= cp <= 0x1F19A  # CL/COOL/FREE/ID/NEW/NG/OK/SOS/UP/VS
            or 0x1F201 <= cp <= 0x1F251
            or 0x1F004 == cp or 0x1F0CF == cp
            or cp in (0x267F, 0x2693, 0x26A1, 0x26AA, 0x26AB, 0x26BD, 0x26BE,
                      0x26C4, 0x26C5, 0x26CE, 0x26D4, 0x26EA, 0x26F2, 0x26F3,
                      0x26F5, 0x26FA, 0x26FD, 0x2702, 0x2705, 0x2708, 0x2709,
                      0x270A, 0x270B, 0x270C, 0x270F, 0x2712, 0x2714, 0x2716,
                      0x271D, 0x2721, 0x2728, 0x2733, 0x2734, 0x2744, 0x2747,
                      0x274C, 0x274E, 0x2753, 0x2754, 0x2755, 0x2757, 0x2763,
                      0x2764, 0x2795, 0x2796, 0x2797, 0x27A1, 0x27B0, 0x27BF,
                      0x2934, 0x2935, 0x2B05, 0x2B06, 0x2B07, 0x2B1B, 0x2B1C,
                      0x2B50, 0x2B55, 0x3030, 0x303D, 0x3297, 0x3299,
                      0x0023, 0x002A)  # # и * (для 1️⃣ и т.д. обрабатываем ниже)
        )
        # Числа 0-9 с FE0F+20E3 (1️⃣ и т.д.)
        if (0x0030 <= cp <= 0x0039 or cp in (0x0023, 0x002A)):
            j = i + 1
            if j < len(text) and ord(text[j]) == 0xFE0F:
                j += 1
                if j < len(text) and ord(text[j]) == 0x20E3:
                    result.append(text[i:j+1])
                    i = j + 1
                    continue
            i += 1
            continue
        # Региональные индикаторы (флаги стран) — пары
        if 0x1F1E0 <= cp <= 0x1F1FF:
            cluster = text[i]
            i += 1
            if i < len(text) and 0x1F1E0 <= ord(text[i]) <= 0x1F1FF:
                cluster += text[i]
                i += 1
            result.append(cluster)
            continue
        if is_emoji:
            cluster = text[i]
            i += 1
            # Собираем весь кластер
            while i < len(text):
                ncp = ord(text[i])
                if ncp in (0x200D, 0xFE0F, 0xFE0E, 0x20E3):
                    cluster += text[i]
                    i += 1
                    if i < len(text):
                        cluster += text[i]
                        i += 1
                elif 0x1F3FB <= ncp <= 0x1F3FF:  # скины
                    cluster += text[i]
                    i += 1
                elif 0x1F1E0 <= ncp <= 0x1F1FF:  # флаги
                    cluster += text[i]
                    i += 1
                elif 0xE0020 <= ncp <= 0xE007F:  # теги
                    cluster += text[i]
                    i += 1
                else:
                    break
            result.append(cluster)
        else:
            # Проверяем: обычный символ + FE0F (например ✔️ ↩️ 🗳️)
            if (i + 1 < len(text) and ord(text[i+1]) == 0xFE0F):
                cluster = text[i] + text[i+1]
                i += 2
                if i < len(text) and ord(text[i]) == 0x20E3:
                    cluster += text[i]
                    i += 1
                result.append(cluster)
            else:
                i += 1
    return result



async def _push_sticker_to_tg(bot, user_id: int, pack_name: str, pack_type: str,
                                title: str, data, fmt, emoji_list: list, stickers_buf: list,
                                is_custom_emoji: bool = False):
    # Если пришёл custom_emoji file_id в обычный стикер-пак — автоматически
    # переключаем тип пака на emoji (CUSTOM_EMOJI), чтобы Telegram принял стикер.
    effective_pack_type = pack_type
    if is_custom_emoji and pack_type == "sticker":
        effective_pack_type = "emoji"
        logger.info("Premium emoji detected in sticker pack — switching to CUSTOM_EMOJI type")

    if effective_pack_type == "sticker":
        sticker_type = StickerType.REGULAR
    elif effective_pack_type == "emoji_adaptive":
        sticker_type = StickerType.CUSTOM_EMOJI
    else:
        sticker_type = StickerType.CUSTOM_EMOJI

    if fmt == StickerFormat.ANIMATED:
        fname = "sticker.tgs"
    elif fmt == StickerFormat.VIDEO:
        fname = "sticker.webm"
    else:
        fname = "sticker.png"

    clean_emoji = [e for e in emoji_list if e and e.strip()]
    if not clean_emoji:
        clean_emoji = ["🙂"]
    logger.info("push_sticker: pack=%s type=%s effective_type=%s fmt=%s emoji=%s is_new=%s data_len=%s is_custom_emoji=%s",
                pack_name, pack_type, effective_pack_type, fmt, clean_emoji, not bool(stickers_buf),
                len(data) if isinstance(data, bytes) else "file_id", is_custom_emoji)

    # Для адаптивного пака — конвертируем в силуэт (белый + прозрачность)
    if isinstance(data, bytes):
        actual_data = data
        if effective_pack_type == "emoji_adaptive" and fmt == StickerFormat.STATIC:
            try:
                _img = Image.open(io.BytesIO(data)).convert("RGBA")
                _img = _img.resize((100, 100), Image.LANCZOS)
                _pixels = _img.load()
                for _y in range(_img.height):
                    for _x in range(_img.width):
                        _r, _g, _b, _a = _pixels[_x, _y]
                        _pixels[_x, _y] = (255, 255, 255, _a) if _a > 30 else (0, 0, 0, 0)
                _out = io.BytesIO()
                _img.save(_out, "PNG")
                actual_data = _out.getvalue()
                logger.info("Adaptive: converted to silhouette")
            except Exception as _e:
                logger.warning("Adaptive conversion failed: %s", _e)
        buf = io.BytesIO(actual_data)
        buf.seek(0)
        upload_file = InputFile(buf, filename=fname)
        logger.info("Uploading via upload_sticker_file...")
        # fmt не может быть None — подставляем STATIC если вдруг не определился
        safe_fmt = fmt if fmt is not None else StickerFormat.STATIC
        uploaded = await bot.upload_sticker_file(
            user_id=user_id,
            sticker=upload_file,
            sticker_format=safe_fmt,
        )
        file_id = uploaded.file_id
        logger.info("Uploaded file_id: %s", file_id)
    else:
        # data — это file_id (например premium custom_emoji)
        file_id = data

    # Определяем строку формата — нужна для PTB 21+ (Bot API 7.2+)
    safe_fmt = fmt if fmt is not None else StickerFormat.STATIC
    if hasattr(safe_fmt, "value"):
        fmt_str = safe_fmt.value.strip()   # "static" / "animated" / "video"
    else:
        fmt_str = str(safe_fmt).strip()
    logger.info("_push_sticker: fmt_str=%r", fmt_str)

    def make_sticker():
        # PTB 21+: format обязателен. PTB 20.x: format отсутствует.
        try:
            return InputSticker(sticker=file_id, emoji_list=clean_emoji[:20], format=fmt_str)
        except TypeError:
            return InputSticker(sticker=file_id, emoji_list=clean_emoji[:20])

    async def _create_set(sticker):
        is_adaptive = (effective_pack_type == "emoji_adaptive")
        kwargs = dict(
            user_id=user_id,
            name=pack_name,
            title=title,
            stickers=[sticker],
            sticker_type=sticker_type,
            sticker_format=safe_fmt,
        )
        if is_adaptive:
            kwargs["needs_repainting"] = True
        try:
            await bot.create_new_sticker_set(**kwargs)
        except TypeError:
            # PTB версия не принимает sticker_format
            kwargs.pop("sticker_format", None)
            await bot.create_new_sticker_set(**kwargs)

    async def _add_to_set(sticker):
        await bot.add_sticker_to_set(user_id=user_id, name=pack_name, sticker=sticker)

    try:
        if not stickers_buf:
            # ── Создаём новый пак ──────────────────────────────────────────
            try:
                await _create_set(make_sticker())
                logger.info("Sticker set CREATED: %s", pack_name)
                return None
            except TelegramError as e:
                err_str = str(e)
                logger.error("create_new_sticker_set error: %s", err_str)
                # Ошибка эмодзи — пробуем с дефолтным 🙂
                if ("emoji" in err_str.lower() or "unicode" in err_str.lower()
                        or "parse sticker" in err_str.lower()):
                    clean_emoji = ["🙂"]
                    try:
                        await _create_set(make_sticker())
                        logger.info("Sticker set CREATED with default emoji: %s", pack_name)
                        return None
                    except TelegramError as e2:
                        err_str = str(e2)
                if ("already occupied" in err_str.lower()
                        or "name is already" in err_str.lower()
                        or "STICKERSET_INVALID" in err_str):
                    logger.info("Pack exists, adding sticker: %s", pack_name)
                    await _add_to_set(make_sticker())
                    logger.info("Sticker ADDED (fallback): %s", pack_name)
                    return None
                retry_after = getattr(e, "retry_after", None)
                if retry_after:
                    return f"retry:{retry_after}"
                return _friendly_tg_error(err_str)
        else:
            # ── Добавляем в существующий пак ──────────────────────────────
            try:
                await _add_to_set(make_sticker())
                logger.info("Sticker ADDED to: %s", pack_name)
                return None
            except TelegramError as e:
                err_str = str(e)
                logger.error("add_sticker_to_set error: %s", err_str)
                # Ошибка эмодзи — пробуем с дефолтным 🙂
                if ("emoji" in err_str.lower() or "unicode" in err_str.lower()
                        or "parse sticker" in err_str.lower()):
                    clean_emoji = ["🙂"]
                    try:
                        await _add_to_set(make_sticker())
                        logger.info("Sticker ADDED with default emoji: %s", pack_name)
                        return None
                    except TelegramError as e2:
                        err_str = str(e2)
                if "STICKERSET_INVALID" in err_str or "not found" in err_str.lower():
                    logger.info("Pack not found, recreating: %s", pack_name)
                    await _create_set(make_sticker())
                    logger.info("Pack RECREATED: %s", pack_name)
                    return None
                retry_after = getattr(e, "retry_after", None)
                if retry_after:
                    return f"retry:{retry_after}"
                return _friendly_tg_error(err_str)

    except TelegramError as e:
        err_str = str(e)
        logger.error("_push_sticker_to_tg TelegramError: %s", err_str)
        retry_after = getattr(e, "retry_after", None)
        if retry_after:
            return f"retry:{retry_after}"
        return _friendly_tg_error(err_str)
    except Exception as e:
        err_str = str(e)
        logger.exception("_push_sticker_to_tg unexpected error: %s", err_str)
        return f"Неожиданная ошибка: {err_str}"


async def add_more(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "Отправь следующий файл 👇\n\n/cancel — отменить"
    )
    return ADDING_STICKER


async def save_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query        = update.callback_query
    await query.answer()
    user_id      = update.effective_user.id
    bot_username = (await context.bot.get_me()).username
    title        = context.user_data.get("new_pack_title", "My Pack")
    suffix       = context.user_data.get("new_pack_suffix") or random_suffix()
    pack_type    = context.user_data.get("creating_type", "sticker")
    stickers     = context.user_data.get("new_pack_stickers", [])
    pack_name    = build_pack_name(bot_username, suffix)
    get_packs(context, user_id).append({
        "title": title, "name": pack_name, "suffix": suffix,
        "type": pack_type, "count": len(stickers),
        "adaptive": context.user_data.get("emoji_adaptive", False),
    })
    save_packs(context, user_id)
    for key in ("new_pack_title", "new_pack_suffix", "new_pack_stickers", "creating_type",
                "fire_emoji_confirmed_new", "fire_btn_shown_new"):
        context.user_data.pop(key, None)
    # Статистика
    if pack_type in ("emoji", "emoji_adaptive"):
        DB.increment_stat("emoji_packs_created")
        DB.increment_stat("emoji_added", len(stickers))
    else:
        DB.increment_stat("sticker_packs_created")
        DB.increment_stat("stickers_added", len(stickers))
    url  = pack_url(pack_name)
    icon = "🖼" if pack_type == "sticker" else "✨"
    kb   = [
        [InlineKeyboardButton("🔗 Открыть пак", url=url)],
        [InlineKeyboardButton("🏠 В меню",      callback_data="begin")],
    ]
    await query.edit_message_text(
        f"{icon} <b>Пак сохранён!</b>\n\nНазвание: <b>{title}</b>\nСтикеров: {len(stickers)}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return ConversationHandler.END


# ── Управление паком ──────────────────────────────────────────────────────────

async def mgmt_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    pack    = get_packs(context, user_id)[idx]
    word    = "стикер" if pack["type"] == "sticker" else "эмодзи"
    await query.edit_message_text(
        f"➕ Отправь <b>{word}</b> для пака <b>{pack['title']}</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
    )
    return ADD_STICKER_FILE


async def receive_add_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        user_id   = update.effective_user.id
        idx       = context.user_data.get("selected_pack_index", 0)
        pack      = get_packs(context, user_id)[idx]
        pack_type = pack["type"]
        msg       = update.message

        # ── Обработка текста с премиум-эмодзи entities ──
        if msg.text and msg.entities:
            from telegram import MessageEntity
            custom_emoji_entities = [
                e for e in msg.entities
                if e.type == MessageEntity.CUSTOM_EMOJI and e.custom_emoji_id
            ]
            if custom_emoji_entities:
                entity = custom_emoji_entities[0]
                data, fmt, is_ce = await get_custom_emoji_sticker_data(
                    context.bot, entity.custom_emoji_id
                )
                if data is None:
                    await msg.reply_text(
                        "❌ Не удалось получить данные премиум-эмодзи. Попробуй другой или /cancel"
                    )
                    return ADD_STICKER_FILE
                context.user_data["pending_data"]         = data
                context.user_data["pending_fmt"]          = fmt
                context.user_data["pending_pack_idx"]     = idx
                context.user_data["pending_custom_emoji"] = True
                await msg.reply_text(
                    "😊 <b>Укажи эмодзи</b> для этого стикера.\nМожно несколько: <code>😎 🔥</code>",
                    parse_mode="HTML",
                )
                return ADD_STICKER_EMOJI

        file_size = None
        if msg.document:    file_size = msg.document.file_size
        elif msg.photo:     file_size = msg.photo[-1].file_size
        elif msg.video:     file_size = msg.video.file_size
        elif msg.animation: file_size = msg.animation.file_size
        if file_size and file_size > MAX_INPUT_BYTES:
            await msg.reply_text(f"❌ Файл слишком большой. Максимум — 10 МБ.")
            return ADD_STICKER_FILE
        sticker_result = await get_sticker_data(context.bot, msg, pack_type)
        if len(sticker_result) == 3:
            data, fmt, is_custom_emoji = sticker_result
        else:
            data, fmt = sticker_result
            is_custom_emoji = False

        if data is None:
            await msg.reply_text("❌ Не получилось распознать файл. Отправь PNG, WEBP, WEBM, TGS или готовый стикер.")
            return ADD_STICKER_FILE
        # Адаптивный пак — только PNG/WEBM
        if pack.get("adaptive") and fmt == StickerFormat.ANIMATED:
            await msg.reply_text(
                "❌ Для адаптивного пака не поддерживается TGS.\n"
                "Отправь <b>PNG/WEBP</b> или <b>WEBM</b>.",
                parse_mode="HTML",
            )
            return ADD_STICKER_FILE
        context.user_data["pending_data"]         = data
        context.user_data["pending_fmt"]          = fmt
        context.user_data["pending_pack_idx"]     = idx
        context.user_data["pending_custom_emoji"] = is_custom_emoji
        # Если уже подтверждено "везде 🔥" в этой сессии — применяем автоматически
        if context.user_data.get("fire_emoji_confirmed_mgmt"):
            is_emoji_pack = pack.get("type", "sticker") in ("emoji", "emoji_adaptive")
            action_word   = "эмодзи" if is_emoji_pack else "стикер"
            status_msg2   = await msg.reply_text(f"⏳ Добавляю {action_word} с эмодзи 🔥...")
            pack_name    = pack["name"]
            stickers_buf = [True] * pack.get("count", 1)
            err = await _push_sticker_to_tg(
                context.bot, user_id, pack_name, pack["type"],
                pack["title"], data, fmt, ["🔥"], stickers_buf,
                is_custom_emoji=is_custom_emoji
            )
            try:
                await status_msg2.delete()
            except Exception:
                pass
            if err:
                await msg.reply_text(f"❌ {err}\n\nПопробуй снова.")
                return ADD_STICKER_FILE
            context.user_data.pop("pending_data", None)
            context.user_data.pop("pending_fmt", None)
            context.user_data.pop("pending_pack_idx", None)
            pack["count"] = pack.get("count", 0) + 1
            save_packs(context, user_id)
            n = pack["count"]
            item_word = "эмодзи" if is_emoji_pack else "стикер"
            word_next = "эмодзи" if is_emoji_pack else "стикер"
            await msg.reply_text(
                f"✅ {item_word.capitalize()} добавлен с 🔥! Теперь в паке: {n} {plural_sticker(n)}\n\n"
                f"Отправь следующий {word_next} или вернись к паку 👇",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")],
                ]),
            )
            return ADD_STICKER_FILE
        # Показываем кнопку "везде 🔥" только если не показывали в этой сессии
        fire_btn_shown = context.user_data.get("fire_btn_shown_mgmt", False)
        kb_rows = []
        if not fire_btn_shown:
            context.user_data["fire_btn_shown_mgmt"] = True
            kb_rows.append([InlineKeyboardButton("везде 🔥", callback_data="fire_all_confirm_mgmt")])
        await msg.reply_text(
            "😊 <b>Укажи эмодзи</b> для этого стикера.\nМожно несколько: <code>😎 🔥</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None,
        )
        return ADD_STICKER_EMOJI
    except Exception as e:
        logger.exception("receive_add_file crashed: %s", e)
        try:
            await update.message.reply_text(f"❌ Ошибка: {e}\n\nПопробуй снова.")
        except Exception:
            pass
        return ADD_STICKER_FILE


async def receive_add_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Эмодзи для стикера при управлении существующим паком."""
    try:
        text       = update.message.text.strip()
        emoji_list = _extract_emoji(text)
        if not emoji_list:
            await update.message.reply_text(
                "❌ Не нашёл эмодзи! Отправь только смайлики, например: 😎 🔥\n\n"
                "Попробуй ещё раз:"
            )
            return ADD_STICKER_EMOJI

        user_id    = update.effective_user.id
        idx        = context.user_data.get("pending_pack_idx",
                      context.user_data.get("selected_pack_index", 0))
        pack       = get_packs(context, user_id)[idx]
        data       = context.user_data.get("pending_data")
        fmt        = context.user_data.get("pending_fmt", StickerFormat.STATIC)
        is_custom_emoji = context.user_data.get("pending_custom_emoji", False)

        if data is None:
            await update.message.reply_text(
                "❌ Данные файла потерялись. Отправь файл ещё раз."
            )
            return ADD_STICKER_FILE

        pack_type_cur = pack.get("type", "sticker")
        action_word   = "эмодзи" if "emoji" in pack_type_cur else "стикер"
        status_msg2   = await update.message.reply_text(f"⏳ Добавляю {action_word}...")

        pack_name    = pack["name"]
        stickers_buf = [True] * pack.get("count", 1)  # непустой → add_sticker_to_set

        err = await _push_sticker_to_tg(
            context.bot, user_id, pack_name, pack["type"],
            pack["title"], data, fmt, emoji_list, stickers_buf,
            is_custom_emoji=is_custom_emoji
        )

        try:
            await status_msg2.delete()
        except Exception:
            pass

        if err:
            await update.message.reply_text(
                f"❌ {err}\n\nПопробуй другой эмодзи или /cancel"
            )
            return ADD_STICKER_EMOJI

        # Успех — чистим временные данные
        context.user_data.pop("pending_data", None)
        context.user_data.pop("pending_fmt", None)
        context.user_data.pop("pending_pack_idx", None)
        # Сбрасываем флаг "везде 🔥" для следующего захода в пак
        context.user_data.pop("fire_emoji_confirmed", None)
        pack["count"] = pack.get("count", 0) + 1
        save_packs(context, user_id)
        n = pack["count"]
        is_emoji_pack = pack.get("type", "sticker") in ("emoji", "emoji_adaptive")
        item_word = "эмодзи" if is_emoji_pack else "стикер"
        word_next = "эмодзи" if is_emoji_pack else "стикер"
        # Статистика
        if is_emoji_pack:
            DB.increment_stat("emoji_added")
        else:
            DB.increment_stat("stickers_added")
        await update.message.reply_text(
            f"✅ {item_word.capitalize()} добавлен! Теперь в паке: {n} {plural_sticker(n)}\n\n"
            f"Отправь следующий {word_next} или вернись к паку 👇",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")],
            ]),
        )
        return ADD_STICKER_FILE

    except Exception as e:
        logger.exception("receive_add_emoji crashed: %s", e)
        try:
            await update.message.reply_text(f"❌ Внутренняя ошибка: {e}\n\nПопробуй снова или /cancel")
        except Exception:
            pass
        return ADD_STICKER_EMOJI


async def fire_all_confirm_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь нажал 'везде 🔥' при создании нового пака — просим подтвердить."""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "🔥 Точно хочешь указать эмодзи 🔥 для <b>всех</b> стикеров в этой сессии?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, везде 🔥", callback_data="fire_all_yes_new"),
             InlineKeyboardButton("❌ Нет", callback_data="fire_all_no_new")],
        ]),
    )
    return WAITING_EMOJI


async def fire_all_yes_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждено 'везде 🔥' для нового пака — применяем к текущей очереди и всем следующим."""
    query = update.callback_query
    await query.answer()
    context.user_data["fire_emoji_confirmed_new"] = True
    await query.message.reply_text("✅ Принято! Для всех стикеров этой сессии будет 🔥")
    # Применяем 🔥 ко всем файлам в текущей очереди сразу
    await _apply_fire_to_queue(context.bot, context)
    # Показываем итог (как _finish_batch, но без сброса очереди полностью)
    return await _finish_batch(update, context)


async def fire_all_no_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отказ от 'везде 🔥' — просто спрашиваем эмодзи вручную."""
    query = update.callback_query
    await query.answer()
    queue = context.user_data.get("file_queue", [])
    done  = context.user_data.get("file_queue_done", 0)
    total = len(queue)
    idx_label = f" {done + 1}/{total}" if total > 1 else ""
    await query.message.reply_text(
        f"😊 <b>Укажи эмодзи</b> для стикера{idx_label}\n"
        "Можно несколько через пробел: <code>😎 🔥</code>\n\n/cancel — отменить",
        parse_mode="HTML",
    )
    return WAITING_EMOJI


async def fire_all_confirm_mgmt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь нажал 'везде 🔥' при добавлении в существующий пак."""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "🔥 Точно хочешь указать эмодзи 🔥 для <b>всех</b> стикеров, пока не выйдешь из пака?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, везде 🔥", callback_data="fire_all_yes_mgmt"),
             InlineKeyboardButton("❌ Нет", callback_data="fire_all_no_mgmt")],
        ]),
    )
    return ADD_STICKER_EMOJI


async def fire_all_yes_mgmt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждено 'везде 🔥' для существующего пака — применяем сейчас и далее."""
    query = update.callback_query
    await query.answer()
    context.user_data["fire_emoji_confirmed_mgmt"] = True
    await query.message.reply_text("✅ Принято! Все следующие стикеры получат 🔥")
    # Применяем к текущему pending стикеру
    user_id = update.effective_user.id
    idx     = context.user_data.get("pending_pack_idx",
              context.user_data.get("selected_pack_index", 0))
    packs   = get_packs(context, user_id)
    if idx >= len(packs):
        return ADD_STICKER_FILE
    pack    = packs[idx]
    data    = context.user_data.get("pending_data")
    fmt     = context.user_data.get("pending_fmt", StickerFormat.STATIC)
    is_custom_emoji = context.user_data.get("pending_custom_emoji", False)
    if data is None:
        return ADD_STICKER_FILE
    is_emoji_pack = pack.get("type", "sticker") in ("emoji", "emoji_adaptive")
    action_word   = "эмодзи" if is_emoji_pack else "стикер"
    status_msg    = await query.message.reply_text(f"⏳ Добавляю {action_word} с эмодзи 🔥...")
    stickers_buf  = [True] * pack.get("count", 1)
    err = await _push_sticker_to_tg(
        context.bot, user_id, pack["name"], pack["type"],
        pack["title"], data, fmt, ["🔥"], stickers_buf,
        is_custom_emoji=is_custom_emoji
    )
    try:
        await status_msg.delete()
    except Exception:
        pass
    if err:
        await query.message.reply_text(f"❌ {err}\n\nОтправь следующий файл.")
        return ADD_STICKER_FILE
    context.user_data.pop("pending_data", None)
    context.user_data.pop("pending_fmt", None)
    context.user_data.pop("pending_pack_idx", None)
    pack["count"] = pack.get("count", 0) + 1
    save_packs(context, user_id)
    n = pack["count"]
    item_word = "эмодзи" if is_emoji_pack else "стикер"
    word_next = "эмодзи" if is_emoji_pack else "стикер"
    await query.message.reply_text(
        f"✅ {item_word.capitalize()} добавлен с 🔥! Теперь в паке: {n} {plural_sticker(n)}\n\n"
        f"Отправь следующий {word_next} или вернись к паку 👇",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")],
        ]),
    )
    return ADD_STICKER_FILE


async def fire_all_no_mgmt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отказ от 'везде 🔥' — просто спрашиваем эмодзи вручную."""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "😊 <b>Укажи эмодзи</b> для этого стикера.\nМожно несколько: <code>😎 🔥</code>",
        parse_mode="HTML",
    )
    return ADD_STICKER_EMOJI


async def mgmt_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    packs   = get_packs(context, user_id)
    if idx >= len(packs):
        await query.edit_message_text("❌ Пак не найден.")
        return PACK_SELECTED
    pack = packs[idx]
    is_emoji = pack.get("type", "sticker") in ("emoji", "emoji_adaptive")
    item_label = "эмодзи" if is_emoji else "стикер"
    url = pack_url(pack["name"])
    await query.edit_message_text(
        f"🗑 <b>Удаление {item_label}а</b>\n\n"
        f"Открой пак по ссылке, найди нужный {item_label} и <b>перешли его сюда</b>:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Открыть пак", url=url)],
            back_btn("◀️ Отмена", f"select_pack_{idx}"),
        ]),
    )
    return DELETE_STICKER


async def confirm_delete_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает пересланный стикер и удаляет его из пака."""
    msg     = update.message
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    packs   = get_packs(context, user_id)
    if idx >= len(packs):
        await msg.reply_text("❌ Пак не найден.")
        return PACK_SELECTED
    pack     = packs[idx]
    is_emoji = pack.get("type", "sticker") in ("emoji", "emoji_adaptive")
    item_word = "Эмодзи" if is_emoji else "Стикер"
    item_label = "эмодзи" if is_emoji else "стикер"

    if not msg.sticker:
        await msg.reply_text(
            f"❌ Нужен {item_label} из этого пака.\n"
            "Открой пак, нажми на нужный и перешли его сюда.",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
        )
        return DELETE_STICKER

    s = msg.sticker
    # Проверяем что стикер из нужного пака
    if (s.set_name or "").lower() != pack["name"].lower():
        await msg.reply_text(
            f"❌ Этот {item_label} не из пака <b>{pack['title']}</b>.\n"
            "Перешли стикер именно из этого пака.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
        )
        return DELETE_STICKER

    try:
        await context.bot.delete_sticker_from_set(s.file_id)
        logger.info("Deleted sticker %s from %s", s.file_unique_id, pack["name"])
    except TelegramError as e:
        await msg.reply_text(
            f"❌ Не удалось удалить: {_friendly_tg_error(str(e))}",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Назад", f"select_pack_{idx}")]),
        )
        return DELETE_STICKER

    pack["count"] = max(0, pack.get("count", 1) - 1)
    save_packs(context, user_id)
    n = pack["count"]
    await msg.reply_text(
        f"✅ {item_word} удалён!\nОсталось в паке: {n} {plural_sticker(n)}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Удалить ещё",  callback_data="mgmt_delete")],
            [InlineKeyboardButton("⚙️ К паку",       callback_data=f"select_pack_{idx}")],
        ]),
    )
    return PACK_SELECTED


async def mgmt_icon(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx      = context.user_data.get("selected_pack_index", 0)
    user_id2 = update.effective_user.id
    pack2    = get_packs(context, user_id2)[idx]
    is_emoji = pack2["type"] in ("emoji", "emoji_adaptive")
    item_label = "эмодзи" if is_emoji else "стикер"
    await query.edit_message_text(
        f"🖼 <b>Сменить иконку пака</b>\n\n"
        f"Скинь {item_label}, который будет отображаться в качестве иконки в панели стикеров:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            back_btn("◀️ Отмена", f"select_pack_{idx}"),
        ]),
    )
    return CHANGE_ICON


async def receive_icon(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает стикер/эмодзи и устанавливает его как иконку пака."""
    msg     = update.message
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    packs   = get_packs(context, user_id)
    if idx >= len(packs):
        await msg.reply_text("❌ Пак не найден.")
        return PACK_SELECTED
    pack       = packs[idx]
    is_emoji   = pack["type"] in ("emoji", "emoji_adaptive")
    item_label = "эмодзи" if is_emoji else "стикер"

    if not msg.sticker:
        await msg.reply_text(
            f"❌ Нужен {item_label} из пака.\n"
            "Открой пак, нажми на нужный стикер и перешли его сюда.",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
        )
        return CHANGE_ICON

    s = msg.sticker

    # Проверяем что стикер из нужного пака
    if (s.set_name or "").lower() != pack["name"].lower():
        pack_link = f't.me/addstickers/{pack["name"]}'
        await msg.reply_text(
            f"❌ Этот {item_label} не из пака <b>{pack['title']}</b>.\n\n"
            f"Открой пак по ссылке, нажми на любой {item_label} и перешли его сюда:\n"
            f"<code>{pack_link}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
        )
        return CHANGE_ICON

    # Определяем формат стикера через get_sticker_set для надёжности
    try:
        sticker_set = await context.bot.get_sticker_set(pack["name"])
        icon_fmt    = sticker_set.sticker_format
    except Exception:
        icon_fmt = s.format if hasattr(s, "format") else StickerFormat.STATIC

    try:
        await context.bot.set_sticker_set_thumbnail(
            name=pack["name"],
            user_id=user_id,
            thumbnail=s.file_id,
            format=icon_fmt,
        )
        logger.info("Icon set for pack %s via file_id %s", pack["name"], s.file_id)
    except TelegramError as e:
        err_str = str(e)
        err     = _friendly_tg_error(err_str)
        if "STICKER_INVALID" in err_str or "invalid" in err_str.lower():
            err += f"\n\nУбедись что отправляешь {item_label} именно из этого пака."
        await msg.reply_text(
            f"❌ Не удалось установить иконку: {err}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Назад", f"select_pack_{idx}")]),
        )
        return CHANGE_ICON
    except Exception as e:
        await msg.reply_text(
            f"❌ Ошибка: {e}",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Назад", f"select_pack_{idx}")]),
        )
        return CHANGE_ICON

    await msg.reply_text(
        f"✅ <b>Иконка пака обновлена!</b>\n\n"
        f"Теперь в панели стикеров будет отображаться выбранный {item_label}.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")]]),
    )
    return PACK_SELECTED


async def mgmt_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx   = context.user_data.get("selected_pack_index", 0)
    await query.edit_message_text(
        "⚠️ <b>Удалить весь пак?</b>\n\nЭто нельзя отменить.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_pack_{idx}")],
            back_btn("◀️ Отмена", f"select_pack_{idx}"),
        ]),
    )
    return PACK_SELECTED


async def confirm_delete_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    idx     = int(query.data.replace("confirm_delete_pack_", ""))
    packs   = get_packs(context, user_id)
    pack    = packs[idx]
    name    = pack["title"]
    tg_err  = None
    try:
        await context.bot.delete_sticker_set(pack["name"])
        logger.info("Sticker set deleted from TG: %s", pack["name"])
    except TelegramError as e:
        tg_err = str(e)
        logger.warning("TG delete_sticker_set failed: %s", tg_err)
    # Удаляем из локального списка в любом случае
    packs.pop(idx)
    save_packs(context, user_id)
    context.user_data.pop("selected_pack_index", None)
    note = ""
    if tg_err:
        note = f"\n⚠️ Из Telegram удалить не удалось: {_friendly_tg_error(tg_err)}"
    await query.edit_message_text(
        f"🗑 Пак <b>{name}</b> удалён из вашего списка.{note}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([back_btn("📋 К списку паков", "list_packs")]),
    )
    return CHOOSE_TYPE


async def mgmt_rename(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает новое название пака."""
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    pack    = get_packs(context, user_id)[idx]
    await query.edit_message_text(
        f"✏️ <b>Переименовать пак</b>\n\n"
        f"Текущее название: <b>{pack['title']}</b>\n\n"
        "Напиши новое название:\n\n/cancel — отменить",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
    )
    return RENAMING_PACK


async def receive_rename(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает новое название и переименовывает пак в Telegram."""
    new_title = update.message.text.strip()
    if not new_title:
        await update.message.reply_text("Название не может быть пустым. Попробуй ещё раз:")
        return RENAMING_PACK
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    pack    = get_packs(context, user_id)[idx]
    old_title = pack["title"]
    try:
        await context.bot.set_sticker_set_title(name=pack["name"], title=new_title)
        pack["title"] = new_title
        save_packs(context, user_id)
        logger.info("Pack renamed: %s -> %s", old_title, new_title)
    except TelegramError as e:
        await update.message.reply_text(
            f"❌ Не удалось переименовать: {_friendly_tg_error(str(e))}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")]]),
        )
        return PACK_SELECTED
    await update.message.reply_text(
        f"✅ Пак переименован!\n<b>{old_title}</b> → <b>{new_title}</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")]]),
    )
    return PACK_SELECTED


# ── Номерные знаки: выбор страны ──────────────────────────────────────────────

async def plate_choose_country(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    kb = [
        [InlineKeyboardButton("🇷🇺 Россия",    callback_data="pc_RU"),
         InlineKeyboardButton("🇺🇦 Украина",   callback_data="pc_UA")],
        [InlineKeyboardButton("🇧🇾 Беларусь",  callback_data="pc_BY"),
         InlineKeyboardButton("🇰🇿 Казахстан", callback_data="pc_KZ")],
        back_btn(),
    ]
    await query.edit_message_text(
        "🚗 <b>Создать номерной знак</b>\n\nВыбери страну:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PLATE_COUNTRY


# ── Номерные знаки: выбор региона (с пагинацией) ─────────────────────────────

def _region_page_kb(country: str, page: int) -> InlineKeyboardMarkup:
    codes  = list(REGIONS[country].keys())
    total  = len(codes)
    start  = page * REGIONS_PER_PAGE
    chunk  = codes[start: start + REGIONS_PER_PAGE]
    kb     = []
    row    = []
    for i, code in enumerate(chunk):
        row.append(InlineKeyboardButton(code, callback_data=f"pr_{country}_{code}"))
        if len(row) == 4:
            kb.append(row); row = []
    if row:
        kb.append(row)
    # навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"prp_{country}_{page-1}"))
    pages = (total + REGIONS_PER_PAGE - 1) // REGIONS_PER_PAGE
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if start + REGIONS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"prp_{country}_{page+1}"))
    kb.append(nav)
    kb.append(back_btn("◀️ Назад к странам", "create_plate"))
    return InlineKeyboardMarkup(kb)


async def plate_select_country(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    country = query.data.replace("pc_", "")
    context.user_data["plate_country"] = country
    flag    = COUNTRY_NAMES[country]
    await query.edit_message_text(
        f"🚗 {flag}\n\nВыбери код региона:",
        parse_mode="HTML",
        reply_markup=_region_page_kb(country, 0),
    )
    return PLATE_REGION


async def plate_region_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    _, country, page_str = query.data.split("_", 2)
    page    = int(page_str)
    flag    = COUNTRY_NAMES[country]
    await query.edit_message_text(
        f"🚗 {flag}\n\nВыбери код региона:",
        parse_mode="HTML",
        reply_markup=_region_page_kb(country, page),
    )
    return PLATE_REGION


async def plate_select_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    # data format: pr_RU_77
    parts   = query.data.split("_", 2)
    country = parts[1]
    region  = parts[2]
    context.user_data["plate_country"] = country
    context.user_data["plate_region"]  = region
    region_name = REGIONS[country].get(region, "")
    scheme_text = PLATE_SCHEMES[country]
    await query.edit_message_text(
        f"✅ Регион: <b>{region}</b> — {region_name}\n\n"
        f"📋 {scheme_text}\n\n"
        "Напиши номер (только символы, без кода региона):\n\n"
        "/cancel — отменить",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            back_btn("◀️ Сменить регион", f"pc_{country}")
        ]),
    )
    return PLATE_INPUT


async def plate_receive_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw     = update.message.text.strip().upper()
    country = context.user_data.get("plate_country", "RU")
    region  = context.user_data.get("plate_region", "77")
    region_name = REGIONS.get(country, {}).get(region, "")

    # ── Строгая валидация по схеме страны ────────────────────────────────────
    import re as _re
    # RU: Л ЦЦЦ ЛЛ  — буквы только А В Е К М Н О Р С Т У Х (кириллица)
    RU_LETTERS = "АВЕКМНОРСТУХ"
    VALID = {
        "RU": _re.compile(
            rf"^[{RU_LETTERS}]\s*\d{{3}}\s*[{RU_LETTERS}]{{2}}$"
        ),
        # UA: ЦЦЦЦ ЛЛ
        "UA": _re.compile(r"^\d{4}\s*[А-ЯA-Z]{2}$"),
        # BY: ЦЦЦЦ ЛЛ
        "BY": _re.compile(r"^\d{4}\s*[А-ЯA-Z]{2}$"),
        # KZ: ЦЦЦ ЛЛЛ (латинские)
        "KZ": _re.compile(r"^\d{3}\s*[A-Z]{3}$"),
    }
    clean = raw.replace(" ", "").upper()
    pattern = VALID.get(country)
    if pattern and not pattern.match(raw.strip()):
        await update.message.reply_text(
            f"❌ Неверный формат! Проверь схему:\n\n{PLATE_SCHEMES[country]}\n\n"
            "Введи номер ещё раз:",
            parse_mode="HTML",
        )
        return PLATE_INPUT

    plate_status = await update.message.reply_text("⏳ Генерирую номерной знак...")

    try:
        img_bytes = generate_plate_image(country, raw, region, region_name)
        # Сохраняем для конвертации в стикер/эмодзи
        context.user_data["last_plate_bytes"]       = img_bytes
        context.user_data["last_plate_country"]     = country
        context.user_data["last_plate_chars"]       = raw
        context.user_data["last_plate_region"]      = region
        context.user_data["last_plate_region_name"] = region_name
        kb = [
            [InlineKeyboardButton("🖼️ В стикер-пак ⭐️",  callback_data="plate_buy_sticker"),
             InlineKeyboardButton("✨ В эмодзи-пак ⭐️",   callback_data="plate_buy_emoji")],
            [InlineKeyboardButton("🔄 Другой номер",        callback_data=f"plate_other_{country}")],
            [InlineKeyboardButton("🏠 В меню",              callback_data="plate_menu")],
        ]
        try:
            await plate_status.delete()
        except Exception:
            pass
        await update.message.reply_photo(
            photo=InputFile(io.BytesIO(img_bytes), filename="plate.png"),
            caption=f"🚗 <b>{raw}</b> | <b>{region}</b>\n{region_name}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
    except Exception as e:
        logger.exception("plate generation error")
        await update.message.reply_text(f"❌ Ошибка генерации: {e}")

    return PLATE_INPUT



# ── Конвертация номера в стикер / эмодзи ──────────────────────────────────────

PLATE_COUNTRY_EMOJI = {"RU": "🇷🇺", "KZ": "🇰🇿", "BY": "🇧🇾", "UA": "🇺🇦"}


def _get_plate_pack(context, user_id: int, pack_type: str) -> dict | None:
    """Возвращает уже созданный пак для номерных знаков (стикер или эмодзи).
    Ищет в постоянном хранилище паков по флагу plate_pack.
    """
    packs = get_packs(context, user_id)
    for p in packs:
        if p.get("plate_pack") == pack_type:
            return p
    # Фолбэк: старый способ через bot_data (для обратной совместимости)
    key = f"plate_{pack_type}_pack"
    return context.bot_data.setdefault(str(user_id), {}).get(key)


def _save_plate_pack(context, user_id: int, pack_type: str, pack: dict):
    """Сохраняет пак номерных знаков с флагом plate_pack в постоянном хранилище."""
    pack["plate_pack"] = pack_type
    key = f"plate_{pack_type}_pack"
    context.bot_data.setdefault(str(user_id), {})[key] = pack
    packs = get_packs(context, user_id)
    # Обновляем существующий или добавляем новый
    for i, p in enumerate(packs):
        if p["name"] == pack["name"] or p.get("plate_pack") == pack_type:
            packs[i] = pack
            save_packs(context, user_id)
            return
    packs.append(pack)
    save_packs(context, user_id)


async def _plate_add_to_pack(bot, user_id: int, pack_data: dict,
                              pack: dict, emoji: str) -> str | None:
    """Добавляет номерной знак в пак. pack_data содержит country/chars/region/region_name."""
    pack_type = pack["type"]
    # Стикеры: 512px, эмодзи: 100px (требование Telegram)
    target_size = 100 if pack_type == "emoji" else 512
    processed = generate_plate_sticker(
        country=pack_data["country"],
        chars=pack_data["chars"],
        region=pack_data["region"],
        region_name=pack_data["region_name"],
        target_size=target_size,
    )
    stickers_buf = [1] if pack.get("count", 0) > 0 else []
    return await _push_sticker_to_tg(
        bot, user_id, pack["name"], pack_type,
        pack["title"], processed, StickerFormat.STATIC,
        [emoji], stickers_buf,
    )


def _generate_plate_emoji(country: str, chars: str, region: str) -> bytes:
    """
    Горизонтальный номерной знак для эмодзи-пака.
    Итоговый размер: 100×100px (требование Telegram).
    Знак занимает всю ширину и ~28px по высоте (как реальный номер в строке чата).
    Рисуем в 8× (800×800), знак — 800×224px по центру, даунсемплируем до 100×100.
    Шрифт максимально крупный чтобы в 100px текст был чётким.
    """
    # Рисуем в 8× для идеального качества после даунсемплинга
    DRAW_W = 800
    DRAW_H = 800

    # Знак: занимает всю ширину холста, высота ~28% (пропорция реального номера ~3.5:1)
    # Небольшой отступ по бокам и вертикали чтобы тень не обрезалась
    PAD_X = 12
    PAD_Y = 290   # центрируем вертикально — знак в середине квадрата
    PW = DRAW_W - PAD_X * 2          # ширина знака
    PH = DRAW_H - PAD_Y * 2          # высота знака (~220px из 800)

    # sc — сколько пикселей в 1 единице оригинальной системы координат
    # Оригинальный знак RU: 365×118 → sc = PW/365
    BASE_W = {"RU": 365, "UA": 450, "BY": 490, "KZ": 520}
    base_w = BASE_W.get(country, 450)
    sc = PW / base_w
    # Пересчитываем PH чтобы соблюдать пропорции знака (118 единиц высоты)
    PH = int(118 * sc)
    # Пересчитываем вертикальный отступ
    PAD_Y = (DRAW_H - PH) // 2

    img  = Image.new("RGBA", (DRAW_W, DRAW_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    px = PAD_X
    py = PAD_Y
    pw = PW
    ph = PH
    cx = px + pw // 2
    cy = py + ph // 2

    border  = max(2, int(4  * sc))
    radius  = int(10 * sc)
    sh_off  = int(4  * sc)

    def fnt_b(size):
        return ImageFont.truetype(FONT_BOLD, max(4, int(size * sc)))
    def fnt_r(size):
        return ImageFont.truetype(FONT_REG,  max(4, int(size * sc)))

    # ── Тень ────────────────────────────────────────────────────────────────
    draw.rounded_rectangle(
        [px+sh_off, py+sh_off, px+pw+sh_off, py+ph+sh_off],
        radius=radius, fill=(0, 0, 0, 80))

    if country == "RU":
        draw.rounded_rectangle([px, py, px+pw, py+ph],
                                radius=radius, fill="white",
                                outline="#111111", width=border)
        right_w = int(92 * sc)
        rdx = px + pw - right_w
        draw.line([(rdx, py+int(6*sc)), (rdx, py+ph-int(6*sc))],
                  fill="#111111", width=max(1, int(3*sc)))
        draw.text((px + (pw - right_w) // 2, cy), chars,
                  fill="#111111", font=fnt_b(66), anchor="mm")
        rcx = rdx + right_w // 2
        rpt = py + int(6*sc)
        rph_i = (py + ph - int(6*sc)) - rpt
        draw.text((rcx, rpt + int(rph_i*0.42)), region,
                  fill="#111111", font=fnt_b(44), anchor="mm")
        fw, fh = int(20*sc), int(14*sc)
        f_rus = fnt_r(12)
        rus_w = int(f_rus.getlength("RUS"))
        gap = int(3*sc)
        total_w = rus_w + gap + fw
        rus_cy = rpt + int(rph_i * 0.82)
        tx = rcx - total_w // 2
        draw.text((tx, rus_cy), "RUS", fill="#111111", font=f_rus, anchor="lm")
        _ru_flag(draw, tx + rus_w + gap, rus_cy - fh//2, fw=fw, fh=fh)

    elif country == "UA":
        draw.rounded_rectangle([px, py, px+pw, py+ph],
                                radius=int(8*sc), fill="white",
                                outline="#111111", width=border)
        strip_w = int(58 * sc)
        draw.rounded_rectangle([px+2, py+2, px+strip_w, py+ph-2],
                                radius=int(7*sc), fill="#003DA5")
        draw.line([(px+strip_w, py+int(4*sc)), (px+strip_w, py+ph-int(4*sc))],
                  fill="#111111", width=max(1, int(3*sc)))
        ffw, ffh = int(36*sc), int(24*sc)
        ffx = px + (strip_w - ffw) // 2
        ffy = py + int(18*sc)
        draw.rectangle([ffx, ffy,        ffx+ffw, ffy+ffh//2], fill="#005BBB")
        draw.rectangle([ffx, ffy+ffh//2, ffx+ffw, ffy+ffh],   fill="#FFD500")
        draw.text((px+strip_w//2, py+ph-int(14*sc)), "UA",
                  fill="white", font=fnt_b(17), anchor="mm")
        c = chars.strip().upper().replace(" ", "")
        body = (f"{region} {c[:4]} {c[4:]}" if len(c)==6 else
                f"{region} {c}" if region else c)
        draw.text((px+strip_w+(pw-strip_w)//2, cy), body,
                  fill="#111111", font=fnt_b(62), anchor="mm")

    elif country == "BY":
        draw.rounded_rectangle([px, py, px+pw, py+ph],
                                radius=int(8*sc), fill="white",
                                outline="#111111", width=border)
        zone_w = int(90*sc); fl_w = int(72*sc); fl_h = int(48*sc)
        fnt_by2 = fnt_b(16)
        by_h = fnt_by2.getbbox("BY")[3] - fnt_by2.getbbox("BY")[1]
        fl_x = px + (zone_w-fl_w)//2
        fl_y = py + (ph - fl_h - int(4*sc) - by_h)//2
        red_h = round(fl_h * 2/3)
        draw.rectangle([fl_x, fl_y, fl_x+fl_w, fl_y+red_h], fill="#CF101A")
        draw.rectangle([fl_x, fl_y+red_h, fl_x+fl_w, fl_y+fl_h], fill="#007828")
        orn_w = max(4, fl_w//9)
        draw.rectangle([fl_x, fl_y, fl_x+orn_w, fl_y+fl_h], fill="white")
        step = max(4, orn_w+1); ocx = fl_x+orn_w//2
        for yi in range(fl_y, fl_y+fl_h, step):
            y_mid = yi+step//2
            col = "#CF101A" if y_mid < fl_y+red_h else "#007828"
            draw.polygon([(ocx,yi),(fl_x+orn_w-1,y_mid),(ocx,min(yi+step,fl_y+fl_h)),(fl_x+1,y_mid)], fill=col)
        draw.text((fl_x+fl_w//2, fl_y+fl_h+int(4*sc)), "BY", fill="#111111", font=fnt_by2, anchor="mt")
        c = chars.replace(" ","").upper()
        body = (f"{c[:4]} {c[4:6]}-{region}" if len(c)>=6 else
                f"{c[:4]} {c[4:]}-{region}" if len(c)>=4 else f"{chars}-{region}")
        draw.text((px+zone_w+int(6*sc)+(pw-zone_w-int(14*sc))//2, cy),
                  body, fill="#111111", font=fnt_b(68), anchor="mm")

    elif country == "KZ":
        draw.rounded_rectangle([px, py, px+pw, py+ph],
                                radius=radius, fill="white",
                                outline="#1a1a1a", width=border)
        SW = int(96*sc); RW = int(68*sc)
        col_cx = px + SW//2
        flag_h_kz = int(56*sc)
        fnt_kz = fnt_b(18)
        kz_h = fnt_kz.getbbox("KZ")[3] - fnt_kz.getbbox("KZ")[1]
        flag_drawn = False
        if KZ_FLAG_PATH and os.path.exists(KZ_FLAG_PATH):
            try:
                fi = Image.open(KZ_FLAG_PATH).convert("RGBA")
                max_fw = SW - int(16*sc)
                rh = flag_h_kz / fi.height
                nw2 = int(fi.width*rh)
                if nw2 > max_fw: rh = max_fw/fi.width
                nw2 = max(1,int(fi.width*rh)); nh2 = max(1,int(fi.height*rh))
                rf = fi.resize((nw2,nh2), Image.LANCZOS)
                tot = rf.height+int(3*sc)+kz_h
                fy2 = py+(ph-tot)//2
                img.paste(rf,(col_cx-rf.width//2, fy2), rf)
                flag_bottom = fy2+rf.height
                flag_drawn = True
            except Exception: pass
        if not flag_drawn:
            fw2,fh2 = int(60*sc),int(40*sc)
            fy2 = py+(ph-fh2-int(3*sc)-kz_h)//2
            draw.rectangle([col_cx-fw2//2,fy2,col_cx+fw2//2,fy2+fh2],fill="#00AFCA")
            flag_bottom = fy2+fh2
        draw.text((col_cx, flag_bottom+int(3*sc)), "KZ", fill="#111111", font=fnt_kz, anchor="mt")
        rdx = px+pw-RW
        draw.line([(rdx,py+int(10*sc)),(rdx,py+ph-int(10*sc))],fill="#bbbbbb",width=max(1,int(2*sc)))
        draw.text((rdx+RW//2, cy), region, fill="#111111", font=fnt_b(42), anchor="mm")
        c = chars.replace(" ","")
        body = f"{c[:3]} {c[3:]}" if len(c)==6 else chars
        draw.text((px+SW+(rdx-px-SW)//2, cy), body, fill="#111111", font=fnt_b(78), anchor="mm")

    # Даунсемплируем 800×800 → 100×100 через LANCZOS
    out = img.resize((100, 100), Image.LANCZOS)

    buf = io.BytesIO()
    out.save(buf, "PNG", optimize=False, compress_level=1)
    return buf.getvalue()

def generate_plate_sticker(country: str, chars: str, region: str,
                            region_name: str, target_size: int = 512) -> bytes:
    """
    Рисует ТОЛЬКО номерной знак на прозрачном фоне.
    Для эмодзи (100px) — вертикальная компоновка: флаг сверху, номер снизу,
    чтобы максимально использовать квадратный холст и сделать текст читаемым.
    Для стикеров (512px) — стандартная горизонтальная компоновка.
    Рисуем в 4× разрешении, затем LANCZOS-даунсемплинг — без пикселей.
    """
    # Для эмодзи используем специальную вертикальную компоновку
    if target_size <= 100:
        return _generate_plate_emoji(country, chars, region)

    # Рисуем в 4× разрешении, потом уменьшаем — антиалиасинг без артефактов
    SCALE = 4
    S = target_size * SCALE  # размер холста для рисования

    # Пропорции знака по стране (ширина : высота)
    RATIOS = {
        "RU": (365, 118),
        "UA": (450, 118),
        "BY": (490, 118),
        "KZ": (520, 118),
    }
    pw_base, ph_base = RATIOS.get(country, (450, 118))

    # Масштабируем знак так, чтобы он занимал ~94% S по ширине
    fill_frac = 0.94
    sc = (S * fill_frac) / pw_base
    pw = int(pw_base * sc)
    ph = int(ph_base * sc)

    PAD = int(8 * sc)
    W = pw + PAD * 2
    H = ph + PAD * 2

    # Если знак выше холста — уменьшаем масштаб
    if H > S:
        sc = sc * (S * 0.94) / H
        pw = int(pw_base * sc)
        ph = int(ph_base * sc)
        PAD = int(8 * sc)
        W = pw + PAD * 2
        H = ph + PAD * 2

    img  = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    cx = W // 2
    cy = H // 2
    px = cx - pw // 2
    py = cy - ph // 2

    def fnt_b(size):
        return ImageFont.truetype(FONT_BOLD, max(6, int(size * sc)))

    def fnt_r(size):
        return ImageFont.truetype(FONT_REG, max(6, int(size * sc)))

    border = max(2, int(4 * sc))
    radius = int(10 * sc)
    shadow_off = int(5 * sc)

    if country == "RU":
        # Тень
        draw.rounded_rectangle(
            [px + shadow_off, py + shadow_off, px + pw + shadow_off, py + ph + shadow_off],
            radius=radius, fill=(0, 0, 0, 80))
        # Белый корпус с чёрной рамкой
        draw.rounded_rectangle([px, py, px + pw, py + ph],
                                radius=radius, fill="white", outline="#111111", width=border)
        right_w = int(92 * sc)
        rdx = px + pw - right_w
        draw.line([(rdx, py + int(6*sc)), (rdx, py + ph - int(6*sc))],
                  fill="#111111", width=max(1, int(3*sc)))
        # Основные символы (левая часть)
        draw.text((px + (pw - right_w) // 2, cy), chars,
                  fill="#111111", font=fnt_b(66), anchor="mm")
        # Правая панель
        rcx = rdx + right_w // 2
        rpt = py + int(6*sc)
        rph_inner = (py + ph - int(6*sc)) - rpt
        region_cy = rpt + int(rph_inner * 0.42)
        draw.text((rcx, region_cy), region, fill="#111111", font=fnt_b(44), anchor="mm")
        # RUS + флаг
        fw, fh = int(20*sc), int(14*sc)
        f_rus = fnt_r(12)
        rus_w = int(f_rus.getlength("RUS"))
        gap = int(3*sc)
        total_w = rus_w + gap + fw
        rus_cy = rpt + int(rph_inner * 0.82)
        tx = rcx - total_w // 2
        fx = tx + rus_w + gap
        fy = rus_cy - fh // 2
        draw.text((tx, rus_cy), "RUS", fill="#111111", font=f_rus, anchor="lm")
        _ru_flag(draw, fx, fy, fw=fw, fh=fh)

    elif country == "UA":
        draw.rounded_rectangle(
            [px + shadow_off, py + shadow_off, px + pw + shadow_off, py + ph + shadow_off],
            radius=int(8*sc), fill=(0, 0, 0, 80))
        draw.rounded_rectangle([px, py, px + pw, py + ph],
                                radius=int(8*sc), fill="white", outline="#111111", width=border)
        strip_w = int(58 * sc)
        draw.rounded_rectangle([px+2, py+2, px+strip_w, py+ph-2],
                                radius=int(7*sc), fill="#003DA5")
        draw.line([(px+strip_w, py+int(4*sc)), (px+strip_w, py+ph-int(4*sc))],
                  fill="#111111", width=max(1, int(3*sc)))
        ffw, ffh = int(36*sc), int(24*sc)
        ffx = px + (strip_w - ffw) // 2
        ffy = py + int(18*sc)
        draw.rectangle([ffx, ffy,        ffx+ffw, ffy+ffh//2], fill="#005BBB")
        draw.rectangle([ffx, ffy+ffh//2, ffx+ffw, ffy+ffh],   fill="#FFD500")
        draw.text((px + strip_w//2, py + ph - int(14*sc)), "UA",
                  fill="white", font=fnt_b(17), anchor="mm")
        c = chars.strip().upper().replace(" ", "")
        if len(c) == 6:
            body = f"{region} {c[:4]} {c[4:]}" if region else f"{c[:4]} {c[4:]}"
        elif len(c) == 4:
            body = f"{region} {c}" if region else c
        else:
            body = f"{region} {c}" if region else c
        content_cx = px + strip_w + (pw - strip_w) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_b(62), anchor="mm")

    elif country == "BY":
        draw.rounded_rectangle(
            [px + shadow_off, py + shadow_off, px + pw + shadow_off, py + ph + shadow_off],
            radius=int(8*sc), fill=(0, 0, 0, 80))
        draw.rounded_rectangle([px, py, px + pw, py + ph],
                                radius=int(8*sc), fill="white", outline="#111111", width=border)
        zone_w = int(90 * sc)
        fl_w   = int(72 * sc)
        fl_h   = int(48 * sc)
        fnt_by2 = fnt_b(16)
        by_bbox = fnt_by2.getbbox("BY")
        by_h    = by_bbox[3] - by_bbox[1]
        total_h_flag = fl_h + int(4*sc) + by_h
        fl_x = px + (zone_w - fl_w) // 2
        fl_y = py + (ph - total_h_flag) // 2
        red_h = round(fl_h * 2 / 3)
        draw.rectangle([fl_x, fl_y, fl_x+fl_w, fl_y+red_h], fill="#CF101A")
        draw.rectangle([fl_x, fl_y+red_h, fl_x+fl_w, fl_y+fl_h], fill="#007828")
        orn_w = max(4, fl_w // 9)
        draw.rectangle([fl_x, fl_y, fl_x+orn_w, fl_y+fl_h], fill="white")
        step = max(4, orn_w + 1)
        ocx  = fl_x + orn_w // 2
        for yi in range(fl_y, fl_y + fl_h, step):
            y_top = yi
            y_mid = yi + step // 2
            y_bot = min(yi + step, fl_y + fl_h)
            col   = "#CF101A" if y_mid < (fl_y + red_h) else "#007828"
            draw.polygon([(ocx, y_top), (fl_x+orn_w-1, y_mid),
                           (ocx, y_bot), (fl_x+1, y_mid)], fill=col)
        draw.text((fl_x + fl_w//2, fl_y + fl_h + int(4*sc)), "BY",
                  fill="#111111", font=fnt_by2, anchor="mt")
        text_x = px + zone_w + int(6*sc)
        c = chars.replace(" ", "").upper()
        if len(c) >= 6:
            body = f"{c[:4]} {c[4:6]}-{region}"
        elif len(c) >= 4:
            body = f"{c[:4]} {c[4:]}-{region}"
        else:
            body = f"{chars}-{region}"
        content_cx = text_x + (px + pw - int(8*sc) - text_x) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_b(68), anchor="mm")

    elif country == "KZ":
        draw.rounded_rectangle(
            [px + shadow_off, py + shadow_off, px + pw + shadow_off, py + ph + shadow_off],
            radius=radius, fill=(0, 0, 0, 80))
        draw.rounded_rectangle([px, py, px + pw, py + ph],
                                radius=radius, fill="white", outline="#1a1a1a", width=border)
        SW = int(96 * sc)
        RW = int(68 * sc)
        col_cx = px + SW // 2
        target_flag_h = int(56 * sc)
        fnt_kz_label = fnt_b(18)
        kz_bbox = fnt_kz_label.getbbox("KZ")
        kz_h = kz_bbox[3] - kz_bbox[1]
        flag_drawn = False
        if KZ_FLAG_PATH and os.path.exists(KZ_FLAG_PATH):
            try:
                flag_img = Image.open(KZ_FLAG_PATH).convert("RGBA")
                max_flag_w = SW - int(16*sc)
                ratio_h = target_flag_h / flag_img.height
                new_w   = int(flag_img.width * ratio_h)
                if new_w > max_flag_w:
                    ratio_h = max_flag_w / flag_img.width
                new_w = max(1, int(flag_img.width  * ratio_h))
                new_h = max(1, int(flag_img.height * ratio_h))
                resized_flag = flag_img.resize((new_w, new_h), Image.LANCZOS)
                total_h_flag = resized_flag.height + int(3*sc) + kz_h
                flag_y  = py + (ph - total_h_flag) // 2
                flag_x  = col_cx - resized_flag.width // 2
                img.paste(resized_flag, (flag_x, flag_y), resized_flag)
                flag_bottom = flag_y + resized_flag.height
                flag_drawn = True
            except Exception:
                pass
        if not flag_drawn:
            fl_w2, fl_h2 = int(60*sc), int(40*sc)
            total_h_flag = fl_h2 + int(3*sc) + kz_h
            flag_y = py + (ph - total_h_flag) // 2
            fl_x2  = col_cx - fl_w2 // 2
            draw.rectangle([fl_x2, flag_y, fl_x2+fl_w2, flag_y+fl_h2], fill="#00AFCA")
            flag_bottom = flag_y + fl_h2
        draw.text((col_cx, flag_bottom + int(3*sc)), "KZ",
                  fill="#111111", font=fnt_kz_label, anchor="mt")
        rdx = px + pw - RW
        draw.line([(rdx, py+int(10*sc)), (rdx, py+ph-int(10*sc))],
                  fill="#bbbbbb", width=max(1, int(2*sc)))
        draw.text((rdx + RW // 2, cy), region, fill="#111111", font=fnt_b(42), anchor="mm")
        c    = chars.replace(" ", "")
        body = f"{c[:3]} {c[3:]}" if len(c) == 6 else chars
        num_cx = px + SW + (rdx - px - SW) // 2
        draw.text((num_cx, cy), body, fill="#111111", font=fnt_b(78), anchor="mm")

    # Даунсемплируем в квадрат target_size с прозрачным фоном
    out = Image.new("RGBA", (target_size, target_size), (0, 0, 0, 0))
    ratio = min(target_size / W, target_size / H)
    nw = max(1, int(W * ratio))
    nh = max(1, int(H * ratio))
    # LANCZOS даунсемплинг из 4× — идеальное качество без пикселей
    plate_resized = img.resize((nw, nh), Image.LANCZOS)
    off_x = (target_size - nw) // 2
    off_y = (target_size - nh) // 2
    out.paste(plate_resized, (off_x, off_y), plate_resized)

    buf = io.BytesIO()
    out.save(buf, "PNG", optimize=False, compress_level=1)
    data_out = buf.getvalue()
    if len(data_out) > MAX_STICKER_BYTES:
        buf2 = io.BytesIO()
        out.save(buf2, "PNG", optimize=True, compress_level=9)
        data_out = buf2.getvalue()
    return data_out


async def plate_buy_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускает покупку за звезду для сохранения номера в стикер-пак."""
    query = update.callback_query
    await query.answer()
    context.user_data["plate_pending_pack_type"] = "sticker"
    price = int(DB.get_setting("price_plate_sticker", "1"))

    # Отправляем превью стикера
    await context.bot.send_sticker(
        chat_id=query.message.chat_id,
        sticker="CAACAgIAAxkBAALDnWnfpxKQfbcdxE5oTpMVAAEx5zooAANKngACW-vZSgSGJtWxpXx0OwQ",
    )

    await context.bot.send_invoice(
        chat_id=query.message.chat_id,
        title="Получить этот номер в формате стикера",
        description="После покупки создаётся стикер-пак, в котором сохранится этот номер и все последующие номера",
        payload="plate_save_sticker",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice("В стикер-пак", price)],
    )
    return PLATE_INPUT


async def plate_buy_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускает покупку за звезду для сохранения номера в эмодзи-пак."""
    query = update.callback_query
    await query.answer()
    context.user_data["plate_pending_pack_type"] = "emoji"
    price = int(DB.get_setting("price_plate_emoji", "1"))

    # Отправляем превью премиум эмодзи
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text='<tg-emoji emoji-id="5395700970820772281">🇷🇺</tg-emoji>',
        parse_mode="HTML",
    )

    await context.bot.send_invoice(
        chat_id=query.message.chat_id,
        title="Получить этот номер в формате эмодзи",
        description="После покупки создаётся эмодзи-пак, в котором сохранится этот номер и все последующие номера",
        payload="plate_save_emoji",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice("В эмодзи-пак", price)],
    )
    return PLATE_INPUT


async def plate_pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Подтверждает pre-checkout для платежей за номерные знаки."""
    query = update.pre_checkout_query
    if query.invoice_payload in ("plate_save_sticker", "plate_save_emoji"):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Неизвестный платёж")


async def plate_successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """После успешной оплаты звездой — выполняет сохранение номера в пак."""
    payload = update.message.successful_payment.invoice_payload
    if payload == "plate_save_sticker":
        pack_type = "sticker"
    elif payload == "plate_save_emoji":
        pack_type = "emoji"
    else:
        return PLATE_INPUT

    user_id   = update.effective_user.id
    DB.record_purchase(user_id, stars=1)
    country   = context.user_data.get("last_plate_country", "RU")
    emoji     = PLATE_COUNTRY_EMOJI.get(country, "🚗")
    pack      = _get_plate_pack(context, user_id, pack_type)
    img_bytes = context.user_data.get("last_plate_bytes")
    pack_data = {
        "country":     country,
        "chars":       context.user_data.get("last_plate_chars", ""),
        "region":      context.user_data.get("last_plate_region", ""),
        "region_name": context.user_data.get("last_plate_region_name", ""),
    }
    type_label = "стикер-пак" if pack_type == "sticker" else "эмодзи-пак"

    if not img_bytes:
        await update.message.reply_text("❌ Изображение номера не найдено. Сгенерируй номер заново.")
        return PLATE_INPUT

    if pack:
        if "plate_pack" not in pack:
            pack["plate_pack"] = pack_type
        status = await update.message.reply_text(f"⏳ Добавляю в {type_label}...")
        err = await _plate_add_to_pack(context.bot, user_id, pack_data, pack, emoji)
        try:
            await status.delete()
        except Exception:
            pass
        if err:
            await update.message.reply_text(f"❌ {err}")
        else:
            pack["count"] = pack.get("count", 0) + 1
            save_packs(context, user_id)
            DB.increment_stat("plates_created")
            url  = pack_url(pack["name"])
            icon = "🖼️" if pack_type == "sticker" else "✨"
            await update.message.reply_text(
                f"{icon} <b>Номер добавлен в «{pack['title']}»!</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{icon} Открыть пак", url=url)],
                    [InlineKeyboardButton("🏠 В меню", callback_data="begin")],
                ]),
            )
        return PLATE_INPUT

    # Пака нет — спрашиваем название
    context.user_data["plate_pack_creating"] = pack_type
    icon = "🖼️" if pack_type == "sticker" else "✨"
    await update.message.reply_text(
        f"{icon} <b>Создаём {type_label} для номерных знаков</b>\n\n"
        f"Эмодзи будет автоматически: {emoji}\n\n"
        "✍️ <b>Введи название</b> пака:\n\n/cancel — отменить",
        parse_mode="HTML",
    )
    return PLATE_PACK_NAME


async def plate_save_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _plate_save_start(update, context, "sticker")


async def plate_save_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _plate_save_start(update, context, "emoji")


async def _plate_save_start(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              pack_type: str) -> int:
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    country     = context.user_data.get("last_plate_country", "RU")
    emoji       = PLATE_COUNTRY_EMOJI.get(country, "🚗")
    pack        = _get_plate_pack(context, user_id, pack_type)
    img_bytes   = context.user_data.get("last_plate_bytes")
    pack_data   = {
        "country":     country,
        "chars":       context.user_data.get("last_plate_chars", ""),
        "region":      context.user_data.get("last_plate_region", ""),
        "region_name": context.user_data.get("last_plate_region_name", ""),
    }

    if not img_bytes:
        await query.message.reply_text("❌ Изображение номера не найдено. Сгенерируй номер заново.")
        return PLATE_INPUT

    type_label = "стикер-пак" if pack_type == "sticker" else "эмодзи-пак"

    # Убираем кнопки из сообщения — чтобы нельзя было нажать повторно
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    if pack:
        # Убеждаемся что флаг plate_pack стоит (на случай старых паков без флага)
        if "plate_pack" not in pack:
            pack["plate_pack"] = pack_type
        # Пак уже есть — добавляем сразу
        status = await query.message.reply_text(f"⏳ Добавляю в {type_label}...")
        err = await _plate_add_to_pack(context.bot, user_id, pack_data, pack, emoji)
        try:
            await status.delete()
        except Exception:
            pass
        if err:
            await query.message.reply_text(f"❌ {err}")
        else:
            pack["count"] = pack.get("count", 0) + 1
            save_packs(context, user_id)
            url = pack_url(pack["name"])
            icon = "🖼️" if pack_type == "sticker" else "✨"
            kb = [
                [InlineKeyboardButton(f"{icon} Открыть пак", url=url)],
                [InlineKeyboardButton("🏠 В меню", callback_data="begin")],
            ]
            await query.message.reply_text(
                f"{icon} <b>Номер добавлен в «{pack['title']}»!</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb),
            )
        return PLATE_INPUT

    # Пака нет — спрашиваем название
    context.user_data["plate_pack_creating"] = pack_type
    icon = "🖼️" if pack_type == "sticker" else "✨"
    await query.message.reply_text(
        (f"{icon} <b>Создаём {type_label} для номерных знаков</b>\n\n"
        f"Эмодзи будет автоматически: {emoji}\n\n"
        "✍️ <b>Введи название</b> пака:\n\n/cancel — отменить"),
        parse_mode="HTML",
    )
    return PLATE_PACK_NAME


async def plate_pack_receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    title = update.message.text.strip()
    if not title:
        await update.message.reply_text("Название не может быть пустым:")
        return PLATE_PACK_NAME
    context.user_data["plate_pack_title"] = title
    kb = [[InlineKeyboardButton("🎲 Случайная ссылка", callback_data="plate_random_link")]]
    await update.message.reply_text(
        "🔗 <b>Придумай короткую ссылку</b> для пака\n\nТолько латиница, цифры и <code>_</code>.\n\n/cancel — отменить",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PLATE_PACK_LINK


async def plate_pack_random_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    suffix = random_suffix()
    context.user_data["plate_pack_suffix"] = suffix
    return await _plate_pack_create(update.callback_query.message, context, reply=False)


async def plate_pack_receive_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw    = update.message.text.strip()
    suffix = sanitize_suffix(raw)
    if suffix != raw.lower():
        await update.message.reply_text(
            f"ℹ️ Ссылка скорректирована: <code>{suffix}</code>", parse_mode="HTML"
        )
    context.user_data["plate_pack_suffix"] = suffix
    return await _plate_pack_create(update.message, context, reply=True)


async def _plate_pack_create(msg, context: ContextTypes.DEFAULT_TYPE, reply: bool) -> int:
    user_id   = context._user_id if hasattr(context, "_user_id") else None
    # Достаём user_id через update — msg это telegram Message
    user_id   = msg.chat_id
    pack_type = context.user_data.get("plate_pack_creating", "sticker")
    title     = context.user_data.get("plate_pack_title", "Номерные знаки")
    suffix    = context.user_data.get("plate_pack_suffix") or random_suffix()
    country     = context.user_data.get("last_plate_country", "RU")
    emoji       = PLATE_COUNTRY_EMOJI.get(country, "🚗")
    img_bytes   = context.user_data.get("last_plate_bytes")
    pack_data   = {
        "country":     country,
        "chars":       context.user_data.get("last_plate_chars", ""),
        "region":      context.user_data.get("last_plate_region", ""),
        "region_name": context.user_data.get("last_plate_region_name", ""),
    }
    bot_me    = await context.bot.get_me()
    pack_name = build_pack_name(bot_me.username, suffix)
    icon      = "🖼️" if pack_type == "sticker" else "✨"

    if not img_bytes:
        await msg.reply_text("❌ Изображение не найдено. Сгенерируй номер заново.")
        return PLATE_INPUT

    status = await msg.reply_text(f"⏳ Создаю {icon} пак...")

    pack = {"title": title, "name": pack_name, "suffix": suffix,
            "type": pack_type, "count": 0, "adaptive": False}

    err = await _plate_add_to_pack(context.bot, user_id, pack_data, pack, emoji)
    try:
        await status.delete()
    except Exception:
        pass

    if err:
        await msg.reply_text(f"❌ {err}\n\nПопробуй другую ссылку /cancel")
        return PLATE_PACK_LINK

    pack["count"] = 1
    _save_plate_pack(context, user_id, pack_type, pack)

    # Чистим временные данные
    for k in ("plate_pack_creating", "plate_pack_title", "plate_pack_suffix"):
        context.user_data.pop(k, None)

    url = pack_url(pack_name)
    kb  = [
        [InlineKeyboardButton(f"{icon} Открыть пак", url=url)],
        [InlineKeyboardButton("🏠 В меню", callback_data="plate_menu")],
    ]
    await msg.reply_text(
        f"{icon} <b>Пак создан!</b>\n\n<b>{title}</b>\nНомер добавлен!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PLATE_INPUT

async def plate_btn_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Кнопка 'В меню' под фото номера."""
    query = update.callback_query
    await query.answer()
    kb = [
        [InlineKeyboardButton("🖼  Создать стикер-пак",   callback_data="create_sticker")],
        [InlineKeyboardButton("✨  Создать эмодзи-пак",    callback_data="create_emoji")],
        [InlineKeyboardButton("🚗  Создать номерной знак", callback_data="create_plate")],
    ]
    await query.message.reply_text(
        "👋 <b>Что хочешь создать?</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return CHOOSE_TYPE


async def plate_btn_other(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Кнопка 'Другой номер' под фото."""
    query   = update.callback_query
    await query.answer()
    country = query.data.replace("plate_other_", "")
    context.user_data["plate_country"] = country
    flag    = COUNTRY_NAMES.get(country, country)
    await query.message.reply_text(
        f"🚗 {flag}\n\nВыбери код региона:",
        parse_mode="HTML",
        reply_markup=_region_page_kb(country, 0),
    )
    return PLATE_REGION


async def plate_noop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    return PLATE_REGION




# ══════════════════════════════════════════════════════════════════════════════
# КОПИРОВАНИЕ ПАКА
# ══════════════════════════════════════════════════════════════════════════════

def _copy_wait(err_s: str, attempt: int) -> int:
    """Вычисляет время ожидания перед ретраем по строке ошибки."""
    if err_s.startswith("retry:"):
        try:
            return int(err_s.split(":")[1]) + 1
        except Exception:
            return 10
    if "flood" in err_s.lower() or "retry" in err_s.lower() or "временно" in err_s.lower():
        return min(60, 10 * (attempt + 1))
    if "timeout" in err_s.lower() or "timed out" in err_s.lower():
        return 5
    if "bad gateway" in err_s.lower() or "network" in err_s.lower():
        return min(30, 5 * (attempt + 1))
    return min(30, 3 * (attempt + 1))

async def copy_pack_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Нажата кнопка 'Скопировать пак' в главном меню."""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("copy_pack_url", None)
    context.user_data.pop("copy_pack_stickers", None)
    context.user_data.pop("copy_pack_source_name", None)
    await query.edit_message_text(
        "📋 <b>Копирование пака</b>\n\n"
        "Отправьте мне ссылку на набор, который хотите скопировать\n\n"
        "/cancel — для отмены",
        parse_mode="HTML",
    )
    return COPY_PACK_LINK


async def copy_pack_receive_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получаем ссылку на пак для копирования."""
    text = update.message.text.strip()

    # Принимаем любые форматы:
    #   https://t.me/addstickers/NAME  |  https://t.me/addemoji/NAME
    #   t.me/addstickers/NAME          |  t.me/addemoji/NAME
    #   просто NAME
    pack_name = text
    for prefix in ("t.me/addstickers/", "t.me/addemoji/"):
        if prefix in text:
            pack_name = text.split(prefix)[-1].split("?")[0].strip("/").strip()
            break
    else:
        # Убираем протокол и лишние сегменты если осталось что-то нестандартное
        pack_name = re.sub(r'^https?://', '', pack_name).strip("/").strip()
        if "/" in pack_name:
            pack_name = pack_name.split("/")[-1].strip()

    if not pack_name or not re.match(r'^[a-zA-Z0-9_]+$', pack_name):
        await update.message.reply_text(
            "❌ Неверная ссылка. Отправьте ссылку вида:\n"
            "<code>https://t.me/addstickers/pack_name</code>\n"
            "<code>https://t.me/addemoji/pack_name</code>\n\n/cancel — отмена",
            parse_mode="HTML",
        )
        return COPY_PACK_LINK

    status_msg = await update.message.reply_text("🔍 Загружаю информацию о паке...")
    try:
        sticker_set = await context.bot.get_sticker_set(pack_name)
    except TelegramError as e:
        await status_msg.edit_text(
            f"❌ Не удалось найти пак: <code>{_friendly_tg_error(str(e))}</code>\n\n"
            "Попробуйте другую ссылку или /cancel",
            parse_mode="HTML",
        )
        return COPY_PACK_LINK

    stickers = sticker_set.stickers
    if not stickers:
        await status_msg.edit_text(
            "❌ Пак пустой. Попробуйте другую ссылку или /cancel"
        )
        return COPY_PACK_LINK

    # Определяем тип исходного пака
    first = stickers[0]
    source_type = "emoji" if (
        getattr(first, "type", None) == StickerType.CUSTOM_EMOJI or
        str(getattr(first, "type", "")) == "custom_emoji"
    ) else "sticker"

    context.user_data["copy_pack_source_name"] = pack_name
    context.user_data["copy_pack_source_type"] = source_type
    context.user_data["copy_pack_source_title"] = sticker_set.title
    context.user_data["copy_pack_sticker_count"] = len(stickers)

    await status_msg.delete()

    source_label = "эмодзи-пак" if source_type == "emoji" else "стикер-пак"

    if source_type == "emoji":
        sticker_btn_label = "🖼 Стикер-пак (конвертировать)"
        emoji_btn_label   = "✨ Эмодзи-пак (точная копия)"
    else:
        sticker_btn_label = "🖼 Стикер-пак (точная копия)"
        emoji_btn_label   = "✨ Эмодзи-пак (конвертировать)"

    kb = [
        [InlineKeyboardButton(sticker_btn_label,          callback_data="copy_as_sticker")],
        [InlineKeyboardButton(emoji_btn_label,             callback_data="copy_as_emoji")],
        [InlineKeyboardButton("➕ Добавить в созданный",  callback_data="copy_add_to_existing")],
        [InlineKeyboardButton("◀️ Назад", callback_data="begin")],
    ]
    await update.message.reply_text(
        f"✅ Найден <b>{source_label}</b>: <b>{sticker_set.title}</b>\n"
        f"Стикеров/эмодзи: {len(stickers)}\n\n"
        "Выберите тип нового набора кнопкой ниже\n"
        "/cancel — для отмены",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return COPY_PACK_TYPE


async def _do_copy_pack(update_or_msg, context: ContextTypes.DEFAULT_TYPE,
                        target_type: str, target_pack_name: str = None,
                        target_pack_title: str = None) -> int:
    """Выполняет копирование стикеров/эмодзи из исходного пака в новый или существующий.
    target_type: 'sticker' | 'emoji'
    target_pack_name: если задан — добавляем в существующий пак, иначе создаём новый.
    """
    import asyncio

    user_id = context.user_data.get("_user_id_copy", 0)
    if not user_id and hasattr(update_or_msg, "effective_user") and update_or_msg.effective_user:
        user_id = update_or_msg.effective_user.id
    source_name  = context.user_data.get("copy_pack_source_name", "")
    source_title = context.user_data.get("copy_pack_source_title", "Копия")

    logger.info(
        "_do_copy_pack START: user_id=%s source=%s target_type=%s "
        "target_pack_name=%s creating_new=%s",
        user_id, source_name, target_type, target_pack_name, (target_pack_name is None),
    )

    # Определяем объект для отправки сообщений
    if hasattr(update_or_msg, "callback_query") and update_or_msg.callback_query:
        send_to = update_or_msg.callback_query.message
    elif hasattr(update_or_msg, "message") and update_or_msg.message:
        send_to = update_or_msg.message
    else:
        send_to = update_or_msg

    status_msg = await send_to.reply_text("⏳ Загружаю информацию о паке...")

    # ── Получаем исходный пак с ретраем ──────────────────────────────────────
    sticker_set = None
    for attempt in range(3):
        try:
            sticker_set = await context.bot.get_sticker_set(source_name)
            break
        except TelegramError as e:
            if "flood" in str(e).lower() or "retry" in str(e).lower():
                await asyncio.sleep(5 * (attempt + 1))
            else:
                await status_msg.edit_text(f"❌ Ошибка получения пака: {_friendly_tg_error(str(e))}")
                return CHOOSE_TYPE
    if sticker_set is None:
        await status_msg.edit_text("❌ Не удалось загрузить пак после нескольких попыток.")
        return CHOOSE_TYPE

    stickers = sticker_set.stickers
    if not stickers:
        await status_msg.edit_text("❌ Исходный пак пустой.")
        return CHOOSE_TYPE

    bot_me = await context.bot.get_me()
    bot_username = bot_me.username
    context.user_data["_bot_username"] = bot_username

    creating_new = target_pack_name is None
    if creating_new:
        suffix = random_suffix()
        target_pack_name  = build_pack_name(bot_username, suffix)
        if not target_pack_title:
            target_pack_title = f"Копия {source_title}"

    # Определяем: источник — эмодзи пак?
    first_src = stickers[0]
    source_is_emoji = (
        str(getattr(first_src, "type", "")) == "custom_emoji" or
        getattr(first_src, "type", None) == StickerType.CUSTOM_EMOJI
    )

    added  = 0
    errors = 0
    total  = len(stickers)

    await status_msg.edit_text(f"⏳ Копирую... 0/{total}")

    for i, src_sticker in enumerate(stickers):
        # Обновляем статус
        try:
            await status_msg.edit_text(f"⏳ Копирую... {i}/{total}")
        except Exception:
            pass

        # ── Определяем формат источника ──────────────────────────────────────
        src_fmt = getattr(src_sticker, "format", None)
        if src_fmt is None:
            if getattr(src_sticker, "is_animated", False):
                src_fmt = StickerFormat.ANIMATED
            elif getattr(src_sticker, "is_video", False):
                src_fmt = StickerFormat.VIDEO
            else:
                src_fmt = StickerFormat.STATIC

        is_src_custom_emoji = (
            str(getattr(src_sticker, "type", "")) == "custom_emoji" or
            getattr(src_sticker, "type", None) == StickerType.CUSTOM_EMOJI
        )

        # ── Эмодзи-теги ──────────────────────────────────────────────────────
        emoji_list  = list(src_sticker.emoji) if src_sticker.emoji else ["🙂"]
        clean_emoji = [e for e in emoji_list if e and e.strip()] or ["🙂"]

        # Если добавляем в существующий пак — буфер всегда должен быть непустым,
        # чтобы _push_sticker_to_tg вызвал add_sticker_to_set, а не create_new_sticker_set.
        if not creating_new:
            stickers_buf_dummy = [{}]  # сигнал «пак уже существует»
        else:
            stickers_buf_dummy = [{}] * added

        # ── БЕСКОНЕЧНЫЙ РЕТРАЙ — бот обязан загрузить каждый стикер ─────────
        # Единственный выход из цикла: успех или пак заполнен (120 стикеров).
        # Никаких "пропустить", никаких break по ошибке.
        global_attempt = 0
        pack_full      = False

        while True:
            global_attempt += 1

            # ── ШАГ 1: Получаем актуальный file_id ───────────────────────────
            actual_file_id = src_sticker.file_id
            if is_src_custom_emoji:
                custom_emoji_id = getattr(src_sticker, "custom_emoji_id", None)
                if custom_emoji_id:
                    try:
                        fresh = await context.bot.get_custom_emoji_stickers([custom_emoji_id])
                        if fresh:
                            actual_file_id = fresh[0].file_id
                    except TelegramError as ce_err:
                        retry_after = getattr(ce_err, "retry_after", None)
                        wait = int(retry_after) + 1 if retry_after else 5
                        logger.info("copy_pack: get_custom_emoji_stickers flood, wait %ds", wait)
                        await asyncio.sleep(wait)
                        continue
                    except Exception as ce_err:
                        logger.warning("copy_pack: get_custom_emoji_stickers error: %s", ce_err)
                        await asyncio.sleep(3)
                        continue

            # ── ШАГ 2: Скачиваем файл ────────────────────────────────────────
            raw               = None
            use_file_id_only  = False

            try:
                f   = await context.bot.get_file(actual_file_id)
                buf = io.BytesIO()
                await f.download_to_memory(buf)
                buf.seek(0)
                raw = buf.read()
                if not raw:
                    logger.warning("copy_pack: empty download sticker %d attempt %d", i, global_attempt)
                    if is_src_custom_emoji:
                        use_file_id_only = True
                    else:
                        await asyncio.sleep(3)
                        continue
            except TelegramError as dl_err:
                retry_after = getattr(dl_err, "retry_after", None)
                err_s_dl = str(dl_err).lower()
                if retry_after:
                    wait = int(retry_after) + 1
                    logger.info("copy_pack: download RetryAfter %ds sticker %d", wait, i)
                    await asyncio.sleep(wait)
                    continue
                if "flood" in err_s_dl or "retry" in err_s_dl or "too many" in err_s_dl:
                    wait = min(30, 5 * global_attempt)
                    logger.info("copy_pack: download flood sticker %d, wait %ds", i, wait)
                    await asyncio.sleep(wait)
                    continue
                # Любая другая ошибка скачивания:
                if is_src_custom_emoji:
                    # custom_emoji нельзя скачать — сразу используем file_id
                    logger.info("copy_pack: download error custom_emoji %d → file_id directly: %s", i, dl_err)
                    use_file_id_only = True
                else:
                    wait = min(30, 5 * global_attempt)
                    logger.warning("copy_pack: download TG error sticker %d attempt %d: %s, wait %ds",
                                   i, global_attempt, dl_err, wait)
                    await asyncio.sleep(wait)
                    continue
            except Exception as dl_err:
                if is_src_custom_emoji:
                    logger.info("copy_pack: download exception custom_emoji %d → file_id directly: %s", i, dl_err)
                    use_file_id_only = True
                else:
                    wait = min(30, 5 * global_attempt)
                    logger.warning("copy_pack: download exception sticker %d attempt %d: %s, wait %ds",
                                   i, global_attempt, dl_err, wait)
                    await asyncio.sleep(wait)
                    continue

            # ── ШАГ 3: Конвертация ────────────────────────────────────────────
            if use_file_id_only:
                data       = actual_file_id
                fmt        = src_fmt
                push_type  = target_type
                push_is_ce = True
            else:
                try:
                    if target_type == "sticker":
                        if src_fmt == StickerFormat.STATIC:
                            data = process_image_for_sticker(raw, "sticker")
                            fmt  = StickerFormat.STATIC
                        elif src_fmt == StickerFormat.VIDEO:
                            result = convert_to_webm(raw, "sticker")
                            data   = result if result else raw
                            fmt    = StickerFormat.VIDEO
                        elif src_fmt == StickerFormat.ANIMATED:
                            data = raw
                            fmt  = StickerFormat.ANIMATED
                        else:
                            data = process_image_for_sticker(raw, "sticker")
                            fmt  = StickerFormat.STATIC
                        push_type  = "sticker"
                        push_is_ce = False
                    else:
                        if src_fmt == StickerFormat.STATIC:
                            data = process_image_for_sticker(raw, "emoji")
                            fmt  = StickerFormat.STATIC
                        elif src_fmt == StickerFormat.VIDEO:
                            result = convert_to_webm(raw, "emoji")
                            data   = result if result else raw
                            fmt    = StickerFormat.VIDEO
                        elif src_fmt == StickerFormat.ANIMATED:
                            data = raw
                            fmt  = StickerFormat.ANIMATED
                        else:
                            data = process_image_for_sticker(raw, "emoji")
                            fmt  = StickerFormat.STATIC
                        push_type  = "emoji"
                        push_is_ce = False
                except Exception as conv_err:
                    # Конвертация упала — fallback на raw без обработки
                    logger.warning("copy_pack: conversion failed sticker %d: %s, using raw", i, conv_err)
                    data       = raw
                    fmt        = src_fmt
                    push_type  = target_type
                    push_is_ce = False

            # ── ШАГ 4: Загружаем в Telegram ──────────────────────────────────
            err = await _push_sticker_to_tg(
                context.bot, user_id, target_pack_name, push_type,
                target_pack_title, data, fmt, clean_emoji, stickers_buf_dummy,
                is_custom_emoji=push_is_ce
            )

            if err is None:
                # Успех!
                added += 1
                await asyncio.sleep(0.5)
                break

            err_s = str(err)
            logger.warning("copy_pack: push sticker %d attempt %d error: %s", i, global_attempt, err_s)

            # Пак заполнен — останавливаемся
            if "STICKERS_TOO_MUCH" in err_s or "максимальное" in err_s:
                pack_full = True
                break

            # Ошибка эмодзи — заменяем на дефолтный и сразу повторяем (не ждём)
            if ("emoji" in err_s.lower() or "unicode" in err_s.lower()
                    or "неверный эмодзи" in err_s.lower() or "parse sticker" in err_s.lower()):
                logger.info("copy_pack: bad emoji sticker %d, replacing with 🙂", i)
                clean_emoji = ["🙂"]
                continue  # повторяем ШАГ 4 сразу с новым эмодзи, без ожидания

            # Лимит попыток — 15. После этого пропускаем стикер чтобы не зависнуть.
            if global_attempt >= 15:
                logger.error("copy_pack: giving up sticker %d after %d attempts, last error: %s",
                             i, global_attempt, err_s)
                errors += 1
                break

            # При любой другой ошибке — ждём и повторяем с шага 1
            if err_s.startswith("retry:"):
                try:
                    wait = int(err_s.split(":")[1]) + 1
                except Exception:
                    wait = 10
            elif getattr(err, "retry_after", None):
                wait = int(err.retry_after) + 1
            elif "flood" in err_s.lower() or "retry" in err_s.lower() or "временно" in err_s.lower():
                wait = min(60, 10 * global_attempt)
            elif "timeout" in err_s.lower() or "timed out" in err_s.lower():
                wait = 5
            else:
                wait = min(30, 5 * global_attempt)

            logger.info("copy_pack: waiting %ds before retry sticker %d", wait, i)
            await asyncio.sleep(wait)
            # НЕ сбрасываем use_file_id_only — если скачивание не работает,
            # продолжаем использовать file_id напрямую

        if pack_full:
            try:
                await status_msg.edit_text(
                    f"⚠️ Пак заполнен (максимум 120)!\n\nСкопировано: <b>{added}/{total}</b>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            break

    # ── Итог ─────────────────────────────────────────────────────────────────
    if added == 0:
        await status_msg.edit_text(
            "❌ Не удалось скопировать ни одного стикера."
        )
        return CHOOSE_TYPE

    url        = pack_url(target_pack_name)
    item_label = "эмодзи" if target_type == "emoji" else "стикеров"
    # Если pack_full — сообщаем сколько вошло до лимита
    if pack_full:
        result_text = (
            f"✅ <b>Готово!</b>\n\n"
            f"Скопировано: <b>{added}/{total}</b> {item_label}\n"
            f"⚠️ Пак заполнен (лимит 120)\n\n"
            f"<b>{target_pack_title}</b>"
        )
    else:
        result_text = (
            f"✅ <b>Готово!</b>\n\n"
            f"Скопировано: <b>{added}/{total}</b> {item_label}\n\n"
            f"<b>{target_pack_title}</b>"
        )

    kb = [[InlineKeyboardButton("🔗 Открыть пак", url=url)]]

    if creating_new:
        kb.append([InlineKeyboardButton("💾 Сохранить в мои паки", callback_data="copy_save_pack")])
        context.user_data["copy_result_pack_name"]  = target_pack_name
        context.user_data["copy_result_pack_title"] = target_pack_title
        context.user_data["copy_result_pack_type"]  = target_type
        context.user_data["copy_result_pack_count"] = added

    kb.append([InlineKeyboardButton("🏠 В меню", callback_data="begin")])

    await status_msg.edit_text(
        result_text, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
        disable_web_page_preview=True,
    )
    return COPY_PACK_TYPE


async def copy_as_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Копировать как стикер-пак."""
    query = update.callback_query
    await query.answer()
    context.user_data["_user_id_copy"] = update.effective_user.id
    context.user_data["copy_target_type"] = "sticker"
    await query.message.reply_text(
        "⏳ Начинаю копирование как стикер-пак..."
    )
    return await _do_copy_pack(update, context, target_type="sticker")


async def copy_as_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Копировать как эмодзи-пак."""
    query = update.callback_query
    await query.answer()
    context.user_data["_user_id_copy"] = update.effective_user.id
    context.user_data["copy_target_type"] = "emoji"
    await query.message.reply_text(
        "⏳ Начинаю копирование как эмодзи-пак..."
    )
    return await _do_copy_pack(update, context, target_type="emoji")


async def copy_add_to_existing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает список паков пользователя для добавления стикеров."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    context.user_data["_user_id_copy"] = user_id
    packs = get_packs(context, user_id)

    if not packs:
        await query.edit_message_text(
            "У тебя пока нет созданных паков.\n\n"
            "Сначала создай пак, а потом копируй в него стикеры.\n\n"
            "/cancel — отмена",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="copy_pack")]]),
        )
        return COPY_PACK_TYPE

    kb = []
    for i, p in enumerate(packs):
        if p.get("plate_pack"):
            continue  # паки с номерами не показываем
        if p["type"] == "sticker":
            icon = "🖼"
        elif p.get("adaptive"):
            icon = "🖼️"
        else:
            icon = "✨"
        n = p.get("count", 0)
        kb.append([InlineKeyboardButton(
            f"{icon} {p['title']} ({n} {plural_sticker(n)})",
            callback_data=f"copy_into_pack_{i}",
        )])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="copy_pack")])

    if len(kb) == 1:  # только кнопка "Назад" — все паки номерные
        await query.edit_message_text(
            "У тебя нет подходящих паков для добавления стикеров.\n\n"
            "Паки с номерными знаками не поддерживаются.\n\n"
            "/cancel — отмена",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="copy_pack")]]),
        )
        return COPY_PACK_TYPE

    await query.edit_message_text(
        "➕ <b>Выберите пак для добавления стикеров:</b>\n\n"
        "Все стикеры из скопированного набора будут добавлены в выбранный пак.\n\n"
        "/cancel — отмена",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return COPY_PACK_SELECT


async def copy_into_pack_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пользователь выбрал пак для добавления."""
    query = update.callback_query
    await query.answer()
    idx = int(query.data.replace("copy_into_pack_", ""))
    user_id = update.effective_user.id
    packs = get_packs(context, user_id)

    if idx >= len(packs):
        await query.edit_message_text("❌ Пак не найден.")
        return CHOOSE_TYPE

    pack = packs[idx]
    target_name  = pack["name"]
    target_title = pack["title"]
    raw_type     = pack.get("type", "sticker")
    # emoji_adaptive → emoji для копирования; всё что не emoji → sticker
    if raw_type in ("emoji", "emoji_adaptive"):
        target_type = "emoji"
    else:
        target_type = "sticker"

    logger.info("copy_into_pack_selected: idx=%d name=%s type=%s→%s",
                idx, target_name, raw_type, target_type)

    await query.message.reply_text(
        f"⏳ Добавляю стикеры в пак <b>{target_title}</b>...",
        parse_mode="HTML",
    )
    return await _do_copy_pack(
        update, context,
        target_type=target_type,
        target_pack_name=target_name,
        target_pack_title=target_title,
    )


async def copy_save_pack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохранить скопированный пак в список паков пользователя."""
    query = update.callback_query
    await query.answer()
    user_id     = update.effective_user.id
    pack_name   = context.user_data.get("copy_result_pack_name")
    pack_title  = context.user_data.get("copy_result_pack_title", "Копия")
    pack_type   = context.user_data.get("copy_result_pack_type", "sticker")
    pack_count  = context.user_data.get("copy_result_pack_count", 0)

    if not pack_name:
        await query.answer("Нет данных для сохранения", show_alert=True)
        return COPY_PACK_TYPE

    packs = get_packs(context, user_id)
    # Проверяем дубликат
    if any(p.get("name") == pack_name for p in packs):
        await query.answer("Пак уже сохранён!", show_alert=True)
        return COPY_PACK_TYPE

    packs.append({
        "name":  pack_name,
        "title": pack_title,
        "type":  pack_type,
        "count": pack_count,
    })
    save_packs(context, user_id)

    # Обновляем статистику
    if pack_type == "sticker":
        DB.increment_stat("sticker_packs_created")
    else:
        DB.increment_stat("emoji_packs_created")

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(
        f"✅ Пак <b>{pack_title}</b> сохранён в твои паки!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 В меню", callback_data="begin")],
            [InlineKeyboardButton("📋 Мои паки", callback_data="list_packs")],
        ]),
    )
    return COPY_PACK_TYPE


# ── Повторный вход в copy_pack из меню ──────────────────────────────────────
async def copy_pack_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Повторный нажим 'Скопировать пак' — возвращает к вводу ссылки."""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("copy_pack_source_name", None)
    context.user_data.pop("copy_pack_source_type", None)
    context.user_data.pop("copy_pack_source_title", None)
    # Ставим флаг чтобы /cancel знал что мы в процессе копирования
    context.user_data["_user_id_copy"] = update.effective_user.id
    await query.edit_message_text(
        "📋 <b>Копирование пака</b>\n\n"
        "Отправьте мне ссылку на набор, который хотите скопировать\n\n"
        "/cancel — для отмены",
        parse_mode="HTML",
    )
    return COPY_PACK_LINK


# ── Сборка ────────────────────────────────────────────────────────────────────

def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Переменная окружения BOT_TOKEN не задана!")

    # Инициализируем БД (создаём таблицу если нет)
    DB.init()

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start",  start),
            CommandHandler("menu",   menu_command),
            CommandHandler("cancel", cancel_command),
            CommandHandler("packs",  packs_command),
            CallbackQueryHandler(begin,             pattern="^begin$"),
            CallbackQueryHandler(lambda u,c: u.callback_query.answer(), pattern="^noop_ps$"),
            CallbackQueryHandler(copy_pack_restart, pattern="^copy_pack$"),
        ],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(begin,          pattern="^begin$"),
                CallbackQueryHandler(list_packs,     pattern="^list_packs$"),
                CallbackQueryHandler(start_begin_cb, pattern="^start_begin$"),
                CallbackQueryHandler(start_packs_cb, pattern="^start_packs$"),
                CallbackQueryHandler(select_pack,    pattern="^select_pack_\\d+$"),
                CallbackQueryHandler(mgmt_rename,    pattern="^mgmt_rename$"),
                CallbackQueryHandler(mgmt_add,       pattern="^mgmt_add$"),
                CallbackQueryHandler(mgmt_delete,    pattern="^mgmt_delete$"),
                CallbackQueryHandler(mgmt_icon,      pattern="^mgmt_icon$"),
                CallbackQueryHandler(mgmt_delete_all,pattern="^mgmt_delete_all$"),
                CallbackQueryHandler(confirm_delete_pack, pattern="^confirm_delete_pack_\\d+$"),
                CallbackQueryHandler(copy_pack_restart, pattern="^copy_pack$"),
                CallbackQueryHandler(paint_start,    pattern="^paint_start$"),
            ],
            CHOOSE_TYPE: [
                CallbackQueryHandler(begin,                pattern="^begin$"),
                CallbackQueryHandler(start_create,         pattern="^create_(sticker|emoji)$"),
                CallbackQueryHandler(list_packs,           pattern="^list_packs$"),
                CallbackQueryHandler(start_packs_cb,       pattern="^start_packs$"),
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
                CallbackQueryHandler(copy_pack_restart,    pattern="^copy_pack$"),
                CallbackQueryHandler(paint_start,          pattern="^paint_start$"),
            ],
            PACK_NAME: [
                CallbackQueryHandler(set_adaptive, pattern="^set_adaptive$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_pack_name),
            ],
            PACK_LINK: [
                CallbackQueryHandler(use_random_link, pattern="^random_link$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_pack_link),
            ],
            ADDING_STICKER: [
                CallbackQueryHandler(save_pack, pattern="^save_pack$"),
                CallbackQueryHandler(add_more,  pattern="^add_more$"),
                MessageHandler(
                    filters.PHOTO | filters.VIDEO | filters.Sticker.ALL |
                    filters.ANIMATION | filters.Document.ALL,
                    receive_sticker_file,
                ),
                # Текст с premium emoji entities → обрабатываем как файл стикера
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & filters.Entity("custom_emoji"),
                    receive_sticker_file,
                ),
                # Текст только если ждём эмодзи после альбома
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_album_emoji),
            ],
            WAITING_EMOJI: [
                CallbackQueryHandler(fire_all_confirm_new, pattern="^fire_all_confirm_new$"),
                CallbackQueryHandler(fire_all_yes_new,     pattern="^fire_all_yes_new$"),
                CallbackQueryHandler(fire_all_no_new,      pattern="^fire_all_no_new$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_emoji_for_new),
            ],
            PACK_SELECTED: [
                CallbackQueryHandler(begin,                  pattern="^begin$"),
                CallbackQueryHandler(list_packs,             pattern="^list_packs$"),
                CallbackQueryHandler(select_pack,            pattern="^select_pack_\\d+$"),
                CallbackQueryHandler(mgmt_rename,            pattern="^mgmt_rename$"),
                CallbackQueryHandler(mgmt_add,               pattern="^mgmt_add$"),
                CallbackQueryHandler(mgmt_delete,            pattern="^mgmt_delete$"),
                CallbackQueryHandler(mgmt_icon,              pattern="^mgmt_icon$"),
                CallbackQueryHandler(mgmt_delete_all,        pattern="^mgmt_delete_all$"),
                CallbackQueryHandler(confirm_delete_pack,    pattern="^confirm_delete_pack_\\d+$"),
                CallbackQueryHandler(paint_from_pack,        pattern="^paint_from_pack:.+$"),
            ],
            RENAMING_PACK: [
                CallbackQueryHandler(select_pack, pattern="^select_pack_\\d+$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_rename),
            ],
            ADD_STICKER_FILE: [
                CallbackQueryHandler(select_pack,              pattern="^select_pack_\\d+$"),
                CallbackQueryHandler(fire_all_confirm_mgmt,    pattern="^fire_all_confirm_mgmt$"),
                CallbackQueryHandler(fire_all_yes_mgmt,        pattern="^fire_all_yes_mgmt$"),
                CallbackQueryHandler(fire_all_no_mgmt,         pattern="^fire_all_no_mgmt$"),
                MessageHandler(
                    filters.PHOTO | filters.VIDEO | filters.Sticker.ALL |
                    filters.ANIMATION | filters.Document.ALL,
                    receive_add_file,
                ),
                # Текст с premium emoji entities → обрабатываем как файл
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & filters.Entity("custom_emoji"),
                    receive_add_file,
                ),
            ],
            ADD_STICKER_EMOJI: [
                CallbackQueryHandler(fire_all_confirm_mgmt, pattern="^fire_all_confirm_mgmt$"),
                CallbackQueryHandler(fire_all_yes_mgmt,     pattern="^fire_all_yes_mgmt$"),
                CallbackQueryHandler(fire_all_no_mgmt,      pattern="^fire_all_no_mgmt$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_add_emoji),
            ],
            DELETE_STICKER: [
                CallbackQueryHandler(begin,       pattern="^begin$"),
                CallbackQueryHandler(list_packs,  pattern="^list_packs$"),
                CallbackQueryHandler(select_pack, pattern="^select_pack_\\d+$"),
                CallbackQueryHandler(mgmt_delete, pattern="^mgmt_delete$"),
                MessageHandler(filters.Sticker.ALL, confirm_delete_sticker),
            ],
            CHANGE_ICON: [
                CallbackQueryHandler(begin,       pattern="^begin$"),
                CallbackQueryHandler(list_packs,  pattern="^list_packs$"),
                CallbackQueryHandler(select_pack, pattern="^select_pack_\\d+$"),
                MessageHandler(filters.Sticker.ALL, receive_icon),
            ],
            # ── Покраска стикеров ─────────────────────────────────────────────
            PAINT_SCOPE: [
                CallbackQueryHandler(begin,               pattern="^begin$"),
                CallbackQueryHandler(paint_save_pack,     pattern="^paint_save_pack$"),
                CallbackQueryHandler(paint_scope_cb,      pattern="^paint_scope:(single|pack)$"),
                CallbackQueryHandler(paint_color_cb,      pattern="^paint_clr:.+$"),
                MessageHandler(filters.Sticker.ALL,       paint_receive_sticker),
                # Премиум-эмодзи (custom_emoji entity) — до обычного TEXT чтобы перехватить первым
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & filters.Entity("custom_emoji"),
                    paint_receive_custom_emoji,
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, paint_receive_link),
            ],
            PAINT_COLOR: [
                CallbackQueryHandler(begin,               pattern="^begin$"),
                CallbackQueryHandler(paint_color_cb,      pattern="^paint_clr:.+$"),
                CallbackQueryHandler(paint_from_pack,     pattern="^paint_from_pack:.+$"),
            ],
            PAINT_HEX: [
                CallbackQueryHandler(begin,               pattern="^begin$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, paint_hex_input),
            ],
            # ── Номерные знаки ──────────────────────────────────────────────
            PLATE_COUNTRY: [
                CallbackQueryHandler(begin,                pattern="^begin$"),
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
                CallbackQueryHandler(plate_select_country, pattern="^pc_(RU|UA|BY|KZ)$"),
            ],
            PLATE_REGION: [
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
                CallbackQueryHandler(plate_select_country, pattern="^pc_(RU|UA|BY|KZ)$"),
                CallbackQueryHandler(plate_region_page,    pattern="^prp_(RU|UA|BY|KZ)_\\d+$"),
                CallbackQueryHandler(plate_select_region,  pattern="^pr_(RU|UA|BY|KZ)_.+$"),
                CallbackQueryHandler(plate_noop,           pattern="^noop$"),
            ],
            PLATE_INPUT: [
                CallbackQueryHandler(plate_buy_sticker,    pattern="^plate_buy_sticker$"),
                CallbackQueryHandler(plate_buy_emoji,      pattern="^plate_buy_emoji$"),
                CallbackQueryHandler(plate_save_sticker,   pattern="^plate_save_sticker$"),
                CallbackQueryHandler(plate_save_emoji,     pattern="^plate_save_emoji$"),
                CallbackQueryHandler(plate_btn_menu,       pattern="^plate_menu$"),
                CallbackQueryHandler(plate_btn_other,      pattern="^plate_other_(RU|UA|BY|KZ)$"),
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
                CallbackQueryHandler(plate_select_country, pattern="^pc_(RU|UA|BY|KZ)$"),
                CallbackQueryHandler(begin,                pattern="^begin$"),
                MessageHandler(filters.SUCCESSFUL_PAYMENT, plate_successful_payment),
                MessageHandler(filters.TEXT & ~filters.COMMAND, plate_receive_input),
            ],
            PLATE_PACK_NAME: [
                MessageHandler(filters.SUCCESSFUL_PAYMENT, plate_successful_payment),
                MessageHandler(filters.TEXT & ~filters.COMMAND, plate_pack_receive_name),
            ],
            PLATE_PACK_LINK: [
                CallbackQueryHandler(plate_pack_random_link, pattern="^plate_random_link$"),
                MessageHandler(filters.SUCCESSFUL_PAYMENT, plate_successful_payment),
                MessageHandler(filters.TEXT & ~filters.COMMAND, plate_pack_receive_link),
            ],
            # ── Копирование пака ──────────────────────────────────────────────
            COPY_PACK_LINK: [
                CommandHandler("cancel", cancel_command),
                CallbackQueryHandler(begin,             pattern="^begin$"),
                CallbackQueryHandler(copy_pack_restart, pattern="^copy_pack$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, copy_pack_receive_link),
            ],
            COPY_PACK_TYPE: [
                CommandHandler("cancel", cancel_command),
                CallbackQueryHandler(begin,                  pattern="^begin$"),
                CallbackQueryHandler(copy_as_sticker,        pattern="^copy_as_sticker$"),
                CallbackQueryHandler(copy_as_emoji,          pattern="^copy_as_emoji$"),
                CallbackQueryHandler(copy_add_to_existing,   pattern="^copy_add_to_existing$"),
                CallbackQueryHandler(copy_save_pack,         pattern="^copy_save_pack$"),
                CallbackQueryHandler(copy_pack_restart,      pattern="^copy_pack$"),
                CallbackQueryHandler(list_packs,             pattern="^list_packs$"),
            ],
            COPY_PACK_SELECT: [
                CallbackQueryHandler(begin,                      pattern="^begin$"),
                CommandHandler("cancel", cancel_command),
                CallbackQueryHandler(copy_pack_restart,          pattern="^copy_pack$"),
                CallbackQueryHandler(copy_into_pack_selected,    pattern="^copy_into_pack_\\d+$"),
            ],
            # ── Админ-панель ────────────────────────────────────────────────
            ADMIN_MENU: [
                CallbackQueryHandler(adm_users,            pattern="^adm_users$"),
                CallbackQueryHandler(adm_stats,            pattern="^adm_stats$"),
                CallbackQueryHandler(adm_stats_page,       pattern="^adm_stats_p_\\d+$"),
                CallbackQueryHandler(adm_daily,            pattern="^adm_daily$"),
                CallbackQueryHandler(adm_broadcast_start,  pattern="^adm_broadcast$"),
                CallbackQueryHandler(adm_schedule_start,   pattern="^adm_schedule$"),
                CallbackQueryHandler(adm_prices,           pattern="^adm_prices$"),
                CallbackQueryHandler(adm_price_select,     pattern="^adm_setprice_.+$"),
                CallbackQueryHandler(adm_maintenance,      pattern="^adm_maintenance$"),
                CallbackQueryHandler(adm_back,             pattern="^adm_back$"),
            ],
            ADMIN_BROADCAST: [
                CallbackQueryHandler(adm_back, pattern="^adm_back$"),
                MessageHandler(
                    (filters.TEXT | filters.PHOTO) & ~filters.COMMAND,
                    adm_broadcast_receive,
                ),
            ],
            ADMIN_SCHEDULED_MSG: [
                CallbackQueryHandler(adm_back, pattern="^adm_back$"),
                MessageHandler(
                    (filters.TEXT | filters.PHOTO) & ~filters.COMMAND,
                    adm_schedule_receive_msg,
                ),
            ],
            ADMIN_SCHEDULED_TIME: [
                CallbackQueryHandler(adm_back, pattern="^adm_back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, adm_schedule_receive_time),
            ],
            ADMIN_SET_PRICE: [
                CallbackQueryHandler(adm_back, pattern="^adm_back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, adm_price_receive),
            ],
        },
        fallbacks=[
            CommandHandler("start",  start),
            CommandHandler("menu",   menu_command),
            CommandHandler("cancel", cancel_command),
            CommandHandler("packs",  packs_command),
            CommandHandler("admin",  admin_command),
            CallbackQueryHandler(begin,             pattern="^begin$"),
            CallbackQueryHandler(start_packs_cb,    pattern="^start_packs$"),
            CallbackQueryHandler(start_begin_cb,    pattern="^start_begin$"),
            CallbackQueryHandler(adm_back,          pattern="^adm_back$"),
            CallbackQueryHandler(copy_pack_restart, pattern="^copy_pack$"),
        ],
        allow_reentry=True,
        per_message=False,
    )

    async def post_init(application):
        admin_user = None
        try:
            # Получаем chat_id админа чтобы установить команды только ему
            # (Telegram не поддерживает per-user commands по username, только по scope)
            # Устанавливаем глобальные команды (без /admin)
            await application.bot.set_my_commands([
                ("start",  "👤 Я"),
                ("menu",   "🏠 Главное меню"),
                ("packs",  "📋 Паки"),
                ("cancel", "❌ Отменить действие"),
            ])
        except Exception as e:
            logger.warning("set_my_commands failed: %s", e)

    app.post_init = post_init
    app.add_handler(conv)

    # Обработчик тех. перерыва — перехватывает ВСЕ обновления кроме /admin
    MAINTENANCE_TEXT = (
        '<tg-emoji emoji-id="5345906554510012647">🔧</tg-emoji> '
        "В данный момент проводятся технические работы. Пожалуйста, попробуйте позже."
    )

    async def _maintenance_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_maintenance() or is_admin(update):
            return  # не блокируем
        # Запоминаем пользователя — оповестим когда перерыв закончится
        if update.effective_user:
            DB.add_maintenance_waiter(update.effective_user.id)
        if update.callback_query:
            await update.callback_query.answer(
                "🔧 Технические работы. Попробуйте позже.", show_alert=True
            )
        elif update.effective_message:
            await update.effective_message.reply_text(
                MAINTENANCE_TEXT, parse_mode="HTML"
            )
        raise ApplicationHandlerStop  # останавливает передачу в другие хендлеры

    from telegram.ext import filters as _f
    # Перехватываем команды (кроме /admin)
    app.add_handler(MessageHandler(
        _f.COMMAND & ~_f.Regex(r"^/admin"),
        _maintenance_block,
    ), group=-1)
    # Перехватываем любые текстовые/медиа сообщения
    app.add_handler(MessageHandler(
        ~_f.COMMAND,
        _maintenance_block,
    ), group=-1)
    # Перехватываем нажатия на inline-кнопки
    app.add_handler(CallbackQueryHandler(_maintenance_block), group=-1)

    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("menu",   menu_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("packs",  packs_command))
    app.add_handler(CommandHandler("admin",  admin_command))
    app.add_handler(CallbackQueryHandler(begin,             pattern="^begin$"))
    app.add_handler(CallbackQueryHandler(start_begin_cb,    pattern="^start_begin$"))
    app.add_handler(CallbackQueryHandler(copy_pack_restart, pattern="^copy_pack$"))
    app.add_handler(PreCheckoutQueryHandler(plate_pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, plate_successful_payment))

    # Job: проверка отложенных рассылок каждую минуту
    app.job_queue.run_repeating(check_scheduled_broadcasts, interval=60, first=10)

    logger.info("Бот запущен ✅")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
