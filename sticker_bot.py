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


# ── Состояния ─────────────────────────────────────────────────────────────────
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
) = range(18)

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


def pack_url(pack_name: str) -> str:
    return f"https://t.me/addstickers/{pack_name}"


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
    if "STICKER_EMOJI_INVALID" in e or "emoji" in e.lower():
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
    ]
    text = (
        "👋 <b>Что хочешь создать?</b>\n\n"
        "• <b>Стикер-пак</b> — обычные стикеры\n"
        "• <b>Эмодзи-пак</b> — premium emoji\n"
        "• <b>Номерной знак</b> — красивое фото номера"
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

    text = (
        f"👤 <b>Профиль {name}</b>\n\n"
        f"<blockquote>"
        f"🛍️ Покупок: <b>{purchases}</b>\n"
        f"⭐ Потрачено: <b>{stars_spent}</b>\n"
        f"📅 Регистрация: <b>{reg_str}</b>"
        f"</blockquote>"
    )

    kb = [
        [InlineKeyboardButton("Начать ▶️", callback_data="start_begin"),
         InlineKeyboardButton("Мои паки 📁", callback_data="start_packs")],
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
    ]
    text = (
        "👋 <b>Что хочешь создать?</b>\n\n"
        "• <b>Стикер-пак</b> — обычные стикеры\n"
        "• <b>Эмодзи-пак</b> — premium emoji\n"
        "• <b>Номерной знак</b> — красивое фото номера"
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
            icon = "🎨" if p["type"] == "sticker" else "📷"
        elif p["type"] == "sticker":
            icon = "🖼"
        elif p.get("adaptive"):
            icon = "🎨"
        else:
            icon = "✨"
        n = p.get("count", 0)
        kb.append([InlineKeyboardButton(
            f"{icon} {p['title']} ({n} {plural_sticker(n)})",
            callback_data=f"select_pack_{i}",
        )])
    await query.message.reply_text(
        "📋 <b>Твои паки:</b>\n\nВыбери пак для управления.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    for key in (
        "new_pack_title", "new_pack_suffix", "new_pack_stickers",
        "creating_type", "selected_pack_index", "pending_data", "pending_fmt",
        "plate_country", "plate_region", "file_queue", "file_queue_done",
        "fire_emoji_confirmed_new", "fire_btn_shown_new",
        "fire_emoji_confirmed_mgmt", "fire_btn_shown_mgmt",
    ):
        context.user_data.pop(key, None)
    return await send_main_menu(update, context)


async def getid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отвечает file_id видео/фото/стикера — для настройки PROFILE_VIDEO_FILE_ID."""
    msg = update.message
    if msg.video:
        fid = msg.video.file_id
        await msg.reply_text("\U0001f3ac Video file_id:\n<code>" + fid + "</code>", parse_mode="HTML")
    elif msg.animation:
        fid = msg.animation.file_id
        await msg.reply_text("\U0001f39e Animation file_id:\n<code>" + fid + "</code>", parse_mode="HTML")
    elif msg.sticker:
        fid = msg.sticker.file_id
        await msg.reply_text("\U0001f5bc Sticker file_id:\n<code>" + fid + "</code>", parse_mode="HTML")
    elif msg.photo:
        fid = msg.photo[-1].file_id
        await msg.reply_text("\U0001f4f7 Photo file_id:\n<code>" + fid + "</code>", parse_mode="HTML")
    elif msg.document:
        fid = msg.document.file_id
        await msg.reply_text("\U0001f4c4 Document file_id:\n<code>" + fid + "</code>", parse_mode="HTML")
    else:
        await msg.reply_text("\u0421начала отправь видео боту, потом напиши /getid", parse_mode="HTML")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await menu_command(update, context)
    return CHOOSE_TYPE


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
            icon = "🎨" if p["type"] == "sticker" else "📷"
        elif p["type"] == "sticker":
            icon = "🖼"
        elif p.get("adaptive"):
            icon = "🎨"
        else:
            icon = "✨"
        n    = p.get("count", 0)
        kb.append([InlineKeyboardButton(
            f"{icon} {p['title']} ({n} {plural_sticker(n)})",
            callback_data=f"select_pack_{i}",
        )])
    kb.append(back_btn())
    await query.edit_message_text(
        "📋 <b>Твои паки:</b>\n\nВыбери пак для управления.",
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
        icon = "🎨" if pack["type"] == "sticker" else "📷"
    elif pack["type"] == "sticker":
        icon = "🖼"
    elif is_adaptive:
        icon = "🎨"
    else:
        icon = "✨"
    n   = pack.get("count", 0)
    url = pack_url(pack["name"])
    is_plate     = bool(pack.get("plate_pack"))
    add_label    = "➕ Добавить эмодзи"  if is_emoji else "➕ Добавить стикер"
    delete_label = "🗑 Удалить эмодзи"   if is_emoji else "🗑 Удалить стикер"
    item_word    = "эмодзи" if is_emoji else "стикеров"
    paint_label  = "🎨 Покрасить эмодзи" if is_emoji else "🎨 Покрасить стикер"
    kb = [
        [InlineKeyboardButton("🔗 Открыть пак",        url=url)],
        [InlineKeyboardButton("✏️ Переименовать пак",   callback_data="mgmt_rename")],
        *([[InlineKeyboardButton(add_label,              callback_data="mgmt_add")]] if not is_plate else []),
        [InlineKeyboardButton(delete_label,             callback_data="mgmt_delete")],
        [InlineKeyboardButton("🖼 Сменить иконку пака", callback_data="mgmt_icon")],
        *([[InlineKeyboardButton(paint_label,            url="https://t.me/Meganaytbot?start=start")]] if not is_plate else []),
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
                # Пак с таким именем уже есть — добавляем в него
                if ("already occupied" in err_str.lower()
                        or "name is already" in err_str.lower()
                        or "STICKERSET_INVALID" in err_str):
                    logger.info("Pack exists, adding sticker: %s", pack_name)
                    await _add_to_set(make_sticker())
                    logger.info("Sticker ADDED (fallback): %s", pack_name)
                    return None
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
                # Пак не существует — создаём заново
                if "STICKERSET_INVALID" in err_str or "not found" in err_str.lower():
                    logger.info("Pack not found, recreating: %s", pack_name)
                    await _create_set(make_sticker())
                    logger.info("Pack RECREATED: %s", pack_name)
                    return None
                return _friendly_tg_error(err_str)

    except TelegramError as e:
        err_str = str(e)
        logger.error("_push_sticker_to_tg TelegramError: %s", err_str)
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
    pack = packs[idx]
    is_emoji = pack["type"] in ("emoji", "emoji_adaptive")
    item_label = "эмодзи" if is_emoji else "стикер"

    if not msg.sticker:
        await msg.reply_text(
            f"❌ Нужен {item_label}. Отправь его сюда.",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
        )
        return CHANGE_ICON

    s        = msg.sticker
    icon_fmt = s.format if hasattr(s, "format") else StickerFormat.STATIC

    try:
        await context.bot.set_sticker_set_thumbnail(
            name=pack["name"],
            user_id=user_id,
            thumbnail=s.file_id,
            format=icon_fmt,
        )
        logger.info("Icon set for pack %s via file_id", pack["name"])
    except TelegramError as e:
        err = _friendly_tg_error(str(e))
        await msg.reply_text(
            f"❌ Не удалось установить иконку: {err}",
            reply_markup=InlineKeyboardMarkup([back_btn("◀️ Назад", f"select_pack_{idx}")]),
        )
        return CHANGE_ICON
    except Exception as e:
        await msg.reply_text(f"❌ Ошибка: {e}")
        return CHANGE_ICON

    await msg.reply_text(
        "✅ Иконка пака обновлена!",
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
            [InlineKeyboardButton("🎨 В стикер-пак ⭐️1",  callback_data="plate_buy_sticker"),
             InlineKeyboardButton("📷 В эмодзи-пак ⭐️1",   callback_data="plate_buy_emoji")],
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
    """Запускает покупку за 1 звезду для сохранения номера в стикер-пак."""
    query = update.callback_query
    await query.answer()
    context.user_data["plate_pending_pack_type"] = "sticker"
    await context.bot.send_invoice(
        chat_id=query.message.chat_id,
        title="В стикер-пак",
        description="Добавить номерной знак в стикер-пак",
        payload="plate_save_sticker",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice("В стикер-пак", 1)],
    )
    return PLATE_INPUT


async def plate_buy_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запускает покупку за 1 звезду для сохранения номера в эмодзи-пак."""
    query = update.callback_query
    await query.answer()
    context.user_data["plate_pending_pack_type"] = "emoji"
    await context.bot.send_invoice(
        chat_id=query.message.chat_id,
        title="В эмодзи-пак",
        description="Добавить номерной знак в эмодзи-пак",
        payload="plate_save_emoji",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice("В эмодзи-пак", 1)],
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
            url  = pack_url(pack["name"])
            icon = "🎨" if pack_type == "sticker" else "📷"
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
    icon = "🎨" if pack_type == "sticker" else "📷"
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
            icon = "🎨" if pack_type == "sticker" else "📷"
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
    icon = "🎨" if pack_type == "sticker" else "📷"
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
    icon      = "🎨" if pack_type == "sticker" else "📷"

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
        [InlineKeyboardButton("📋  Мои паки",              callback_data="list_packs")],
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
            CallbackQueryHandler(begin, pattern="^begin$"),
        ],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(begin,          pattern="^begin$"),
                CallbackQueryHandler(list_packs,     pattern="^list_packs$"),
                CallbackQueryHandler(start_begin_cb, pattern="^start_begin$"),
                CallbackQueryHandler(start_packs_cb, pattern="^start_packs$"),
            ],
            CHOOSE_TYPE: [
                CallbackQueryHandler(begin,                pattern="^begin$"),
                CallbackQueryHandler(start_create,         pattern="^create_(sticker|emoji)$"),
                CallbackQueryHandler(list_packs,           pattern="^list_packs$"),
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
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
        },
        fallbacks=[
            CommandHandler("start",  start),
            CommandHandler("menu",   menu_command),
            CommandHandler("cancel", cancel_command),
            CallbackQueryHandler(begin, pattern="^begin$"),
        ],
        allow_reentry=True,
        per_message=False,
    )

    async def post_init(application):
        await application.bot.set_my_commands([
            ("start",  "👤 Я"),
            ("menu",   "🏠 Главное меню"),
            ("cancel", "❌ Отменить текущее действие"),
        ])

    app.post_init = post_init
    app.add_handler(conv)
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("menu",   menu_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("getid", getid_command))
    app.add_handler(CallbackQueryHandler(begin,          pattern="^begin$"))
    app.add_handler(CallbackQueryHandler(start_begin_cb, pattern="^start_begin$"))
    app.add_handler(CallbackQueryHandler(start_packs_cb, pattern="^start_packs$"))
    app.add_handler(PreCheckoutQueryHandler(plate_pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, plate_successful_payment))
    logger.info("Бот запущен ✅")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
