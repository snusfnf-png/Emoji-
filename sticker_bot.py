"""
Telegram Sticker / Emoji Pack Bot  +  Номерные знаки
Токен берётся из переменной окружения BOT_TOKEN.
"""

import io
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
    filters,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

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
) = range(14)

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
    th = fh // 3
    draw.rectangle([fx, fy, fx + fw, fy + th], fill="#CF101A")
    draw.rectangle([fx, fy + th, fx + fw, fy + th * 2], fill="#009A44")
    draw.rectangle([fx, fy + th * 2, fx + fw, fy + fh], fill="#CF101A")


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
    fnt_ftr = ImageFont.truetype(FONT_REG, 12)
    draw.text((W // 2, 22), "HOMEPA  —  CARDROP", fill="#aaaaaa", font=fnt_hdr, anchor="mm")

    cx, cy = W // 2, H // 2

    if country == "RU":
        pw, ph = 365, 118
        px, py = cx - pw // 2, cy - ph // 2
        # Shadow
        draw.rounded_rectangle([px + 5, py + 5, px + pw + 5, py + ph + 5], radius=10, fill="#b8b8b8")
        # Plate body
        draw.rounded_rectangle([px, py, px + pw, py + ph], radius=10, fill="white",
                                outline="#111111", width=4)
        # Vertical divider
        right_w = 92
        rdx = px + pw - right_w
        draw.line([(rdx, py + 6), (rdx, py + ph - 6)], fill="#111111", width=3)
        # Main characters (left side)
        fnt_pl = ImageFont.truetype(FONT_BOLD, 66)
        draw.text((px + (pw - right_w) // 2, py + ph // 2), chars,
                  fill="#111111", font=fnt_pl, anchor="mm")
        # Right panel center x
        rcx = rdx + right_w // 2
        # Right panel top/bottom inner bounds (inside border)
        rpt = py + 6
        rpb = py + ph - 6
        rph = rpb - rpt  # usable height of right panel
        # 1. Region code — centered in top 65% of right panel
        region_cy = rpt + int(rph * 0.42)
        fnt_r = ImageFont.truetype(FONT_BOLD, 44)
        draw.text((rcx, region_cy), region, fill="#111111", font=fnt_r, anchor="mm")
        # 2. RUS then flag — centered in bottom portion
        fw, fh = 20, 14
        fnt_rus = ImageFont.truetype(FONT_BOLD, 12)
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
        # Украинский номер: белый фон, синяя полоса слева с флагом UA вверху и "UA" внизу,
        # справа — регион + номер в формате "АА 1234 ВВ"
        pw, ph = 420, 120
        px, py = cx - pw // 2, cy - ph // 2
        # Тень
        draw.rounded_rectangle([px+4, py+4, px+pw+4, py+ph+4], radius=8, fill="#b0b0b0")
        # Белый корпус
        draw.rounded_rectangle([px, py, px+pw, py+ph], radius=8, fill="white",
                                outline="#111111", width=4)
        # Синяя полоса слева
        strip_w = 58
        draw.rounded_rectangle([px, py, px+strip_w, py+ph], radius=8, fill="#003DA5")
        # Перекрыть правый скруглённый край полосы прямоугольником
        draw.rectangle([px+strip_w//2, py+4, px+strip_w, py+ph-4], fill="#003DA5")
        # Вертикальная черта
        draw.line([(px+strip_w, py+4), (px+strip_w, py+ph-4)], fill="#111111", width=3)
        # Флаг Украины внутри синей полосы (верхняя часть)
        flag_w, flag_h = 34, 22
        flag_x = px + (strip_w - flag_w) // 2
        flag_y = py + 14
        _ua_flag(draw, flag_x, flag_y, fw=flag_w, fh=flag_h)
        # Надпись "UA" внизу синей полосы
        fnt_ua = ImageFont.truetype(FONT_BOLD, 16)
        draw.text((px + strip_w//2, py + ph - 18), "UA", fill="white", font=fnt_ua, anchor="mm")
        # Основной текст: разбиваем chars — первые 2 буквы, 4 цифры, 2 буквы
        # Формат UA: АА 1234 ВВ (регион уже включён в chars, или separate)
        # chars приходит как "АА1234ВВ" → форматируем с пробелами
        c = chars.replace(" ","")
        if len(c) == 8:
            body = f"{c[:2]} {c[2:6]} {c[6:]}"
        elif len(c) == 4 and region:
            body = f"{region} {c}"
        else:
            body = chars
        fnt_pl = ImageFont.truetype(FONT_BOLD, 62)
        content_cx = px + strip_w + (pw - strip_w) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_pl, anchor="mm")

    elif country == "BY":
        # Белорусский номер: белый фон, левая полоса с настоящим флагом BY + надпись "BY",
        # номер в формате "1234 АХ-3"
        pw, ph = 440, 120
        px, py = cx - pw // 2, cy - ph // 2
        # Тень
        draw.rounded_rectangle([px+4, py+4, px+pw+4, py+ph+4], radius=8, fill="#b0b0b0")
        # Белый корпус с чёрной рамкой
        draw.rounded_rectangle([px, py, px+pw, py+ph], radius=8, fill="white",
                                outline="#111111", width=5)
        # Левая полоса с флагом BY
        strip_w = 62
        # Рисуем настоящий флаг Беларуси: красная (2/3) + зелёная (1/3) полосы
        flag_x = px + 5
        flag_y = py + 8
        flag_w2 = strip_w - 12
        flag_h2 = ph - 16
        red_h = int(flag_h2 * 0.67)
        grn_h = flag_h2 - red_h
        draw.rectangle([flag_x, flag_y, flag_x+flag_w2, flag_y+red_h], fill="#CF101A")
        draw.rectangle([flag_x, flag_y+red_h, flag_x+flag_w2, flag_y+flag_h2], fill="#009A44")
        # Белый узор (орнамент) на красной полосе — упрощённая имитация
        orn_w = 6
        for yi in range(flag_y+4, flag_y+red_h-4, 8):
            draw.rectangle([flag_x+2, yi, flag_x+orn_w, yi+4], fill="white")
        # "BY" под флагом
        fnt_by = ImageFont.truetype(FONT_BOLD, 13)
        draw.text((flag_x + flag_w2//2, flag_y + flag_h2 + 9), "BY",
                  fill="#111111", font=fnt_by, anchor="mm")
        # Вертикальный разделитель
        draw.line([(px+strip_w, py+4), (px+strip_w, py+ph-4)], fill="#111111", width=3)
        # Основной текст — формат: "ЦЦЦЦ СС-Р" (4 цифры, 2 буквы, -регион)
        c = chars.replace(" ", "")
        if len(c) >= 6:
            body = f"{c[:4]} {c[4:6]}-{region}"
        else:
            body = f"{chars}-{region}"
        fnt_pl = ImageFont.truetype(FONT_BOLD, 58)
        content_cx = px + strip_w + (pw - strip_w) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_pl, anchor="mm")

    elif country == "KZ":
        # Казахстанский номер: белый фон, голубая полоса слева с флагом KZ и надписью "KZ",
        # центр — номер "ЦЦЦ ЛЛЛ", справа — код региона (2 цифры)
        pw, ph = 460, 120
        px, py = cx - pw // 2, cy - ph // 2
        # Тень
        draw.rounded_rectangle([px+4, py+4, px+pw+4, py+ph+4], radius=8, fill="#b0b0b0")
        # Белый корпус
        draw.rounded_rectangle([px, py, px+pw, py+ph], radius=8, fill="white",
                                outline="#111111", width=4)
        # Голубая полоса слева
        strip_w = 62
        draw.rounded_rectangle([px, py, px+strip_w, py+ph], radius=8, fill="#00AFCA")
        draw.rectangle([px+strip_w//2, py+4, px+strip_w, py+ph-4], fill="#00AFCA")
        draw.line([(px+strip_w, py+4), (px+strip_w, py+ph-4)], fill="#111111", width=3)
        # Флаг Казахстана в полосе — голубой прямоугольник с солнцем
        fkx = px + 8
        fky = py + 12
        fkw, fkh = strip_w - 18, 28
        draw.rectangle([fkx, fky, fkx+fkw, fky+fkh], fill="#00AFCA", outline=None)
        # Солнце — жёлтый круг с лучами
        sun_cx = fkx + fkw // 2
        sun_cy = fky + fkh // 2
        draw.ellipse([sun_cx-6, sun_cy-6, sun_cx+6, sun_cy+6], fill="#FFD700")
        for angle_deg in range(0, 360, 45):
            import math
            a = math.radians(angle_deg)
            x1 = int(sun_cx + 8*math.cos(a))
            y1 = int(sun_cy + 8*math.sin(a))
            x2 = int(sun_cx + 12*math.cos(a))
            y2 = int(sun_cy + 12*math.sin(a))
            draw.line([(x1,y1),(x2,y2)], fill="#FFD700", width=2)
        # Надпись "KZ" под флагом
        fnt_kz = ImageFont.truetype(FONT_BOLD, 14)
        draw.text((px + strip_w//2, py + ph - 18), "KZ", fill="white", font=fnt_kz, anchor="mm")
        # Правая панель с кодом региона
        right_w = 52
        rdx = px + pw - right_w
        draw.line([(rdx, py+4), (rdx, py+ph-4)], fill="#111111", width=3)
        fnt_reg = ImageFont.truetype(FONT_BOLD, 32)
        draw.text((rdx + right_w//2, cy), region, fill="#111111", font=fnt_reg, anchor="mm")
        # Основной текст — "ЦЦЦ ЛЛЛ"
        c = chars.replace(" ", "")
        if len(c) == 6:
            body = f"{c[:3]} {c[3:]}"
        else:
            body = chars
        fnt_pl = ImageFont.truetype(FONT_BOLD, 62)
        content_cx = px + strip_w + (rdx - px - strip_w) // 2
        draw.text((content_cx, cy), body, fill="#111111", font=fnt_pl, anchor="mm")

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
    return context.bot_data.setdefault(str(user_id), {}).setdefault("packs", [])


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
        return "Неверный формат. Отправь PNG, WEBP, WEBM или TGS."
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
    # Ensure RGBA
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    img.thumbnail((size, size), Image.LANCZOS)
    canvas = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    offset = ((size - img.width) // 2, (size - img.height) // 2)
    # Use img itself as mask (its alpha channel)
    canvas.paste(img, offset, mask=img)
    # Try compression levels 1-9 first
    data = b""
    for compress in range(1, 10):
        buf = io.BytesIO()
        canvas.save(buf, format="PNG", optimize=True, compress_level=compress)
        data = buf.getvalue()
        if len(data) <= max_bytes:
            return data
    # If still too big, shrink image
    scale = 0.9
    while len(data) > max_bytes and scale > 0.3:
        new_w = int(img.width * scale)
        new_h = int(img.height * scale)
        if new_w < 10 or new_h < 10:
            break
        resized = img.resize((new_w, new_h), Image.LANCZOS)
        final = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        off = ((size - new_w) // 2, (size - new_h) // 2)
        final.paste(resized, off, mask=resized)
        buf = io.BytesIO()
        final.save(buf, format="PNG", optimize=True, compress_level=9)
        data = buf.getvalue()
        scale -= 0.1
    return data


def process_image_for_sticker(raw_bytes: bytes, pack_type: str = "sticker") -> bytes:
    return process_image(raw_bytes, 512 if pack_type == "sticker" else 100)


def process_image_for_thumbnail(raw_bytes: bytes) -> bytes:
    return process_image(raw_bytes, 100, max_bytes=32 * 1024)


async def get_sticker_data(bot, msg, pack_type: str = "sticker"):
    async def dl(file_id: str) -> bytes:
        f   = await bot.get_file(file_id)
        buf = io.BytesIO()
        await f.download_to_memory(buf)
        buf.seek(0)
        return buf.read()

    if msg.sticker:
        s   = msg.sticker
        fmt = s.format if hasattr(s, "format") else StickerFormat.STATIC
        raw = await dl(s.file_id)
        if fmt == StickerFormat.STATIC:
            return process_image_for_sticker(raw, pack_type), fmt
        return raw, fmt

    if msg.document:
        d    = msg.document
        mime = (d.mime_type or "").lower()
        name = (d.file_name or "").lower()
        if "webm" in mime or name.endswith(".webm"):
            return await dl(d.file_id), StickerFormat.VIDEO
        if "tgs" in mime or name.endswith(".tgs"):
            return await dl(d.file_id), StickerFormat.ANIMATED
        # PNG, WEBP, JPEG и т.д.
        raw = await dl(d.file_id)
        return process_image_for_sticker(raw, pack_type), StickerFormat.STATIC

    if msg.photo:
        raw = await dl(msg.photo[-1].file_id)
        return process_image_for_sticker(raw, pack_type), StickerFormat.STATIC

    if msg.video:
        return await dl(msg.video.file_id), StickerFormat.VIDEO

    if msg.animation:
        return await dl(msg.animation.file_id), StickerFormat.VIDEO

    return None, None


# ── Главное меню ──────────────────────────────────────────────────────────────

async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    kb = [
        [InlineKeyboardButton("🖼  Создать стикер-пак",      callback_data="create_sticker")],
        [InlineKeyboardButton("✨  Создать эмодзи-пак",       callback_data="create_emoji")],
        [InlineKeyboardButton("🚗  Создать номерной знак",    callback_data="create_plate")],
        [InlineKeyboardButton("📋  Мои паки",                 callback_data="list_packs")],
    ]
    text = (
        "👋 <b>Что хочешь создать?</b>\n\n"
        "• <b>Стикер-пак</b> — обычные стикеры\n"
        "• <b>Эмодзи-пак</b> — premium emoji\n"
        "• <b>Номерной знак</b> — красивое фото номера\n\n"
        "<i>Поддерживаются: фото, PNG, WEBP, WEBM, TGS</i>"
    )
    markup = InlineKeyboardMarkup(kb)
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
    else:
        await update.effective_message.reply_text(text, parse_mode="HTML", reply_markup=markup)
    return CHOOSE_TYPE


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    name = f"@{user.username}" if user.username else user.first_name
    kb   = [[InlineKeyboardButton("😎 Начать", callback_data="begin")]]
    await update.message.reply_text(
        f"Привет, {name}!\n\nСоздавай стикеры, premium emoji и номерные знаки 🚗",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return MAIN_MENU


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    for key in (
        "new_pack_title", "new_pack_suffix", "new_pack_stickers",
        "creating_type", "selected_pack_index", "pending_data", "pending_fmt",
        "plate_country", "plate_region", "file_queue", "file_queue_done",
    ):
        context.user_data.pop(key, None)
    return await send_main_menu(update, context)


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
        icon = "🖼" if p["type"] == "sticker" else "✨"
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
    user_id = update.effective_user.id
    pack    = get_packs(context, user_id)[idx]
    icon    = "🖼" if pack["type"] == "sticker" else "✨"
    n       = pack.get("count", 0)
    url     = pack_url(pack["name"])
    kb = [
        [InlineKeyboardButton("🔗 Открыть пак",        url=url)],
        [InlineKeyboardButton("➕ Добавить стикер",     callback_data="mgmt_add")],
        [InlineKeyboardButton("🗑 Удалить стикер",      callback_data="mgmt_delete")],
        [InlineKeyboardButton("🖼 Сменить иконку пака", callback_data="mgmt_icon")],
        [InlineKeyboardButton("❌ Удалить весь пак",    callback_data="mgmt_delete_all")],
        back_btn("◀️ К списку паков", "list_packs"),
    ]
    await query.edit_message_text(
        f"{icon} <b>{pack['title']}</b>\nСтикеров: {n}",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PACK_SELECTED


# ── Создание пака ─────────────────────────────────────────────────────────────

async def start_create(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query     = update.callback_query
    await query.answer()
    pack_type = "sticker" if query.data == "create_sticker" else "emoji"
    context.user_data["creating_type"] = pack_type
    word = "стикер-пак" if pack_type == "sticker" else "эмодзи-пак"
    await query.edit_message_text(
        f"📝 <b>Создание {word}</b>\n\nНапиши <b>название</b> набора:\n\n/cancel — отменить",
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


async def _prompt_first_sticker(update: Update, suffix: str) -> int:
    text = (
        f"✅ Ссылка: <code>{suffix}</code>\n\n"
        "Отправь первый стикер 👇\n"
        "<i>Поддерживаются: фото, PNG, WEBP, WEBM, TGS, готовые стикеры TG.</i>\n\n/cancel — отменить"
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
    return await _prompt_first_sticker(update, suffix)


async def receive_pack_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw    = update.message.text.strip()
    suffix = sanitize_suffix(raw)
    context.user_data["new_pack_suffix"] = suffix
    if suffix != raw.lower():
        await update.message.reply_text(
            f"ℹ️ Ссылка скорректирована: <code>{suffix}</code>", parse_mode="HTML"
        )
    return await _prompt_first_sticker(update, suffix)


async def _ask_emoji_for_next(update_or_msg, context: ContextTypes.DEFAULT_TYPE) -> int:
    queue = context.user_data.get("file_queue", [])
    done  = context.user_data.get("file_queue_done", 0)
    total = len(queue)
    msg   = update_or_msg.message if hasattr(update_or_msg, "message") else update_or_msg
    if done >= total:
        return await _finish_batch(update_or_msg, context)
    idx_label = f" {done + 1}/{total}" if total > 1 else ""
    await msg.reply_text(
        f"😊 <b>Укажи эмодзи</b> для стикера{idx_label}\n"
        "Можно несколько через пробел: <code>😎 🔥</code>\n\n/cancel — отменить",
        parse_mode="HTML",
    )
    return WAITING_EMOJI


async def _finish_batch(update_or_msg, context: ContextTypes.DEFAULT_TYPE) -> int:
    stickers_buf = context.user_data.get("new_pack_stickers", [])
    suffix       = context.user_data.get("new_pack_suffix") or random_suffix()
    context.user_data["new_pack_suffix"] = suffix
    bot_username = context.user_data.get("_bot_username", "")
    pack_name    = build_pack_name(bot_username, suffix)
    n            = len(stickers_buf)
    url          = pack_url(pack_name)
    title        = context.user_data.get("new_pack_title", "My Pack")

    msg = update_or_msg.message if hasattr(update_or_msg, "message") else update_or_msg

    if n == 0:
        await msg.reply_text(
            "⚠️ Не удалось добавить ни одного стикера.\n\n"
            "Попробуй снова — отправь файл 👇\n\n/cancel — отменить"
        )
        context.user_data["file_queue"]      = []
        context.user_data["file_queue_done"] = 0
        return ADDING_STICKER

    kb = [
        [InlineKeyboardButton("🔗 Открыть пак",   url=url)],
        [InlineKeyboardButton("➕ Добавить ещё",  callback_data="add_more")],
        [InlineKeyboardButton("💾 Сохранить пак", callback_data="save_pack")],
    ]
    added = context.user_data.pop("file_queue_added", 0)
    text = (
        f"🎉 <b>Пак создан!</b>\n<b>{title}</b>\n\nСтикеров: {n} {plural_sticker(n)}\n\nДобавь ещё или сохрани 👇"
        if added == n
        else f"✅ Добавлено {added} {plural_sticker(added)}!\nВсего: {n} {plural_sticker(n)}\n\nПродолжай или сохрани 👇"
    )
    await msg.reply_text(
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
        data, fmt = await get_sticker_data(context.bot, msg, pack_type)
        if data is None:
            await msg.reply_text("❌ Не получилось распознать файл. Отправь PNG, WEBP, WEBM или TGS.")
            return ADDING_STICKER

        logger.info("receive_sticker_file: got data len=%s fmt=%s", len(data) if isinstance(data, bytes) else "id", fmt)
        queue = context.user_data.setdefault("file_queue", [])
        queue.append({"data": data, "fmt": fmt})

        # ── Обработка альбома (media_group) ──
        # Telegram шлёт фото альбома отдельными сообщениями с одинаковым media_group_id.
        # Ждём 1.5 сек — если за это время придут ещё файлы с тем же group_id,
        # они добавятся в очередь. Потом запускаем опрос эмодзи.
        if msg.media_group_id:
            group_id = msg.media_group_id
            context.user_data["_last_media_group"] = group_id

            # Отменяем предыдущий таймер если он есть
            old_task = context.user_data.pop("_album_task", None)
            if old_task and not old_task.done():
                old_task.cancel()

            # Сохраняем chat_id чтобы отправить сообщение после сбора альбома
            chat_id = msg.chat_id
            bot_ref = context.bot

            async def _album_flush():
                await asyncio.sleep(1.5)
                if context.user_data.get("_last_media_group") != group_id:
                    return
                context.user_data.pop("_last_media_group", None)
                q = context.user_data.get("file_queue", [])
                logger.info("Album flush: %d files collected", len(q))
                if not q:
                    return
                # Инициализируем очередь только если ещё не начали
                if context.user_data.get("file_queue_done", -1) < 0:
                    context.user_data["file_queue_done"]  = 0
                    context.user_data["file_queue_added"] = 0
                done = context.user_data.get("file_queue_done", 0)
                total = len(q)
                if done >= total:
                    return
                idx_label = f" {done+1}/{total}" if total > 1 else ""
                text_out = (
                    f"📥 Принято <b>{total}</b> файл(ов) из альбома.\n\n"
                    f"😊 <b>Укажи эмодзи</b> для стикера{idx_label}\n"
                    "Можно несколько через пробел: <code>😎 🔥</code>\n\n/cancel — отменить"
                )
                await bot_ref.send_message(
                    chat_id=chat_id,
                    text=text_out,
                    parse_mode="HTML",
                )

            task = asyncio.create_task(_album_flush())
            context.user_data["_album_task"] = task
            return ADDING_STICKER

        # ── Одиночный файл ──
        if len(queue) == 1:
            context.user_data["file_queue_done"]  = 0
            context.user_data["file_queue_added"] = 0
            return await _ask_emoji_for_next(update, context)
        # Несколько файлов без альбома — просто подтверждаем
        await msg.reply_text(f"📥 Файл #{len(queue)} принят.")
        return ADDING_STICKER

    except Exception as e:
        logger.exception("receive_sticker_file crashed: %s", e)
        try:
            await update.message.reply_text(f"❌ Ошибка при обработке файла: {e}\n\nПопробуй снова.")
        except Exception:
            pass
        return ADDING_STICKER


def _extract_emoji(text: str) -> list:
    """Извлекает эмодзи из текста. Возвращает список эмодзи-строк."""
    import re
    # Расширенный паттерн для Unicode emoji (включая ZWJ-последовательности, флаги, скины)
    EMOJI_PATTERN = re.compile(
        "(?:"
        "(?:\U0001F1E0-\U0001F1FF){2}"            # флаги (региональные индикаторы)
        "|[\U0001F600-\U0001F64F]"                 # эмоции/лица
        "|[\U0001F300-\U0001F5FF]"                 # разные символы
        "|[\U0001F680-\U0001F6FF]"                 # транспорт
        "|[\U0001F700-\U0001F77F]"                 # алхимия
        "|[\U0001F780-\U0001F7FF]"                 # геометрия
        "|[\U0001F800-\U0001F8FF]"                 # дополнительные стрелки
        "|[\U0001F900-\U0001F9FF]"                 # дополнительные символы
        "|[\U0001FA00-\U0001FA6F]"                 # шахматы, тела
        "|[\U0001FA70-\U0001FAFF]"                 # еда, спорт
        "|[\U00002600-\U000027BF]"                 # Miscellaneous symbols
        "|[\U00002702-\U000027B0]"                 # Dingbats
        "|[\U0000231A-\U00002B55]"                 # прочие символы
        "|[\U00003030-\U00003299]"                 # CJK символы
        ")"
        "(?:\uFE0F?"                               # вариант-16
        "(?:\u200D"                                # ZWJ
        "(?:"
        "[\U0001F600-\U0001FAFF\U00002600-\U000027BF]"
        "|\uFE0F"
        ")*"
        ")?"
        "(?:[\U0001F3FB-\U0001F3FF])?"             # модификаторы скина
        ")*",
        re.UNICODE,
    )
    result = []
    for m in EMOJI_PATTERN.finditer(text):
        e = m.group(0).strip()
        if e:
            result.append(e)
    return result


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
        action_word = "эмодзи" if pack_type == "emoji" else "стикер"
        await update.message.reply_text(f"⏳ Добавляю {action_word}...")

        err = await _push_sticker_to_tg(
            context.bot, user_id, pack_name, pack_type,
            title, data, fmt, emoji_list, stickers_buf
        )

        if err:
            await update.message.reply_text(
                f"❌ {err}\n\nПопробуй другой эмодзи или /cancel"
            )
            return WAITING_EMOJI

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


async def _push_sticker_to_tg(bot, user_id: int, pack_name: str, pack_type: str,
                                title: str, data, fmt, emoji_list: list, stickers_buf: list):
    sticker_type = StickerType.REGULAR if pack_type == "sticker" else StickerType.CUSTOM_EMOJI

    # Определяем имя файла
    if fmt == StickerFormat.ANIMATED:
        fname = "sticker.tgs"
    elif fmt == StickerFormat.VIDEO:
        fname = "sticker.webm"
    else:
        fname = "sticker.png"

    # Убеждаемся что emoji_list содержит хотя бы один валидный эмодзи
    clean_emoji = [e for e in emoji_list if e and e.strip()]
    if not clean_emoji:
        clean_emoji = ["🙂"]
    logger.info("push_sticker: pack=%s type=%s fmt=%s emoji=%s is_new=%s data_len=%s",
                pack_name, pack_type, fmt, clean_emoji, not bool(stickers_buf),
                len(data) if isinstance(data, bytes) else "file_id")

    # PTB 20.7: InputSticker.sticker должен быть file_id строкой.
    # Сначала загружаем файл через upload_sticker_file → получаем file_id.
    if isinstance(data, bytes):
        buf = io.BytesIO(data)
        buf.seek(0)
        upload_file = InputFile(buf, filename=fname)
        logger.info("Uploading sticker file via upload_sticker_file...")
        uploaded = await bot.upload_sticker_file(
            user_id=user_id,
            sticker=upload_file,
            sticker_format=fmt,
        )
        file_id = uploaded.file_id
        logger.info("Uploaded sticker file_id: %s", file_id)
    else:
        # data уже является file_id строкой
        file_id = data
        logger.info("Using existing file_id: %s", file_id)

    def make_sticker():
        return InputSticker(sticker=file_id, emoji_list=clean_emoji[:20])

    async def _create_set(sticker):
        # sticker_format был удалён в более новых версиях PTB — пробуем с ним, потом без
        try:
            await bot.create_new_sticker_set(
                user_id=user_id,
                name=pack_name,
                title=title,
                stickers=[sticker],
                sticker_type=sticker_type,
                sticker_format=fmt,
            )
        except TypeError:
            # Старая версия PTB без sticker_format
            await bot.create_new_sticker_set(
                user_id=user_id,
                name=pack_name,
                title=title,
                stickers=[sticker],
                sticker_type=sticker_type,
            )

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
    })
    for key in ("new_pack_title", "new_pack_suffix", "new_pack_stickers", "creating_type"):
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
        file_size = None
        if msg.document:    file_size = msg.document.file_size
        elif msg.photo:     file_size = msg.photo[-1].file_size
        elif msg.video:     file_size = msg.video.file_size
        elif msg.animation: file_size = msg.animation.file_size
        if file_size and file_size > MAX_INPUT_BYTES:
            await msg.reply_text(f"❌ Файл слишком большой. Максимум — 10 МБ.")
            return ADD_STICKER_FILE
        data, fmt = await get_sticker_data(context.bot, msg, pack_type)
        if data is None:
            await msg.reply_text("❌ Не получилось распознать файл.")
            return ADD_STICKER_FILE
        context.user_data["pending_data"] = data
        context.user_data["pending_fmt"]  = fmt
        context.user_data["pending_pack_idx"] = idx
        await msg.reply_text(
            "😊 <b>Укажи эмодзи</b> для этого стикера.\nМожно несколько: <code>😎 🔥</code>",
            parse_mode="HTML",
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

        if data is None:
            await update.message.reply_text(
                "❌ Данные файла потерялись. Отправь файл ещё раз."
            )
            return ADD_STICKER_FILE

        pack_type_cur = pack.get("type", "sticker")
        action_word   = "эмодзи" if pack_type_cur == "emoji" else "стикер"
        await update.message.reply_text(f"⏳ Добавляю {action_word}...")

        pack_name    = pack["name"]
        stickers_buf = [True] * pack.get("count", 1)  # непустой → add_sticker_to_set

        err = await _push_sticker_to_tg(
            context.bot, user_id, pack_name, pack["type"],
            pack["title"], data, fmt, emoji_list, stickers_buf
        )

        if err:
            await update.message.reply_text(
                f"❌ {err}\n\nПопробуй другой эмодзи или /cancel"
            )
            return ADD_STICKER_EMOJI

        # Успех — чистим временные данные
        context.user_data.pop("pending_data", None)
        context.user_data.pop("pending_fmt", None)
        context.user_data.pop("pending_pack_idx", None)
        pack["count"] = pack.get("count", 0) + 1
        n = pack["count"]
        await update.message.reply_text(
            f"✅ Стикер добавлен! Теперь в паке: {n} {plural_sticker(n)}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")]]),
        )
        return PACK_SELECTED

    except Exception as e:
        logger.exception("receive_add_emoji crashed: %s", e)
        try:
            await update.message.reply_text(f"❌ Внутренняя ошибка: {e}\n\nПопробуй снова или /cancel")
        except Exception:
            pass
        return ADD_STICKER_EMOJI


async def mgmt_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    pack    = get_packs(context, user_id)[idx]
    try:
        tg_pack = await context.bot.get_sticker_set(pack["name"])
    except TelegramError as e:
        await query.answer(_friendly_tg_error(str(e)), show_alert=True)
        return PACK_SELECTED
    if not tg_pack.stickers:
        await query.answer("В паке нет стикеров.", show_alert=True)
        return PACK_SELECTED
    kb = []
    for i, s in enumerate(tg_pack.stickers[:50]):
        kb.append([InlineKeyboardButton(f"#{i+1} {s.emoji or '?'}",
                                         callback_data=f"del_sticker_{s.file_unique_id}")])
    kb.append(back_btn("◀️ Отмена", f"select_pack_{idx}"))
    await query.edit_message_text("🗑 Выбери стикер для удаления:",
                                   reply_markup=InlineKeyboardMarkup(kb))
    return DELETE_STICKER


async def confirm_delete_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query          = update.callback_query
    await query.answer()
    user_id        = update.effective_user.id
    idx            = context.user_data.get("selected_pack_index", 0)
    pack           = get_packs(context, user_id)[idx]
    file_unique_id = query.data.replace("del_sticker_", "")
    try:
        tg_pack = await context.bot.get_sticker_set(pack["name"])
    except TelegramError as e:
        await query.answer(_friendly_tg_error(str(e)), show_alert=True)
        return PACK_SELECTED
    target = next((s for s in tg_pack.stickers if s.file_unique_id == file_unique_id), None)
    if not target:
        await query.answer("Стикер не найден.", show_alert=True)
        return DELETE_STICKER
    try:
        await context.bot.delete_sticker_from_set(target.file_id)
    except TelegramError as e:
        await query.answer(_friendly_tg_error(str(e)), show_alert=True)
        return DELETE_STICKER
    pack["count"] = max(0, pack.get("count", 1) - 1)
    await query.edit_message_text(
        f"✅ Стикер удалён. Осталось: {pack['count']} {plural_sticker(pack['count'])}",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⚙️ К паку", callback_data=f"select_pack_{idx}")]]),
    )
    return PACK_SELECTED


async def mgmt_icon(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx   = context.user_data.get("selected_pack_index", 0)
    await query.edit_message_text(
        "🖼 <b>Сменить иконку пака</b>\n\nОтправь стикер из этого пака:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([back_btn("◀️ Отмена", f"select_pack_{idx}")]),
    )
    return CHANGE_ICON


async def receive_icon(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    msg     = update.message
    user_id = update.effective_user.id
    idx     = context.user_data.get("selected_pack_index", 0)
    pack    = get_packs(context, user_id)[idx]
    if not msg.sticker:
        await msg.reply_text("Нужен стикер из этого пака.")
        return CHANGE_ICON
    try:
        f   = await context.bot.get_file(msg.sticker.file_id)
        buf = io.BytesIO()
        await f.download_to_memory(buf)
        thumb = process_image_for_thumbnail(buf.getvalue())
    except Exception as e:
        await msg.reply_text(f"❌ Ошибка: {e}")
        return CHANGE_ICON
    try:
        await context.bot.set_sticker_set_thumbnail(
            name=pack["name"], user_id=user_id, thumbnail=thumb,
            format=msg.sticker.format if hasattr(msg.sticker, "format") else StickerFormat.STATIC,
        )
    except TelegramError as e:
        await msg.reply_text(f"❌ {_friendly_tg_error(str(e))}")
        return CHANGE_ICON
    await msg.reply_text(
        "✅ Иконка обновлена!",
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
    try:
        await context.bot.delete_sticker_set(pack["name"])
    except TelegramError:
        pass
    name = pack["title"]
    packs.pop(idx)
    context.user_data.pop("selected_pack_index", None)
    await query.edit_message_text(
        f"🗑 Пак <b>{name}</b> удалён.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([back_btn("📋 К списку паков", "list_packs")]),
    )
    return CHOOSE_TYPE


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

    await update.message.reply_text("⏳ Генерирую номерной знак...")

    try:
        img_bytes = generate_plate_image(country, raw, region, region_name)
        kb = [
            [InlineKeyboardButton("🔄 Другой номер",      callback_data=f"plate_other_{country}")],
            [InlineKeyboardButton("🏠 В меню",             callback_data="plate_menu")],
        ]
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

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start",  start),
            CommandHandler("menu",   menu_command),
            CommandHandler("cancel", cancel_command),
            CallbackQueryHandler(begin, pattern="^begin$"),
        ],
        states={
            MAIN_MENU: [CallbackQueryHandler(begin, pattern="^begin$")],
            CHOOSE_TYPE: [
                CallbackQueryHandler(begin,                pattern="^begin$"),
                CallbackQueryHandler(start_create,         pattern="^create_(sticker|emoji)$"),
                CallbackQueryHandler(list_packs,           pattern="^list_packs$"),
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
            ],
            PACK_NAME: [
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
            ],
            WAITING_EMOJI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_emoji_for_new),
            ],
            PACK_SELECTED: [
                CallbackQueryHandler(begin,                  pattern="^begin$"),
                CallbackQueryHandler(list_packs,             pattern="^list_packs$"),
                CallbackQueryHandler(select_pack,            pattern="^select_pack_\\d+$"),
                CallbackQueryHandler(mgmt_add,               pattern="^mgmt_add$"),
                CallbackQueryHandler(mgmt_delete,            pattern="^mgmt_delete$"),
                CallbackQueryHandler(confirm_delete_sticker, pattern="^del_sticker_.+$"),
                CallbackQueryHandler(mgmt_icon,              pattern="^mgmt_icon$"),
                CallbackQueryHandler(mgmt_delete_all,        pattern="^mgmt_delete_all$"),
                CallbackQueryHandler(confirm_delete_pack,    pattern="^confirm_delete_pack_\\d+$"),
            ],
            ADD_STICKER_FILE: [
                CallbackQueryHandler(select_pack, pattern="^select_pack_\\d+$"),
                MessageHandler(
                    filters.PHOTO | filters.VIDEO | filters.Sticker.ALL |
                    filters.ANIMATION | filters.Document.ALL,
                    receive_add_file,
                ),
            ],
            ADD_STICKER_EMOJI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_add_emoji),
            ],
            DELETE_STICKER: [
                CallbackQueryHandler(select_pack,            pattern="^select_pack_\\d+$"),
                CallbackQueryHandler(confirm_delete_sticker, pattern="^del_sticker_.+$"),
            ],
            CHANGE_ICON: [
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
                CallbackQueryHandler(plate_btn_menu,       pattern="^plate_menu$"),
                CallbackQueryHandler(plate_btn_other,      pattern="^plate_other_(RU|UA|BY|KZ)$"),
                CallbackQueryHandler(plate_choose_country, pattern="^create_plate$"),
                CallbackQueryHandler(plate_select_country, pattern="^pc_(RU|UA|BY|KZ)$"),
                CallbackQueryHandler(begin,                pattern="^begin$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, plate_receive_input),
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
            ("start",  "👋 Приветствие"),
            ("menu",   "🏠 Главное меню"),
            ("cancel", "❌ Отменить текущее действие"),
        ])

    app.post_init = post_init
    app.add_handler(conv)
    app.add_handler(CommandHandler("start",  start))
    app.add_handler(CommandHandler("menu",   menu_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CallbackQueryHandler(begin, pattern="^begin$"))
    logger.info("Бот запущен ✅")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
