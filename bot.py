import asyncio
import html
import logging
import os
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import asyncpg
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
)
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIGURATION ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]

CIS_COUNTRIES = [
    "🇷🇺 Россия", "🇧🇾 Беларусь", "🇰🇿 Казахстан", "🇦🇲 Армения",
    "🇦🇿 Азербайджан", "🇰🇬 Кыргызстан", "🇺🇿 Узбекистан",
    "🇹🇯 Таджикистан", "🇹🇲 Туркменистан", "🇲🇩 Молдова"
]

RARITIES = ["Редкая", "Очень редкая", "Эпическая", "Мифическая", "Легендарная", "Секретная"]
DAILY_RARITIES_CHANCES = {
    "Редкая": 0.35,
    "Очень редкая": 0.30,
    "Эпическая": 0.20,
    "Мифическая": 0.10,
    "Легендарная": 0.05,
    "Секретная": 0.00
}

STREAK_REWARDS = {
    1: {"type": "coins", "amount": 2500, "desc": "2 500 RPLCoin"},
    2: {"type": "coins", "amount": 5000, "desc": "5 000 RPLCoin"},
    3: {"type": "coins", "amount": 7500, "desc": "7 500 RPLCoin"},
    4: {"type": "card", "min_ovr": 75, "max_ovr": 85, "desc": "Карточка 75-85 OVR"},
    5: {"type": "coins", "amount": 25000, "desc": "25 000 RPLCoin"},
    6: {"type": "coins", "amount": 35000, "desc": "35 000 RPLCoin"},
    7: {"type": "card", "min_ovr": 80, "max_ovr": 88, "desc": "Карточка 80-88 OVR"},
}

ROSTER_SLOTS = ["skater_1", "skater_2", "skater_3", "skater_4", "goalie"]
SLOT_NAMES = {
    "skater_1": "🏒 Полевой #1",
    "skater_2": "🏒 Полевой #2",
    "skater_3": "🏒 Полевой #3",
    "skater_4": "🏒 Полевой #4",
    "goalie": "🧤 Вратарь"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# ==================== LEVEL & PROGRESS LOGIC ====================
def calculate_level_and_progress(total_exp: int) -> Tuple[int, int, int]:
    lvl = 1
    req = 1000
    rem = max(0, total_exp)
    while rem >= req:
        rem -= req
        lvl += 1
        req += 1000
    return lvl, rem, req

# ==================== DATABASE & AUTO-MIGRATIONS ====================
class Database:
    def __init__(self, url: str):
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        self.url = url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        base_url = self.url.split("?")[0]
        try:
            self.pool = await asyncpg.create_pool(base_url, max_size=20, min_size=2)
            logger.info("Database pool created successfully.")
        except Exception as e:
            logger.warning(f"Connecting with default SSL failed: {e}. Retrying with ssl=False...")
            self.pool = await asyncpg.create_pool(base_url, ssl=False, max_size=20, min_size=2)
            logger.info("Database pool connected without SSL.")

        await self.init_tables()

    async def init_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    team_name VARCHAR(100),
                    country VARCHAR(100),
                    player_name VARCHAR(100),
                    username VARCHAR(100),
                    mmr INT DEFAULT 500,
                    rpl_coins BIGINT DEFAULT 1000,
                    exp INT DEFAULT 50,
                    wins INT DEFAULT 0,
                    losses INT DEFAULT 0,
                    last_daily_claim TIMESTAMP,
                    streak_days INT DEFAULT 0,
                    last_streak_claim TIMESTAMP,
                    notified_cd BOOLEAN DEFAULT TRUE,
                    jail_until TIMESTAMP,
                    job_coach_cd TIMESTAMP,
                    job_illegal_cd TIMESTAMP,
                    job_photo_cd TIMESTAMP,
                    job_merch_cd TIMESTAMP,
                    registered_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS teams (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    photo_id TEXT
                );

                CREATE TABLE IF NOT EXISTS collections (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cards (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    rarity VARCHAR(50) NOT NULL,
                    country VARCHAR(100) NOT NULL,
                    ovr INT NOT NULL,
                    position VARCHAR(20) NOT NULL,
                    team_name VARCHAR(100) DEFAULT 'Без команды',
                    collection_name VARCHAR(100) DEFAULT 'Основная',
                    photo_id TEXT,
                    is_starter BOOLEAN DEFAULT FALSE
                );

                CREATE TABLE IF NOT EXISTS user_cards (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                    card_id INT REFERENCES cards(id) ON DELETE CASCADE,
                    in_lineup BOOLEAN DEFAULT FALSE,
                    slot VARCHAR(20),
                    acquired_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS packs (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    price INT NOT NULL,
                    card_ids INT[] NOT NULL
                );

                CREATE TABLE IF NOT EXISTS promo_codes (
                    code VARCHAR(50) PRIMARY KEY,
                    reward_type VARCHAR(20) NOT NULL,
                    reward_value INT NOT NULL,
                    max_uses INT DEFAULT 1,
                    used_count INT DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS promo_uses (
                    user_id BIGINT,
                    code VARCHAR(50),
                    PRIMARY KEY(user_id, code)
                );

                CREATE TABLE IF NOT EXISTS market_listings (
                    id SERIAL PRIMARY KEY,
                    seller_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                    card_id INT REFERENCES cards(id) ON DELETE CASCADE,
                    price BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)

            await conn.execute("""
                ALTER TABLE users ADD COLUMN IF NOT EXISTS player_name VARCHAR(100);
                ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(100);
                ALTER TABLE users ADD COLUMN IF NOT EXISTS streak_days INT DEFAULT 0;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS last_streak_claim TIMESTAMP;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS notified_cd BOOLEAN DEFAULT TRUE;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS jail_until TIMESTAMP;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS job_coach_cd TIMESTAMP;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS job_illegal_cd TIMESTAMP;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS job_photo_cd TIMESTAMP;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS job_merch_cd TIMESTAMP;
                ALTER TABLE users ALTER COLUMN rpl_coins TYPE BIGINT;
                ALTER TABLE cards ADD COLUMN IF NOT EXISTS is_starter BOOLEAN DEFAULT FALSE;
                ALTER TABLE cards ALTER COLUMN photo_id DROP NOT NULL;
                ALTER TABLE teams ADD COLUMN IF NOT EXISTS photo_id TEXT;
                ALTER TABLE user_cards ADD COLUMN IF NOT EXISTS slot VARCHAR(20);
                CREATE UNIQUE INDEX IF NOT EXISTS user_cards_user_card_uniq ON user_cards (user_id, card_id);
            """)
            logger.info("Database schemas and auto-migrations verified successfully.")

db = Database(DATABASE_URL)

# ==================== FSM STATES ====================
class RegState(StatesGroup):
    entering_team_name = State()
    choosing_country = State()

class AdminBatchCard(StatesGroup):
    waiting_for_text = State()

class AdminCardManual(StatesGroup):
    name = State()
    rarity = State()
    country = State()
    ovr = State()
    position = State()
    team = State()
    collection = State()
    is_starter = State()
    photo = State()

class AdminTeamState(StatesGroup):
    team_name = State()
    team_photo = State()

class AdminCollectionState(StatesGroup):
    collection_name = State()

class AdminPackState(StatesGroup):
    name = State()
    price = State()
    card_ids = State()

class AdminPromoState(StatesGroup):
    code = State()
    reward_type = State()
    reward_value = State()
    max_uses = State()

class AdminStartersState(StatesGroup):
    card_ids = State()

class UserPromoState(StatesGroup):
    code = State()

class MarketSellState(StatesGroup):
    card_id = State()
    price = State()

class CoinState(StatesGroup):
    bet = State()

class RpsState(StatesGroup):
    bet = State()

class SlotsState(StatesGroup):
    bet = State()

# ==================== HELPER: GRANT STARTER CARDS ====================
async def grant_starter_cards_for_user(conn, user_id: int):
    try:
        starters = await conn.fetch("SELECT id, position FROM cards WHERE is_starter = TRUE ORDER BY id ASC")
        starters_list = [dict(s) for s in starters]

        if len(starters_list) < 5:
            existing_ids = [s["id"] for s in starters_list] or [0]
            has_goalie = any("goalie" in s.get("position", "").lower() or "врат" in s.get("position", "").lower() for s in starters_list)
            
            if not has_goalie:
                extra_g = await conn.fetch("""
                    SELECT id, position FROM cards 
                    WHERE (position ILIKE '%goalie%' OR position ILIKE '%врат%')
                      AND NOT (id = ANY($1::int[]))
                    ORDER BY ovr ASC LIMIT 1
                """, existing_ids)
                if extra_g:
                    starters_list.append(dict(extra_g[0]))
                    existing_ids.append(extra_g[0]["id"])

            needed = 5 - len(starters_list)
            if needed > 0:
                extra = await conn.fetch("""
                    SELECT id, position FROM cards 
                    WHERE NOT (id = ANY($1::int[]))
                    ORDER BY ovr ASC LIMIT $2
                """, existing_ids, needed)
                for e in extra:
                    starters_list.append(dict(e))

        if not starters_list:
            return

        skater_slots = ["skater_1", "skater_2", "skater_3", "skater_4"]
        skater_idx = 0
        goalie_assigned = False

        for st in starters_list[:5]:
            pos = st.get("position", "")
            if ("goalie" in pos.lower() or "врат" in pos.lower()) and not goalie_assigned:
                slot_name = "goalie"
                goalie_assigned = True
            elif skater_idx < len(skater_slots):
                slot_name = skater_slots[skater_idx]
                skater_idx += 1
            elif not goalie_assigned:
                slot_name = "goalie"
                goalie_assigned = True
            else:
                slot_name = None

            has_card = await conn.fetchrow("SELECT id, slot FROM user_cards WHERE user_id = $1 AND card_id = $2", user_id, st["id"])
            if not has_card:
                await conn.execute("""
                    INSERT INTO user_cards (user_id, card_id, in_lineup, slot)
                    VALUES ($1, $2, TRUE, $3)
                """, user_id, st["id"], slot_name)
            else:
                if not has_card["slot"] and slot_name:
                    await conn.execute("UPDATE user_cards SET in_lineup = TRUE, slot = $1 WHERE id = $2", slot_name, has_card["id"])
    except Exception as e:
        logger.error(f"Error in grant_starter_cards_for_user: {e}", exc_info=True)

# ==================== 50+ DYNAMIC MATCH EVENTS ====================
MATCH_EVENTS_CATALOG = [
    # Броски, штанги, перекладины
    "🎯 Звонкая ШТАНГА! <b>{att_p}</b> ({att_team}) сотрясает каркас ворот {def_team} кистевым выстрелом!",
    "💥 ПЕРЕКЛАДИНА! <b>{att_p}</b> ({att_team}) зарядил щелчком от синей линии — ворота {def_team} звенят на всю арену!",
    "🏒 <b>{att_p}</b> ({att_team}) выкатывается из-за ворот и бросает с неудобной руки — штанга спасает {def_team}!",
    "🎯 Коварный подпил шайбы от <b>{att_p}</b> ({att_team})! Шайба чиркает по перекладине ворот {def_team}!",
    "🏒 Выход 2 в 1 в пользу {att_team}! <b>{att_p}</b> берет инициативу на себя и стреляет в ближний угол — стойка ворот!",
    "⚡ <b>{att_p}</b> ({att_team}) щелкает в касание из правого круга вбрасывания — заградительное стекло дрожит!",
    "💥 Пушечный залп от синей линии в исполнении <b>{att_p}</b> ({att_team})! Шайба просвистела рядом со штангой!",
    "🏒 <b>{att_p}</b> ({att_team}) замыкает прострел на дальнем пятаке — звон штанги ворот {def_team}!",
    "🎯 Хитрый бросок <b>{att_p}</b> ({att_team}) из-под защитника {def_p} — каркас спасает {def_team}!",

    # Сейвы вратарей
    "🧤 ЧУДЕСНЫЙ СЕЙВ! Вратарь <b>{def_g}</b> ({def_team}) в феноменальном шпагате достает мертвую шайбу после броска <b>{att_p}</b> ({att_team})!",
    "🧤 Голкипер <b>{def_g}</b> ({def_team}) намертво фиксирует шайбу ловушкой, ликвидировав выход один в один от <b>{att_p}</b> ({att_team})!",
    "🛡 Вратарь <b>{def_g}</b> ({def_team}) щитком парирует коварное добивание форварда <b>{att_p}</b> ({att_team})!",
    "🧤 Невероятный рефлекс! <b>{def_g}</b> ({def_team}) блином переводит через стекло мощнейший выстрел <b>{att_p}</b> ({att_team})!",
    "🧤 Голкипер <b>{def_g}</b> ({def_team}) кончиком клюшки сбивает шайбу прямо с крюка нападающего <b>{att_p}</b> ({att_team})!",
    "🛡 Роботоподобная реакция! Вратарь <b>{def_g}</b> ({def_team}) шлемом отражает ракетный залп <b>{att_p}</b> ({att_team})!",
    "🧤 <b>{def_g}</b> ({def_team}) надежно забирает шайбу в ловушку после броска <b>{att_p}</b> ({att_team})!",
    "🧤 <b>{def_g}</b> ({def_team}) закрывает 'домик' щитками и не пускает шайбу после резкого виража <b>{att_p}</b> ({att_team})!",
    "🛡 Голкипер <b>{def_g}</b> ({def_team}) в отчаянном прыжке парирует щелчок форварда <b>{att_p}</b> ({att_team})!",

    # Блокировки и действия защиты
    "🛡 Защитник <b>{def_p}</b> ({def_team}) бросается под пушечный щелчок <b>{att_p}</b> ({att_team}) и спасает ворота!",
    "🛡 <b>{def_p}</b> ({def_team}) ювелирно подбивает клюшку игроку <b>{att_p}</b> ({att_team}) в момент броска по открытому углу!",
    "🛡 <b>{def_p}</b> ({def_team}) намертво вяжет форварда <b>{att_p}</b> на пятаке и прерывает острейшую атаку {att_team}!",
    "🛡 Самоотверженный блок-шот! <b>{def_p}</b> ({def_team}) телом блокирует щелчок <b>{att_p}</b> ({att_team})!",
    "🛡 Защитник <b>{def_p}</b> ({def_team}) чисто выбивает шайбу при выходе <b>{att_p}</b> ({att_team}) на рандеву с вратарем!",

    # Силовые приемы и борьба
    "💥 СОКРУШИТЕЛЬНЫЙ СИЛОВОЙ! <b>{def_p}</b> ({def_team}) впечатывает в борт форварда <b>{att_p}</b> ({att_team}) — стекла трещат!",
    "💥 Зубодробительный прием на синей линии! <b>{def_p}</b> ({def_team}) жестко остановил <b>{att_p}</b> ({att_team})!",
    "💥 <b>{def_p}</b> ({def_team}) эффектной мельницей роняет на лед мчащегося на скорости <b>{att_p}</b> ({att_team})!",
    "⚡ Жаркая потасовка на пятаке ворот {def_team}! <b>{def_p}</b> и <b>{att_p}</b> сцепились в борьбе за шайбу!",
    "💥 Мощное столкновение в центре площадки: <b>{def_p}</b> ({def_team}) сбивает атакующий порыв <b>{att_p}</b> ({att_team})!",

    # Атаки, пасы и комбинации
    "🌪 <b>{att_p}</b> ({att_team}) на сумасшедшей скорости прорезает оборону соперника и обыгрывает двоих на носовом платке!",
    "🏒 <b>{att_p}</b> ({att_team}) отдает скрытый диагональный пас, разрезая всю оборону {def_team}!",
    "🌪 Молниеносный перехват в средней зоне! <b>{att_p}</b> ({att_team}) мчит в контратаку три в два!",
    "⚡ <b>{att_p}</b> ({att_team}) ложным замахом укладывает защитника на лед и бросает с острого угла!",
    "🏒 Затяжная позиционная осада в исполнении {att_team}: <b>{att_p}</b> опасно простреливает вдоль ворот {def_team}!",
    "🌪 Острейший прессинг от {att_team}! <b>{att_p}</b> заставляет ошибиться защитника {def_p} ({def_team})!",
    "⚡ <b>{att_p}</b> ({att_team}) демонстрирует филигранный дриблинг за воротами {def_team} и пасует на пятак!",

    # Курьезы и неудачи
    "🏒 ДРАМА! У <b>{att_p}</b> ({att_team}) трескается пополам клюшка в момент броска по открытому углу ворот!",
    "🌪 Неприятная осечка! <b>{att_p}</b> ({att_team}) теряет равновесие у синей линии и упускает шайбу!",
    "🏒 Шайба встала на ребро и предательски сходит с крюка нападающего <b>{att_p}</b> ({att_team})!",
    "🌪 Арбитр случайно задел шайбу коньком у борта, сорвав перспективную атаку команды {att_team}!",
    "🏒 <b>{att_p}</b> ({att_team}) щелкнул по воротам, но шайба угодила в коньки своему же партнеру по звену!",

    # Удаления и штрафное время
    "⏱ СВИСТОК СУДЬИ! Задержка клюшкой! <b>{def_p}</b> ({def_team}) удален на 2 минуты! Численное большинство у {att_team}!",
    "⏱ <b>{def_p}</b> ({def_team}) грубо нарушает правила против <b>{att_p}</b>! {att_team} получает шанс в большинстве!",
    "⏱ Опасная игра высоко поднятой клюшкой со стороны <b>{def_p}</b> ({def_team})! 2 минуты штрафа!",
    "⏱ <b>{def_p}</b> ({def_team}) отправляется на скамейку штрафников за блокировку игрока <b>{att_p}</b> ({att_team})!"
]

# Ясно и красиво расписанные шаблоны голов
GOAL_EVENTS = [
    "🚨 <b>ГООООЛ забивает {att_team}!</b>\n   🎯 <b>{att_p}</b> ювелирным кистевым выстрелом в девятку прошивает вратаря {def_g} ({def_team})!",
    "🚨 <b>ГООООЛ в ворота {def_team}!</b>\n   💥 <b>{att_p}</b> ({att_team}) мощнейшим щелчком в касание вонзает шайбу мимо {def_g}!",
    "🚨 <b>ГООООЛ! {att_team} выходит вперед!</b>\n   🏒 <b>{att_p}</b> продавливает пятак и с мясом заталкивает шайбу за спину вратарю {def_g} ({def_team})!",
    "🚨 <b>ГООООЛ забивает {att_team}!</b>\n   🌪 <b>{att_p}</b> на паузе обыгрывает голкипера {def_g} ({def_team}) и хладнокровно закатывает шайбу в пустые ворота!",
    "🚨 <b>ГООООЛ! Шайба в сетке ворот {def_team}!</b>\n   ⚡ <b>{att_p}</b> ({att_team}) наносит пушечный бросок под перекладину, {def_g} бессилен!",
    "🚨 <b>ГООООЛ забивает {att_team}!</b>\n   🎯 <b>{att_p}</b> идеально замыкает диагональную передачу на дальнюю штангу ворот {def_team}!"
]

def simulate_match(team1_name: str, ovr1: float, team2_name: str, ovr2: float, t1_roster: List[dict], t2_roster: List[dict]) -> dict:
    diff = ovr1 - ovr2
    prob1 = 0.50 + (diff * 0.02)
    prob1 = max(0.20, min(0.80, prob1))

    # Goals per period
    p1_t1, p1_t2 = random.choices([0, 1, 2], weights=[0.55, 0.35, 0.10])[0], random.choices([0, 1, 2], weights=[0.55, 0.35, 0.10])[0]
    p2_t1, p2_t2 = random.choices([0, 1, 2], weights=[0.50, 0.35, 0.15])[0], random.choices([0, 1, 2], weights=[0.50, 0.35, 0.15])[0]
    p3_t1, p3_t2 = random.choices([0, 1, 2], weights=[0.50, 0.35, 0.15])[0], random.choices([0, 1, 2], weights=[0.50, 0.35, 0.15])[0]

    if random.random() < prob1:
        if random.random() < 0.45:
            p2_t1 += 1
    else:
        if random.random() < 0.45:
            p2_t2 += 1

    score1 = p1_t1 + p2_t1 + p3_t1
    score2 = p1_t2 + p2_t2 + p3_t2

    overtime = False
    shootout = False
    ot_goal_by = None
    ot_scorer = None
    winner = None

    if score1 == score2:
        overtime = True
        ot_roll = random.random()
        if ot_roll < 0.50:
            if random.random() < prob1:
                score1 += 1
                ot_goal_by = team1_name
                ot_scorer = random.choice([c["name"] for c in t1_roster if c.get("position") == "Skater"] or [t1_roster[0]["name"]])
                winner = team1_name
            else:
                score2 += 1
                ot_goal_by = team2_name
                ot_scorer = random.choice([c["name"] for c in t2_roster if c.get("position") == "Skater"] or [t2_roster[0]["name"]])
                winner = team2_name
        else:
            shootout = True
            so_win = 1 if random.random() < prob1 else 2
            if so_win == 1:
                score1 += 1
                ot_scorer = random.choice([c["name"] for c in t1_roster if c.get("position") == "Skater"] or [t1_roster[0]["name"]])
                winner = team1_name
            else:
                score2 += 1
                ot_scorer = random.choice([c["name"] for c in t2_roster if c.get("position") == "Skater"] or [t2_roster[0]["name"]])
                winner = team2_name
    else:
        winner = team1_name if score1 > score2 else team2_name

    def get_skaters_and_goalie(roster: List[dict]):
        g = next((c["name"] for c in roster if "goalie" in c.get("position", "").lower() or "врат" in c.get("position", "").lower()), roster[0]["name"])
        skaters = [c["name"] for c in roster if c["name"] != g] or [roster[0]["name"]]
        return skaters, g

    t1_skaters, t1_g = get_skaters_and_goalie(t1_roster)
    t2_skaters, t2_g = get_skaters_and_goalie(t2_roster)

    # 4 events per period
    def generate_period_timeline(p_num: int, goals_t1: int, goals_t2: int):
        start_min = (p_num - 1) * 20 + 1
        end_min = p_num * 20
        mins = sorted(random.sample(range(start_min, end_min + 1), 4))
        events = []

        g1_left = goals_t1
        g2_left = goals_t2

        for m in mins:
            is_goal = False
            is_t1_att = (random.random() < 0.5)

            if g1_left > 0 and (is_t1_att or g2_left == 0):
                is_goal = True
                is_t1_att = True
                g1_left -= 1
            elif g2_left > 0 and (not is_t1_att or g1_left == 0):
                is_goal = True
                is_t1_att = False
                g2_left -= 1

            if is_t1_att:
                att_t = team1_name
                def_t = team2_name
                att_p = random.choice(t1_skaters)
                def_p = random.choice(t2_skaters)
                def_g = t2_g
            else:
                att_t = team2_name
                def_t = team1_name
                att_p = random.choice(t2_skaters)
                def_p = random.choice(t1_skaters)
                def_g = t1_g

            if is_goal:
                tmpl = random.choice(GOAL_EVENTS)
            else:
                tmpl = random.choice(MATCH_EVENTS_CATALOG)

            desc = tmpl.format(
                att_team=att_t,
                def_team=def_t,
                att_p=att_p,
                def_p=def_p,
                def_g=def_g
            )
            events.append((m, desc, is_goal, att_t))
        return events

    p1_events = generate_period_timeline(1, p1_t1, p1_t2)
    p2_events = generate_period_timeline(2, p2_t1, p2_t2)
    p3_events = generate_period_timeline(3, p3_t1, p3_t2)

    return {
        "score1": score1,
        "score2": score2,
        "p1": (p1_t1, p1_t2),
        "p2": (p2_t1, p2_t2),
        "p3": (p3_t1, p3_t2),
        "p1_events": p1_events,
        "p2_events": p2_events,
        "p3_events": p3_events,
        "overtime": overtime,
        "shootout": shootout,
        "ot_goal_by": ot_goal_by,
        "ot_scorer": ot_scorer,
        "winner": winner
    }

# ==================== MATCH ENGINE & QUEUE ====================
class MatchQueue:
    def __init__(self):
        self.queue: Dict[int, Tuple[asyncio.Event, dict]] = {}
        self.lock = asyncio.Lock()

    async def add_player(self, user_id: int, user_data: dict) -> Tuple[bool, Optional[dict]]:
        async with self.lock:
            if user_id in self.queue:
                return False, None
            if self.queue:
                opponent_id, (event, opp_data) = next(iter(self.queue.items()))
                del self.queue[opponent_id]
                opp_data["matched_with"] = user_data
                event.set()
                return True, opp_data

            event = asyncio.Event()
            self.queue[user_id] = (event, user_data)
            return False, None

    async def wait_or_cancel(self, user_id: int, timeout: float = 45.0) -> Optional[dict]:
        event, data = self.queue.get(user_id, (None, None))
        if not event:
            return None
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return data.get("matched_with")
        except asyncio.TimeoutError:
            async with self.lock:
                self.queue.pop(user_id, None)
            return None

match_queue = MatchQueue()

# ==================== BOT SETUP & MIDDLEWARE ====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class GroupCheckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        if not isinstance(event, Message) or not event.from_user:
            return await handler(event, data)

        if event.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
            if db.pool:
                try:
                    async with db.pool.acquire() as conn:
                        user = await conn.fetchrow("SELECT team_name FROM users WHERE user_id = $1", event.from_user.id)
                        if not user or not user["team_name"]:
                            bot_user = await event.bot.get_me()
                            kb = InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="🏒 Начать играть в ЛС", url=f"https://t.me/{bot_user.username}?start=reg")]
                            ])
                            await event.reply(
                                f"⚠️ {event.from_user.first_name}, вы еще не создали команду!\n"
                                f"Перейдите в личные сообщения с ботом.",
                                reply_markup=kb
                            )
                            return
                except Exception as e:
                    logger.error(f"Middleware DB check error: {e}")
        return await handler(event, data)

dp.message.middleware(GroupCheckMiddleware())

# ==================== KEYBOARDS ====================
def main_menu_keyboard(is_private: bool = True) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="⚔️ Начать поиск игры", callback_data="search_game")],
        [InlineKeyboardButton(text="🔍 Найти карточку", callback_data="claim_daily_btn"),
         InlineKeyboardButton(text="💼 Работы", callback_data="jobs_menu")],
        [InlineKeyboardButton(text="🏒 Состав (4+1)", callback_data="view_roster"),
         InlineKeyboardButton(text="🏪 Рынок карточек", callback_data="market_menu")],
        [InlineKeyboardButton(text="🎁 Ежедневные призы", callback_data="streak_rewards"),
         InlineKeyboardButton(text="🎰 Казино", callback_data="casino_menu")],
        [InlineKeyboardButton(text="📦 Магазин паков", callback_data="pack_shop"),
         InlineKeyboardButton(text="🏆 Рейтинг MMR", callback_data="leaderboard_mmr")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="enter_promo")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def cis_countries_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for c in CIS_COUNTRIES:
        row.append(InlineKeyboardButton(text=c, callback_data=f"setcountry:{c}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Назначить начальных игроков", callback_data="adm_starters")],
        [InlineKeyboardButton(text="⚡ Автодобавление карточек", callback_data="adm_card_batch")],
        [InlineKeyboardButton(text="➕ Создать карточку (Вручную)", callback_data="adm_card_manual")],
        [InlineKeyboardButton(text="🏒 Команды (с фото)", callback_data="adm_teams"),
         InlineKeyboardButton(text="📂 Коллекции", callback_data="adm_collections")],
        [InlineKeyboardButton(text="📦 Создать пак", callback_data="adm_packs"),
         InlineKeyboardButton(text="🎟 Создать промокод", callback_data="adm_promo")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])

def jobs_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏒 Тренер бросков (КД 2ч)", callback_data="job_coach")],
        [InlineKeyboardButton(text="🕵️ Нелегал (Дела с риском)", callback_data="job_illegal_menu")],
        [InlineKeyboardButton(text="📸 Спортивный фотограф (КД 6ч)", callback_data="job_photo")],
        [InlineKeyboardButton(text="🎟 Продавец атрибутики (КД 2ч)", callback_data="job_merch_menu")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])

def illegal_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏦 Ограбить банк (100k RPL)", callback_data="do_illegal:bank")],
        [InlineKeyboardButton(text="👛 Украсть кошелёк (до 10k RPL)", callback_data="do_illegal:wallet")],
        [InlineKeyboardButton(text="🛒 Украсть продукты (до 15k RPL)", callback_data="do_illegal:shop")],
        [InlineKeyboardButton(text="🔙 К работам", callback_data="jobs_menu")]
    ])

def merch_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏷 Дешёвая цена (65% шанс | 2 000–3 000)", callback_data="do_merch:cheap")],
        [InlineKeyboardButton(text="🏷 Средняя цена (30% шанс | 4 000–6 000)", callback_data="do_merch:medium")],
        [InlineKeyboardButton(text="🏷 Дорогая цена (15% шанс | 8 000–12 000)", callback_data="do_merch:expensive")],
        [InlineKeyboardButton(text="🔙 К работам", callback_data="jobs_menu")]
    ])

def casino_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🦅 Орёл и Решка (/coin)", callback_data="casino_coin")],
        [InlineKeyboardButton(text="✂️ Камень-Ножницы-Бумага (/rps)", callback_data="casino_rps")],
        [InlineKeyboardButton(text="🎰 Слоты (/slots)", callback_data="casino_slots")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])

# ==================== HELPER: GET USER STATS & ROSTER 4+1 ====================
async def get_user_roster_ovr(user_id: int) -> Tuple[float, Dict[str, Optional[dict]], List[dict]]:
    async with db.pool.acquire() as conn:
        all_cards_records = await conn.fetch("""
            SELECT c.*, uc.id as user_card_id, uc.in_lineup, uc.slot,
                   COALESCE(c.photo_id, t.photo_id) AS effective_photo
            FROM user_cards uc
            JOIN cards c ON uc.card_id = c.id
            LEFT JOIN teams t ON c.team_name = t.name
            WHERE uc.user_id = $1
            ORDER BY c.ovr DESC
        """, user_id)

    all_cards = [dict(r) for r in all_cards_records]
    lineup_dict: Dict[str, Optional[dict]] = {slot: None for slot in ROSTER_SLOTS}

    for c in all_cards:
        if c.get("slot") in lineup_dict and lineup_dict[c["slot"]] is None:
            lineup_dict[c["slot"]] = c

    used_ids = {c["id"] for c in lineup_dict.values() if c}
    for slot in ["skater_1", "skater_2", "skater_3", "skater_4"]:
        if lineup_dict[slot] is None:
            for c in all_cards:
                if c["position"] == "Skater" and c["id"] not in used_ids:
                    lineup_dict[slot] = c
                    used_ids.add(c["id"])
                    break

    if lineup_dict["goalie"] is None:
        for c in all_cards:
            if c["position"] == "Goalie" and c["id"] not in used_ids:
                lineup_dict["goalie"] = c
                used_ids.add(c["id"])
                break

    filled_cards = [c for c in lineup_dict.values() if c]
    avg_ovr = round(sum(c["ovr"] for c in filled_cards) / len(filled_cards), 1) if filled_cards else 50.0
    return avg_ovr, lineup_dict, all_cards

def render_profile_text(user: dict, full_name: str) -> str:
    lvl, cur_xp, req_xp = calculate_level_and_progress(user["exp"] or 0)
    p_name = html.escape(user["player_name"] or full_name or "Хоккеист")
    team_n = html.escape(user["team_name"] or "Без команды")
    country_n = html.escape(user["country"] or "Не указана")
    coins = user["rpl_coins"] if user["rpl_coins"] is not None else 1000
    mmr = user["mmr"] if user["mmr"] is not None else 500

    return (
        "🇷🇺 <b>Russian Puck League Bot!</b>\n\n"
        f"👤 <b>Игрок:</b> {p_name}\n"
        f"🏒 <b>Команда:</b> {team_n}\n"
        f"🌍 <b>Страна:</b> {country_n}\n"
        f"🎖 <b>Уровень:</b> {lvl}\n"
        f"⭐️ <b>Опыт:</b> {cur_xp}/{req_xp} XP\n"
        f"💰 <b>Баланс:</b> {coins} RPLCoin\n"
        f"🏆 <b>MMR:</b> {mmr}\n"
    )

# ==================== /START COMMAND ====================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    try:
        await state.clear()
        user_id = message.from_user.id
        user_name = message.from_user.full_name or message.from_user.username or "Хоккеист"
        username = message.from_user.username

        async with db.pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

        if user and user["team_name"]:
            async with db.pool.acquire() as conn:
                await conn.execute("UPDATE users SET player_name = $1, username = $2 WHERE user_id = $3", user_name, username, user_id)
                c_cnt = await conn.fetchval("SELECT COUNT(*) FROM user_cards WHERE user_id = $1", user_id)
                if c_cnt < 5:
                    await grant_starter_cards_for_user(conn, user_id)
                user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

            is_private = (message.chat.type == ChatType.PRIVATE)
            text = render_profile_text(user, user_name)
            await message.answer(text, reply_markup=main_menu_keyboard(is_private), parse_mode=ParseMode.HTML)
        else:
            await state.set_state(RegState.entering_team_name)
            await message.answer(
                "👋 <b>Добро пожаловать в Russian Puck League!</b>\n\n"
                "Вы еще не зарегистрировали свою хоккейную команду.\n"
                "Придумайте и отправьте <b>название вашей команды</b>:",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Error in /start handler: {e}", exc_info=True)
        await message.answer("⚠️ Произошла ошибка. Попробуйте еще раз через несколько секунд.")

@dp.message(RegState.entering_team_name)
async def process_team_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2 or len(name) > 35:
        await message.answer("⚠️ Название команды должно быть от 2 до 35 символов. Попробуйте еще раз:")
        return

    await state.update_data(team_name=name)
    await state.set_state(RegState.choosing_country)
    await message.answer(
        f"Отличное имя команды: <b>{html.escape(name)}</b>!\nТеперь выберите вашу страну из списка СНГ:",
        reply_markup=cis_countries_keyboard(),
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(RegState.choosing_country, F.data.startswith("setcountry:"))
async def process_country_choice(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("setcountry:")[1]
    data = await state.get_data()
    team_name = data["team_name"]
    user_id = callback.from_user.id
    player_name = callback.from_user.full_name or "Хоккеист"
    username = callback.from_user.username

    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, team_name, country, player_name, username, mmr, rpl_coins, exp)
            VALUES ($1, $2, $3, $4, $5, 500, 1000, 50)
            ON CONFLICT (user_id) DO UPDATE SET team_name = $2, country = $3, player_name = $4, username = $5
        """, user_id, team_name, country, player_name, username)

        await grant_starter_cards_for_user(conn, user_id)
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    await state.clear()
    profile_text = render_profile_text(user, player_name)
    await callback.message.edit_text(
        f"🎉 <b>Команда успешно зарегистрирована!</b>\n"
        f"Вам выданы 5 стартовых карточек полного звена (4 полевых + 1 вратарь)!\n\n{profile_text}",
        reply_markup=main_menu_keyboard(callback.message.chat.type == ChatType.PRIVATE),
        parse_mode=ParseMode.HTML
    )

# ==================== MAIN MENU NAVIGATION ====================
@dp.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    if not user:
        return

    text = render_profile_text(user, callback.from_user.full_name)
    is_private = (callback.message.chat.type == ChatType.PRIVATE)
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=main_menu_keyboard(is_private), parse_mode=ParseMode.HTML)
    else:
        await callback.message.edit_text(text, reply_markup=main_menu_keyboard(is_private), parse_mode=ParseMode.HTML)

# ==================== /CARD COMMAND & CLAIM LOGIC ====================
async def execute_card_claim(user_id: int) -> Tuple[bool, str, Optional[str]]:
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not user or not user["team_name"]:
            return False, "⚠️ Сначала напишите /start и зарегистрируйте команду!", None

        now = datetime.utcnow()
        if user["last_daily_claim"]:
            diff = now - user["last_daily_claim"]
            if diff < timedelta(hours=12):
                remaining = timedelta(hours=12) - diff
                hours, rem = divmod(int(remaining.total_seconds()), 3600)
                minutes, _ = divmod(rem, 60)
                return False, f"⏳ Карточка уже открывалась! До следующей попытки осталось: <b>{hours}ч {minutes}м</b>.", None

        if random.random() < 0.05:
            await conn.execute("UPDATE users SET last_daily_claim = $1, notified_cd = FALSE WHERE user_id = $2", now, user_id)
            return True, "💨 <b>Вам выпало:</b> Ничего!\nУвы, удача отвернулась в этот раз. Возвращайтесь через 12 часов!", None

        r = random.random()
        cumulative = 0.0
        chosen_rarity = "Редкая"
        for rarity, chance in DAILY_RARITIES_CHANCES.items():
            cumulative += chance
            if r <= cumulative:
                chosen_rarity = rarity
                break

        available_cards = await conn.fetch("""
            SELECT c.*, COALESCE(c.photo_id, t.photo_id) AS effective_photo
            FROM cards c
            LEFT JOIN teams t ON c.team_name = t.name
            WHERE c.rarity = $1 
              AND c.id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $2)
        """, chosen_rarity, user_id)

        if not available_cards:
            available_cards = await conn.fetch("""
                SELECT c.*, COALESCE(c.photo_id, t.photo_id) AS effective_photo
                FROM cards c
                LEFT JOIN teams t ON c.team_name = t.name
                WHERE c.rarity != 'Секретная' 
                  AND c.id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $1)
            """, user_id)

        if not available_cards:
            return False, "🏆 Невероятно! Вы собрали абсолютно все доступные карточки в игре!", None

        card = random.choice(available_cards)
        await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, card["id"])
        await conn.execute("UPDATE users SET last_daily_claim = $1, notified_cd = FALSE WHERE user_id = $2", now, user_id)

    caption = (
        f"🎁 <b>Вам выпала новая карточка!</b>\n\n"
        f"🌟 <b>{html.escape(card['name'])}</b> (OVR: {card['ovr']})\n"
        f"Позиция: <b>{html.escape(card['position'])}</b> | Редкость: <b>{html.escape(card['rarity'])}</b>\n"
        f"Команда: <b>{html.escape(card['team_name'])}</b> | Коллекция: <b>{html.escape(card['collection_name'])}</b>\n"
        f"Страна: <b>{html.escape(card['country'])}</b>\n\n"
        f"✅ Карточка сохранена в вашей коллекции!"
    )
    return True, caption, card["effective_photo"]

@dp.message(Command("card"))
async def cmd_card(message: Message):
    success, text, photo_id = await execute_card_claim(message.from_user.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
    if photo_id:
        await message.answer_photo(photo=photo_id, caption=text, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.callback_query(F.data == "claim_daily_btn")
async def cb_claim_daily(callback: CallbackQuery):
    success, text, photo_id = await execute_card_claim(callback.from_user.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
    if photo_id:
        await callback.message.delete()
        await callback.message.answer_photo(photo=photo_id, caption=text, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

# ==================== BACKGROUND 12-HOUR CD NOTIFIER ====================
async def card_cooldown_notifier():
    logger.info("Card cooldown background notifier started.")
    while True:
        try:
            await asyncio.sleep(45)
            if not db.pool:
                continue

            async with db.pool.acquire() as conn:
                ready_users = await conn.fetch("""
                    SELECT user_id, player_name 
                    FROM users 
                    WHERE last_daily_claim IS NOT NULL 
                      AND last_daily_claim <= NOW() - INTERVAL '12 hours'
                      AND (notified_cd IS FALSE OR notified_cd IS NULL)
                """)

                for u in ready_users:
                    u_id = u["user_id"]
                    try:
                        kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🔍 Найти карточку", callback_data="claim_daily_btn")]
                        ])
                        await bot.send_message(
                            chat_id=u_id,
                            text=(
                                "⏰ <b>Кулдаун на карточку прошел!</b>\n\n"
                                "Прошло 12 часов с момента последнего открытия. "
                                "Ваша новая карточка уже ждет вас на льду!\n\n"
                                "👉 Нажмите кнопку <b>«Найти карточку»</b> ниже или введите команду <b>/card</b>:"
                            ),
                            reply_markup=kb,
                            parse_mode=ParseMode.HTML
                        )
                        await conn.execute("UPDATE users SET notified_cd = TRUE WHERE user_id = $1", u_id)
                    except Exception as send_err:
                        logger.warning(f"Failed to send cooldown notification to {u_id}: {send_err}")
                        await conn.execute("UPDATE users SET notified_cd = TRUE WHERE user_id = $1", u_id)
        except Exception as e:
            logger.error(f"Error in card_cooldown_notifier loop: {e}")
            await asyncio.sleep(30)

# ==================== 7-DAY STREAK REWARDS ====================
@dp.callback_query(F.data == "streak_rewards")
async def cb_streak_rewards(callback: CallbackQuery):
    user_id = callback.from_user.id
    now = datetime.utcnow()

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT streak_days, last_streak_claim FROM users WHERE user_id = $1", user_id)
        current_streak = user["streak_days"] or 0
        last_claim = user["last_streak_claim"]

        can_claim = False
        streak_to_claim = 1

        if last_claim is None:
            can_claim = True
            streak_to_claim = 1
        else:
            diff = now - last_claim
            if diff < timedelta(hours=24):
                rem = timedelta(hours=24) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Вы уже забрали награду сегодня! Ждите {h}ч {m}м.", show_alert=True)
                return
            elif diff <= timedelta(hours=48):
                can_claim = True
                streak_to_claim = 1 if current_streak >= 7 else current_streak + 1
            else:
                can_claim = True
                streak_to_claim = 1

        reward_info = STREAK_REWARDS[streak_to_claim]
        if reward_info["type"] == "coins":
            await conn.execute("""
                UPDATE users 
                SET rpl_coins = rpl_coins + $1, streak_days = $2, last_streak_claim = $3 
                WHERE user_id = $4
            """, reward_info["amount"], streak_to_claim, now, user_id)
            bonus_str = f"Вам начислено <b>{reward_info['amount']} RPLCoin</b>!"
            card_photo = None
        else:
            min_o, max_o = reward_info["min_ovr"], reward_info["max_ovr"]
            free_cards = await conn.fetch("""
                SELECT c.*, COALESCE(c.photo_id, t.photo_id) AS effective_photo
                FROM cards c
                LEFT JOIN teams t ON c.team_name = t.name
                WHERE c.ovr BETWEEN $1 AND $2 
                  AND c.id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $3)
            """, min_o, max_o, user_id)

            if free_cards:
                c = random.choice(free_cards)
                await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, c["id"])
                bonus_str = f"Вам выпала карточка <b>{html.escape(c['name'])}</b> (OVR: {c['ovr']}, {c['rarity']})!"
                card_photo = c["effective_photo"]
            else:
                await conn.execute("UPDATE users SET rpl_coins = rpl_coins + 15000 WHERE user_id = $1", user_id)
                bonus_str = "Все карточки этого диапазона уже у вас! Начислена компенсация <b>15 000 RPLCoin</b>."
                card_photo = None

            await conn.execute("UPDATE users SET streak_days = $1, last_streak_claim = $2 WHERE user_id = $3", streak_to_claim, now, user_id)

    schedule_text = (
        "📅 <b>КАЛЕНДАРЬ ЕЖЕДНЕВНЫХ ПРИЗОВ (7 ДНЕЙ):</b>\n"
        f"{'🟢' if streak_to_claim == 1 else '⚪️'} День 1: 2 500 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 2 else '⚪️'} День 2: 5 000 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 3 else '⚪️'} День 3: 7 500 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 4 else '⚪️'} День 4: Карточка 75-85 OVR\n"
        f"{'🟢' if streak_to_claim == 5 else '⚪️'} День 5: 25 000 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 6 else '⚪️'} День 6: 35 000 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 7 else '⚪️'} День 7: Карточка 80-88 OVR\n\n"
        f"🎉 <b>Получена награда за День {streak_to_claim}!</b>\n"
        f"{bonus_str}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]])
    if card_photo:
        await callback.message.delete()
        await callback.message.answer_photo(photo=card_photo, caption=schedule_text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await callback.message.edit_text(schedule_text, reply_markup=kb, parse_mode=ParseMode.HTML)

# ==================== ROSTER 4+1 ====================
@dp.callback_query(F.data == "view_roster")
async def cb_view_roster(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_pm = (callback.message.chat.type == ChatType.PRIVATE)
    avg_ovr, lineup, all_cards = await get_user_roster_ovr(user_id)

    text = (
        f"🏒 <b>СОСТАВ КОМАНДЫ (Формат 4+1)</b>\n"
        f"Средний OVR активного звена: <b>{avg_ovr}</b>\n\n"
        f"<b>Текущее звено на льду:</b>\n"
    )
    for slot in ROSTER_SLOTS:
        c = lineup.get(slot)
        if c:
            text += f"• <b>{SLOT_NAMES[slot]}:</b> {html.escape(c['name'])} (OVR {c['ovr']}, {c['rarity']})\n"
        else:
            text += f"• <b>{SLOT_NAMES[slot]}:</b> <i>[Пусто]</i>\n"

    buttons = []
    if is_pm:
        text += "\n🛠 <i>Нажмите на позицию ниже, чтобы выбрать или заменить игрока:</i>"
        for slot in ROSTER_SLOTS:
            c = lineup.get(slot)
            cur_title = f"{c['name']} ({c['ovr']})" if c else "Выбрать игрока"
            buttons.append([InlineKeyboardButton(
                text=f"{SLOT_NAMES[slot]}: {cur_title}",
                callback_data=f"pick_slot:{slot}"
            )])
    else:
        bot_user = await bot.get_me()
        buttons.append([InlineKeyboardButton(text="✏️ Редактировать состав в ЛС", url=f"https://t.me/{bot_user.username}?start=roster")])

    buttons.append([InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("pick_slot:"))
async def cb_pick_slot(callback: CallbackQuery):
    slot = callback.data.split("pick_slot:")[1]
    user_id = callback.from_user.id
    req_pos = "Goalie" if slot == "goalie" else "Skater"

    async with db.pool.acquire() as conn:
        cards = await conn.fetch("""
            SELECT c.*, uc.id as user_card_id, uc.slot 
            FROM user_cards uc
            JOIN cards c ON uc.card_id = c.id
            WHERE uc.user_id = $1 AND c.position = $2
            ORDER BY c.ovr DESC
        """, user_id, req_pos)

    buttons = []
    for c in cards:
        status_ico = f" (Занят: {SLOT_NAMES.get(c['slot'], '')})" if c['slot'] else ""
        btn_text = f"{c['name']} (OVR {c['ovr']}){status_ico}"
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"set_slot:{slot}:{c['user_card_id']}"
        )])

    buttons.append([InlineKeyboardButton(text="🔙 Назад к составу", callback_data="view_roster")])
    await callback.message.edit_text(
        f"🏒 <b>Выбор игрока на позицию: {SLOT_NAMES[slot]}</b>\n"
        f"Требуемая позиция: <b>{req_pos}</b>\n\n"
        f"Выберите карточку из доступных:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data.startswith("set_slot:"))
async def cb_set_slot(callback: CallbackQuery):
    parts = callback.data.split(":")
    slot = parts[1]
    uc_id = int(parts[2])
    user_id = callback.from_user.id

    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE user_cards SET slot = NULL, in_lineup = FALSE WHERE user_id = $1 AND slot = $2", user_id, slot)
        await conn.execute("UPDATE user_cards SET slot = $1, in_lineup = TRUE WHERE user_id = $2 AND id = $3", slot, user_id, uc_id)

    await cb_view_roster(callback)

# ==================== MARKETPLACE / ТОРГОВАЯ ПЛОЩАДКА ====================
@dp.message(Command("market"))
async def cmd_market(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Купить карточки", callback_data="market_browse:0")],
        [InlineKeyboardButton(text="🏷 Выставить карточку на продажу", callback_data="market_sell_list")],
        [InlineKeyboardButton(text="📦 Мои лоты на продаже", callback_data="market_my_listings")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await message.answer(
        "🏪 <b>ТОРГОВАЯ ПЛОЩАДКА RPL</b>\n\n"
        "Здесь игроки могут свободно покупать и продавать любые карточки за RPLCoin!\n\n"
        "Выберите раздел:",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == "market_menu")
async def cb_market_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Купить карточки", callback_data="market_browse:0")],
        [InlineKeyboardButton(text="🏷 Выставить карточку на продажу", callback_data="market_sell_list")],
        [InlineKeyboardButton(text="📦 Мои лоты на продаже", callback_data="market_my_listings")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    text = (
        "🏪 <b>ТОРГОВАЯ ПЛОЩАДКА RPL</b>\n\n"
        "Здесь игроки могут свободно покупать и продавать любые карточки за RPLCoin!\n\n"
        "Выберите раздел:"
    )
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("market_browse:"))
async def cb_market_browse(callback: CallbackQuery):
    page = int(callback.data.split(":")[1])
    page_size = 5
    offset = page * page_size

    async with db.pool.acquire() as conn:
        listings = await conn.fetch("""
            SELECT m.id as listing_id, m.price, m.seller_id, 
                   c.name, c.ovr, c.rarity, c.position, c.team_name,
                   u.player_name, u.team_name as seller_team
            FROM market_listings m
            JOIN cards c ON m.card_id = c.id
            JOIN users u ON m.seller_id = u.user_id
            WHERE m.seller_id != $1
            ORDER BY m.created_at DESC
            LIMIT $2 OFFSET $3
        """, callback.from_user.id, page_size + 1, offset)

    has_next = len(listings) > page_size
    current_listings = listings[:page_size]

    if not current_listings:
        text = "🛒 <b>ТОРГОВАЯ ПЛОЩАДКА — ЛОТЫ</b>\n\nНа данный момент активных лотов от других игроков нет."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏷 Выставить свою карту", callback_data="market_sell_list")],
            [InlineKeyboardButton(text="🔙 Назад к рынку", callback_data="market_menu")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    text = f"🛒 <b>ТОРГОВАЯ ПЛОЩАДКА (Стр. {page + 1})</b>\nНажмите на лот для покупки:\n\n"
    buttons = []
    for l in current_listings:
        btn_title = f"{l['name']} (OVR {l['ovr']}) — 💰 {l['price']} RPL"
        buttons.append([InlineKeyboardButton(text=btn_title, callback_data=f"market_view:{l['listing_id']}")])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"market_browse:{page - 1}"))
    if has_next:
        nav_row.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"market_browse:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(text="🔙 Назад к рынку", callback_data="market_menu")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("market_view:"))
async def cb_market_view(callback: CallbackQuery):
    listing_id = int(callback.data.split(":")[1])
    async with db.pool.acquire() as conn:
        item = await conn.fetchrow("""
            SELECT m.id as listing_id, m.price, m.seller_id, m.card_id,
                   c.name, c.ovr, c.rarity, c.position, c.team_name, c.country,
                   COALESCE(c.photo_id, t.photo_id) AS effective_photo,
                   u.player_name, u.team_name as seller_team
            FROM market_listings m
            JOIN cards c ON m.card_id = c.id
            LEFT JOIN teams t ON c.team_name = t.name
            JOIN users u ON m.seller_id = u.user_id
            WHERE m.id = $1
        """, listing_id)

    if not item:
        await callback.answer("Этот лот уже продан или снят с продажи!", show_alert=True)
        return

    text = (
        f"🏷 <b>ЛОТ #{item['listing_id']}</b>\n\n"
        f"🌟 <b>{html.escape(item['name'])}</b> (OVR: {item['ovr']})\n"
        f"Позиция: <b>{html.escape(item['position'])}</b> | Редкость: <b>{html.escape(item['rarity'])}</b>\n"
        f"Команда игрока: <b>{html.escape(item['team_name'])}</b> | Страна: <b>{html.escape(item['country'])}</b>\n\n"
        f"👤 Продавец: <b>{html.escape(item['player_name'] or 'Хоккеист')}</b> ({html.escape(item['seller_team'] or 'RPL')})\n"
        f"💰 Цена: <b>{item['price']} RPLCoin</b>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Купить за {item['price']} RPL", callback_data=f"market_buy:{item['listing_id']}")],
        [InlineKeyboardButton(text="🔙 К списку лотов", callback_data="market_browse:0")]
    ])

    if item["effective_photo"]:
        await callback.message.delete()
        await callback.message.answer_photo(photo=item["effective_photo"], caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("market_buy:"))
async def cb_market_buy(callback: CallbackQuery):
    listing_id = int(callback.data.split(":")[1])
    buyer_id = callback.from_user.id

    async with db.pool.acquire() as conn:
        async with conn.transaction():
            listing = await conn.fetchrow("SELECT * FROM market_listings WHERE id = $1 FOR UPDATE", listing_id)
            if not listing:
                await callback.answer("❌ Лот больше недоступен!", show_alert=True)
                return

            if listing["seller_id"] == buyer_id:
                await callback.answer("❌ Вы не можете купить собственную карточку!", show_alert=True)
                return

            buyer = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1 FOR UPDATE", buyer_id)
            price = listing["price"]

            if buyer["rpl_coins"] < price:
                await callback.answer(f"❌ Недостаточно средств! Нужно: {price} RPL, у вас: {buyer['rpl_coins']} RPL.", show_alert=True)
                return

            has_card = await conn.fetchrow("SELECT 1 FROM user_cards WHERE user_id = $1 AND card_id = $2", buyer_id, listing["card_id"])
            if has_card:
                await callback.answer("❌ У вас уже есть эта карточка в коллекции!", show_alert=True)
                return

            await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", price, buyer_id)
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", price, listing["seller_id"])
            await conn.execute("DELETE FROM market_listings WHERE id = $1", listing_id)
            await conn.execute("INSERT INTO user_cards (user_id, card_id, in_lineup) VALUES ($1, $2, FALSE)", buyer_id, listing["card_id"])

    try:
        await bot.send_message(
            chat_id=listing["seller_id"],
            text=f"🎉 <b>Ваша карточка успешно продана на рынке!</b>\nНа ваш баланс зачислено: <b>+{price} RPLCoin</b>!",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

    await callback.answer("🎉 Карточка успешно куплена и добавлена в вашу коллекцию!", show_alert=True)
    await cb_market_browse(callback)

# --- Selling flow ---
@dp.callback_query(F.data == "market_sell_list")
async def cb_market_sell_list(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user_cards = await conn.fetch("""
            SELECT c.*, uc.id as user_card_id, uc.slot
            FROM user_cards uc
            JOIN cards c ON uc.card_id = c.id
            WHERE uc.user_id = $1
            ORDER BY c.ovr DESC
        """, user_id)

    if not user_cards:
        await callback.answer("У вас нет карточек для продажи!", show_alert=True)
        return

    text = "🏷 <b>ВЫСТАВЛЕНИЕ КАРТОЧКИ НА РЫНОК</b>\n\nВыберите карточку, которую хотите продать:"
    buttons = []
    for c in user_cards:
        slot_str = f" [{SLOT_NAMES.get(c['slot'], '')}]" if c['slot'] else ""
        btn_text = f"{c['name']} (OVR {c['ovr']}, {c['rarity']}){slot_str}"
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"market_select_card:{c['id']}")])

    buttons.append([InlineKeyboardButton(text="🔙 Назад к рынку", callback_data="market_menu")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("market_select_card:"))
async def cb_market_select_card(callback: CallbackQuery, state: FSMContext):
    card_id = int(callback.data.split(":")[1])
    await state.update_data(sell_card_id=card_id)
    await state.set_state(MarketSellState.price)

    async with db.pool.acquire() as conn:
        card = await conn.fetchrow("SELECT name, ovr, rarity FROM cards WHERE id = $1", card_id)

    await callback.message.edit_text(
        f"🏷 Продажа карточки: <b>{html.escape(card['name'])}</b> (OVR {card['ovr']}, {card['rarity']})\n\n"
        f"Введите <b>цену в RPLCoin</b>, за которую хотите выставить эту карточку:\n"
        f"<i>(Вы можете указать любую желаемую сумму от 100 RPL)</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="market_menu")]])
    )

@dp.message(MarketSellState.price)
async def process_market_price(message: Message, state: FSMContext):
    val = message.text.strip()
    if not val.isdigit() or int(val) < 100:
        await message.answer("❌ Введите корректную сумму в RPLCoin (минимум 100):")
        return

    price = int(val)
    data = await state.get_data()
    card_id = data.get("sell_card_id")
    user_id = message.from_user.id

    async with db.pool.acquire() as conn:
        uc = await conn.fetchrow("SELECT id FROM user_cards WHERE user_id = $1 AND card_id = $2", user_id, card_id)
        if not uc:
            await message.answer("❌ Карточка не найдена у вас в инвентаре!")
            await state.clear()
            return

        await conn.execute("DELETE FROM user_cards WHERE user_id = $1 AND card_id = $2", user_id, card_id)
        await conn.execute("INSERT INTO market_listings (seller_id, card_id, price) VALUES ($1, $2, $3)", user_id, card_id, price)
        card = await conn.fetchrow("SELECT name, ovr FROM cards WHERE id = $1", card_id)

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 На рынок", callback_data="market_menu")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await message.answer(
        f"✅ Карточка <b>{html.escape(card['name'])}</b> (OVR {card['ovr']}) выставлена на продажу за <b>{price} RPLCoin</b>!\n\n"
        f"Когда другой игрок приобретет ее, вам автоматически начислятся средства.",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

# --- My listings ---
@dp.callback_query(F.data == "market_my_listings")
async def cb_market_my_listings(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        my_items = await conn.fetch("""
            SELECT m.id as listing_id, m.price, c.name, c.ovr, c.rarity, c.id as card_id
            FROM market_listings m
            JOIN cards c ON m.card_id = c.id
            WHERE m.seller_id = $1
            ORDER BY m.created_at DESC
        """, user_id)

    if not my_items:
        text = "📦 <b>МОИ ЛОТЫ НА ПРОДАЖЕ</b>\n\nУ вас сейчас нет активных лотов на рынке."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏷 Выставить карту", callback_data="market_sell_list")],
            [InlineKeyboardButton(text="🔙 Назад к рынку", callback_data="market_menu")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    text = "📦 <b>МОИ ЛОТЫ НА ПРОДАЖЕ</b>\nНажмите на лот, чтобы снять его с продажи и вернуть карту:\n\n"
    buttons = []
    for it in my_items:
        buttons.append([InlineKeyboardButton(
            text=f"❌ Снять: {it['name']} ({it['ovr']}) — {it['price']} RPL",
            callback_data=f"market_cancel:{it['listing_id']}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Назад к рынку", callback_data="market_menu")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("market_cancel:"))
async def cb_market_cancel(callback: CallbackQuery):
    listing_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id

    async with db.pool.acquire() as conn:
        item = await conn.fetchrow("DELETE FROM market_listings WHERE id = $1 AND seller_id = $2 RETURNING card_id", listing_id, user_id)
        if item:
            await conn.execute("INSERT INTO user_cards (user_id, card_id, in_lineup) VALUES ($1, $2, FALSE)", user_id, item["card_id"])

    await callback.answer("Карточка возвращена в вашу коллекцию!", show_alert=True)
    await cb_market_my_listings(callback)

# ==================== CASINO / КАЗИНО ====================
@dp.callback_query(F.data == "casino_menu")
async def cb_casino_menu(callback: CallbackQuery):
    text = (
        "🎰 <b>ХОККЕЙНОЕ КАЗИНО RPL</b>\n\n"
        "Испытайте свою удачу и приумножьте баланс RPLCoin!\n\n"
        "🦅 <b>Орёл и Решка (/coin)</b>\n"
        "• Шанс на победу: <b>20%</b>\n"
        "• Выигрыш: <b>ставка × 4</b>\n\n"
        "✂️ <b>Камень-Ножницы-Бумага (/rps)</b>\n"
        "• Победа (42%): +ставка\n"
        "• Ничья (16%): ставка возвращается\n"
        "• Поражение (42%): -ставка\n\n"
        "🎰 <b>Слоты (/slots)</b>\n"
        "• Символы: 🏒 🥅 ⭐️ 🍒 7️⃣ 💎\n"
        "• 3 редких (7️⃣/💎/⭐️) [3%]: <b>множитель x10</b>\n"
        "• 3 любых одинаковых [7%]: <b>множитель x5</b>\n"
        "• 2 одинаковых [15%]: <b>множитель x2</b>\n"
        "• Проигрыш [75%]\n\n"
        "Выберите игру:"
    )
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=casino_menu_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await callback.message.edit_text(text, reply_markup=casino_menu_keyboard(), parse_mode=ParseMode.HTML)

# --- 1. Eagle & Tails (/coin) ---
@dp.callback_query(F.data == "casino_coin")
async def cb_casino_coin(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CoinState.bet)
    await callback.message.edit_text(
        "🦅 <b>ОРЁЛ И РЕШКА (/coin)</b>\n\n"
        "Введите сумму ставки в RPLCoin:\n"
        "<i>Шанс победы: 20% | Множитель: x4</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]])
    )

@dp.message(Command("coin"))
async def cmd_coin(message: Message, state: FSMContext):
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        bet = int(args[1])
        await process_coin_bet_value(message, state, bet)
    else:
        await state.set_state(CoinState.bet)
        await message.answer(
            "🦅 <b>ОРЁЛ И РЕШКА</b>\n\nВведите сумму ставки в RPLCoin:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]])
        )

@dp.message(CoinState.bet)
async def process_coin_bet_input(message: Message, state: FSMContext):
    if not message.text.strip().isdigit() or int(message.text.strip()) <= 0:
        await message.answer("❌ Введите корректную ставку (число больше нуля):")
        return
    await process_coin_bet_value(message, state, int(message.text.strip()))

async def process_coin_bet_value(message: Message, state: FSMContext, bet: int):
    user_id = message.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", user_id)
        if not user or user["rpl_coins"] < bet:
            await message.answer(f"❌ Недостаточно средств! Ваш баланс: {user['rpl_coins'] if user else 0} RPL.")
            await state.clear()
            return

    await state.update_data(bet=bet)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🦅 Орёл", callback_data="coin_side:eagle"),
            InlineKeyboardButton(text="🪙 Решка", callback_data="coin_side:tails")
        ],
        [InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]
    ])
    await message.answer(f"💰 Ставка: <b>{bet} RPLCoin</b>.\nВыберите сторону монеты:", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("coin_side:"))
async def cb_coin_play(callback: CallbackQuery, state: FSMContext):
    choice = callback.data.split(":")[1]
    choice_str = "🦅 Орёл" if choice == "eagle" else "🪙 Решка"
    data = await state.get_data()
    bet = data.get("bet")
    user_id = callback.from_user.id

    if not bet:
        await callback.answer("Ставка не найдена, начните заново!", show_alert=True)
        return

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", user_id)
        if user["rpl_coins"] < bet:
            await callback.answer("Недостаточно средств на балансе!", show_alert=True)
            await state.clear()
            return

        is_win = (random.random() < 0.20)
        actual_side = choice if is_win else ("tails" if choice == "eagle" else "eagle")
        actual_str = "🦅 Орёл" if actual_side == "eagle" else "🪙 Решка"

        if is_win:
            winnings = bet * 4
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", winnings, user_id)
            res_text = (
                f"🪙 <b>РЕЗУЛЬТАТ БРОСКА:</b> {actual_str}!\n"
                f"Ваш выбор: <b>{choice_str}</b>\n\n"
                f"🎉 <b>ПОБЕДА!</b> Шанс 20% сработал!\n"
                f"💰 Начислено: <b>+{winnings} RPLCoin</b> (ставка x4)!"
            )
        else:
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", bet, user_id)
            res_text = (
                f"🪙 <b>РЕЗУЛЬТАТ БРОСКА:</b> {actual_str}!\n"
                f"Ваш выбор: <b>{choice_str}</b>\n\n"
                f"😢 <b>ПОРАЖЕНИЕ!</b>\n"
                f"💸 Списано: <b>-{bet} RPLCoin</b>."
            )

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сыграть снова", callback_data="casino_coin")],
        [InlineKeyboardButton(text="🎰 В меню казино", callback_data="casino_menu")]
    ])
    await callback.message.edit_text(res_text, reply_markup=kb, parse_mode=ParseMode.HTML)

# --- 2. Rock Paper Scissors (/rps) ---
@dp.callback_query(F.data == "casino_rps")
async def cb_casino_rps(callback: CallbackQuery, state: FSMContext):
    await state.set_state(RpsState.bet)
    await callback.message.edit_text(
        "✂️ <b>КАМЕНЬ-НОЖНИЦЫ-БУМАГА (/rps)</b>\n\n"
        "Введите сумму ставки в RPLCoin:\n"
        "<i>Шансы: 42% победа (+ставка) | 16% ничья | 42% поражение (-ставка)</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]])
    )

@dp.message(Command("rps"))
async def cmd_rps(message: Message, state: FSMContext):
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        await process_rps_bet_value(message, state, int(args[1]))
    else:
        await state.set_state(RpsState.bet)
        await message.answer(
            "✂️ <b>КАМЕНЬ-НОЖНИЦЫ-БУМАГА</b>\n\nВведите сумму ставки в RPLCoin:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]])
        )

@dp.message(RpsState.bet)
async def process_rps_bet_input(message: Message, state: FSMContext):
    if not message.text.strip().isdigit() or int(message.text.strip()) <= 0:
        await message.answer("❌ Введите корректную ставку (число больше нуля):")
        return
    await process_rps_bet_value(message, state, int(message.text.strip()))

async def process_rps_bet_value(message: Message, state: FSMContext, bet: int):
    user_id = message.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", user_id)
        if not user or user["rpl_coins"] < bet:
            await message.answer(f"❌ Недостаточно средств! Ваш баланс: {user['rpl_coins'] if user else 0} RPL.")
            await state.clear()
            return

    await state.update_data(bet=bet)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🪨 Камень", callback_data="rps_choice:rock"),
            InlineKeyboardButton(text="✂️ Ножницы", callback_data="rps_choice:scissors"),
            InlineKeyboardButton(text="📄 Бумага", callback_data="rps_choice:paper")
        ],
        [InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]
    ])
    await message.answer(f"💰 Ставка: <b>{bet} RPLCoin</b>.\nСделайте ваш ход:", reply_markup=kb, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("rps_choice:"))
async def cb_rps_play(callback: CallbackQuery, state: FSMContext):
    user_choice = callback.data.split(":")[1]
    data = await state.get_data()
    bet = data.get("bet")
    user_id = callback.from_user.id

    if not bet:
        await callback.answer("Ставка не найдена, начните заново!", show_alert=True)
        return

    choice_names = {"rock": "🪨 Камень", "scissors": "✂️ Ножницы", "paper": "📄 Бумага"}
    beaten_by = {"rock": "paper", "scissors": "rock", "paper": "scissors"}
    beats = {"rock": "scissors", "scissors": "paper", "paper": "rock"}

    roll = random.random()
    if roll < 0.42:
        outcome = "win"
        bot_choice = beats[user_choice]
    elif roll < 0.58:
        outcome = "draw"
        bot_choice = user_choice
    else:
        outcome = "lose"
        bot_choice = beaten_by[user_choice]

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", user_id)
        if user["rpl_coins"] < bet:
            await callback.answer("Недостаточно средств на балансе!", show_alert=True)
            await state.clear()
            return

        if outcome == "win":
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", bet, user_id)
            res_str = f"🎉 <b>ПОБЕДА!</b> (+{bet} RPLCoin)"
        elif outcome == "draw":
            res_str = "🤝 <b>НИЧЬЯ!</b> Ставка возвращается."
        else:
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", bet, user_id)
            res_str = f"😢 <b>ПОРАЖЕНИЕ!</b> (-{bet} RPLCoin)"

    text = (
        f"✂️ <b>КАМЕНЬ-НОЖНИЦЫ-БУМАГА</b>\n\n"
        f"Ваш выбор: <b>{choice_names[user_choice]}</b>\n"
        f"Выбор дилера: <b>{choice_names[bot_choice]}</b>\n\n"
        f"{res_str}"
    )

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сыграть снова", callback_data="casino_rps")],
        [InlineKeyboardButton(text="🎰 В меню казино", callback_data="casino_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

# --- 3. Slots (/slots) ---
@dp.callback_query(F.data == "casino_slots")
async def cb_casino_slots(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SlotsState.bet)
    await callback.message.edit_text(
        "🎰 <b>ХОККЕЙНЫЕ СЛОТЫ (/slots)</b>\n\n"
        "Символы: 🏒 🥅 ⭐️ 🍒 7️⃣ 💎\n\n"
        "Шансы выигрыша:\n"
        "• 3 редких (7️⃣/💎/⭐️) [3%] — <b>множитель x10</b>\n"
        "• 3 любых одинаковых [7%] — <b>множитель x5</b>\n"
        "• 2 одинаковых символа [15%] — <b>множитель x2</b>\n"
        "• Проигрыш [75%]\n\n"
        "Введите сумму ставки:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]])
    )

@dp.message(Command("slots"))
async def cmd_slots(message: Message, state: FSMContext):
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        await run_slots_spin(message, int(args[1]))
    else:
        await state.set_state(SlotsState.bet)
        await message.answer(
            "🎰 <b>СЛОТЫ</b>\n\nВведите сумму ставки в RPLCoin:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="casino_menu")]])
        )

@dp.message(SlotsState.bet)
async def process_slots_bet(message: Message, state: FSMContext):
    if not message.text.strip().isdigit() or int(message.text.strip()) <= 0:
        await message.answer("❌ Введите корректную ставку (число больше нуля):")
        return
    await state.clear()
    await run_slots_spin(message, int(message.text.strip()))

async def run_slots_spin(message: Message, bet: int):
    user_id = message.from_user.id
    rare_symbols = ["7️⃣", "💎", "⭐️"]
    common_symbols = ["🏒", "🥅", "🍒"]
    all_symbols = ["🏒", "🥅", "⭐️", "🍒", "7️⃣", "💎"]

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", user_id)
        if not user or user["rpl_coins"] < bet:
            await message.answer(f"❌ Недостаточно средств! Ваш баланс: {user['rpl_coins'] if user else 0} RPL.")
            return

        await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", bet, user_id)

        roll = random.random()
        if roll < 0.03:
            sym = random.choice(rare_symbols)
            reels = [sym, sym, sym]
            multiplier = 10
        elif roll < 0.10:
            sym = random.choice(common_symbols)
            reels = [sym, sym, sym]
            multiplier = 5
        elif roll < 0.25:
            sym = random.choice(all_symbols)
            other = random.choice([x for x in all_symbols if x != sym])
            reels = [sym, sym, other]
            random.shuffle(reels)
            multiplier = 2
        else:
            reels = random.sample(all_symbols, 3)
            multiplier = 0

        payout = bet * multiplier
        if payout > 0:
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", payout, user_id)

    reels_display = f"🎰 [ {reels[0]} | {reels[1]} | {reels[2]} ] 🎰"
    if multiplier > 0:
        res_str = (
            f"🎉 <b>ДЖЕКПОТ! МНОЖИТЕЛЬ x{multiplier}!</b>\n"
            f"💰 Выигрыш: <b>+{payout} RPLCoin</b>!"
        )
    else:
        res_str = (
            f"😢 <b>Не повезло!</b>\n"
            f"💸 Списана ставка: <b>-{bet} RPLCoin</b>."
        )

    text = (
        f"🎰 <b>КРУТИМ БАРАБАНЫ...</b>\n\n"
        f"{reels_display}\n\n"
        f"{res_str}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Крутить снова", callback_data="casino_slots")],
        [InlineKeyboardButton(text="🎰 В меню казино", callback_data="casino_menu")]
    ])
    await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)

# ==================== JOBS SYSTEM ====================
async def check_jail(user_id: int) -> Tuple[bool, str]:
    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT jail_until FROM users WHERE user_id = $1", user_id)
        if user and user["jail_until"] and user["jail_until"] > now:
            diff = user["jail_until"] - now
            hours, rem = divmod(int(diff.total_seconds()), 3600)
            mins, _ = divmod(rem, 60)
            return True, f"🚨 <b>ВЫ НАХОДИТЕСЬ В ТЮРЬМЕ!</b>\n\nДо освобождения осталось: <b>{hours}ч {mins}м</b>.\nВам запрещено работать на любых работах!"
    return False, ""

@dp.callback_query(F.data == "jobs_menu")
async def cb_jobs_menu(callback: CallbackQuery):
    is_in_jail, jail_msg = await check_jail(callback.from_user.id)
    if is_in_jail:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    text = (
        "💼 <b>РАЗДЕЛ РАБОТ И ПОДРАБОТОК RPL</b>\n\n"
        "Зарабатывайте RPLCoin для усиления команды и покупки паков!\n\n"
        "🏒 <b>Тренер бросков</b> — шанс 40%, награда 2 500 RPL (КД 2ч)\n"
        "🕵️ <b>Нелегал</b> — дела с высоким риском тюрьмы до 48ч\n"
        "📸 <b>Спортивный фотограф</b> — стабильная работа со съемкой матчей (КД 6ч)\n"
        "🎟 <b>Продавец атрибутики</b> — выбор цены товара и выручки (КД 2ч)\n\n"
        "Выберите желаемую работу:"
    )
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=jobs_menu_keyboard())

# --- 1. Coach Job ---
@dp.callback_query(F.data == "job_coach")
async def cb_job_coach(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_in_jail, jail_msg = await check_jail(user_id)
    if is_in_jail:
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]]))
        return

    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT job_coach_cd FROM users WHERE user_id = $1", user_id)
        if user and user["job_coach_cd"]:
            diff = now - user["job_coach_cd"]
            if diff < timedelta(hours=2):
                rem = timedelta(hours=2) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Тренировка еще не окончена! Ждите {h}ч {m}м.", show_alert=True)
                return

        is_success = (random.random() < 0.40)
        await conn.execute("UPDATE users SET job_coach_cd = $1 WHERE user_id = $2", now, user_id)

        if is_success:
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + 2500 WHERE user_id = $1", user_id)
            res_text = (
                "🏒 <b>ТРЕНИРОВКА БРОСКОВ ЗАВЕРШЕНА УСПЕШНО!</b>\n\n"
                "Ваши подопечные отлично усвоили кистевой бросок и щелчок от синей линии!\n"
                "💰 Вы заработали: <b>+2 500 RPLCoin</b>\n"
                "⏳ Кулдаун: 2 часа."
            )
        else:
            res_text = (
                "🏒 <b>ТРЕНИРОВКА ПРОВАЛЕНА!</b>\n\n"
                "Игроки сломали клюшки и переругались на льду. Руководство школы отказалось оплачивать тренировку.\n"
                "❌ Выплата: 0 RPLCoin\n"
                "⏳ Кулдаун: 2 часа."
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 К работам", callback_data="jobs_menu")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await callback.message.edit_text(res_text, parse_mode=ParseMode.HTML, reply_markup=kb)

# --- 2. Illegal Jobs ---
@dp.callback_query(F.data == "job_illegal_menu")
async def cb_job_illegal_menu(callback: CallbackQuery):
    is_in_jail, jail_msg = await check_jail(callback.from_user.id)
    if is_in_jail:
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]]))
        return

    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT job_illegal_cd FROM users WHERE user_id = $1", callback.from_user.id)
        if user and user["job_illegal_cd"]:
            diff = now - user["job_illegal_cd"]
            if diff < timedelta(hours=12):
                rem = timedelta(hours=12) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Уличная суета улеглась. Новое дело доступно через {h}ч {m}м.", show_alert=True)
                return

    text = (
        "🕵️ <b>НЕЛЕГАЛЬНЫЕ ДЕЛА</b>\n\n"
        "⚠️ <i>Внимание: провал любого дела грозит тюрьмой на 48 часов! В тюрьме заблокированы абсолютно все работы.</i>\n\n"
        "🏦 <b>Ограбить банк:</b>\n"
        "• Успех (15%): 100 000 RPLCoin\n"
        "• Неудача (15%): ничего\n"
        "• Тюрьма (70%): арест на 48 часов\n\n"
        "👛 <b>Украсть кошелёк:</b>\n"
        "• Успех (40%): до 10 000 RPLCoin\n"
        "• Неудача (40%): ничего\n"
        "• Тюрьма (20%): арест на 48 часов\n\n"
        "🛒 <b>Украсть продукты в магазине:</b>\n"
        "• Успех (30%): до 15 000 RPLCoin\n"
        "• Неудача (30%): ничего\n"
        "• Тюрьма (40%): арест на 48 часов\n\n"
        "<i>После успеха или неудачи без ареста — КД 12 часов.</i>\n"
        "Выберите дело на свой страх и риск:"
    )
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=illegal_menu_keyboard())

@dp.callback_query(F.data.startswith("do_illegal:"))
async def cb_do_illegal(callback: CallbackQuery):
    crime = callback.data.split("do_illegal:")[1]
    user_id = callback.from_user.id

    is_in_jail, jail_msg = await check_jail(user_id)
    if is_in_jail:
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]]))
        return

    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT job_illegal_cd FROM users WHERE user_id = $1", user_id)
        if user and user["job_illegal_cd"]:
            diff = now - user["job_illegal_cd"]
            if diff < timedelta(hours=12):
                rem = timedelta(hours=12) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Следующее дело можно начать через {h}ч {m}м.", show_alert=True)
                return

        roll = random.random()
        if crime == "bank":
            if roll < 0.15:
                outcome = "success"
                gain = 100000
            elif roll < 0.30:
                outcome = "fail"
                gain = 0
            else:
                outcome = "jail"
                gain = 0
        elif crime == "wallet":
            if roll < 0.40:
                outcome = "success"
                gain = random.randint(2500, 10000)
            elif roll < 0.80:
                outcome = "fail"
                gain = 0
            else:
                outcome = "jail"
                gain = 0
        else: # shop
            if roll < 0.30:
                outcome = "success"
                gain = random.randint(4000, 15000)
            elif roll < 0.70:
                outcome = "fail"
                gain = 0
            else:
                outcome = "jail"
                gain = 0

        if outcome == "success":
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1, job_illegal_cd = $2 WHERE user_id = $3", gain, now, user_id)
            res_text = (
                "💰 <b>ДЕЛО ВЫГОРЕЛО! ЧИСТЫЙ КУШ!</b>\n\n"
                f"Вы успешно провернули дело и скрылись от патрулей!\n"
                f"Награда: <b>+{gain} RPLCoin</b>\n"
                "⏳ Кулдаун: 12 часов."
            )
        elif outcome == "fail":
            await conn.execute("UPDATE users SET job_illegal_cd = $1 WHERE user_id = $2", now, user_id)
            res_text = (
                "💨 <b>ОСЕЧКА БЕЗ ПОСЛЕДСТВИЙ</b>\n\n"
                "Куш сорвался в последний момент, но вам чудом удалось скрыться в подворотне без встречи с полицией.\n"
                "❌ Выручка: 0 RPLCoin\n"
                "⏳ Кулдаун: 12 часов."
            )
        else: # jail
            jail_time = now + timedelta(hours=48)
            await conn.execute("UPDATE users SET jail_until = $1, job_illegal_cd = $2 WHERE user_id = $3", jail_time, now, user_id)
            res_text = (
                "🚨 <b>ВАС ПОЙМАЛИ С ПОЛИЧНЫМ! АРЕСТ!</b>\n\n"
                "Сработала сигнализация, спецназ окружил здание! Вас задержали и отправили в тюрьму.\n"
                "🔒 <b>Срок заключения: 48 часов.</b>\n"
                "⚠️ На время заключения вам заблокированы ВСЕ работы!"
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 К работам", callback_data="jobs_menu")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await callback.message.edit_text(res_text, parse_mode=ParseMode.HTML, reply_markup=kb)

# --- 3. Sports Photographer Job ---
@dp.callback_query(F.data == "job_photo")
async def cb_job_photo(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_in_jail, jail_msg = await check_jail(user_id)
    if is_in_jail:
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]]))
        return

    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT job_photo_cd FROM users WHERE user_id = $1", user_id)
        if user and user["job_photo_cd"]:
            diff = now - user["job_photo_cd"]
            if diff < timedelta(hours=6):
                rem = timedelta(hours=6) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Обработка фотографий матча! Ждите {h}ч {m}м.", show_alert=True)
                return

        roll = random.random()
        if roll < 0.60:
            reward = 2500
            desc = "Рядовой репортаж со скамейки запасных. Редакция приняла стандартные кадры."
        elif roll < 0.90:
            reward = 5000
            desc = "Отличные динамичные снимки силовых приемов! Журнал выделил вам бонус."
        else:
            reward = 10000
            desc = "🔥 <b>СЕНСАЦИОННЫЙ КАДР!</b> Вы поймали решающий гол в овертайме в девятку! Главная полоса спортивных газет!"

        await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1, job_photo_cd = $2 WHERE user_id = $3", reward, now, user_id)

    res_text = (
        "📸 <b>ФОТОСЕССИЯ ХОККЕЙНОГО МАТЧА</b>\n\n"
        f"{desc}\n\n"
        f"💰 Заработано: <b>+{reward} RPLCoin</b>\n"
        "⏳ Кулдаун: 6 часов."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 К работам", callback_data="jobs_menu")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await callback.message.edit_text(res_text, parse_mode=ParseMode.HTML, reply_markup=kb)

# --- 4. Merch Seller Job ---
@dp.callback_query(F.data == "job_merch_menu")
async def cb_job_merch_menu(callback: CallbackQuery):
    is_in_jail, jail_msg = await check_jail(callback.from_user.id)
    if is_in_jail:
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]]))
        return

    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT job_merch_cd FROM users WHERE user_id = $1", callback.from_user.id)
        if user and user["job_merch_cd"]:
            diff = now - user["job_merch_cd"]
            if diff < timedelta(hours=2):
                rem = timedelta(hours=2) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Торговая точка на перерыве! Ждите {h}ч {m}м.", show_alert=True)
                return

    text = (
        "🎟 <b>ПРОДАВЕЦ ХОККЕЙНОЙ АТРИБУТИКИ</b>\n\n"
        "Вы торгуете клубными шарфами, джерси и шайбами перед ареной!\n"
        "Выберите ценовую категорию партии товара:\n\n"
        "• <b>Дешёвая</b> — 65% шанс продажи, выручка 2 000–3 000 RPLCoin\n"
        "• <b>Средняя</b> — 30% шанс продажи, выручка 4 000–6 000 RPLCoin\n"
        "• <b>Дорогая</b> — 15% шанс продажи, выручка 8 000–12 000 RPLCoin\n\n"
        "⏳ Кулдаун: 2 часа."
    )
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=merch_menu_keyboard())

@dp.callback_query(F.data.startswith("do_merch:"))
async def cb_do_merch(callback: CallbackQuery):
    price_cat = callback.data.split("do_merch:")[1]
    user_id = callback.from_user.id

    is_in_jail, jail_msg = await check_jail(user_id)
    if is_in_jail:
        await callback.message.edit_text(jail_msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]]))
        return

    now = datetime.utcnow()
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT job_merch_cd FROM users WHERE user_id = $1", user_id)
        if user and user["job_merch_cd"]:
            diff = now - user["job_merch_cd"]
            if diff < timedelta(hours=2):
                rem = timedelta(hours=2) - diff
                h, m = divmod(int(rem.total_seconds()), 3600)
                m, _ = divmod(m, 60)
                await callback.answer(f"⏳ Торговая смена уже закрыта! Ждите {h}ч {m}м.", show_alert=True)
                return

        roll = random.random()
        if price_cat == "cheap":
            success = (roll < 0.65)
            gain = random.randint(2000, 3000)
        elif price_cat == "medium":
            success = (roll < 0.30)
            gain = random.randint(4000, 6000)
        else:
            success = (roll < 0.15)
            gain = random.randint(8000, 12000)

        await conn.execute("UPDATE users SET job_merch_cd = $1 WHERE user_id = $2", now, user_id)

        if success:
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", gain, user_id)
            res_text = (
                "🎉 <b>АНШЛАГ У ПАВИЛЬОНА! ПАРТИЯ РАСПРОДАНА!</b>\n\n"
                f"Болельщики раскупили всю клубную атрибутику до чистой полки!\n"
                f"💰 Выручка: <b>+{gain} RPLCoin</b>\n"
                "⏳ Кулдаун: 2 часа."
            )
        else:
            res_text = (
                "💨 <b>ТОВАР НЕ КУПИЛИ</b>\n\n"
                "Погода испортилась, а болельщики быстро разошлись по трибунам. Выручка за эту смену составила ноль.\n"
                "❌ Выручка: 0 RPLCoin\n"
                "⏳ Кулдаун: 2 часа."
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 К работам", callback_data="jobs_menu")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await callback.message.edit_text(res_text, parse_mode=ParseMode.HTML, reply_markup=kb)

# ==================== MATCHMAKING & BEAUTIFUL DYNAMIC GAME ====================
@dp.callback_query(F.data == "search_game")
async def cb_search_game(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    ovr, lineup, all_cards = await get_user_roster_ovr(user_id)

    if not all_cards:
        await callback.answer("У вас нет карточек для матча!", show_alert=True)
        return

    active_cards = []
    goalie_c = lineup.get("goalie")
    if goalie_c:
        active_cards.append(goalie_c)
    for s in ["skater_1", "skater_2", "skater_3", "skater_4"]:
        if lineup.get(s):
            active_cards.append(lineup[s])

    if len(active_cards) < 5:
        existing_ids = {c["id"] for c in active_cards}
        for c in all_cards:
            if c["id"] not in existing_ids:
                active_cards.append(c)
                if len(active_cards) == 5:
                    break

    user_info = {
        "user_id": user_id,
        "player_name": user["player_name"] or callback.from_user.full_name or "Хоккеист",
        "username": user["username"],
        "team_name": user["team_name"],
        "country": user["country"] or "🇷🇺 Россия",
        "mmr": user["mmr"] or 500,
        "wins": user["wins"] or 0,
        "losses": user["losses"] or 0,
        "ovr": ovr,
        "roster": active_cards,
        "card_ids": [c["id"] for c in all_cards],
        "message": callback.message
    }

    await callback.message.edit_text(
        "🔎 <b>Поиск соперника...</b>\n\n"
        "Ожидание оппонента (до 45 секунд).\n"
        "Если соперник не найдется, вы сыграете с ИИ!",
        parse_mode=ParseMode.HTML
    )

    matched, opp_data = await match_queue.add_player(user_id, user_info)
    if matched:
        asyncio.create_task(run_game_session(opp_data, user_info, is_pvp=True))
    else:
        opp = await match_queue.wait_or_cancel(user_id, timeout=45.0)
        if not opp:
            await run_ai_game_session(user_info)

def format_team_presentation(p: dict, color_tag: str) -> str:
    nick = f"(@{p['username']})" if p.get("username") else ""
    p_name = html.escape(p["player_name"])
    t_name = html.escape(p["team_name"])
    country = html.escape(p["country"])

    lines = [
        f"{color_tag} <b>{t_name}</b>",
        f"   👤 {p_name} {nick}  •  🌍 {country}",
        f"   ⭐️ <b>OVR {p['ovr']}</b>  •  🏆 MMR <b>{p['mmr']}</b>"
    ]

    roster = p.get("roster", [])
    total_cards = len(roster)
    for idx, card in enumerate(roster, 1):
        is_last = (idx == total_cards)
        branch = "└" if is_last else "├"
        pos_icon = "🧤" if "goalie" in card.get("position", "").lower() or "врат" in card.get("position", "").lower() else "🏒"
        lines.append(f"   {branch} {pos_icon} {html.escape(card['name'])} (OVR {card['ovr']})")

    return "\n".join(lines)

def format_match_profile_header(p1: dict, p2: dict, s1: int, s2: int, status_title: str) -> str:
    t1_block = format_team_presentation(p1, "🔴 🐻")
    t2_block = format_team_presentation(p2, "🔵 ❄️")

    return (
        f"🏆 <b>RUSSIAN PUCK LEAGUE — МАТЧ В ЭФИРЕ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{t1_block}\n\n"
        f"          ⚔️  <b>V S</b>  ⚔️\n\n"
        f"{t2_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>ТАБЛО МАТЧА:</b>\n"
        f"🔴 {html.escape(p1['team_name'])}  <b>[ {s1} : {s2} ]</b>  {html.escape(p2['team_name'])} 🔵\n"
        f"📍 <b>Статус:</b> {status_title}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>ХРОНИКА МАТЧА:</b>\n"
    )

async def run_game_session(player1: dict, player2: dict, is_pvp: bool = True):
    p1_name = player1["team_name"]
    p2_name = player2["team_name"]
    ovr1 = player1["ovr"]
    ovr2 = player2["ovr"]

    res = simulate_match(p1_name, ovr1, p2_name, ovr2, player1.get("roster", []), player2.get("roster", []))

    cur_s1 = 0
    cur_s2 = 0
    accumulated_log: List[str] = []

    async def update_screen(status_label: str):
        header = format_match_profile_header(player1, player2, cur_s1, cur_s2, status_label)
        log_text = "\n".join(accumulated_log[-7:]) if accumulated_log else "<i>Шайба вброшена! Матч начался!</i>"
        full_msg = f"{header}{log_text}"

        try:
            await player1["message"].edit_text(full_msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass
        if is_pvp and "message" in player2:
            try:
                await player2["message"].edit_text(full_msg, parse_mode=ParseMode.HTML)
            except Exception:
                pass

    # Presentation step (showing rosters & stats)
    await update_screen("⏳ Представление звеньев команд и разминка...")
    await asyncio.sleep(2.8)

    # 1 Period (4 events)
    accumulated_log.append("<b>━━━ 1-Й ПЕРИОД ━━━</b>")
    for minute, text, is_goal, scoring_team in res["p1_events"]:
        await asyncio.sleep(2.2)
        accumulated_log.append(f"⏱ <b>{minute}'</b> {text}")
        if is_goal:
            if scoring_team == player1["team_name"]:
                cur_s1 += 1
            else:
                cur_s2 += 1
        await update_screen(f"⏳ 1-й период ({minute}')")

    cur_s1 = res["p1"][0]
    cur_s2 = res["p1"][1]
    await update_screen(f"☕ Перерыв после 1-го периода ({cur_s1}:{cur_s2})")
    await asyncio.sleep(2.5)

    # 2 Period (4 events)
    accumulated_log.append("<b>━━━ 2-Й ПЕРИОД ━━━</b>")
    for minute, text, is_goal, scoring_team in res["p2_events"]:
        await asyncio.sleep(2.2)
        accumulated_log.append(f"⏱ <b>{minute}'</b> {text}")
        if is_goal:
            if scoring_team == player1["team_name"]:
                cur_s1 += 1
            else:
                cur_s2 += 1
        await update_screen(f"⏳ 2-й период ({minute}')")

    cur_s1 = res["p1"][0] + res["p2"][0]
    cur_s2 = res["p1"][1] + res["p2"][1]
    await update_screen(f"☕ Перерыв после 2-го периода ({cur_s1}:{cur_s2})")
    await asyncio.sleep(2.5)

    # 3 Period (4 events)
    accumulated_log.append("<b>━━━ 3-Й ПЕРИОД ━━━</b>")
    for minute, text, is_goal, scoring_team in res["p3_events"]:
        await asyncio.sleep(2.2)
        accumulated_log.append(f"⏱ <b>{minute}'</b> {text}")
        if is_goal:
            if scoring_team == player1["team_name"]:
                cur_s1 += 1
            else:
                cur_s2 += 1
        await update_screen(f"⏳ 3-й период ({minute}')")

    cur_s1 = res["p1"][0] + res["p2"][0] + res["p3"][0]
    cur_s2 = res["p1"][1] + res["p2"][1] + res["p3"][1]

    # Overtime & Shootout
    if res["overtime"]:
        accumulated_log.append("<b>━━━ ОВЕРТАЙМ 3x3 ━━━</b>")
        await update_screen("⚠️ Ничья в основное время! Овертайм 3х3 до первого гола!")
        await asyncio.sleep(2.5)

        if res["ot_goal_by"]:
            scorer = res["ot_scorer"]
            scoring_team = res["ot_goal_by"]
            defending_team = p2_name if scoring_team == p1_name else p1_name
            accumulated_log.append(
                f"⏱ <b>64'</b> 🚨 <b>ЗОЛОТОЙ ГОЛ В ОВЕРТАЙМЕ!</b>\n   🎯 <b>{scorer}</b> забивает за <b>{scoring_team}</b> в ворота {defending_team} и приносит триумфальную победу!"
            )
            if scoring_team == p1_name:
                cur_s1 += 1
            else:
                cur_s2 += 1
            await update_screen("🏁 Золотой гол в овертайме!")
            await asyncio.sleep(2.0)
        elif res["shootout"]:
            accumulated_log.append("⏱ <b>65'</b> Овертайм без заброшенных шайб! <b>СЕРИЯ БУЛЛИТОВ!</b>")
            await update_screen("🏒 Серия послематчевых буллитов!")
            await asyncio.sleep(2.5)

            winner_name = res["winner"]
            scorer = res["ot_scorer"]
            losing_team = p2_name if winner_name == p1_name else p1_name
            accumulated_log.append(
                f"🎯 <b>ПОБЕДНЫЙ БУЛЛИТ!</b> <b>{scorer}</b> хладнокровно реализует попытку за <b>{winner_name}</b>, переиграв голкипера команды {losing_team}!"
            )
            if winner_name == p1_name:
                cur_s1 += 1
            else:
                cur_s2 += 1
            await update_screen("🏁 Финал серии буллитов!")
            await asyncio.sleep(2.0)

    winner_name = res["winner"]
    is_p1_win = (winner_name == p1_name)

    async with db.pool.acquire() as conn:
        if is_p1_win:
            await conn.execute("UPDATE users SET mmr = mmr + 25, rpl_coins = rpl_coins + 1000, exp = exp + 50, wins = wins + 1 WHERE user_id = $1", player1["user_id"])
            if is_pvp:
                await conn.execute("UPDATE users SET mmr = GREATEST(0, mmr - 15), rpl_coins = rpl_coins + 200, losses = losses + 1 WHERE user_id = $1", player2["user_id"])
        else:
            await conn.execute("UPDATE users SET mmr = GREATEST(0, mmr - 15), rpl_coins = rpl_coins + 200, losses = losses + 1 WHERE user_id = $1", player1["user_id"])
            if is_pvp:
                await conn.execute("UPDATE users SET mmr = mmr + 25, rpl_coins = rpl_coins + 1000, exp = exp + 50, wins = wins + 1 WHERE user_id = $1", player2["user_id"])

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])

    period_breakdown = f"({res['p1'][0]}:{res['p1'][1]}, {res['p2'][0]}:{res['p2'][1]}, {res['p3'][0]}:{res['p3'][1]})"
    ot_tag = " (ОТ)" if res["overtime"] and not res["shootout"] else (" (Б)" if res["shootout"] else "")

    final_log = "\n".join(accumulated_log[-7:])

    header1 = format_match_profile_header(player1, player2, res["score1"], res["score2"], f"🏁 МАТЧ ОКОНЧЕН {ot_tag}")
    p1_award = "🎉 <b>ПОБЕДА В МАТЧЕ!</b>\nНаграда: <b>+25 MMR | +1 000 RPLCoin | +50 XP</b>" if is_p1_win else "😢 <b>ПОРАЖЕНИЕ В МАТЧЕ</b>\nИтог: <b>-15 MMR | +200 RPLCoin | +0 XP</b>"
    text1 = (
        f"{header1}{final_log}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 <b>ПОБЕДИТЕЛЬ:</b> {html.escape(winner_name)}\n"
        f"Счет по периодам: {period_breakdown}\n\n"
        f"{p1_award}"
    )

    try:
        await player1["message"].edit_text(text1, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception:
        pass

    if is_pvp and "message" in player2:
        header2 = format_match_profile_header(player1, player2, res["score1"], res["score2"], f"🏁 МАТЧ ОКОНЧЕН {ot_tag}")
        p2_award = "🎉 <b>ПОБЕДА В МАТЧЕ!</b>\nНаграда: <b>+25 MMR | +1 000 RPLCoin | +50 XP</b>" if not is_p1_win else "😢 <b>ПОРАЖЕНИЕ В МАТЧЕ</b>\nИтог: <b>-15 MMR | +200 RPLCoin | +0 XP</b>"
        text2 = (
            f"{header2}{final_log}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 <b>ПОБЕДИТЕЛЬ:</b> {html.escape(winner_name)}\n"
            f"Счет по периодам: {period_breakdown}\n\n"
            f"{p2_award}"
        )
        try:
            await player2["message"].edit_text(text2, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            pass

async def run_ai_game_session(player1: dict):
    user_card_ids = player1["card_ids"]
    user_ovr = player1["ovr"]

    async with db.pool.acquire() as conn:
        ai_cards_available = await conn.fetch("SELECT id, name, ovr, position FROM cards WHERE NOT (id = ANY($1))", user_card_ids)

    ai_roster = []
    if ai_cards_available:
        ai_cards_list = [dict(r) for r in ai_cards_available]
        g_cand = [c for c in ai_cards_list if "goalie" in c["position"].lower() or "врат" in c["position"].lower()]
        sk_cand = [c for c in ai_cards_list if c not in g_cand]

        if g_cand:
            ai_roster.append(random.choice(g_cand))
        if sk_cand:
            ai_roster.extend(random.sample(sk_cand, min(4, len(sk_cand))))

        while len(ai_roster) < 5 and ai_cards_list:
            rem = [c for c in ai_cards_list if c not in ai_roster]
            if not rem:
                break
            ai_roster.append(random.choice(rem))

    if len(ai_roster) < 5:
        ai_roster = [
            {"name": "Третьяк ИИ", "ovr": int(user_ovr), "position": "Goalie"},
            {"name": "Харламов ИИ", "ovr": int(user_ovr) + 1, "position": "Skater"},
            {"name": "Мальцев ИИ", "ovr": int(user_ovr), "position": "Skater"},
            {"name": "Фетисов ИИ", "ovr": int(user_ovr) - 1, "position": "Skater"},
            {"name": "Касатонов ИИ", "ovr": int(user_ovr), "position": "Skater"}
        ]

    ai_ovr = round(sum(c["ovr"] for c in ai_roster) / len(ai_roster), 1)

    ai_names = ["ХК Ледяные Волки", "ХК Северные Медведи", "ХК Красная Звезда ИИ", "ХК Стальные Буйволы", "ХК Сибирские Снайперы"]
    ai_team_name = random.choice(ai_names)

    ai_team = {
        "user_id": 0,
        "player_name": "ИИ Гроссмейстер",
        "username": None,
        "team_name": ai_team_name,
        "country": "🇷🇺 Россия",
        "mmr": max(100, int(player1["mmr"]) + random.randint(-20, 20)),
        "wins": random.randint(10, 40),
        "losses": random.randint(10, 40),
        "ovr": ai_ovr,
        "roster": ai_roster
    }
    await run_game_session(player1, ai_team, is_pvp=False)

# ==================== LEADERBOARD ====================
@dp.callback_query(F.data == "leaderboard_mmr")
async def cb_leaderboard(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        top_users = await conn.fetch("""
            SELECT player_name, username, team_name, country, mmr, wins, losses 
            FROM users 
            WHERE team_name IS NOT NULL 
            ORDER BY mmr DESC LIMIT 10
        """)

    text = "🏆 <b>ТАБЛИЦА ЛИДЕРОВ RPL (ТОП-10 MMR)</b>\n\n"
    for idx, u in enumerate(top_users, 1):
        medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"<b>{idx}.</b>"
        p_name = html.escape(u["player_name"] or "Хоккеист")
        u_name = f"(@{html.escape(u['username'])})" if u["username"] else "(без @username)"
        t_name = html.escape(u["team_name"] or "Без команды")
        c_name = html.escape(u["country"] or "СНГ")
        wins = u["wins"] or 0
        losses = u["losses"] or 0
        mmr = u["mmr"] or 500

        text += (
            f"{medal} <b>{p_name}</b> {u_name}\n"
            f"🏒 {t_name} ({c_name})\n"
            f"🏆 <b>{mmr} MMR</b> | 🟢 Победы: {wins} | 🔴 Поражения: {losses}\n\n"
        )

    if not top_users:
        text += "Пока нет сыгранных матчей."

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

# ==================== PACKS & SHOP ====================
@dp.callback_query(F.data == "pack_shop")
async def cb_pack_shop(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        packs = await conn.fetch("SELECT * FROM packs ORDER BY price ASC")
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", callback.from_user.id)

    if not packs:
        await callback.answer("Паки скоро поступят в магазин!", show_alert=True)
        return

    text = f"📦 <b>МАГАЗИН ПАКОВ</b>\nВаш баланс: <b>{user['rpl_coins']} RPLCoin</b>\n\nВыберите пак:"
    buttons = []
    for p in packs:
        buttons.append([InlineKeyboardButton(text=f"📦 {p['name']} — {p['price']} RPL", callback_data=f"buy_pack:{p['id']}")])
    buttons.append([InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")])

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("buy_pack:"))
async def cb_buy_pack(callback: CallbackQuery):
    pack_id = int(callback.data.split("buy_pack:")[1])
    user_id = callback.from_user.id

    async with db.pool.acquire() as conn:
        pack = await conn.fetchrow("SELECT * FROM packs WHERE id = $1", pack_id)
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", user_id)

        if not pack or user["rpl_coins"] < pack["price"]:
            await callback.answer("❌ Недостаточно средств для покупки этого пака!", show_alert=True)
            return

        pack_card_ids = list(pack["card_ids"])
        available_ids = await conn.fetch("""
            SELECT id FROM cards 
            WHERE id = ANY($1) 
              AND id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $2)
        """, pack_card_ids, user_id)

        if not available_ids:
            await callback.answer("❌ У вас уже есть все карточки из этого пака!", show_alert=True)
            return

        chosen_id = random.choice(available_ids)["id"]
        card = await conn.fetchrow("""
            SELECT c.*, COALESCE(c.photo_id, t.photo_id) AS effective_photo
            FROM cards c
            LEFT JOIN teams t ON c.team_name = t.name
            WHERE c.id = $1
        """, chosen_id)

        await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", pack["price"], user_id)
        await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, chosen_id)

    caption = (
        f"🎉 <b>ВЫ ОТКРЫЛИ {html.escape(pack['name'])}!</b>\n\n"
        f"🌟 <b>{html.escape(card['name'])}</b> (OVR: {card['ovr']})\n"
        f"Позиция: <b>{html.escape(card['position'])}</b> | Редкость: <b>{html.escape(card['rarity'])}</b>\n"
        f"Команда: <b>{html.escape(card['team_name'])}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Еще паки", callback_data="pack_shop")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]
    ])
    if card["effective_photo"]:
        await callback.message.delete()
        await callback.message.answer_photo(photo=card["effective_photo"], caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await callback.message.edit_text(caption, reply_markup=kb, parse_mode=ParseMode.HTML)

# ==================== PROMO CODES ====================
@dp.callback_query(F.data == "enter_promo")
async def cb_enter_promo(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserPromoState.code)
    await callback.message.edit_text(
        "🎟 <b>Введите промокод:</b>\nОтправьте текст промокода ответным сообщением.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="back_main")]])
    )

@dp.message(UserPromoState.code)
async def process_user_promo(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id

    async with db.pool.acquire() as conn:
        promo = await conn.fetchrow("SELECT * FROM promo_codes WHERE code = $1", code)
        if not promo:
            await message.answer("❌ Такого промокода не существует!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_main")]]))
            await state.clear()
            return

        used = await conn.fetchrow("SELECT 1 FROM promo_uses WHERE user_id = $1 AND code = $2", user_id, code)
        if used:
            await message.answer("⚠️ Вы уже использовали данный промокод!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_main")]]))
            await state.clear()
            return

        if promo["used_count"] >= promo["max_uses"]:
            await message.answer("❌ Лимит активаций данного промокода исчерпан!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_main")]]))
            await state.clear()
            return

        await conn.execute("INSERT INTO promo_uses (user_id, code) VALUES ($1, $2)", user_id, code)
        await conn.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code = $1", code)

        if promo["reward_type"] == "money":
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", promo["reward_value"], user_id)
            res_text = f"🎉 Промокод активирован! Вы получили <b>{promo['reward_value']} RPLCoin</b>!"
        else:
            card = await conn.fetchrow("SELECT name, ovr FROM cards WHERE id = $1", promo["reward_value"])
            has_c = await conn.fetchrow("SELECT 1 FROM user_cards WHERE user_id = $1 AND card_id = $2", user_id, promo["reward_value"])
            if not has_c:
                await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, promo["reward_value"])
            card_title = f"{html.escape(card['name'])} (OVR {card['ovr']})" if card else f"Карточка #{promo['reward_value']}"
            res_text = f"🎉 Промокод активирован! Вы получили: <b>{card_title}</b>!"

    await state.clear()
    await message.answer(res_text, reply_markup=main_menu_keyboard(message.chat.type == ChatType.PRIVATE), parse_mode=ParseMode.HTML)

# ==================== ADMIN PANEL (/adminrpl) ====================
@dp.message(Command("adminrpl"))
async def cmd_admin_rpl(message: Message, state: FSMContext):
    await state.clear()
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("⚙️ <b>ПАНЕЛЬ АДМИНИСТРАТОРА RPL</b>\nВыберите действие:", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if callback.from_user.id not in ADMIN_IDS:
        return
    await callback.message.edit_text("⚙️ <b>ПАНЕЛЬ АДМИНИСТРАТОРА RPL</b>\nВыберите действие:", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.HTML)

# --- Admin: Setup Starter Players by IDs ---
@dp.callback_query(F.data == "adm_starters")
async def cb_adm_starters(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminStartersState.card_ids)

    async with db.pool.acquire() as conn:
        current_starters = await conn.fetch("SELECT id, name, ovr, position FROM cards WHERE is_starter = TRUE ORDER BY id ASC")

    st_text = "⭐ <b>НАЗНАЧИТЬ НАЧАЛЬНЫХ ИГРОКОВ НОВИЧКАМ</b>\n\n"
    if current_starters:
        st_text += "<b>Текущие стартовые карточки:</b>\n"
        for s in current_starters:
            st_text += f"• #{s['id']} {s['name']} (OVR {s['ovr']}, {s['position']})\n"
    else:
        st_text += "Стартовых игроков еще не назначено.\n"

    st_text += "\nОтправьте <b>ID 5 карточек через запятую</b> (4 полевых и 1 вратарь), которые будут стартовым набором:\n<i>Пример: 1, 2, 3, 4, 5</i>"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]])
    await callback.message.edit_text(st_text, parse_mode=ParseMode.HTML, reply_markup=kb)

@dp.message(AdminStartersState.card_ids)
async def process_starters_ids(message: Message, state: FSMContext):
    try:
        ids = [int(x.strip()) for x in message.text.split(",") if x.strip().isdigit()]
        if not ids:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверный формат! Отправьте список ID через запятую (например: 1, 4, 8, 12, 15):")
        return

    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE cards SET is_starter = FALSE")
        await conn.execute("UPDATE cards SET is_starter = TRUE WHERE id = ANY($1)", ids)
        updated = await conn.fetch("SELECT id, name, ovr, position FROM cards WHERE is_starter = TRUE")

    await state.clear()
    res_list = "\n".join([f"• #{c['id']} {c['name']} (OVR {c['ovr']}, {c['position']})" for c in updated])
    await message.answer(
        f"✅ <b>Стартовые карточки успешно обновлены!</b>\n\n{res_list}\n\nТеперь все новые игроки гарантированно будут получать этот набор из 5 карт!",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.HTML
    )

# --- Admin: Batch Auto-Add Cards ---
@dp.callback_query(F.data == "adm_card_batch")
async def cb_adm_batch(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminBatchCard.waiting_for_text)
    instructions = (
        "⚡ <b>АВТОМАТИЧЕСКОЕ ДОБАВЛЕНИЕ КАРТОЧЕК</b>\n\n"
        "Отправьте список игроков в столбик. <b>Каждый игрок с новой строки:</b>\n\n"
        "<code>Nick # | Редкость | Страна | OVR | Позиция | ID Команды | ID Коллекции</code>\n\n"
        "📌 <b>Пример:</b>\n"
        "<code>Аверин 10 | Редкая | 🇷🇺 Россия | 76 | Skater | 1 | 1\n"
        "Бобров 20 | Очень редкая | 🇧🇾 Беларусь | 81 | Goalie | 2 | 1\n"
        "Овечкин 8 | Легендарная | 🇷🇺 Россия | 95 | Skater | 1 | 1</code>\n\n"
        "<i>Фото карточки автоматически возьмется от команды (по ID Команды)!</i>"
    )
    await callback.message.edit_text(instructions, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminBatchCard.waiting_for_text)
async def process_batch_text(message: Message, state: FSMContext):
    raw_lines = [l.strip() for l in message.text.strip().split("\n") if l.strip()]
    if not raw_lines:
        await message.answer("❌ Пустое сообщение! Отправьте список строк с игроками:")
        return

    parsed_cards = []
    errors = []

    async with db.pool.acquire() as conn:
        for idx, line in enumerate(raw_lines, 1):
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 7:
                errors.append(f"Строка {idx}: не 7 параметров ({line})")
                continue

            name, rarity, country, ovr_str, position, team_id_str, col_id_str = parts[:7]

            if rarity not in RARITIES:
                errors.append(f"Строка {idx}: неверная редкость '{rarity}'")
                continue

            if not ovr_str.isdigit() or not (50 <= int(ovr_str) <= 99):
                errors.append(f"Строка {idx}: OVR должен быть числом 50-99 ({ovr_str})")
                continue

            pos_clean = "Goalie" if "goalie" in position.lower() or "врат" in position.lower() else "Skater"

            team_id = int(team_id_str) if team_id_str.isdigit() else 0
            team_name = "Без команды"
            if team_id > 0:
                t_row = await conn.fetchrow("SELECT name FROM teams WHERE id = $1", team_id)
                if t_row:
                    team_name = t_row["name"]

            col_id = int(col_id_str) if col_id_str.isdigit() else 0
            col_name = "Основная"
            if col_id > 0:
                c_row = await conn.fetchrow("SELECT name FROM collections WHERE id = $1", col_id)
                if c_row:
                    col_name = c_row["name"]

            parsed_cards.append({
                "name": name,
                "rarity": rarity,
                "country": country,
                "ovr": int(ovr_str),
                "position": pos_clean,
                "team_name": team_name,
                "collection_name": col_name
            })

        if not parsed_cards:
            err_msg = "\n".join(errors[:10])
            await message.answer(f"❌ Не удалось распознать игроков:\n{err_msg}")
            return

        created_ids = []
        for c in parsed_cards:
            row = await conn.fetchrow("""
                INSERT INTO cards (name, rarity, country, ovr, position, team_name, collection_name, photo_id, is_starter)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, FALSE)
                RETURNING id
            """, c["name"], c["rarity"], c["country"], c["ovr"], c["position"], c["team_name"], c["collection_name"])
            created_ids.append(row["id"])

    await state.clear()
    err_suffix = f"\n⚠️ Пропущено некорректных строк: {len(errors)}" if errors else ""
    await message.answer(
        f"🎉 <b>Успешно добавлено {len(created_ids)} карточек!</b>{err_suffix}\n\n"
        f"Диапазон присвоенных ID: #{created_ids[0]} — #{created_ids[-1]}\n"
        f"Карточки автоматически наследуют фото своих команд!",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.HTML
    )

# --- Admin: Manual Card ---
@dp.callback_query(F.data == "adm_card_manual")
async def cb_adm_manual(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminCardManual.name)
    await callback.message.edit_text("📝 <b>РУЧНОЕ СОЗДАНИЕ КАРТОЧКИ</b>\nВведите имя игрока:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminCardManual.name)
async def process_man_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminCardManual.rarity)
    buttons = [[InlineKeyboardButton(text=r, callback_data=f"man_r:{r}")] for r in RARITIES]
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="admin_panel")])
    await message.answer("Выберите редкость карточки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.rarity, F.data.startswith("man_r:"))
async def process_man_rarity(callback: CallbackQuery, state: FSMContext):
    rarity = callback.data.split("man_r:")[1]
    await state.update_data(rarity=rarity)
    await state.set_state(AdminCardManual.country)
    buttons = [[InlineKeyboardButton(text=c, callback_data=f"man_c:{c}")] for c in CIS_COUNTRIES]
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="admin_panel")])
    await callback.message.edit_text("Выберите страну игрока:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.country, F.data.startswith("man_c:"))
async def process_man_country(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("man_c:")[1]
    await state.update_data(country=country)
    await state.set_state(AdminCardManual.ovr)
    await callback.message.edit_text("Введите OVR карточки (50 - 99):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminCardManual.ovr)
async def process_man_ovr(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (50 <= int(message.text) <= 99):
        await message.answer("❌ Введите корректный OVR (50 - 99):")
        return
    await state.update_data(ovr=int(message.text))
    await state.set_state(AdminCardManual.position)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Полевой (Skater)", callback_data="man_p:Skater")],
        [InlineKeyboardButton(text="Вратарь (Goalie)", callback_data="man_p:Goalie")],
        [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
    ])
    await message.answer("Выберите позицию:", reply_markup=kb)

@dp.callback_query(AdminCardManual.position, F.data.startswith("man_p:"))
async def process_man_position(callback: CallbackQuery, state: FSMContext):
    pos = callback.data.split("man_p:")[1]
    await state.update_data(position=pos)
    await state.set_state(AdminCardManual.team)
    async with db.pool.acquire() as conn:
        teams = await conn.fetch("SELECT name FROM teams")
    buttons = [[InlineKeyboardButton(text="Без команды", callback_data="man_t:Без команды")]]
    for t in teams:
        buttons.append([InlineKeyboardButton(text=t["name"], callback_data=f"man_t:{t['name']}")])
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="admin_panel")])
    await callback.message.edit_text("Выберите команду:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.team, F.data.startswith("man_t:"))
async def process_man_team(callback: CallbackQuery, state: FSMContext):
    team = callback.data.split("man_t:")[1]
    await state.update_data(team_name=team)
    await state.set_state(AdminCardManual.collection)
    async with db.pool.acquire() as conn:
        cols = await conn.fetch("SELECT name FROM collections")
    buttons = [[InlineKeyboardButton(text="Основная", callback_data="man_col:Основная")]]
    for c in cols:
        buttons.append([InlineKeyboardButton(text=c["name"], callback_data=f"man_col:{c['name']}")])
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="admin_panel")])
    await callback.message.edit_text("Выберите коллекцию:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.collection, F.data.startswith("man_col:"))
async def process_man_collection(callback: CallbackQuery, state: FSMContext):
    col = callback.data.split("man_col:")[1]
    await state.update_data(collection_name=col)
    await state.set_state(AdminCardManual.is_starter)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, стартовая для новичков", callback_data="man_s:1")],
        [InlineKeyboardButton(text="Нет, обычная", callback_data="man_s:0")],
        [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
    ])
    await callback.message.edit_text("Выдавать эту карту новичкам при старте?", reply_markup=kb)

@dp.callback_query(AdminCardManual.is_starter, F.data.startswith("man_s:"))
async def process_man_starter(callback: CallbackQuery, state: FSMContext):
    is_starter = (callback.data.split("man_s:")[1] == "1")
    data = await state.get_data()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO cards (name, rarity, country, ovr, position, team_name, collection_name, photo_id, is_starter)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8)
            RETURNING id
        """, data["name"], data["rarity"], data["country"], data["ovr"], data["position"], data["team_name"], data["collection_name"], is_starter)

    await state.clear()
    await callback.message.edit_text(
        f"✅ Карточка <b>{html.escape(data['name'])}</b> создана! ID: <b>#{row['id']}</b>\n"
        f"Фото будет автоматически отображаться из команды <b>{html.escape(data['team_name'])}</b>.",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.HTML
    )

# --- Admin: Teams Management ---
@dp.callback_query(F.data == "adm_teams")
async def cb_adm_teams(callback: CallbackQuery, state: FSMContext):
    async with db.pool.acquire() as conn:
        teams = await conn.fetch("SELECT * FROM teams ORDER BY id ASC")
    t_text = "🏒 <b>УПРАВЛЕНИЕ КОМАНДАМИ:</b>\n\n"
    for t in teams:
        has_ph = "🖼 Есть фото" if t["photo_id"] else "❌ Без фото"
        t_text += f"• <b>ID {t['id']}</b>: {html.escape(t['name'])} ({has_ph})\n"
    if not teams:
        t_text += "Команд пока нет.\n"
    t_text += "\nОтправьте <b>название новой команды</b>:"

    await state.set_state(AdminTeamState.team_name)
    await callback.message.edit_text(t_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminTeamState.team_name)
async def process_adm_team_name(message: Message, state: FSMContext):
    name = message.text.strip()
    await state.update_data(team_name=name)
    await state.set_state(AdminTeamState.team_photo)
    await message.answer(
        f"📸 Теперь отправьте <b>фотографию/логотип</b> для команды <b>{html.escape(name)}</b>:\n"
        f"<i>(Это фото будет автоматически показываться для всех карточек этой команды!)</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]])
    )

@dp.message(AdminTeamState.team_photo, F.photo)
async def process_adm_team_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    team_name = data["team_name"]

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO teams (name, photo_id) VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE SET photo_id = $2
            RETURNING id
        """, team_name, photo_id)

    await state.clear()
    await message.answer(
        f"✅ Команда <b>{html.escape(team_name)}</b> сохранена с ID <b>#{row['id']}</b> и привязанным фото!",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.HTML
    )

# --- Admin: Collections ---
@dp.callback_query(F.data == "adm_collections")
async def cb_adm_collections(callback: CallbackQuery, state: FSMContext):
    async with db.pool.acquire() as conn:
        cols = await conn.fetch("SELECT * FROM collections ORDER BY id ASC")
    c_text = "📂 <b>СПИСОК КОЛЛЕКЦИЙ:</b>\n\n"
    for c in cols:
        c_text += f"• <b>ID {c['id']}</b>: {html.escape(c['name'])}\n"
    if not cols:
        c_text += "Коллекций пока нет.\n"
    c_text += "\nОтправьте название новой коллекции для добавления:"

    await state.set_state(AdminCollectionState.collection_name)
    await callback.message.edit_text(c_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]])
    )

@dp.message(AdminCollectionState.collection_name)
async def process_adm_col(message: Message, state: FSMContext):
    name = message.text.strip()
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO collections (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id", name)
    await state.clear()
    await message.answer(f"✅ Коллекция <b>{html.escape(name)}</b> сохранена с <b>ID: {row['id']}</b>!", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.HTML)

# --- Admin: Packs ---
@dp.callback_query(F.data == "adm_packs")
async def cb_adm_packs(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPackState.name)
    await callback.message.edit_text("Введите название пака:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminPackState.name)
async def process_pack_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminPackState.price)
    await message.answer("Введите цену пака в RPLCoin:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminPackState.price)
async def process_pack_price(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Цена должна быть числом!")
        return
    await state.update_data(price=int(message.text))
    await state.set_state(AdminPackState.card_ids)
    await message.answer("Введите ID карточек через запятую (например: 1, 5, 8, 12):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminPackState.card_ids)
async def process_pack_cards(message: Message, state: FSMContext):
    try:
        ids = [int(x.strip()) for x in message.text.split(",") if x.strip().isdigit()]
        if not ids:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверный формат! Введите ID через запятую:")
        return

    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO packs (name, price, card_ids) VALUES ($1, $2, $3)", data["name"], data["price"], ids)

    await state.clear()
    await message.answer(f"✅ Пак <b>{html.escape(data['name'])}</b> создан!", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.HTML)

# --- Admin: Promo ---
@dp.callback_query(F.data == "adm_promo")
async def cb_adm_promo(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPromoState.code)
    await callback.message.edit_text("Введите код нового промокода (например: RPLOVE):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminPromoState.code)
async def process_promo_code(message: Message, state: FSMContext):
    await state.update_data(code=message.text.strip().upper())
    await state.set_state(AdminPromoState.reward_type)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 RPLCoins", callback_data="pr_t:money")],
        [InlineKeyboardButton(text="🃏 Карточка (ID)", callback_data="pr_t:card")],
        [InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]
    ])
    await message.answer("Выберите тип награды:", reply_markup=kb)

@dp.callback_query(AdminPromoState.reward_type, F.data.startswith("pr_t:"))
async def process_promo_type(callback: CallbackQuery, state: FSMContext):
    rw = callback.data.split("pr_t:")[1]
    await state.update_data(reward_type=rw)
    await state.set_state(AdminPromoState.reward_value)
    prompt = "Введите количество монет:" if rw == "money" else "Введите ID карточки для выдачи:"
    await callback.message.edit_text(prompt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminPromoState.reward_value)
async def process_promo_val(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Введите число!")
        return
    await state.update_data(reward_value=int(message.text))
    await state.set_state(AdminPromoState.max_uses)
    await message.answer("Введите максимальное количество использований:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminPromoState.max_uses)
async def process_promo_uses(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Введите число!")
        return
    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO promo_codes (code, reward_type, reward_value, max_uses)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code) DO UPDATE SET reward_type = $2, reward_value = $3, max_uses = $4, used_count = 0
        """, data["code"], data["reward_type"], data["reward_value"], int(message.text))

    await state.clear()
    await message.answer(f"✅ Промокод <b>{html.escape(data['code'])}</b> готов!", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.HTML)

# ==================== MAIN ENTRY POINT ====================
async def main():
    if not BOT_TOKEN or not DATABASE_URL:
        logger.error("❌ BOT_TOKEN or DATABASE_URL is missing!")
        return

    logger.info("Connecting to Database...")
    await db.connect()
    logger.info("Database connected successfully!")

    asyncio.create_task(card_cooldown_notifier())

    logger.info("Starting Telegram Bot Polling...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
