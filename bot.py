import asyncio
import logging
import os
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
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
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
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

# 7-day streak rewards definition
STREAK_REWARDS = {
    1: {"type": "coins", "amount": 2500, "desc": "2 500 RPLCoin"},
    2: {"type": "coins", "amount": 5000, "desc": "5 000 RPLCoin"},
    3: {"type": "coins", "amount": 7500, "desc": "7 500 RPLCoin"},
    4: {"type": "card", "min_ovr": 75, "max_ovr": 85, "desc": "Карточка 75-85 OVR"},
    5: {"type": "coins", "amount": 25000, "desc": "25 000 RPLCoin"},
    6: {"type": "coins", "amount": 35000, "desc": "35 000 RPLCoin"},
    7: {"type": "card", "min_ovr": 80, "max_ovr": 88, "desc": "Карточка 80-88 OVR"},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== LEVEL & XP MATH ====================
# Level 1 requires 1000 XP, Level 2 requires 2000 XP, Level 3 requires 3000 XP...
# Returns (level, xp_into_current_level, xp_needed_for_next_level)
def calculate_level_and_progress(total_exp: int) -> Tuple[int, int, int]:
    lvl = 1
    req = 1000
    rem = max(0, total_exp)
    while rem >= req:
        rem -= req
        lvl += 1
        req += 1000
    return lvl, rem, req

# ==================== DATABASE ====================
class Database:
    def __init__(self, url: str):
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        self.url = url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.url, max_size=20, min_size=5)
        await self.init_tables()

    async def init_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    team_name VARCHAR(100),
                    country VARCHAR(100),
                    player_name VARCHAR(100),
                    mmr INT DEFAULT 500,
                    rpl_coins BIGINT DEFAULT 1000,
                    exp INT DEFAULT 50,
                    wins INT DEFAULT 0,
                    losses INT DEFAULT 0,
                    last_daily_claim TIMESTAMP,
                    streak_days INT DEFAULT 0,
                    last_streak_claim TIMESTAMP,
                    registered_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS teams (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL
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
                    photo_id TEXT NOT NULL,
                    is_starter BOOLEAN DEFAULT FALSE
                );

                CREATE TABLE IF NOT EXISTS user_cards (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                    card_id INT REFERENCES cards(id) ON DELETE CASCADE,
                    in_lineup BOOLEAN DEFAULT FALSE,
                    acquired_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT unique_user_card UNIQUE (user_id, card_id)
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
            """)

db = Database(DATABASE_URL)

# ==================== FSM STATES ====================
class RegState(StatesGroup):
    entering_team_name = State()
    choosing_country = State()

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

class AdminCardQuick(StatesGroup):
    raw_data = State()
    photo = State()

class AdminTeamState(StatesGroup):
    team_name = State()

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

class UserPromoState(StatesGroup):
    code = State()

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

HOCKEY_EVENTS = [
    "⚡ Мощнейший щелчок от синей линии! {attacking_team} штурмует ворота, но голкипер парирует щитком!",
    "🏒 Ой-ой-ой! У лидера атак {attacking_team} ломается клюшка прямо в момент убойного броска!",
    "🛡 Защитник бросается под шайбу и ценой ушиба спасает команду {defending_team}!",
    "💥 Жесткий силовой прием у борта! Борта звенят, болельщики взревели!",
    "🧤 Невероятный шпагат! Вратарь {defending_team} вытаскивает мертвую шайбу из самой девятки!",
    "🎯 Опаснейший выход 2 в 1! Нападающий {attacking_team} бросает — ЗВОНКАЯ ШТАНГА!",
    "⏱ Грубый зацеп клюшкой в средней зоне! Удаление на 2 минуты в составе {defending_team}!",
    "🚨 ГООООЛ! Ювелирный бросок под перекладину оформляет звено {attacking_team}!",
    "🚨 ГООООЛ! Рикошет на пятаке, и шайба заползает в ворота {defending_team}!",
    "🧤 Вратарь {defending_team} намертво фиксирует шайбу ловушкой под овации сектора.",
    "🌪 Перехват на синей линии! {attacking_team} убегает в стремительный контрвыпад!"
]

def simulate_match(team1_name: str, ovr1: int, team2_name: str, ovr2: int) -> dict:
    diff = ovr1 - ovr2
    prob1 = 0.50 + (diff * 0.02)
    prob1 = max(0.20, min(0.80, prob1))

    # Higher tendency to tie in regular time
    p1_t1, p1_t2 = random.choices([0, 1, 2], weights=[0.50, 0.35, 0.15])[0], random.choices([0, 1, 2], weights=[0.50, 0.35, 0.15])[0]
    p2_t1, p2_t2 = random.choices([0, 1, 2], weights=[0.45, 0.35, 0.20])[0], random.choices([0, 1, 2], weights=[0.45, 0.35, 0.20])[0]
    p3_t1, p3_t2 = random.choices([0, 1, 2], weights=[0.45, 0.35, 0.20])[0], random.choices([0, 1, 2], weights=[0.45, 0.35, 0.20])[0]

    if random.random() < prob1:
        if random.random() < 0.60:
            p2_t1 += 1
    else:
        if random.random() < 0.60:
            p2_t2 += 1

    score1 = p1_t1 + p2_t1 + p3_t1
    score2 = p1_t2 + p2_t2 + p3_t2

    overtime = False
    shootout = False
    winner = None

    if score1 == score2:
        overtime = True
        ot1, ot2 = random.choices([(0, 0), (1, 0), (0, 1)], weights=[0.50, 0.25, 0.25])[0]
        if ot1 != ot2:
            score1 += ot1
            score2 += ot2
            winner = team1_name if score1 > score2 else team2_name
        else:
            shootout = True
            so_win = 1 if random.random() < prob1 else 2
            if so_win == 1:
                score1 += 1
                winner = team1_name
            else:
                score2 += 1
                winner = team2_name
    else:
        winner = team1_name if score1 > score2 else team2_name

    events = []
    for _ in range(5):
        ev = random.choice(HOCKEY_EVENTS)
        if random.random() > 0.5:
            ev = ev.format(attacking_team=team1_name, defending_team=team2_name)
        else:
            ev = ev.format(attacking_team=team2_name, defending_team=team1_name)
        events.append(ev)

    return {
        "score1": score1,
        "score2": score2,
        "p1": (p1_t1, p1_t2),
        "p2": (p2_t1, p2_t2),
        "p3": (p3_t1, p3_t2),
        "overtime": overtime,
        "shootout": shootout,
        "winner": winner,
        "events": events
    }

# ==================== BOT SETUP ====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ==================== CHAT RESTRICTION MIDDLEWARE ====================
@dp.message.outer_middleware()
async def check_chat_and_registration(handler, event: Message, data: dict):
    if event.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        async with db.pool.acquire() as conn:
            user = await conn.fetchrow("SELECT team_name FROM users WHERE user_id = $1", event.from_user.id)
            if not user or not user["team_name"]:
                bot_user = await bot.get_me()
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏒 Начать играть в ЛС", url=f"https://t.me/{bot_user.username}?start=reg")]
                ])
                await event.reply(
                    f"⚠️ {event.from_user.first_name}, вы еще не зарегистрировали свою команду!\n"
                    f"Начните диалог в личных сообщениях с ботом.",
                    reply_markup=kb
                )
                return
    return await handler(event, data)

# ==================== KEYBOARDS ====================
def main_menu_keyboard(is_private: bool = True) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="⚔️ Начать поиск игры", callback_data="search_game")],
        [InlineKeyboardButton(text="🏒 Состав", callback_data="view_roster")],
        [InlineKeyboardButton(text="🎁 Ежедневные призы (7 дней)", callback_data="streak_rewards")],
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
        [InlineKeyboardButton(text="➕ Создать карточку (Быстро)", callback_data="adm_card_quick"),
         InlineKeyboardButton(text="➕ Создать карточку (Вручную)", callback_data="adm_card_manual")],
        [InlineKeyboardButton(text="🏒 Управление командами", callback_data="adm_teams"),
         InlineKeyboardButton(text="📂 Управление коллекциями", callback_data="adm_collections")],
        [InlineKeyboardButton(text="📦 Создать/Настроить паки", callback_data="adm_packs"),
         InlineKeyboardButton(text="🎟 Создать промокод", callback_data="adm_promo")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]
    ])

# ==================== HELPER: GET USER STATS ====================
async def get_user_roster_ovr(user_id: int) -> Tuple[int, List[dict], List[dict]]:
    async with db.pool.acquire() as conn:
        all_cards = await conn.fetch("""
            SELECT c.*, uc.id as user_card_id, uc.in_lineup 
            FROM user_cards uc
            JOIN cards c ON uc.card_id = c.id
            WHERE uc.user_id = $1
            ORDER BY uc.in_lineup DESC, c.ovr DESC
        """, user_id)
        if not all_cards:
            return 50, [], []

        lineup_cards = [dict(c) for c in all_cards if c["in_lineup"]]
        if not lineup_cards:
            lineup_cards = [dict(c) for c in all_cards[:6]]

        avg_ovr = int(sum(c["ovr"] for c in lineup_cards) / len(lineup_cards))
        return avg_ovr, lineup_cards, [dict(c) for c in all_cards]

def render_profile_text(user: dict, full_name: str) -> str:
    lvl, cur_xp, req_xp = calculate_level_and_progress(user["exp"])
    p_name = user["player_name"] or full_name
    return (
        "🇷🇺 **Russian Puck League Bot!**\n\n"
        f"👤 **Игрок:** {p_name}\n"
        f"🏒 **Команда:** {user['team_name']}\n"
        f"🌍 **Страна:** {user['country']}\n"
        f"🎖 **Уровень:** {lvl}\n"
        f"⭐️ **Опыт:** {cur_xp}/{req_xp} XP\n"
        f"💰 **Баланс:** {user['rpl_coins']} RPLCoin\n"
        f"🏆 **MMR:** {user['mmr']}\n"
    )

# ==================== /START COMMAND ====================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user_name = message.from_user.full_name or message.from_user.username or "Хоккеист"

    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    if user and user["team_name"]:
        # Update user name if changed
        async with db.pool.acquire() as conn:
            await conn.execute("UPDATE users SET player_name = $1 WHERE user_id = $2", user_name, user_id)
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

        is_private = (message.chat.type == ChatType.PRIVATE)
        text = render_profile_text(user, user_name)
        await message.answer(text, reply_markup=main_menu_keyboard(is_private), parse_mode=ParseMode.MARKDOWN)
    else:
        # Onboarding registration
        await state.set_state(RegState.entering_team_name)
        await message.answer(
            "👋 **Добро пожаловать в Russian Puck League!**\n\n"
            "Вы еще не зарегистрировали свою хоккейную команду.\n"
            "Придумайте и отправьте **название вашей команды**:",
            parse_mode=ParseMode.MARKDOWN
        )

@dp.message(RegState.entering_team_name)
async def process_team_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 3 or len(name) > 30:
        await message.answer("⚠️ Название команды должно быть от 3 до 30 символов. Попробуйте еще раз:")
        return

    await state.update_data(team_name=name)
    await state.set_state(RegState.choosing_country)
    await message.answer(
        f"Отличное имя команды: **{name}**!\nТеперь выберите вашу страну из списка СНГ:",
        reply_markup=cis_countries_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.callback_query(RegState.choosing_country, F.data.startswith("setcountry:"))
async def process_country_choice(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("setcountry:")[1]
    data = await state.get_data()
    team_name = data["team_name"]
    user_id = callback.from_user.id
    player_name = callback.from_user.full_name or "Хоккеист"

    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, team_name, country, player_name, mmr, rpl_coins, exp)
            VALUES ($1, $2, $3, $4, 500, 1000, 50)
            ON CONFLICT (user_id) DO UPDATE SET team_name = $2, country = $3, player_name = $4
        """, user_id, team_name, country, player_name)

        starters = await conn.fetch("SELECT id FROM cards WHERE is_starter = TRUE")
        for st in starters:
            await conn.execute("""
                INSERT INTO user_cards (user_id, card_id, in_lineup)
                VALUES ($1, $2, TRUE)
                ON CONFLICT (user_id, card_id) DO NOTHING
            """, user_id, st["id"])

        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    await state.clear()
    profile_text = render_profile_text(user, player_name)
    await callback.message.edit_text(
        f"🎉 **Команда успешно зарегистрирована!**\n\n{profile_text}",
        reply_markup=main_menu_keyboard(callback.message.chat.type == ChatType.PRIVATE),
        parse_mode=ParseMode.MARKDOWN
    )

# ==================== MAIN MENU NAVIGATION ====================
@dp.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    if not user:
        return

    text = render_profile_text(user, callback.from_user.full_name)
    is_private = (callback.message.chat.type == ChatType.PRIVATE)
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=main_menu_keyboard(is_private), parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.message.edit_text(text, reply_markup=main_menu_keyboard(is_private), parse_mode=ParseMode.MARKDOWN)

# ==================== /CARD COMMAND (DAILY CARD & NO DUPLICATES) ====================
@dp.message(Command("card"))
async def cmd_card(message: Message):
    user_id = message.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not user or not user["team_name"]:
            await message.answer("⚠️ Сначала напишите /start и зарегистрируйте команду!")
            return

        now = datetime.utcnow()
        if user["last_daily_claim"]:
            diff = now - user["last_daily_claim"]
            if diff < timedelta(hours=12):
                remaining = timedelta(hours=12) - diff
                hours, rem = divmod(int(remaining.total_seconds()), 3600)
                minutes, _ = divmod(rem, 60)
                await message.answer(f"⏳ Карточка уже открывалась! До следующей попытки осталось: **{hours}ч {minutes}м**.", parse_mode=ParseMode.MARKDOWN)
                return

        # 5% chance to find "Ничего"
        if random.random() < 0.05:
            await conn.execute("UPDATE users SET last_daily_claim = $1 WHERE user_id = $2", now, user_id)
            await message.answer("💨 **Вам выпало:** Ничего!\nУвы, удача отвернулась в этот раз. Возвращайтесь через 12 часов!", parse_mode=ParseMode.MARKDOWN)
            return

        # Roll rarity according to chances (Legendary 5%)
        r = random.random()
        cumulative = 0.0
        chosen_rarity = "Редкая"
        for rarity, chance in DAILY_RARITIES_CHANCES.items():
            cumulative += chance
            if r <= cumulative:
                chosen_rarity = rarity
                break

        # CRITICAL RULE: Card cannot drop a second time! Filter out cards user already owns.
        available_cards = await conn.fetch("""
            SELECT * FROM cards 
            WHERE rarity = $1 
              AND id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $2)
        """, chosen_rarity, user_id)

        # Fallback if user already collected all cards of this rarity
        if not available_cards:
            available_cards = await conn.fetch("""
                SELECT * FROM cards 
                WHERE rarity != 'Секретная' 
                  AND id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $1)
            """, user_id)

        if not available_cards:
            await message.answer("🏆 Невероятно! Вы собрали абсолютно все доступные карточки в игре!")
            return

        card = random.choice(available_cards)
        await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, card["id"])
        await conn.execute("UPDATE users SET last_daily_claim = $1 WHERE user_id = $2", now, user_id)

    caption = (
        f"🎁 **Вам выпала новая карточка!**\n\n"
        f"🌟 **{card['name']}** (OVR: {card['ovr']})\n"
        f"Позиция: **{card['position']}** | Редкость: **{card['rarity']}**\n"
        f"Команда: **{card['team_name']}** | Коллекция: **{card['collection_name']}**\n"
        f"Страна: **{card['country']}**\n\n"
        f"✅ Карточка добавлена в вашу коллекцию!"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
    await message.answer_photo(photo=card["photo_id"], caption=caption, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

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
                # Broken streak
                can_claim = True
                streak_to_claim = 1

        reward_info = STREAK_REWARDS[streak_to_claim]
        reward_desc = reward_info["desc"]

        if reward_info["type"] == "coins":
            await conn.execute("""
                UPDATE users 
                SET rpl_coins = rpl_coins + $1, streak_days = $2, last_streak_claim = $3 
                WHERE user_id = $4
            """, reward_info["amount"], streak_to_claim, now, user_id)
            bonus_str = f"Вам начислено **{reward_info['amount']} RPLCoin**!"
            card_photo = None
        else:
            # Card drop 75-85 or 80-88, no duplicate!
            min_o, max_o = reward_info["min_ovr"], reward_info["max_ovr"]
            free_cards = await conn.fetch("""
                SELECT * FROM cards 
                WHERE ovr BETWEEN $1 AND $2 
                  AND id NOT IN (SELECT card_id FROM user_cards WHERE user_id = $3)
            """, min_o, max_o, user_id)

            if free_cards:
                c = random.choice(free_cards)
                await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, c["id"])
                bonus_str = f"Вам выпала карточка **{c['name']}** (OVR: {c['ovr']}, {c['rarity']})!"
                card_photo = c["photo_id"]
            else:
                # Fallback if all owned
                await conn.execute("UPDATE users SET rpl_coins = rpl_coins + 15000 WHERE user_id = $1", user_id)
                bonus_str = "Все карточки этого диапазона уже у вас! Начислена компенсация **15 000 RPLCoin**."
                card_photo = None

            await conn.execute("UPDATE users SET streak_days = $1, last_streak_claim = $2 WHERE user_id = $3", streak_to_claim, now, user_id)

    schedule_text = (
        "📅 **КАЛЕНДАРЬ ЕЖЕДНЕВНЫХ ПРИЗОВ (7 ДНЕЙ):**\n"
        f"{'🟢' if streak_to_claim == 1 else '⚪️'} День 1: 2 500 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 2 else '⚪️'} День 2: 5 000 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 3 else '⚪️'} День 3: 7 500 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 4 else '⚪️'} День 4: Карточка 75-85 OVR\n"
        f"{'🟢' if streak_to_claim == 5 else '⚪️'} День 5: 25 000 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 6 else '⚪️'} День 6: 35 000 RPLCoin\n"
        f"{'🟢' if streak_to_claim == 7 else '⚪️'} День 7: Карточка 80-88 OVR\n\n"
        f"🎉 **Получена награда за День {streak_to_claim}!**\n"
        f"{bonus_str}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]])
    if card_photo:
        await callback.message.delete()
        await callback.message.answer_photo(photo=card_photo, caption=schedule_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.message.edit_text(schedule_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ==================== ROSTER & LINEUP (VIEW IN GROUP, EDIT IN PM) ====================
@dp.callback_query(F.data == "view_roster")
async def cb_view_roster(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_pm = (callback.message.chat.type == ChatType.PRIVATE)
    avg_ovr, lineup, all_cards = await get_user_roster_ovr(user_id)

    if not all_cards:
        await callback.answer("У вас еще нет карточек!", show_alert=True)
        return

    text = (
        f"🏒 **СОСТАВ КОМАНДЫ**\n"
        f"Средний OVR активного звена: **{avg_ovr}**\n\n"
        f"⭐ **Основной состав (на льду):**\n"
    )
    for i, c in enumerate(lineup, 1):
        text += f"{i}. [{c['position']}] **{c['name']}** — OVR: **{c['ovr']}** ({c['rarity']})\n"

    buttons = []
    if is_pm:
        text += "\n🛠 *В личных сообщениях вы можете изменять состав (нажмите на карту ниже):*\n"
        for c in all_cards[:10]:
            status_ico = "🟢 В составе" if c["in_lineup"] else "⚪️ В запасе"
            buttons.append([InlineKeyboardButton(
                text=f"{status_ico} | {c['name']} ({c['ovr']})",
                callback_data=f"toggle_roster:{c['user_card_id']}"
            )])
    else:
        bot_user = await bot.get_me()
        buttons.append([InlineKeyboardButton(text="✏️ Настроить состав в ЛС", url=f"https://t.me/{bot_user.username}?start=roster")])

    buttons.append([InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data.startswith("toggle_roster:"))
async def cb_toggle_roster(callback: CallbackQuery):
    uc_id = int(callback.data.split("toggle_roster:")[1])
    user_id = callback.from_user.id

    async with db.pool.acquire() as conn:
        item = await conn.fetchrow("SELECT in_lineup FROM user_cards WHERE id = $1 AND user_id = $2", uc_id, user_id)
        if item:
            new_val = not item["in_lineup"]
            # Limit active lineup to 6 cards maximum
            if new_val:
                cnt = await conn.fetchval("SELECT count(*) FROM user_cards WHERE user_id = $1 AND in_lineup = TRUE", user_id)
                if cnt >= 6:
                    await callback.answer("❌ В основном составе может быть максимум 6 игроков!", show_alert=True)
                    return
            await conn.execute("UPDATE user_cards SET in_lineup = $1 WHERE id = $2", new_val, uc_id)

    await cb_view_roster(callback)

# ==================== MATCHMAKING & GAME ====================
@dp.callback_query(F.data == "search_game")
async def cb_search_game(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    ovr, lineup, all_cards = await get_user_roster_ovr(user_id)

    if not all_cards:
        await callback.answer("У вас нет карточек для матча!", show_alert=True)
        return

    user_info = {
        "user_id": user_id,
        "team_name": user["team_name"],
        "ovr": ovr,
        "card_ids": [c["id"] for c in all_cards],
        "message": callback.message
    }

    await callback.message.edit_text(
        "🔎 **Поиск соперника...**\n"
        "Ожидание оппонента (до 45 секунд).\n"
        "Если соперник не найдется, вы сыграете с ИИ!",
        parse_mode=ParseMode.MARKDOWN
    )

    matched, opp_data = await match_queue.add_player(user_id, user_info)
    if matched:
        asyncio.create_task(run_game_session(opp_data, user_info, is_pvp=True))
    else:
        opp = await match_queue.wait_or_cancel(user_id, timeout=45.0)
        if not opp:
            await run_ai_game_session(user_info)

async def run_game_session(player1: dict, player2: dict, is_pvp: bool = True):
    p1_name = player1["team_name"]
    p2_name = player2["team_name"]
    ovr1 = player1["ovr"]
    ovr2 = player2["ovr"]

    match_res = simulate_match(p1_name, ovr1, p2_name, ovr2)

    for period in range(1, 4):
        p1_sub = sum(match_res[f"p{i}"][0] for i in range(1, period + 1))
        p2_sub = sum(match_res[f"p{i}"][1] for i in range(1, period + 1))
        ev = match_res["events"][period - 1]
        msg_text = (
            f"🏒 **ЧЕМПИОНАТ RPL — МАТЧ В ЭФИРЕ**\n\n"
            f"🔴 **{p1_name}** (OVR {ovr1})  **{p1_sub} : {p2_sub}**  🔵 **{p2_name}** (OVR {ovr2})\n"
            f"⏳ **Период {period}/3**\n\n"
            f"📢 *{ev}*"
        )
        try:
            await player1["message"].edit_text(msg_text, parse_mode=ParseMode.MARKDOWN)
            if is_pvp and "message" in player2:
                await player2["message"].edit_text(msg_text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
        await asyncio.sleep(2.5)

    extra_text = ""
    if match_res["overtime"]:
        extra_text += "\n\n⚠️ Основное время завершилось вничью! **Овертайм 3х3**!"
        if match_res["shootout"]:
            extra_text += "\n🏒 **Серия послематчевых буллитов!**"

    winner_name = match_res["winner"]
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

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]])
    p1_str = "🎉 **ПОБЕДА!** (+25 MMR | +1000 RPLCoin | +50 EXP)" if is_p1_win else "😢 **ПОРАЖЕНИЕ** (-15 MMR | +200 RPLCoin | +0 EXP)"
    p2_str = "🎉 **ПОБЕДА!** (+25 MMR | +1000 RPLCoin | +50 EXP)" if not is_p1_win else "😢 **ПОРАЖЕНИЕ** (-15 MMR | +200 RPLCoin | +0 EXP)"

    text1 = (
        f"🏁 **МАТЧ ЗАВЕРШЕН!**\n\n"
        f"🔴 **{p1_name}** **{match_res['score1']} : {match_res['score2']}** 🔵 **{p2_name}**\n"
        f"Счет по периодам: ({match_res['p1'][0]}:{match_res['p1'][1]}, {match_res['p2'][0]}:{match_res['p2'][1]}, {match_res['p3'][0]}:{match_res['p3'][1]})"
        f"{extra_text}\n\n"
        f"Победитель: 🏆 **{winner_name}**\n\n{p1_str}"
    )
    try:
        await player1["message"].edit_text(text1, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

    if is_pvp and "message" in player2:
        text2 = (
            f"🏁 **МАТЧ ЗАВЕРШЕН!**\n\n"
            f"🔴 **{p1_name}** **{match_res['score1']} : {match_res['score2']}** 🔵 **{p2_name}**\n"
            f"Счет по периодам: ({match_res['p1'][0]}:{match_res['p1'][1]}, {match_res['p2'][0]}:{match_res['p2'][1]}, {match_res['p3'][0]}:{match_res['p3'][1]})"
            f"{extra_text}\n\n"
            f"Победитель: 🏆 **{winner_name}**\n\n{p2_str}"
        )
        try:
            await player2["message"].edit_text(text2, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

async def run_ai_game_session(player1: dict):
    # Rule: AI CANNOT have any cards that the player owns!
    user_card_ids = player1["card_ids"]
    user_ovr = player1["ovr"]

    async with db.pool.acquire() as conn:
        ai_cards_available = await conn.fetch("SELECT id, ovr FROM cards WHERE NOT (id = ANY($1))", user_card_ids)

    if ai_cards_available:
        selected_ai = random.sample(ai_cards_available, min(6, len(ai_cards_available)))
        ai_ovr = int(sum(c["ovr"] for c in selected_ai) / len(selected_ai))
    else:
        ai_ovr = max(50, min(99, user_ovr + random.randint(-2, 2)))

    ai_names = ["ХК Ледяные Волки", "ХК Северные Медведи", "ХК Красная Звезда ИИ", "ХК Стальные Буйволы"]
    ai_team = {
        "user_id": 0,
        "team_name": random.choice(ai_names),
        "ovr": ai_ovr
    }
    await run_game_session(player1, ai_team, is_pvp=False)

# ==================== LEADERBOARD (MMR) ====================
@dp.callback_query(F.data == "leaderboard_mmr")
async def cb_leaderboard(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        top_users = await conn.fetch("""
            SELECT player_name, team_name, country, mmr, wins, losses 
            FROM users 
            WHERE team_name IS NOT NULL 
            ORDER BY mmr DESC LIMIT 10
        """)

    text = "🏆 **ТАБЛИЦА ЛИДЕРОВ RPL (ТОП-10 MMR):**\n\n"
    for idx, u in enumerate(top_users, 1):
        medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"{idx}."
        text += f"{medal} **{u['team_name']}** ({u['country']}) — **{u['mmr']} MMR** (П: {u['wins']} | Пр: {u['losses']})\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ==================== PACKS & SHOP ====================
@dp.callback_query(F.data == "pack_shop")
async def cb_pack_shop(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        packs = await conn.fetch("SELECT * FROM packs ORDER BY price ASC")
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", callback.from_user.id)

    if not packs:
        await callback.answer("Паки скоро поступят в магазин!", show_alert=True)
        return

    text = f"📦 **МАГАЗИН ПАКОВ**\nВаш баланс: **{user['rpl_coins']} RPLCoin**\n\nВыберите пак:"
    buttons = []
    for p in packs:
        buttons.append([InlineKeyboardButton(text=f"📦 {p['name']} — {p['price']} RPL", callback_data=f"buy_pack:{p['id']}")])
    buttons.append([InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")])

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.MARKDOWN)

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

        # Find cards from pack that user doesn't already own
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
        card = await conn.fetchrow("SELECT * FROM cards WHERE id = $1", chosen_id)

        await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", pack["price"], user_id)
        await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, chosen_id)

    caption = (
        f"🎉 **ВЫ ОТКРЫЛИ {pack['name']}!**\n\n"
        f"🌟 **{card['name']}** (OVR: {card['ovr']})\n"
        f"Позиция: **{card['position']}** | Редкость: **{card['rarity']}**\n"
        f"Команда: **{card['team_name']}**"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Еще паки", callback_data="pack_shop")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="back_main")]
    ])
    await callback.message.delete()
    await callback.message.answer_photo(photo=card["photo_id"], caption=caption, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

# ==================== PROMO CODES ====================
@dp.callback_query(F.data == "enter_promo")
async def cb_enter_promo(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserPromoState.code)
    await callback.message.edit_text(
        "🎟 **Введите промокод:**\nОтправьте текст промокода сообщением.",
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
            res_text = f"🎉 Промокод активирован! Вы получили **{promo['reward_value']} RPLCoin**!"
        else:
            card = await conn.fetchrow("SELECT name, ovr FROM cards WHERE id = $1", promo["reward_value"])
            await conn.execute("""
                INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)
                ON CONFLICT (user_id, card_id) DO NOTHING
            """, user_id, promo["reward_value"])
            card_title = f"{card['name']} (OVR {card['ovr']})" if card else f"Карточка #{promo['reward_value']}"
            res_text = f"🎉 Промокод активирован! Вы получили: **{card_title}**!"

    await state.clear()
    await message.answer(res_text, reply_markup=main_menu_keyboard(message.chat.type == ChatType.PRIVATE), parse_mode=ParseMode.MARKDOWN)

# ==================== ADMIN PANEL (/adminrpl) ====================
@dp.message(Command("adminrpl"))
async def cmd_admin_rpl(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("⚙️ **ПАНЕЛЬ АДМИНИСТРАТОРА RPL**\nВыберите действие:", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "adm_card_quick")
async def cb_adm_quick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminCardQuick.raw_data)
    await callback.message.edit_text(
        "⚡ **БЫСТРОЕ ДОБАВЛЕНИЕ КАРТОЧКИ**\n\n"
        "Отправьте данные карточки через символ `|`:\n"
        "`Имя|Редкость|Страна|OVR|Позиция|Команда|Коллекция|Стартовая(0 или 1)`\n\n"
        "Пример:\n"
        "`Александр Овечкин|Легендарная|🇷🇺 Россия|95|Skater|Без команды|Основная|0`",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="back_main")]]),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.message(AdminCardQuick.raw_data)
async def process_adm_quick_raw(message: Message, state: FSMContext):
    parts = [p.strip() for p in message.text.split("|")]
    if len(parts) < 8:
        await message.answer("❌ Ошибка формата! Должно быть ровно 8 параметров через `|`.")
        return

    name, rarity, country, ovr_str, position, team, collection, is_starter_str = parts
    if rarity not in RARITIES:
        await message.answer(f"❌ Неверная редкость! Допустимые: {', '.join(RARITIES)}")
        return
    if not ovr_str.isdigit() or not (50 <= int(ovr_str) <= 99):
        await message.answer("❌ OVR должен быть целым числом от 50 до 99!")
        return

    await state.update_data(
        name=name, rarity=rarity, country=country, ovr=int(ovr_str),
        position=position, team_name=team, collection_name=collection,
        is_starter=(is_starter_str == "1")
    )
    await state.set_state(AdminCardQuick.photo)
    await message.answer("📸 Теперь отправьте фото для этой карточки:")

@dp.message(AdminCardQuick.photo, F.photo)
async def process_adm_quick_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO cards (name, rarity, country, ovr, position, team_name, collection_name, photo_id, is_starter)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """, data["name"], data["rarity"], data["country"], data["ovr"], data["position"], data["team_name"], data["collection_name"], photo_id, data["is_starter"])

    await state.clear()
    await message.answer(f"✅ Карточка **{data['name']}** (OVR: {data['ovr']}) сохранена с ID **#{row['id']}**!", reply_markup=admin_main_keyboard())

# --- Admin: Manual Card ---
@dp.callback_query(F.data == "adm_card_manual")
async def cb_adm_manual(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminCardManual.name)
    await callback.message.edit_text("📝 **РУЧНОЕ СОЗДАНИЕ КАРТОЧКИ**\nВведите имя игрока:")

@dp.message(AdminCardManual.name)
async def process_man_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminCardManual.rarity)
    buttons = [[InlineKeyboardButton(text=r, callback_data=f"man_r:{r}")] for r in RARITIES]
    await message.answer("Выберите редкость карточки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.rarity, F.data.startswith("man_r:"))
async def process_man_rarity(callback: CallbackQuery, state: FSMContext):
    rarity = callback.data.split("man_r:")[1]
    await state.update_data(rarity=rarity)
    await state.set_state(AdminCardManual.country)
    buttons = [[InlineKeyboardButton(text=c, callback_data=f"man_c:{c}")] for c in CIS_COUNTRIES]
    await callback.message.edit_text("Выберите страну игрока:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.country, F.data.startswith("man_c:"))
async def process_man_country(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("man_c:")[1]
    await state.update_data(country=country)
    await state.set_state(AdminCardManual.ovr)
    await callback.message.edit_text("Введите OVR карточки (50 - 99):")

@dp.message(AdminCardManual.ovr)
async def process_man_ovr(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (50 <= int(message.text) <= 99):
        await message.answer("❌ Введите корректный OVR (50 - 99):")
        return
    await state.update_data(ovr=int(message.text))
    await state.set_state(AdminCardManual.position)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Полевой (Skater)", callback_data="man_p:Skater")],
        [InlineKeyboardButton(text="Вратарь (Goalie)", callback_data="man_p:Goalie")]
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
    await callback.message.edit_text("Выберите коллекцию:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.collection, F.data.startswith("man_col:"))
async def process_man_collection(callback: CallbackQuery, state: FSMContext):
    col = callback.data.split("man_col:")[1]
    await state.update_data(collection_name=col)
    await state.set_state(AdminCardManual.is_starter)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, стартовая для новичков", callback_data="man_s:1")],
        [InlineKeyboardButton(text="Нет, обычная", callback_data="man_s:0")]
    ])
    await callback.message.edit_text("Выдавать эту карту новичкам при старте?", reply_markup=kb)

@dp.callback_query(AdminCardManual.is_starter, F.data.startswith("man_s:"))
async def process_man_starter(callback: CallbackQuery, state: FSMContext):
    is_starter = (callback.data.split("man_s:")[1] == "1")
    await state.update_data(is_starter=is_starter)
    await state.set_state(AdminCardManual.photo)
    await callback.message.edit_text("Отправьте фото для карточки:")

@dp.message(AdminCardManual.photo, F.photo)
async def process_man_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO cards (name, rarity, country, ovr, position, team_name, collection_name, photo_id, is_starter)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """, data["name"], data["rarity"], data["country"], data["ovr"], data["position"], data["team_name"], data["collection_name"], photo_id, data["is_starter"])

    await state.clear()
    await message.answer(f"✅ Карточка **{data['name']}** создана! ID: **#{row['id']}**", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.MARKDOWN)

# --- Admin: Teams & Collections ---
@dp.callback_query(F.data == "adm_teams")
async def cb_adm_teams(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminTeamState.team_name)
    await callback.message.edit_text("Введите название новой команды:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="back_main")]]))

@dp.message(AdminTeamState.team_name)
async def process_adm_team(message: Message, state: FSMContext):
    name = message.text.strip()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO teams (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
    await state.clear()
    await message.answer(f"✅ Команда **{name}** сохранена!", reply_markup=admin_main_keyboard())

@dp.callback_query(F.data == "adm_collections")
async def cb_adm_collections(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminCollectionState.collection_name)
    await callback.message.edit_text("Введите название новой коллекции:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="back_main")]]))

@dp.message(AdminCollectionState.collection_name)
async def process_adm_col(message: Message, state: FSMContext):
    name = message.text.strip()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO collections (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
    await state.clear()
    await message.answer(f"✅ Коллекция **{name}** сохранена!", reply_markup=admin_main_keyboard())

# --- Admin: Packs ---
@dp.callback_query(F.data == "adm_packs")
async def cb_adm_packs(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPackState.name)
    await callback.message.edit_text("Введите название пака:")

@dp.message(AdminPackState.name)
async def process_pack_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminPackState.price)
    await message.answer("Введите цену пака в RPLCoin:")

@dp.message(AdminPackState.price)
async def process_pack_price(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Цена должна быть числом!")
        return
    await state.update_data(price=int(message.text))
    await state.set_state(AdminPackState.card_ids)
    await message.answer("Введите ID карточек через запятую (например: 1, 5, 8, 12):")

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
    await message.answer(f"✅ Пак **{data['name']}** создан!", reply_markup=admin_main_keyboard())

# --- Admin: Promo ---
@dp.callback_query(F.data == "adm_promo")
async def cb_adm_promo(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPromoState.code)
    await callback.message.edit_text("Введите код нового промокода (например: RPLOVE):")

@dp.message(AdminPromoState.code)
async def process_promo_code(message: Message, state: FSMContext):
    await state.update_data(code=message.text.strip().upper())
    await state.set_state(AdminPromoState.reward_type)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 RPLCoins", callback_data="pr_t:money")],
        [InlineKeyboardButton(text="🃏 Карточка (ID)", callback_data="pr_t:card")]
    ])
    await message.answer("Выберите тип награды:", reply_markup=kb)

@dp.callback_query(AdminPromoState.reward_type, F.data.startswith("pr_t:"))
async def process_promo_type(callback: CallbackQuery, state: FSMContext):
    rw = callback.data.split("pr_t:")[1]
    await state.update_data(reward_type=rw)
    await state.set_state(AdminPromoState.reward_value)
    prompt = "Введите количество монет:" if rw == "money" else "Введите ID карточки для выдачи:"
    await callback.message.edit_text(prompt)

@dp.message(AdminPromoState.reward_value)
async def process_promo_val(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Введите число!")
        return
    await state.update_data(reward_value=int(message.text))
    await state.set_state(AdminPromoState.max_uses)
    await message.answer("Введите максимальное количество использований:")

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
    await message.answer(f"✅ Промокод **{data['code']}** готов!", reply_markup=admin_main_keyboard())

# ==================== MAIN ENTRY POINT ====================
async def main():
    if not BOT_TOKEN or not DATABASE_URL:
        logger.error("❌ BOT_TOKEN or DATABASE_URL is missing!")
        return

    logger.info("Connecting to Database...")
    await db.connect()
    logger.info("Database connected successfully!")

    logger.info("Starting Telegram Bot Polling...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
