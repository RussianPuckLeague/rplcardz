import asyncio
import logging
import os
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== DATABASE ====================
class Database:
    def __init__(self, url: str):
        # Fix Railway URL if needed (postgres:// -> postgresql://)
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
                    mmr INT DEFAULT 500,
                    rpl_coins INT DEFAULT 1000,
                    exp INT DEFAULT 50,
                    wins INT DEFAULT 0,
                    losses INT DEFAULT 0,
                    last_daily_claim TIMESTAMP,
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
                    position VARCHAR(20) NOT NULL, -- 'Skater' or 'Goalie'
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
                    reward_type VARCHAR(20) NOT NULL, -- 'money' or 'card'
                    reward_value INT NOT NULL, -- coin amount or card_id
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

# ==================== MATCHMAKING QUEUE ====================
class MatchQueue:
    def __init__(self):
        self.queue: Dict[int, Tuple[asyncio.Event, dict]] = {}
        self.lock = asyncio.Lock()

    async def add_player(self, user_id: int, user_data: dict) -> Tuple[bool, Optional[dict]]:
        async with self.lock:
            if user_id in self.queue:
                return False, None
            # Check if anyone is waiting
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

# ==================== HOCKEY SIMULATION ENGINE ====================
HOCKEY_EVENTS = [
    "⚡ Мощный щелчок от синей линии! {attacking_team} атакует, но вратарь парирует щитком!",
    "🏒 Ой-ой! У нападающего {attacking_team} ломается клюшка в самый ответственный момент броска!",
    "🛡 Защитник ложится под шайбу и ценой ушиба спасает ворота {defending_team}!",
    "💥 Сокрушительный силовой прием у борта! Зрители на трибунах вскакивают со своих мест!",
    "🧤 Невероятный шпагат! Голкипер {defending_team} тащит мертвую шайбу из самой девятки!",
    "🎯 Опаснейший выход 2 в 1! Нападающий {attacking_team} бросает — ЗВОН ШТАНГИ!",
    "⏱ Грубая игра за воротами! Судья фиксирует удаление на 2 минуты в составе {defending_team}!",
    "🚨 ГООООЛ! Точнейший бросок в домик голкиперу оформляет форвард {attacking_team}!",
    "🚨 ГООООЛ! Ювелирный кистевой бросок после быстрой контратаки {attacking_team}!",
    "🧤 Вратарь {defending_team} намертво фиксирует шайбу ловушкой под свист трибун.",
    "🌪 Перехват в средней зоне! {attacking_team} проводит молниеносный контрвыпад!"
]

def simulate_match(team1_name: str, ovr1: int, team2_name: str, ovr2: int) -> dict:
    diff = ovr1 - ovr2
    # 70% favorite probability factor with frequent draw tendency
    prob1 = 0.50 + (diff * 0.02)
    prob1 = max(0.20, min(0.80, prob1))

    # Base match: 3 periods
    p1_t1, p1_t2 = random.choices([0, 1, 2], weights=[0.5, 0.35, 0.15])[0], random.choices([0, 1, 2], weights=[0.5, 0.35, 0.15])[0]
    p2_t1, p2_t2 = random.choices([0, 1, 2], weights=[0.45, 0.35, 0.2])[0], random.choices([0, 1, 2], weights=[0.45, 0.35, 0.2])[0]
    p3_t1, p3_t2 = random.choices([0, 1, 2], weights=[0.4, 0.4, 0.2])[0], random.choices([0, 1, 2], weights=[0.4, 0.4, 0.2])[0]

    # Skew with probability
    if random.random() < prob1:
        if random.random() < 0.65:
            p2_t1 += 1
    else:
        if random.random() < 0.65:
            p2_t2 += 1

    score1 = p1_t1 + p2_t1 + p3_t1
    score2 = p1_t2 + p2_t2 + p3_t2

    overtime = False
    shootout = False
    winner = None

    if score1 == score2:
        overtime = True
        ot_t1, ot_t2 = random.choices([(0, 0), (1, 0), (0, 1)], weights=[0.50, 0.25, 0.25])[0]
        if ot_t1 != ot_t2:
            score1 += ot_t1
            score2 += ot_t2
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

    # Select random dynamic commentary events
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

# ==================== BOT & DISPATCHER ====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ==================== MIDDLEWARE / CHAT RESTRICTION ====================
@dp.message.outer_middleware()
async def check_chat_and_registration(handler, event: Message, data: dict):
    # Check registration only if not in private chat
    if event.chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        async with db.pool.acquire() as conn:
            user = await conn.fetchrow("SELECT team_name FROM users WHERE user_id = $1", event.from_user.id)
            if not user or not user["team_name"]:
                bot_user = await bot.get_me()
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🏒 Начать играть в ЛС", url=f"https://t.me/{bot_user.username}?start=reg")]
                ])
                await event.reply(
                    f"⚠️ {event.from_user.first_name}, вы еще не создали команду! Перейдите в личные сообщения с ботом.",
                    reply_markup=kb
                )
                return
    return await handler(event, data)

# ==================== KEYBOARDS ====================
def main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="⚔️ Начать поиск игры", callback_data="search_game")],
        [InlineKeyboardButton(text="🎁 Ежедневная карточка (12ч)", callback_data="daily_card")],
        [InlineKeyboardButton(text="🏒 Мой состав & Карточки", callback_data="my_cards"),
         InlineKeyboardButton(text="📦 Магазин паков", callback_data="pack_shop")],
        [InlineKeyboardButton(text="🏆 Рейтинг MMR", callback_data="leaderboard_mmr"),
         InlineKeyboardButton(text="🎟 Промокод", callback_data="enter_promo")]
    ]
    if is_admin:
        kb.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать карточку (Быстро)", callback_data="adm_card_quick"),
         InlineKeyboardButton(text="➕ Создать карточку (Вручную)", callback_data="adm_card_manual")],
        [InlineKeyboardButton(text="🏒 Команды", callback_data="adm_teams"),
         InlineKeyboardButton(text="📂 Коллекции", callback_data="adm_collections")],
        [InlineKeyboardButton(text="📦 Создать/Управлять Паками", callback_data="adm_packs"),
         InlineKeyboardButton(text="🎟 Создать Промокод", callback_data="adm_promo")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])

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

# ==================== HELPER: GET USER STATS & OVR ====================
async def get_user_roster_ovr(user_id: int) -> Tuple[int, List[dict]]:
    async with db.pool.acquire() as conn:
        # Get user's cards sorted by OVR desc
        cards = await conn.fetch("""
            SELECT c.*, uc.id as user_card_id, uc.in_lineup 
            FROM user_cards uc
            JOIN cards c ON uc.card_id = c.id
            WHERE uc.user_id = $1
            ORDER BY c.ovr DESC
        """, user_id)
        if not cards:
            return 50, []
        # Calculate active OVR (top 6 cards or all if < 6)
        top_cards = cards[:6]
        avg_ovr = int(sum(c["ovr"] for c in top_cards) / len(top_cards))
        return avg_ovr, [dict(c) for c in cards]

# ==================== START / ONBOARDING ====================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    if user and user["team_name"]:
        avg_ovr, _ = await get_user_roster_ovr(user_id)
        text = (
            f"🏒 **Добро пожаловать в Хоккейную Арену!**\n\n"
            f"Команда: **{user['team_name']}** ({user['country']})\n"
            f"🏆 MMR: **{user['mmr']}** | ⭐️ EXP: **{user['exp']}**\n"
            f"💰 Баланс: **{user['rpl_coins']} RPLCoin**\n"
            f"📊 Средний OVR состава: **{avg_ovr}**\n\n"
            f"Выберите действие в меню:"
        )
        is_admin = user_id in ADMIN_IDS
        await message.answer(text, reply_markup=main_menu_keyboard(is_admin), parse_mode=ParseMode.MARKDOWN)
    else:
        # Start onboarding
        await state.set_state(RegState.entering_team_name)
        await message.answer(
            "👋 **Добро пожаловать в мир карточного хоккея!**\n\n"
            "Вы еще не зарегистрированы как генеральный менеджер.\n"
            "Придумайте и напишите **название вашей хоккейной команды**:",
            parse_mode=ParseMode.MARKDOWN
        )

@dp.message(RegState.entering_team_name)
async def process_team_name(message: Message, state: FSMContext):
    team_name = message.text.strip()
    if len(team_name) < 3 or len(team_name) > 30:
        await message.answer("⚠️ Название команды должно быть от 3 до 30 символов. Попробуйте еще раз:")
        return

    await state.update_data(team_name=team_name)
    await state.set_state(RegState.choosing_country)
    await message.answer(
        f"Отличное имя команды: **{team_name}**!\nТеперь выберите вашу страну из списка СНГ:",
        reply_markup=cis_countries_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

@dp.callback_query(RegState.choosing_country, F.data.startswith("setcountry:"))
async def process_country_choice(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("setcountry:")[1]
    data = await state.get_data()
    team_name = data["team_name"]
    user_id = callback.from_user.id

    async with db.pool.acquire() as conn:
        # Register user
        await conn.execute("""
            INSERT INTO users (user_id, team_name, country, mmr, rpl_coins, exp)
            VALUES ($1, $2, $3, 500, 1000, 50)
            ON CONFLICT (user_id) DO UPDATE SET team_name = $2, country = $3
        """, user_id, team_name, country)

        # Grant starter cards
        starters = await conn.fetch("SELECT id, name, ovr FROM cards WHERE is_starter = TRUE")
        granted_count = 0
        if starters:
            for c in starters:
                await conn.execute("INSERT INTO user_cards (user_id, card_id, in_lineup) VALUES ($1, $2, TRUE)", user_id, c["id"])
                granted_count += 1

    await state.clear()
    msg_starter = f"Выдано **{granted_count}** стартовых карточек!" if granted_count > 0 else "Стартовые карты скоро появятся в игре!"
    await callback.message.edit_text(
        f"🎉 **Регистрация успешно завершена!**\n\n"
        f"Команда: **{team_name}**\n"
        f"Страна: **{country}**\n"
        f"🏆 Стартовый MMR: **500**\n"
        f"💰 Стартовый капитал: **1000 RPLCoin**\n"
        f"⭐️ Стартовый опыт: **50 EXP**\n\n"
        f"{msg_starter}\n\n"
        f"Готовы выйти на лед?",
        reply_markup=main_menu_keyboard(user_id in ADMIN_IDS),
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
    avg_ovr, _ = await get_user_roster_ovr(user_id)
    text = (
        f"🏒 **Хоккейная Арена**\n\n"
        f"Команда: **{user['team_name']}** ({user['country']})\n"
        f"🏆 MMR: **{user['mmr']}** | ⭐️ EXP: **{user['exp']}**\n"
        f"💰 Баланс: **{user['rpl_coins']} RPLCoin**\n"
        f"📊 OVR состава: **{avg_ovr}**\n\n"
        f"Выберите действие:"
    )
    # Check if current message has photo, delete/replace cleanly to prevent spam
    if callback.message.photo:
        await callback.message.delete()
        await callback.message.answer(text, reply_markup=main_menu_keyboard(user_id in ADMIN_IDS), parse_mode=ParseMode.MARKDOWN)
    else:
        await callback.message.edit_text(text, reply_markup=main_menu_keyboard(user_id in ADMIN_IDS), parse_mode=ParseMode.MARKDOWN)

# ==================== DAILY FREE CARD ====================
@dp.callback_query(F.data == "daily_card")
async def cb_daily_card(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT last_daily_claim FROM users WHERE user_id = $1", user_id)
        now = datetime.utcnow()
        if user["last_daily_claim"]:
            diff = now - user["last_daily_claim"]
            if diff < timedelta(hours=12):
                remaining = timedelta(hours=12) - diff
                hours, rem = divmod(int(remaining.total_seconds()), 3600)
                minutes, _ = divmod(rem, 60)
                await callback.answer(f"⏳ Следующая карточка будет доступна через {hours}ч {minutes}м!", show_alert=True)
                return

        # Determine rarity drop
        r = random.random()
        cumulative = 0.0
        chosen_rarity = "Редкая"
        for rarity, chance in DAILY_RARITIES_CHANCES.items():
            cumulative += chance
            if r <= cumulative:
                chosen_rarity = rarity
                break

        # Pick random card with chosen rarity
        cards = await conn.fetch("SELECT * FROM cards WHERE rarity = $1", chosen_rarity)
        if not cards:
            cards = await conn.fetch("SELECT * FROM cards WHERE rarity != 'Секретная'")

        if not cards:
            await callback.answer("В базе еще нет карточек. Обратитесь к админу!", show_alert=True)
            return

        card = random.choice(cards)
        await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, card["id"])
        await conn.execute("UPDATE users SET last_daily_claim = $1 WHERE user_id = $2", now, user_id)

    caption = (
        f"🎁 **Вам выпала ежедневная карточка!**\n\n"
        f"🌟 **{card['name']}** (OVR: {card['ovr']})\n"
        f"Позиция: **{card['position']}** | Редкость: **{card['rarity']}**\n"
        f"Страна: **{card['country']}**\n"
        f"Команда: **{card['team_name']}** | Коллекция: **{card['collection_name']}**"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])

    if callback.message.photo:
        await callback.message.edit_media(media=InputMediaPhoto(media=card["photo_id"], caption=caption, parse_mode=ParseMode.MARKDOWN), reply_markup=kb)
    else:
        await callback.message.delete()
        await callback.message.answer_photo(photo=card["photo_id"], caption=caption, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

# ==================== VIEW USER ROSTER & CARDS ====================
@dp.callback_query(F.data == "my_cards")
async def cb_my_cards(callback: CallbackQuery):
    user_id = callback.from_user.id
    avg_ovr, cards = await get_user_roster_ovr(user_id)
    if not cards:
        await callback.answer("У вас пока нет карточек в коллекции!", show_alert=True)
        return

    text = f"📋 **Ваш состав и инвентарь (OVR: {avg_ovr}):**\nВсего карточек: {len(cards)}\n\n"
    for i, c in enumerate(cards[:15], 1):
        text += f"{i}. **{c['name']}** [{c['position']}] — OVR: **{c['ovr']}** ({c['rarity']}) #{c['id']}\n"
    if len(cards) > 15:
        text += f"\n...и еще {len(cards) - 15} карточек."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Осмотреть карту по ID", callback_data="inspect_card_prompt")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ==================== LEADERBOARD (MMR) ====================
@dp.callback_query(F.data == "leaderboard_mmr")
async def cb_leaderboard(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        top_users = await conn.fetch("""
            SELECT team_name, country, mmr, wins, losses 
            FROM users 
            WHERE team_name IS NOT NULL 
            ORDER BY mmr DESC LIMIT 10
        """)

    text = "🏆 **ТОП-10 КОМАНД ПО РЕЙТИНГУ MMR:**\n\n"
    for idx, u in enumerate(top_users, 1):
        medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"{idx}."
        text += f"{medal} **{u['team_name']}** ({u['country']}) — **{u['mmr']} MMR** (П: {u['wins']} | Пр: {u['losses']})\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ==================== MATCHMAKING & GAME SEARCH ====================
@dp.callback_query(F.data == "search_game")
async def cb_search_game(callback: CallbackQuery):
    user_id = callback.from_user.id
    async with db.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    ovr, user_cards = await get_user_roster_ovr(user_id)
    if not user_cards:
        await callback.answer("У вас нет карточек в составе для игры!", show_alert=True)
        return

    user_info = {
        "user_id": user_id,
        "team_name": user["team_name"],
        "ovr": ovr,
        "card_ids": [c["id"] for c in user_cards],
        "message": callback.message
    }

    await callback.message.edit_text(
        "🔎 **Поиск соперника...**\n"
        "Ожидание оппонента (таймаут: 45 секунд).\n"
        "Если игрок не найдется, вы сыграете с ИИ!",
        parse_mode=ParseMode.MARKDOWN
    )

    matched, opp_data = await match_queue.add_player(user_id, user_info)
    if matched:
        # We matched immediately with an existing waiter
        asyncio.create_task(run_game_session(opp_data, user_info, is_pvp=True))
    else:
        # Wait up to 45 seconds
        opp = await match_queue.wait_or_cancel(user_id, timeout=45.0)
        if opp:
            # We got matched by someone who joined later
            pass # The joiner already started the run_game_session task
        else:
            # Timeout -> Match vs AI
            await run_ai_game_session(user_info)

async def run_game_session(player1: dict, player2: dict, is_pvp: bool = True):
    p1_name = player1["team_name"]
    p2_name = player2["team_name"]
    ovr1 = player1["ovr"]
    ovr2 = player2["ovr"]

    match_res = simulate_match(p1_name, ovr1, p2_name, ovr2)

    # Live simulation updates in single message
    for period in range(1, 4):
        p1_sub = sum(match_res[f"p{i}"][0] for i in range(1, period + 1))
        p2_sub = sum(match_res[f"p{i}"][1] for i in range(1, period + 1))
        ev = match_res["events"][period - 1]
        msg_text = (
            f"🏒 **МАТЧ НА ЛЕДОВОЙ АРЕНЕ**\n\n"
            f"🔴 **{p1_name}** (OVR {ovr1}) **{p1_sub} : {p2_sub}** 🔵 **{p2_name}** (OVR {ovr2})\n"
            f"⏳ **Идет {period}-й период...**\n\n"
            f"📢 *{ev}*"
        )
        try:
            await player1["message"].edit_text(msg_text, parse_mode=ParseMode.MARKDOWN)
            if is_pvp and "message" in player2:
                await player2["message"].edit_text(msg_text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
        await asyncio.sleep(2.5)

    # Overtime / Shootouts
    extra_text = ""
    if match_res["overtime"]:
        extra_text += "\n\n⚠️ Основное время завершилось вничью! **Овертайм 3х3**!"
        if match_res["shootout"]:
            extra_text += "\n🏒 Ничья продолжается! **Серия буллитов!**"

    winner_name = match_res["winner"]
    is_p1_win = (winner_name == p1_name)

    # Rewards logic: +25 MMR, 1000 RPLCoin, 50 EXP for win; -15 MMR, 200 RPLCoin, 0 EXP for loss
    async with db.pool.acquire() as conn:
        if is_p1_win:
            await conn.execute("UPDATE users SET mmr = mmr + 25, rpl_coins = rpl_coins + 1000, exp = exp + 50, wins = wins + 1 WHERE user_id = $1", player1["user_id"])
            if is_pvp:
                await conn.execute("UPDATE users SET mmr = GREATEST(0, mmr - 15), rpl_coins = rpl_coins + 200, losses = losses + 1 WHERE user_id = $1", player2["user_id"])
        else:
            await conn.execute("UPDATE users SET mmr = GREATEST(0, mmr - 15), rpl_coins = rpl_coins + 200, losses = losses + 1 WHERE user_id = $1", player1["user_id"])
            if is_pvp:
                await conn.execute("UPDATE users SET mmr = mmr + 25, rpl_coins = rpl_coins + 1000, exp = exp + 50, wins = wins + 1 WHERE user_id = $1", player2["user_id"])

    # Final result message
    p1_reward_str = "🎉 **ПОБЕДА!**\n+25 MMR | +1000 RPLCoin | +50 EXP" if is_p1_win else "😢 **ПОРАЖЕНИЕ**\n-15 MMR | +200 RPLCoin | +0 EXP"
    p2_reward_str = "🎉 **ПОБЕДА!**\n+25 MMR | +1000 RPLCoin | +50 EXP" if not is_p1_win else "😢 **ПОРАЖЕНИЕ**\n-15 MMR | +200 RPLCoin | +0 EXP"

    final_text_p1 = (
        f"🏁 **МАТЧ ЗАВЕРШЕН!**\n\n"
        f"🔴 **{p1_name}** **{match_res['score1']} : {match_res['score2']}** 🔵 **{p2_name}**\n"
        f"Периоды: ({match_res['p1'][0]}:{match_res['p1'][1]}, {match_res['p2'][0]}:{match_res['p2'][1]}, {match_res['p3'][0]}:{match_res['p3'][1]})"
        f"{extra_text}\n\n"
        f"Победитель: 🏆 **{winner_name}**\n\n"
        f"{p1_reward_str}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_main")]])
    try:
        await player1["message"].edit_text(final_text_p1, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

    if is_pvp and "message" in player2:
        final_text_p2 = (
            f"🏁 **МАТЧ ЗАВЕРШЕН!**\n\n"
            f"🔴 **{p1_name}** **{match_res['score1']} : {match_res['score2']}** 🔵 **{p2_name}**\n"
            f"Периоды: ({match_res['p1'][0]}:{match_res['p1'][1]}, {match_res['p2'][0]}:{match_res['p2'][1]}, {match_res['p3'][0]}:{match_res['p3'][1]})"
            f"{extra_text}\n\n"
            f"Победитель: 🏆 **{winner_name}**\n\n"
            f"{p2_reward_str}"
        )
        try:
            await player2["message"].edit_text(final_text_p2, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

async def run_ai_game_session(player1: dict):
    # Rule: Pick AI cards that player DOES NOT have!
    user_card_ids = player1["card_ids"]
    user_ovr = player1["ovr"]

    async with db.pool.acquire() as conn:
        ai_available_cards = await conn.fetch("SELECT id, ovr FROM cards WHERE NOT (id = ANY($1))", user_card_ids)

    if ai_available_cards:
        # Pick cards with similar OVR
        ai_cards = random.sample(ai_available_cards, min(6, len(ai_available_cards)))
        ai_ovr = int(sum(c["ovr"] for c in ai_cards) / len(ai_cards))
    else:
        # Fallback if DB has very few cards
        ai_ovr = max(50, min(99, user_ovr + random.randint(-3, 3)))

    ai_names = ["ХК Ледяные Волки", "ХК Стальные Крылья", "ХК Северные Медведи", "ХК Красная Машина ИИ", "ХК Алмазные Тигры"]
    ai_team = {
        "user_id": 0,
        "team_name": random.choice(ai_names),
        "ovr": ai_ovr
    }
    await run_game_session(player1, ai_team, is_pvp=False)

# ==================== PACKS & SHOP ====================
@dp.callback_query(F.data == "pack_shop")
async def cb_pack_shop(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        packs = await conn.fetch("SELECT * FROM packs ORDER BY price ASC")
        user = await conn.fetchrow("SELECT rpl_coins FROM users WHERE user_id = $1", callback.from_user.id)

    if not packs:
        await callback.answer("В магазине пока нет доступных паков!", show_alert=True)
        return

    text = f"📦 **МАГАЗИН ПАКОВ КАРТОЧЕК**\nВаш баланс: **{user['rpl_coins']} RPLCoin**\n\nВыберите пак для открытия:"
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

        if not pack:
            await callback.answer("Пак не найден!", show_alert=True)
            return

        if user["rpl_coins"] < pack["price"]:
            await callback.answer("❌ Недостаточно RPLCoin для покупки этого пака!", show_alert=True)
            return

        # Deduct coins
        await conn.execute("UPDATE users SET rpl_coins = rpl_coins - $1 WHERE user_id = $2", pack["price"], user_id)

        # Draw card from pack
        card_id = random.choice(pack["card_ids"])
        card = await conn.fetchrow("SELECT * FROM cards WHERE id = $1", card_id)
        await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, card_id)

    caption = (
        f"🎉 **ПОЗДРАВЛЯЕМ! ВЫ ОТКРЫЛИ {pack['name']}!**\n\n"
        f"🌟 **{card['name']}** (OVR: {card['ovr']})\n"
        f"Позиция: **{card['position']}** | Редкость: **{card['rarity']}**\n"
        f"Команда: **{card['team_name']}** | Коллекция: **{card['collection_name']}**"
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
    code = message.text.strip()
    user_id = message.from_user.id

    async with db.pool.acquire() as conn:
        promo = await conn.fetchrow("SELECT * FROM promo_codes WHERE code = $1", code)
        if not promo:
            await message.answer("❌ Такого промокода не существует!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_main")]]))
            await state.clear()
            return

        used = await conn.fetchrow("SELECT 1 FROM promo_uses WHERE user_id = $1 AND code = $2", user_id, code)
        if used:
            await message.answer("⚠️ Вы уже активировали этот промокод!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_main")]]))
            await state.clear()
            return

        if promo["used_count"] >= promo["max_uses"]:
            await message.answer("❌ Лимит активаций этого промокода исчерпан!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_main")]]))
            await state.clear()
            return

        await conn.execute("INSERT INTO promo_uses (user_id, code) VALUES ($1, $2)", user_id, code)
        await conn.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code = $1", code)

        if promo["reward_type"] == "money":
            await conn.execute("UPDATE users SET rpl_coins = rpl_coins + $1 WHERE user_id = $2", promo["reward_value"], user_id)
            res_text = f"🎉 Промокод активирован! Вы получили **{promo['reward_value']} RPLCoin**!"
        else:
            card = await conn.fetchrow("SELECT name, ovr FROM cards WHERE id = $1", promo["reward_value"])
            await conn.execute("INSERT INTO user_cards (user_id, card_id) VALUES ($1, $2)", user_id, promo["reward_value"])
            card_title = f"{card['name']} (OVR {card['ovr']})" if card else f"Карточка #{promo['reward_value']}"
            res_text = f"🎉 Промокод активирован! Вы получили карточку: **{card_title}**!"

    await state.clear()
    await message.answer(res_text, reply_markup=main_menu_keyboard(user_id in ADMIN_IDS), parse_mode=ParseMode.MARKDOWN)

# ==================== ADMIN PANEL ====================
@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Доступ закрыт!", show_alert=True)
        return
    await callback.message.edit_text("⚙️ **ПАНЕЛЬ АДМИНИСТРАТОРА**\nВыберите раздел для настройки:", reply_markup=admin_main_keyboard(), parse_mode=ParseMode.MARKDOWN)

# --- Admin: Quick Card Addition ---
@dp.callback_query(F.data == "adm_card_quick")
async def cb_adm_quick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminCardQuick.raw_data)
    await callback.message.edit_text(
        "⚡ **БЫСТРОЕ ДОБАВЛЕНИЕ КАРТОЧКИ**\n\n"
        "Отправьте данные карточки одной строкой через символ `|` в формате:\n"
        "`Имя|Редкость|Страна|OVR|Позиция|Команда|Коллекция|Стартовая(0 или 1)`\n\n"
        "Пример:\n"
        "`Александр Овечкин|Легендарная|🇷🇺 Россия|95|Skater|Без команды|Основная|0`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]])
    )

@dp.message(AdminCardQuick.raw_data)
async def process_adm_quick_raw(message: Message, state: FSMContext):
    parts = [p.strip() for p in message.text.split("|")]
    if len(parts) < 8:
        await message.answer("❌ Ошибка формата! Должно быть 8 параметров через `|`. Попробуйте снова:")
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
    await message.answer("📸 Теперь отправьте **фотографию** для карточки:")

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
    await message.answer(
        f"✅ Карточка **{data['name']}** (OVR: {data['ovr']}) успешно создана с **ID #{row['id']}**!",
        reply_markup=admin_main_keyboard()
    )

# --- Admin: Manual Card Wizard ---
@dp.callback_query(F.data == "adm_card_manual")
async def cb_adm_manual(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminCardManual.name)
    await callback.message.edit_text("📝 **РУЧНОЕ СОЗДАНИЕ КАРТОЧКИ**\nШаг 1: Введите имя игрока:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]]))

@dp.message(AdminCardManual.name)
async def process_man_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminCardManual.rarity)
    buttons = [[InlineKeyboardButton(text=r, callback_data=f"man_rarity:{r}")] for r in RARITIES]
    await message.answer("Шаг 2: Выберите редкость карточки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.rarity, F.data.startswith("man_rarity:"))
async def process_man_rarity(callback: CallbackQuery, state: FSMContext):
    rarity = callback.data.split("man_rarity:")[1]
    await state.update_data(rarity=rarity)
    await state.set_state(AdminCardManual.country)
    buttons = [[InlineKeyboardButton(text=c, callback_data=f"man_country:{c}")] for c in CIS_COUNTRIES]
    await callback.message.edit_text("Шаг 3: Выберите страну:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.country, F.data.startswith("man_country:"))
async def process_man_country(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("man_country:")[1]
    await state.update_data(country=country)
    await state.set_state(AdminCardManual.ovr)
    await callback.message.edit_text("Шаг 4: Введите OVR карточки (число от 50 до 99):")

@dp.message(AdminCardManual.ovr)
async def process_man_ovr(message: Message, state: FSMContext):
    if not message.text.isdigit() or not (50 <= int(message.text) <= 99):
        await message.answer("❌ Введите корректный OVR (50 - 99):")
        return
    await state.update_data(ovr=int(message.text))
    await state.set_state(AdminCardManual.position)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Полевой (Skater)", callback_data="man_pos:Skater")],
        [InlineKeyboardButton(text="Вратарь (Goalie)", callback_data="man_pos:Goalie")]
    ])
    await message.answer("Шаг 5: Выберите позицию:", reply_markup=kb)

@dp.callback_query(AdminCardManual.position, F.data.startswith("man_pos:"))
async def process_man_pos(callback: CallbackQuery, state: FSMContext):
    pos = callback.data.split("man_pos:")[1]
    await state.update_data(position=pos)
    await state.set_state(AdminCardManual.team)

    async with db.pool.acquire() as conn:
        teams = await conn.fetch("SELECT name FROM teams")
    buttons = [[InlineKeyboardButton(text="Без команды", callback_data="man_team:Без команды")]]
    for t in teams:
        buttons.append([InlineKeyboardButton(text=t["name"], callback_data=f"man_team:{t['name']}")])

    await callback.message.edit_text("Шаг 6: Выберите команду игрока:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.team, F.data.startswith("man_team:"))
async def process_man_team(callback: CallbackQuery, state: FSMContext):
    team = callback.data.split("man_team:")[1]
    await state.update_data(team_name=team)
    await state.set_state(AdminCardManual.collection)

    async with db.pool.acquire() as conn:
        cols = await conn.fetch("SELECT name FROM collections")
    buttons = [[InlineKeyboardButton(text="Основная", callback_data="man_col:Основная")]]
    for c in cols:
        buttons.append([InlineKeyboardButton(text=c["name"], callback_data=f"man_col:{c['name']}")])

    await callback.message.edit_text("Шаг 7: Выберите коллекцию:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@dp.callback_query(AdminCardManual.collection, F.data.startswith("man_col:"))
async def process_man_col(callback: CallbackQuery, state: FSMContext):
    col = callback.data.split("man_col:")[1]
    await state.update_data(collection_name=col)
    await state.set_state(AdminCardManual.is_starter)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, стартовая", callback_data="man_start:1")],
        [InlineKeyboardButton(text="Нет, обычная", callback_data="man_start:0")]
    ])
    await callback.message.edit_text("Шаг 8: Включать карточку в стартовый набор для новичков?", reply_markup=kb)

@dp.callback_query(AdminCardManual.is_starter, F.data.startswith("man_start:"))
async def process_man_starter(callback: CallbackQuery, state: FSMContext):
    is_starter = (callback.data.split("man_start:")[1] == "1")
    await state.update_data(is_starter=is_starter)
    await state.set_state(AdminCardManual.photo)
    await callback.message.edit_text("Шаг 9: Отправьте фото для карточки:")

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
    await message.answer(
        f"✅ Карточка **{data['name']}** создана! Присвоен ID: **#{row['id']}**",
        reply_markup=admin_main_keyboard(),
        parse_mode=ParseMode.MARKDOWN
    )

# --- Admin: Teams Management ---
@dp.callback_query(F.data == "adm_teams")
async def cb_adm_teams(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        teams = await conn.fetch("SELECT * FROM teams")
    text = "🏒 **УПРАВЛЕНИЕ КОМАНДАМИ**\n\nСписок текущих команд:\n"
    for t in teams:
        text += f"• #{t['id']} {t['name']}\n"
    if not teams:
        text += "Команд пока нет."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить команду", callback_data="adm_add_team")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "adm_add_team")
async def cb_add_team_btn(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminTeamState.team_name)
    await callback.message.edit_text("Введите название новой команды:")

@dp.message(AdminTeamState.team_name)
async def process_add_team_name(message: Message, state: FSMContext):
    name = message.text.strip()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO teams (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
    await state.clear()
    await message.answer(f"✅ Команда **{name}** добавлена!", reply_markup=admin_main_keyboard())

# --- Admin: Collections Management ---
@dp.callback_query(F.data == "adm_collections")
async def cb_adm_collections(callback: CallbackQuery):
    async with db.pool.acquire() as conn:
        cols = await conn.fetch("SELECT * FROM collections")
    text = "📂 **УПРАВЛЕНИЕ КОЛЛЕКЦИЯМИ**\n\nТекущие коллекции:\n"
    for c in cols:
        text += f"• #{c['id']} {c['name']}\n"
    if not cols:
        text += "Коллекций пока нет."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить коллекцию", callback_data="adm_add_col")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@dp.callback_query(F.data == "adm_add_col")
async def cb_add_col_btn(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminCollectionState.collection_name)
    await callback.message.edit_text("Введите название новой коллекции:")

@dp.message(AdminCollectionState.collection_name)
async def process_add_col_name(message: Message, state: FSMContext):
    name = message.text.strip()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO collections (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
    await state.clear()
    await message.answer(f"✅ Коллекция **{name}** добавлена!", reply_markup=admin_main_keyboard())

# --- Admin: Packs Management ---
@dp.callback_query(F.data == "adm_packs")
async def cb_adm_packs(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPackState.name)
    await callback.message.edit_text(
        "📦 **СОЗДАНИЕ ПАКА**\nВведите название пака (например: Золотой Пак СНГ):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]])
    )

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
    await message.answer("Введите ID карточек, которые входят в этот пак, через запятую (например: `1, 4, 12, 19`):")

@dp.message(AdminPackState.card_ids)
async def process_pack_cards(message: Message, state: FSMContext):
    try:
        card_ids = [int(x.strip()) for x in message.text.split(",") if x.strip().isdigit()]
        if not card_ids:
            raise ValueError()
    except Exception:
        await message.answer("❌ Неверный формат! Введите ID через запятую:")
        return

    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO packs (name, price, card_ids) VALUES ($1, $2, $3)", data["name"], data["price"], card_ids)

    await state.clear()
    await message.answer(f"✅ Пак **{data['name']}** успешно создан!", reply_markup=admin_main_keyboard())

# --- Admin: Promo Creation ---
@dp.callback_query(F.data == "adm_promo")
async def cb_adm_promo(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminPromoState.code)
    await callback.message.edit_text(
        "🎟 **СОЗДАНИЕ ПРОМОКОДА**\nВведите код (например: `START2026`):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="admin_panel")]])
    )

@dp.message(AdminPromoState.code)
async def process_promo_code(message: Message, state: FSMContext):
    await state.update_data(code=message.text.strip().upper())
    await state.set_state(AdminPromoState.reward_type)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Валюта (RPLCoins)", callback_data="pr_type:money")],
        [InlineKeyboardButton(text="🃏 Карточка (Card ID)", callback_data="pr_type:card")]
    ])
    await message.answer("Выберите тип награды за промокод:", reply_markup=kb)

@dp.callback_query(AdminPromoState.reward_type, F.data.startswith("pr_type:"))
async def process_promo_type(callback: CallbackQuery, state: FSMContext):
    rw_type = callback.data.split("pr_type:")[1]
    await state.update_data(reward_type=rw_type)
    await state.set_state(AdminPromoState.reward_value)
    prompt = "Введите количество монет:" if rw_type == "money" else "Введите ID карточки для выдачи:"
    await callback.message.edit_text(prompt)

@dp.message(AdminPromoState.reward_value)
async def process_promo_value(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Введите положительное число:")
        return
    await state.update_data(reward_value=int(message.text))
    await state.set_state(AdminPromoState.max_uses)
    await message.answer("Введите максимальное количество использований промокода:")

@dp.message(AdminPromoState.max_uses)
async def process_promo_uses(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Введите число:")
        return
    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO promo_codes (code, reward_type, reward_value, max_uses)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code) DO UPDATE SET reward_type = $2, reward_value = $3, max_uses = $4, used_count = 0
        """, data["code"], data["reward_type"], data["reward_value"], int(message.text))

    await state.clear()
    await message.answer(f"✅ Промокод **{data['code']}** успешно активирован в системе!", reply_markup=admin_main_keyboard())

# ==================== RUN APPLICATION ====================
async def main():
    if not BOT_TOKEN or not DATABASE_URL:
        logger.error("❌ BOT_TOKEN or DATABASE_URL is not set!")
        return

    logger.info("Connecting to Database...")
    await db.connect()
    logger.info("Database connected successfully!")

    logger.info("Starting Telegram Bot Polling...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
