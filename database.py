import aiosqlite
import sqlite3
import datetime
import random
import os
import json

DB_NAME = "market.db"

async def initialize_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. CORE TABLES (Added username here)
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, 
            username TEXT, 
            balance REAL DEFAULT 500.00, 
            last_daily DATETIME
        )''')
        # 1. CORE TABLES
        await db.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, balance REAL DEFAULT 500.00, last_daily DATETIME)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS markets (
            market_id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            creator_id INTEGER,
            status TEXT DEFAULT 'OPEN',
            created_at DATETIME,
            closes_at DATETIME,
            message_id INTEGER,
            volume REAL DEFAULT 0.0
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS outcomes (outcome_id INTEGER PRIMARY KEY AUTOINCREMENT, market_id INTEGER, label TEXT NOT NULL, pool_balance REAL DEFAULT 5000.0, FOREIGN KEY(market_id) REFERENCES markets(market_id))''')
        await db.execute('''CREATE TABLE IF NOT EXISTS positions (position_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, market_id INTEGER, outcome_id INTEGER, shares_held REAL DEFAULT 0.0)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS jury_votes (vote_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, market_id INTEGER, outcome_id INTEGER, UNIQUE(user_id, market_id))''')
        
        # 2. CASINO TABLES
        await db.execute('''CREATE TABLE IF NOT EXISTS lottery_tickets (ticket_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER)''')
        await db.execute('''CREATE TABLE IF NOT EXISTS system_globals (key TEXT PRIMARY KEY, value REAL)''')

        # 3. INVENTORY & MARKET TABLES
        await db.execute('''CREATE TABLE IF NOT EXISTS inventory (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            item_name TEXT, 
            tier TEXT, 
            is_listed INTEGER DEFAULT 0, 
            list_price REAL DEFAULT 0.0,
            set_name TEXT DEFAULT 'Base_Set',
            item_type TEXT DEFAULT 'Misc',
            slot TEXT,
            atk_bonus INTEGER DEFAULT 0,
            def_bonus INTEGER DEFAULT 0,
            int_bonus INTEGER DEFAULT 0,
            is_equipped INTEGER DEFAULT 0
        )''')
        
        # 4. TOWN SIMULATION TABLE
        await db.execute('''CREATE TABLE IF NOT EXISTS town (
            id INTEGER PRIMARY KEY, 
            level INTEGER DEFAULT 1, 
            treasury REAL DEFAULT 0.0, 
            food INTEGER DEFAULT 100, 
            materials INTEGER DEFAULT 0, 
            tax_rate REAL DEFAULT 0.05,
            famine INTEGER DEFAULT 0,
            board_channel_id INTEGER,
            board_message_id INTEGER
        )''')
        await db.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        
        # Safe Column Migrations
        columns = [
            ("users", "username", "TEXT"),
            ("users", "avatar_hash", "TEXT"),
            ("town", "board_channel_id", "INTEGER"),
            ("town", "board_message_id", "INTEGER"),
            ("town", "last_upkeep", "DATETIME"),
            ("users", "job", "TEXT DEFAULT 'Unemployed'"),
            ("users", "last_work", "DATETIME"),
            ("users", "daily_chat_earnings", "REAL DEFAULT 0.0"),
            ("users", "last_chat_reset", "DATETIME"),
            ("users", "starter_weapon", "TEXT DEFAULT 'Rusty Dagger'"),
            ("users", "rpg_class", "TEXT DEFAULT 'Fighter'"),
            ("users", "max_floor", "INTEGER DEFAULT 0"),
            ("inventory", "set_name", "TEXT DEFAULT 'Base_Set'"),
            ("inventory", "item_type", "TEXT DEFAULT 'Misc'"),
            ("inventory", "slot", "TEXT"),
            ("inventory", "atk_bonus", "INTEGER DEFAULT 0"),
            ("inventory", "def_bonus", "INTEGER DEFAULT 0"),
            ("inventory", "int_bonus", "INTEGER DEFAULT 0"),
            ("inventory", "is_equipped", "INTEGER DEFAULT 0"),
            ("inventory", "head_owner_id", "INTEGER"),
            ("users", "total_donated_gold", "REAL DEFAULT 0.0"),
            ("users", "last_rpg_run_floor", "INTEGER"),
            ("users", "last_rpg_run_at", "DATETIME"),
            ("users", "last_login_at", "DATETIME"),
            ("users", "login_streak", "INTEGER DEFAULT 0"),
            ("users", "guild_id", "INTEGER"),
            ("users", "guild_role", "TEXT"),
        ]
        
        # Guilds table: user-created guilds with their own hall, treasury, world boss
        await db.execute('''CREATE TABLE IF NOT EXISTS guilds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            leader_id INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            treasury REAL DEFAULT 0.0,
            level INTEGER DEFAULT 1,
            food INTEGER DEFAULT 100,
            tax_rate REAL DEFAULT 0.10,
            famine INTEGER DEFAULT 0,
            world_boss_hp REAL DEFAULT 10000.0,
            world_boss_max_hp REAL DEFAULT 10000.0
        )''')
        # Guild wars: head-to-head wars between two guilds
        await db.execute('''CREATE TABLE IF NOT EXISTS guild_wars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            challenger_guild_id INTEGER NOT NULL,
            defender_guild_id INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            accepted_at DATETIME,
            expires_at DATETIME,
            challenger_wins INTEGER DEFAULT 0,
            defender_wins INTEGER DEFAULT 0,
            winner_guild_id INTEGER,
            FOREIGN KEY (challenger_guild_id) REFERENCES guilds(id),
            FOREIGN KEY (defender_guild_id) REFERENCES guilds(id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS guild_war_battles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            war_id INTEGER NOT NULL,
            attacker_user_id INTEGER NOT NULL,
            defender_user_id INTEGER NOT NULL,
            winner_user_id INTEGER,
            fought_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (war_id) REFERENCES guild_wars(id)
        )''')
        
        for table, col, dtype in columns:
            try: 
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
            except: 
                pass # Column already exists
        
        await db.execute("UPDATE users SET balance = ROUND(balance, 2)")

        # 5. RPG ANALYTICS TABLE
        await db.execute('''CREATE TABLE IF NOT EXISTS rpg_analytics (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            floor_reached INTEGER,
            outcome TEXT,
            gold_earned REAL,
            party_size INTEGER,
            party_classes TEXT,
            killer_enemy TEXT
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS user_achievements (
            user_id INTEGER,
            achievement_id TEXT,
            unlocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, achievement_id)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS daily_quest_progress (
            user_id INTEGER,
            quest_id TEXT,
            date TEXT,
            completed INTEGER DEFAULT 0,
            claimed INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, quest_id, date)
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS world_boss (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_hp REAL DEFAULT 10000.0,
            max_hp REAL DEFAULT 10000.0,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        await db.execute("INSERT OR IGNORE INTO world_boss (id, current_hp, max_hp) VALUES (1, 10000.0, 10000.0)")
        await db.commit()

        # 6. FIREWALL TABLE
        await db.execute('''CREATE TABLE IF NOT EXISTS firewalls (
            user_id         INTEGER PRIMARY KEY,
            expires_at      TEXT NOT NULL,
            bolster_count   INTEGER DEFAULT 0,
            compromised_at  TEXT,
            last_hacker_id  INTEGER
        )''')


async def sync_user_data(user_id, username, avatar_hash=None):
    """Ensures the user exists and their username/avatar are up to date."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, avatar_hash) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                avatar_hash = excluded.avatar_hash
            """,
            (user_id, username, avatar_hash),
        )
        await db.commit()
        
        
# --- MARKET CREATION ---
async def create_market_custom_date(question, creator_id, close_time_obj, options=["YES", "NO"]):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''INSERT INTO markets (question, creator_id, status, created_at, closes_at, volume) 
                                     VALUES (?, ?, 'OPEN', datetime('now'), ?, 0.0)''', 
                                  (question, creator_id, close_time_obj))
        market_id = cursor.lastrowid
        for label in options:
            await db.execute("INSERT INTO outcomes (market_id, label, pool_balance) VALUES (?, ?, ?)", (market_id, label, 5000.0))
        await db.commit()
        return market_id

async def get_active_markets():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("DELETE FROM markets WHERE status = 'OPEN' AND closes_at IS NULL")
        await db.commit()
        async with db.execute("SELECT market_id, question, closes_at FROM markets WHERE status = 'OPEN'") as cursor:
            markets = await cursor.fetchall()
        
        results = []
        for m in markets:
            try:
                close_time = datetime.datetime.strptime(m['closes_at'], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                try: close_time = datetime.datetime.strptime(m['closes_at'], "%Y-%m-%d %H:%M:%S")
                except: continue
            
            async with db.execute("SELECT label FROM outcomes WHERE market_id = ?", (m['market_id'],)) as out_cursor:
                outcomes = await out_cursor.fetchall()
                
            results.append({'market_id': m['market_id'], 'question': m['question'], 'close_time': close_time, 'options': [o['label'] for o in outcomes]})
        return results

# --- TRADING LOGIC ---
async def buy_shares(user_id, market_id, outcome_label, investment):
    if investment < 0.01: return False, "Minimum bet is $0.01"
    investment = round(investment, 2)

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
                bal = (await cursor.fetchone())['balance']
                
            if bal < investment: return False, "Insufficient funds."

            async with db.execute("SELECT * FROM outcomes WHERE market_id = ?", (market_id,)) as cursor:
                outcomes = await cursor.fetchall()
                
            target = next((o for o in outcomes if o['label'] == outcome_label), None)
            if not target: return False, "Invalid outcome."

            current_pool = sum(o['pool_balance'] for o in outcomes)
            target_bal = target['pool_balance']
            future_pool = current_pool + investment
            future_target = target_bal + investment
            probability = future_target / future_pool
            shares = investment / probability

            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (investment, user_id))
            await db.execute("UPDATE outcomes SET pool_balance = pool_balance + ? WHERE outcome_id = ?", (investment, target['outcome_id']))
            await db.execute("UPDATE markets SET volume = volume + ? WHERE market_id = ?", (investment, market_id))

            async with db.execute("SELECT position_id FROM positions WHERE user_id = ? AND outcome_id = ?", (user_id, target['outcome_id'])) as cursor:
                existing = await cursor.fetchone()
                
            if existing:
                await db.execute("UPDATE positions SET shares_held = shares_held + ? WHERE position_id = ?", (shares, existing['position_id']))
            else:
                await db.execute("INSERT INTO positions (user_id, market_id, outcome_id, shares_held) VALUES (?, ?, ?, ?)", (user_id, market_id, target['outcome_id'], shares))
            await db.commit()
            return True, f"Bought {shares:.2f} shares @ {int(probability*100)}¢"
        except Exception as e: return False, str(e)

async def resolve_market(market_id, winning_label):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT question FROM markets WHERE market_id = ?", (market_id,)) as cursor:
            m = await cursor.fetchone()
            question = m['question'] if m else "Unknown"
            
        async with db.execute("SELECT outcome_id FROM outcomes WHERE market_id = ? AND label = ?", (market_id, winning_label)) as cursor:
            winner = await cursor.fetchone()
            if not winner: return False, ["Invalid Outcome"], ""
        
        async with db.execute("SELECT user_id, shares_held FROM positions WHERE market_id = ? AND outcome_id = ?", (market_id, winner['outcome_id'])) as cursor:
            winners = await cursor.fetchall()
            
        await db.execute("UPDATE markets SET status = 'RESOLVED' WHERE market_id = ?", (market_id,))
        logs = []
        for w in winners:
            payout = round(w['shares_held'] * 1.0, 2)
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (payout, w['user_id']))
            logs.append(f"<@{w['user_id']}> won ${payout:.2f}")
        await db.commit()
        return True, logs, question

# --- HELPERS ---
async def get_balance(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.commit()
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row['balance']

async def update_balance(user_id, amount):
    amount = round(amount, 2)
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        if amount < 0:
            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
                bal = (await cursor.fetchone())['balance']
                if bal + amount < 0: return False
        await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()
        return True

async def get_odds(market_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM outcomes WHERE market_id = ?", (market_id,)) as cursor:
            outcomes = await cursor.fetchall()
            if not outcomes: return []
            total = sum(o['pool_balance'] for o in outcomes)
            if total == 0: return []
            return [(o['label'], o['pool_balance'] / total) for o in outcomes]

async def get_market_volume(market_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try:
            async with db.execute("SELECT volume FROM markets WHERE market_id = ?", (market_id,)) as cursor:
                row = await cursor.fetchone()
                return row['volume'] if row else 0.0
        except: return 0.0

async def get_expired_markets():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT market_id, question FROM markets WHERE status = 'OPEN' AND closes_at <= datetime('now')") as cursor:
            return await cursor.fetchall()

async def get_leaderboard(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT ?", (limit,)) as cursor:
            return await cursor.fetchall()

async def get_market_message_id(market_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try:
            async with db.execute("SELECT message_id FROM markets WHERE market_id = ?", (market_id,)) as cursor:
                row = await cursor.fetchone()
                return row['message_id'] if row else None
        except: return None

async def set_market_message_id(market_id, message_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE markets SET message_id = ? WHERE market_id = ?", (message_id, market_id))
        await db.commit()

async def close_market_betting(market_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE markets SET status = 'VOTING' WHERE market_id = ?", (market_id,))
        await db.commit()

async def get_user_portfolio(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT m.question, o.label, p.shares_held, m.status, m.market_id
            FROM positions p
            JOIN outcomes o ON p.outcome_id = o.outcome_id
            JOIN markets m ON p.market_id = m.market_id
            WHERE p.user_id = ? AND p.shares_held > 0
            ORDER BY m.status DESC, m.market_id DESC
        """, (user_id,)) as cursor:
            return await cursor.fetchall()

async def cast_jury_vote(user_id, market_id, outcome_label):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT outcome_id FROM outcomes WHERE market_id = ? AND label = ?", (market_id, outcome_label)) as cursor:
            outcome = await cursor.fetchone()
            
        if not outcome: return False, "Invalid Outcome"
        try:
            await db.execute("INSERT INTO jury_votes (user_id, market_id, outcome_id) VALUES (?, ?, ?)", (user_id, market_id, outcome['outcome_id']))
            await db.commit()
            return True, "Vote Cast"
        except sqlite3.IntegrityError: return False, "Already voted."

async def get_market_vote_tally(market_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT o.label, COUNT(j.vote_id) as count
            FROM jury_votes j JOIN outcomes o ON j.outcome_id = o.outcome_id
            WHERE j.market_id = ? GROUP BY o.label
        """, (market_id,)) as cursor:
            rows = await cursor.fetchall()
            return {r['label']: r['count'] for r in rows}

# --- DAILY ---
async def process_daily(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        async with db.execute("SELECT last_daily FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user = await cursor.fetchone()
        
        if user and user['last_daily']:
            try:
                last_daily = datetime.datetime.strptime(user['last_daily'], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                last_daily = datetime.datetime.strptime(user['last_daily'], "%Y-%m-%d %H:%M:%S")
            next_daily = last_daily + datetime.timedelta(hours=24)
            if datetime.datetime.now() < next_daily:
                ts = int(next_daily.timestamp())
                return False, f"Daily already claimed! Try again <t:{ts}:R>."
        
        await db.execute("UPDATE users SET last_daily = ? WHERE user_id = ?", (datetime.datetime.now(), user_id))
        await db.commit()
        
        # NOTE: Using the new async function call
        net, tax = await process_town_payout(user_id, 750.0)
        return True, f"Daily claimed! You received **${net:,.2f}** *(Town Tax: ${tax:,.2f})*."

# --- LOTTERY ---
async def buy_lottery_ticket(user_id, cost=50):
    success = await update_balance(user_id, -cost)
    if success:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT INTO lottery_tickets (user_id) VALUES (?)", (user_id,))
            await db.commit()
            return True
    return False

async def get_lottery_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) as c FROM lottery_tickets") as cursor:
            count = (await cursor.fetchone())['c']
            pot = (count * 75) + 750
            return count, pot

async def draw_lottery_winner():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM lottery_tickets") as cursor:
            tickets = await cursor.fetchall()
            
        if not tickets: return None, 0
        
        winner_row = random.choice(tickets)
        winner_id = winner_row['user_id']
        count = len(tickets)
        pot = (count * 75) + 750
        
        await db.execute("DELETE FROM lottery_tickets")
        await db.commit()
        
        net, tax = await process_town_payout(winner_id, pot)
        return winner_id, net

# --- INVENTORY & P2P MARKET ---
async def add_item(
    user_id,
    item_name,
    tier,
    set_name,
    item_type="Misc",
    slot=None,
    atk_bonus=0,
    def_bonus=0,
    int_bonus=0,
    head_owner_id=None,
):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO inventory
            (user_id, item_name, tier, set_name, item_type, slot, atk_bonus, def_bonus, int_bonus, head_owner_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                item_name,
                tier,
                set_name,
                item_type,
                slot,
                int(atk_bonus),
                int(def_bonus),
                int(int_bonus),
                head_owner_id,
            ),
        )
        await db.commit()


async def get_user_avatar_hash(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT avatar_hash FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row["avatar_hash"] if row and "avatar_hash" in row.keys() else None

async def get_equipped_gear(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT *
            FROM inventory
            WHERE user_id = ? AND is_listed = 0 AND is_equipped = 1
            """,
            (user_id,),
        ) as cursor:
            return await cursor.fetchall()

async def equip_inventory_item(user_id, item_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM inventory WHERE item_id = ? AND user_id = ? AND is_listed = 0",
            (item_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return False, "Item not found, not owned by you, or currently listed."

        item = dict(row)
        slot = item.get("slot")
        if not slot:
            # Backfill slot from stats if possible (older items before RPG meta)
            atk_b = int(item.get("atk_bonus", 0) or 0)
            def_b = int(item.get("def_bonus", 0) or 0)
            int_b = int(item.get("int_bonus", 0) or 0)

            # If there are no stats yet, try to pull them from the lootbox meta for this set/name.
            if atk_b == 0 and def_b == 0 and int_b == 0:
                set_name = item.get("set_name")
                name = item.get("item_name")
                if set_name and name:
                    try:
                        meta_path = os.path.join("lootboxes", set_name, "meta.json")
                        if os.path.exists(meta_path):
                            with open(meta_path, "r", encoding="utf-8") as f:
                                raw_meta = json.load(f)
                            if isinstance(raw_meta, dict):
                                # Normalize keys aggressively: lowercase and strip non-alphanumerics,
                                # so older items with apostrophes still match updated meta.
                                def _norm(s: str) -> str:
                                    s = s.lower()
                                    return "".join(ch for ch in s if ch.isalnum())

                                meta = { _norm(str(k)): v for k, v in raw_meta.items() }
                                key = _norm(str(name))
                                item_meta = meta.get(key)
                                if isinstance(item_meta, dict):
                                    atk_b = int(item_meta.get("atk_bonus", 0) or 0)
                                    def_b = int(item_meta.get("def_bonus", 0) or 0)
                                    int_b = int(item_meta.get("int_bonus", 0) or 0)
                                    await db.execute(
                                        "UPDATE inventory SET atk_bonus = ?, def_bonus = ?, int_bonus = ? WHERE item_id = ?",
                                        (atk_b, def_b, int_b, item_id),
                                    )
                    except Exception:
                        pass

            if atk_b == 0 and def_b == 0 and int_b == 0:
                return False, "That item cannot be equipped."

            # Derive slot from the dominant stat
            if atk_b >= def_b and atk_b >= int_b:
                slot = "weapon"
            elif def_b >= atk_b and def_b >= int_b:
                slot = "armor"
            else:
                slot = "mage"

            await db.execute(
                "UPDATE inventory SET slot = ? WHERE item_id = ?",
                (slot, item_id),
            )

        await db.execute(
            "UPDATE inventory SET is_equipped = 0 WHERE user_id = ? AND slot = ?",
            (user_id, slot),
        )
        await db.execute(
            "UPDATE inventory SET is_equipped = 1 WHERE user_id = ? AND item_id = ?",
            (user_id, item_id),
        )
        await db.commit()
        return True, f"Equipped **{item['item_name']}** as your **{slot}**."

async def unequip_slot(user_id, slot):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE inventory SET is_equipped = 0 WHERE user_id = ? AND slot = ?",
            (user_id, slot),
        )
        await db.commit()
        return True, f"Unequipped your **{slot}** slot."

async def scrap_item(user_id, item_id, scrap_value):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM inventory WHERE item_id = ? AND user_id = ? AND is_listed = 0", (item_id, user_id)) as cursor:
            item = await cursor.fetchone()
            
        if not item: return False, "Item not found or currently listed on market."
        
        await db.execute("DELETE FROM inventory WHERE item_id = ?", (item_id,))
        await db.commit()
        
        net, tax = await process_town_payout(user_id, scrap_value)
        return True, f"Scrapped **{item['item_name']}** for **${net:,.2f}** *(Tax: ${tax:,.2f})*!"

async def scrap_duplicates(user_id, scrap_values):
    """Scrap all duplicate items (same item_name, set_name, tier), keeping one per group. Prefers keeping equipped. Returns (count_scrapped, total_net, message)."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM inventory WHERE user_id = ? AND is_listed = 0 ORDER BY is_equipped DESC, item_id ASC",
            (user_id,),
        ) as cursor:
            rows = await cursor.fetchall()
    items = [{k: r[k] for k in r.keys()} for r in rows]
    key = lambda it: (str(it.get("item_name") or ""), str(it.get("set_name") or ""), str(it.get("tier") or ""))
    seen = {}
    to_scrap = []
    for it in items:
        k = key(it)
        if k not in seen:
            seen[k] = it["item_id"]
            continue
        to_scrap.append(it)
    if not to_scrap:
        return 0, 0.0, "No duplicate items to scrap."
    total_net = 0.0
    async with aiosqlite.connect(DB_NAME) as db:
        for it in to_scrap:
            await db.execute("DELETE FROM inventory WHERE item_id = ?", (it["item_id"],))
        await db.commit()
    for it in to_scrap:
        tier = it.get("tier") or "Common"
        scrap_val = scrap_values.get(tier, 5.0) if isinstance(scrap_values, dict) else 5.0
        net, _ = await process_town_payout(user_id, scrap_val)
        total_net += net
    return len(to_scrap), total_net, f"Scrapped {len(to_scrap)} duplicate(s) for ${total_net:,.2f} total."

async def get_inventory(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM inventory WHERE user_id = ? AND is_listed = 0", (user_id,)) as cursor:
            return await cursor.fetchall()

async def get_item_by_id(item_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM inventory WHERE item_id = ?", (item_id,)) as cursor:
            return await cursor.fetchone()

async def list_item_on_market(user_id, item_id, price):
    if price < 0.01: return False, "Price must be at least $0.01"
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM inventory WHERE item_id = ? AND user_id = ? AND is_listed = 0", (item_id, user_id)) as cursor:
            item = await cursor.fetchone()
            
        if not item: return False, "Item not found or already listed."
        # Listing an item implicitly unequips it.
        await db.execute(
            "UPDATE inventory SET is_listed = 1, list_price = ?, is_equipped = 0 WHERE item_id = ?",
            (round(price, 2), item_id),
        )
        await db.commit()
        return True, "Item listed successfully."

async def delist_market_item(user_id, item_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM inventory WHERE item_id = ? AND user_id = ? AND is_listed = 1", (item_id, user_id)) as cursor:
            item = await cursor.fetchone()
            
        if not item: return False, "Item not found or is not currently listed."
        await db.execute("UPDATE inventory SET is_listed = 0, list_price = 0.0 WHERE item_id = ?", (item_id,))
        await db.commit()
        return True, f"**{item['item_name']}** has been removed from the market."

async def get_market_listings():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM inventory WHERE is_listed = 1") as cursor:
            return await cursor.fetchall()

async def buy_market_item(buyer_id, item_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try:
            async with db.execute("SELECT * FROM inventory WHERE item_id = ? AND is_listed = 1", (item_id,)) as cursor:
                item = await cursor.fetchone()
                
            if not item: return False, "Item no longer available."
            
            seller_id = item['user_id']
            price = item['list_price']
            if buyer_id == seller_id: return False, "You cannot buy your own item."
            
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (buyer_id,))
            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (buyer_id,)) as cursor:
                buyer_bal = (await cursor.fetchone())['balance']
                
            if buyer_bal < price: return False, "Insufficient funds."
            
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (price, buyer_id))
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (price, seller_id))
            # Ownership transfer should not auto-equip for the buyer.
            await db.execute(
                "UPDATE inventory SET user_id = ?, is_listed = 0, list_price = 0.0, is_equipped = 0 WHERE item_id = ?",
                (buyer_id, item_id),
            )
            await db.commit()
            return True, f"You bought {item['item_name']} for ${price:,.2f}!"
        except Exception as e: return False, str(e)


# --- HACKING and FIREWALL ---
async def get_firewall(user_id: int):
    """Returns firewall row or None. Fields: user_id, expires_at, bolster_count, compromised_at, last_hacker_id"""
    async with aiosqlite.connect(DB_NAME) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM firewalls WHERE user_id = ?", (user_id,)
        ) as cursor:
            return await cursor.fetchone()

async def buy_firewall(user_id: int, cost: float, hours: int):
    """Deducts cost and sets a fresh firewall. Returns False if insufficient funds."""
    async with aiosqlite.connect(DB_NAME) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
        if not row or row["balance"] < cost:
            return False
        expires = (datetime.datetime.now() + datetime.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        await conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (cost, user_id))
        await conn.execute("""
            INSERT INTO firewalls (user_id, expires_at, bolster_count, compromised_at, last_hacker_id)
            VALUES (?, ?, 0, NULL, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at = excluded.expires_at,
                bolster_count = 0,
                compromised_at = NULL,
                last_hacker_id = NULL
        """, (user_id, expires))
        await conn.commit()
        return True

async def bolster_firewall(user_id: int, sequences_completed: int):
    """Adds bolster stacks (max 3). Returns new bolster count or None if no active firewall."""
    async with aiosqlite.connect(DB_NAME) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM firewalls WHERE user_id = ?", (user_id,)) as cur:
            fw = await cur.fetchone()
        if not fw:
            return None
        # Check firewall not expired
        expires = datetime.datetime.strptime(fw["expires_at"], "%Y-%m-%d %H:%M:%S")
        if datetime.datetime.now() > expires:
            return None
        new_count = min(3, fw["bolster_count"] + sequences_completed)
        await conn.execute(
            "UPDATE firewalls SET bolster_count = ? WHERE user_id = ?", (new_count, user_id)
        )
        await conn.commit()
        return new_count

async def compromise_firewall(user_id: int, hacker_id: int):
    """
    Marks firewall as compromised and records who did it.
    If the victim has no firewall row at all, creates a minimal expired one
    so the compromised_at and last_hacker_id fields are still stored
    and /career reboot can function.
    """
    async with aiosqlite.connect(DB_NAME) as conn:
        compromised = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Expired timestamp so it doesn't count as an active firewall
        expired = datetime.datetime(2000, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
        await conn.execute("""
            INSERT INTO firewalls (user_id, expires_at, bolster_count, compromised_at, last_hacker_id)
            VALUES (?, ?, 0, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                compromised_at  = excluded.compromised_at,
                last_hacker_id  = excluded.last_hacker_id
        """, (user_id, expired, compromised, hacker_id))
        await conn.commit()

async def reboot_firewall(user_id: int):
    """
    Clears compromised state and resets bolsters.
    Returns last_hacker_id (so victim can attempt to identify them) or None.
    """
    async with aiosqlite.connect(DB_NAME) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM firewalls WHERE user_id = ?", (user_id,)) as cur:
            fw = await cur.fetchone()
        if not fw:
            return None
        hacker_id = fw["last_hacker_id"]
        await conn.execute("""
            UPDATE firewalls SET compromised_at = NULL, last_hacker_id = NULL, bolster_count = 0
            WHERE user_id = ?
        """, (user_id,))
        await conn.commit()
        return hacker_id

# --- JOBS & TOWN INCOME SYSTEM ---
ECONOMY_MULTIPLIER_DEFAULT = 0.7  # Global scale for all payouts (jobs, dungeon, casino). Raise for "prosperity" events.

async def get_economy_multiplier():
    """Return current economy multiplier (for UI / realm prosperity)."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT value FROM system_globals WHERE key = 'economy_multiplier'") as c:
            row = await c.fetchone()
        return float(row['value']) if row and row['value'] is not None else ECONOMY_MULTIPLIER_DEFAULT

async def gross_for_desired_net(desired_net):
    """Returns the gross amount to pass to process_town_payout so the player receives desired_net (before rounding).
     Use this for casino payouts so that e.g. 1.2x multiplier actually gives the player 1.2x their bet."""
    if desired_net <= 0:
        return 0.0
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT value FROM system_globals WHERE key = 'economy_multiplier'") as c:
            row = await c.fetchone()
        economy_mult = float(row['value']) if row and row['value'] is not None else ECONOMY_MULTIPLIER_DEFAULT
        async with db.execute("SELECT level, tax_rate, famine FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
        level = town['level'] if town and town['level'] else 1
        tax_rate = town['tax_rate'] if town and town['tax_rate'] is not None else 0.10
        famine = town['famine'] if town and town['famine'] else 0
        level_mult = 1.0 + (level * 0.03)
        if famine == 1:
            level_mult *= 0.5
        factor = economy_mult * level_mult * (1 - tax_rate)
        if factor <= 0:
            return desired_net
        return desired_net / factor

async def process_town_payout(user_id, amount):
    """MASTER FUNCTION: Applies economy scale, town multipliers, famines, and taxes to new currency generation."""
    if amount <= 0: return 0.0, 0.0
    
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        async with db.execute("SELECT value FROM system_globals WHERE key = 'economy_multiplier'") as c:
            row = await c.fetchone()
        economy_mult = float(row['value']) if row and row['value'] is not None else ECONOMY_MULTIPLIER_DEFAULT
        if row is None:
            await db.execute("INSERT OR IGNORE INTO system_globals (key, value) VALUES ('economy_multiplier', ?)", (economy_mult,))
        async with db.execute("SELECT level, tax_rate, famine FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
        
        level = town['level'] if town and town['level'] else 1
        tax_rate = town['tax_rate'] if town and town['tax_rate'] is not None else 0.10
        famine = town['famine'] if town and town['famine'] else 0
        
        amount = amount * economy_mult
        multiplier = 1.0 + (level * 0.03)
        if famine == 1:
            multiplier *= 0.5 
            
        gross_pay = amount * multiplier
        tax_amount = round(gross_pay * tax_rate, 2)
        net_pay = round(gross_pay - tax_amount, 2)
        
        await db.execute("UPDATE users SET balance = COALESCE(balance, 0.0) + ? WHERE user_id = ?", (net_pay, user_id))
        await db.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        await db.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id = 1", (tax_amount,))
        await db.commit()
        return net_pay, tax_amount

async def get_job_profile(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.commit()
        async with db.execute("SELECT job, last_work, daily_chat_earnings FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def set_job(user_id, job_name):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET job = ? WHERE user_id = ?", (job_name, user_id))
        await db.commit()

async def process_work(user_id, base_payout):
    # Route work payout through the new master function
    net_pay, tax_amount = await process_town_payout(user_id, base_payout)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET last_work = ? WHERE user_id = ?", (datetime.datetime.now(), user_id))
        await db.commit()
        return net_pay, tax_amount

async def process_chat_income(user_id, amount, daily_cap=300.0):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        async with db.execute("SELECT value FROM system_globals WHERE key = 'economy_multiplier'") as c:
            row = await c.fetchone()
        economy_mult = float(row['value']) if row and row['value'] is not None else ECONOMY_MULTIPLIER_DEFAULT
        async with db.execute("SELECT daily_chat_earnings, last_chat_reset FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user = await cursor.fetchone()
        async with db.execute("SELECT level, famine, tax_rate FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
        level = town['level'] if town and town['level'] else 1
        famine = town['famine'] if town and town['famine'] else 0
        tax_rate = town['tax_rate'] if town and town['tax_rate'] is not None else 0.10
        multiplier = 1.0 + (level * 0.03)
        if famine == 1: multiplier *= 0.5
        adjusted_amount = amount * economy_mult * multiplier

        now = datetime.datetime.now()
        earnings = user['daily_chat_earnings'] if user['daily_chat_earnings'] else 0.0
        
        reset_needed = True
        if user['last_chat_reset']:
            try: last_reset = datetime.datetime.strptime(user['last_chat_reset'], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError: last_reset = datetime.datetime.strptime(user['last_chat_reset'], "%Y-%m-%d %H:%M:%S")
            if now.date() == last_reset.date():
                reset_needed = False
                
        if reset_needed: earnings = 0.0
        if earnings >= daily_cap: return False 
            
        actual_amount = min(adjusted_amount, daily_cap - earnings)
        tax_amount = round(actual_amount * tax_rate, 2)
        net_amount = round(actual_amount - tax_amount, 2)
        
        await db.execute("UPDATE users SET balance = COALESCE(balance, 0.0) + ?, daily_chat_earnings = ?, last_chat_reset = ? WHERE user_id = ?", 
                  (net_amount, earnings + actual_amount, now, user_id))
        await db.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id = 1", (tax_amount,))
        await db.commit()
        return True

# --- TOWN SIMULATION HELPERS ---
async def get_town_state():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try: 
            async with db.execute("SELECT * FROM town WHERE id=1") as cursor:
                town_row = await cursor.fetchone()
                if not town_row: return None
                town = dict(town_row)
            
            async with db.execute("SELECT COUNT(user_id) as c FROM users") as cursor:
                user_count = (await cursor.fetchone())['c']
                
            town['user_count'] = user_count if user_count > 0 else 1
            return town
        except: return None

async def add_town_resources(food=0, materials=0):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        await db.execute("UPDATE town SET food = COALESCE(food, 0) + ?, materials = COALESCE(materials, 0) + ? WHERE id=1", (food, materials))
        
        async with db.execute("SELECT food, famine FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
            
        if town and town['famine'] == 1 and town['food'] > 0:
            await db.execute("UPDATE town SET famine = 0 WHERE id=1")
            
        await db.commit()

async def process_balance_decay(decay_floor=1000):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM users WHERE balance >= ?", (decay_floor,)) as cursor:
            rows = await cursor.fetchall()
            
        total = 0.0
        for r in rows:
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (decay_floor, r['user_id']))
            total += decay_floor
            
        await db.commit()
        return len(rows), round(total, 2)

async def set_tax_rate(rate):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        await db.execute("UPDATE town SET tax_rate = ? WHERE id=1", (rate,))
        await db.commit()

async def try_upgrade_town(mat_cost, gold_cost):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT materials, treasury FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
            
        if not town: return False
        
        mats = town['materials'] or 0
        treasury = town['treasury'] or 0.0
        
        if mats < mat_cost or treasury < gold_cost:
            return False
        
        await db.execute("UPDATE town SET materials = materials - ?, treasury = treasury - ?, level = COALESCE(level, 1) + 1 WHERE id=1", (mat_cost, gold_cost))
        await db.commit()
        return True
    
async def embezzle_town_funds(user_id, amount):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT treasury FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
            
        if not town: return False
        treasury = town['treasury'] or 0.0
        if treasury < amount: return False
        
        await db.execute("UPDATE town SET treasury = treasury - ? WHERE id=1", (amount,))
        await db.execute("UPDATE users SET balance = COALESCE(balance, 0.0) + ? WHERE user_id=?", (amount, user_id))
        await db.commit()
        return True
    
async def run_town_daily_upkeep():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try: await db.execute("ALTER TABLE town ADD COLUMN last_upkeep DATETIME")
        except: pass
        
        async with db.execute("SELECT level, food, last_upkeep, famine, tax_rate FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
            
        if not town: return False, False, 0, 0.0
        
        now = datetime.datetime.now()
        if town['last_upkeep']:
            try: last_upkeep = datetime.datetime.strptime(town['last_upkeep'], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError: last_upkeep = datetime.datetime.strptime(town['last_upkeep'], "%Y-%m-%d %H:%M:%S")
            
            if now < last_upkeep + datetime.timedelta(hours=24):
                return False, False, 0, 0.0
                
        async with db.execute("SELECT COUNT(user_id) as c FROM users") as cursor:
            user_count = (await cursor.fetchone())['c']
            
        if user_count == 0: user_count = 1
        drain = user_count * 2 
        
        food = town['food'] or 0
        new_food = food - drain
        famine = 1 if new_food < 0 else 0
        new_food = max(0, new_food)
        
        tax_rate = town['tax_rate'] if town['tax_rate'] is not None else 0.05
        wealth_tax_rate = tax_rate * 0.1
        
        async with db.execute("SELECT SUM(balance) as total FROM users WHERE balance > 0") as cursor:
            total_bal_row = await cursor.fetchone()
            
        total_bal = total_bal_row['total'] if total_bal_row and total_bal_row['total'] else 0.0
        tax_collected = total_bal * wealth_tax_rate
        
        if wealth_tax_rate > 0:
            await db.execute("UPDATE users SET balance = balance - (balance * ?) WHERE balance > 0", (wealth_tax_rate,))
        
        await db.execute("UPDATE town SET food = ?, famine = ?, last_upkeep = ?, treasury = COALESCE(treasury, 0.0) + ? WHERE id=1", 
                  (new_food, famine, now, tax_collected))
        await db.commit()
        return True, famine == 1, drain, tax_collected

async def force_town_upkeep():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try: await db.execute("ALTER TABLE town ADD COLUMN last_upkeep DATETIME")
        except: pass
        
        async with db.execute("SELECT level, food, tax_rate FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
        if not town: return 0.0
        
        async with db.execute("SELECT COUNT(user_id) as c FROM users") as cursor:
            user_count = (await cursor.fetchone())['c']
        if user_count == 0: user_count = 1
        drain = user_count * 2
        
        food = town['food'] or 0
        new_food = food - drain
        famine = 1 if new_food < 0 else 0
        new_food = max(0, new_food)
        
        tax_rate = town['tax_rate'] if town['tax_rate'] is not None else 0.05
        wealth_tax_rate = tax_rate * 0.1
        
        async with db.execute("SELECT SUM(balance) as total FROM users WHERE balance > 0") as cursor:
            total_bal_row = await cursor.fetchone()
        total_bal = total_bal_row['total'] if total_bal_row and total_bal_row['total'] else 0.0
        tax_collected = total_bal * wealth_tax_rate
        
        if wealth_tax_rate > 0:
            await db.execute("UPDATE users SET balance = balance - (balance * ?) WHERE balance > 0", (wealth_tax_rate,))
        
        await db.execute("UPDATE town SET food = ?, famine = ?, last_upkeep = ?, treasury = COALESCE(treasury, 0.0) + ? WHERE id=1", 
                  (new_food, famine, datetime.datetime.now(), tax_collected))
        await db.commit()
        return tax_collected

async def set_town_board(channel_id, message_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        await db.execute("UPDATE town SET board_channel_id=?, board_message_id=? WHERE id=1", (channel_id, message_id))
        await db.commit()

# --- RPG SHOP ---
async def get_rpg_profile(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.commit()
        async with db.execute("SELECT starter_weapon, rpg_class FROM users WHERE user_id = ?", (user_id,)) as cursor:
            profile = await cursor.fetchone()
            
        weapon = profile['starter_weapon'] if profile and profile['starter_weapon'] else "Rusty Dagger"
        rpg_class = profile['rpg_class'] if profile and profile['rpg_class'] else "Fighter"
        return weapon, rpg_class

async def set_rpg_class(user_id, class_name):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET rpg_class = ? WHERE user_id = ?", (class_name, user_id))
        await db.commit()

async def buy_starter_weapon(user_id, weapon_name, cost):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        
        async with db.execute("SELECT balance, starter_weapon FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user = await cursor.fetchone()
        
        if user['balance'] < cost:
            return False, "Insufficient funds in your global balance."
        
        current_gear = user['starter_weapon'] if user['starter_weapon'] else "Rusty Dagger"
        gear_list = [g.strip() for g in current_gear.split(",") if g.strip()]
        
        if weapon_name in gear_list:
            return False, "You already own this gear! Its stats are already permanently active."
            
        if "Rusty Dagger" in gear_list and weapon_name != "Rusty Dagger":
            gear_list.remove("Rusty Dagger")
            
        gear_list.append(weapon_name)
        new_gear = ",".join(gear_list)
            
        await db.execute("UPDATE users SET balance = balance - ?, starter_weapon = ? WHERE user_id = ?", (cost, new_gear, user_id))
        await db.commit()
        return True, f"Successfully purchased the **{weapon_name}**! Its stats have permanently stacked to your profile."

# --- RPG ANALYTICS ---
async def log_rpg_run(floor, outcome, gold, party_data, killer=None):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        classes = ", ".join([p['class'] for p in party_data])
        
        await db.execute("""
            INSERT INTO rpg_analytics 
            (floor_reached, outcome, gold_earned, party_size, party_classes, killer_enemy) 
            VALUES (?, ?, ?, ?, ?, ?)""",
            (floor, outcome, gold, len(party_data), classes, killer)
        )
        
        now = datetime.datetime.now()
        for p in party_data:
            uid = p['user'].id
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
            async with db.execute("SELECT max_floor FROM users WHERE user_id = ?", (uid,)) as cursor:
                user_row = await cursor.fetchone()
                current_max = user_row['max_floor'] if user_row and user_row['max_floor'] else 0
            if floor > current_max:
                await db.execute("UPDATE users SET max_floor = ? WHERE user_id = ?", (floor, uid))
            try:
                await db.execute("UPDATE users SET last_rpg_run_floor = ?, last_rpg_run_at = ? WHERE user_id = ?", (floor, now, uid))
            except Exception:
                pass
        await db.commit()
        
async def get_rpg_leaderboard(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id, max_floor FROM users WHERE max_floor > 0 ORDER BY max_floor DESC LIMIT ?", (limit,)) as cursor:
            return await cursor.fetchall()

async def get_rpg_analytics():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        
        async with db.execute("SELECT COUNT(*) as c FROM rpg_analytics") as c: total_runs = (await c.fetchone())['c']
        if total_runs == 0: return None
        
        async with db.execute("SELECT AVG(floor_reached) as a FROM rpg_analytics") as c: avg_floor = (await c.fetchone())['a']
        async with db.execute("SELECT MAX(floor_reached) as m FROM rpg_analytics") as c: max_floor = (await c.fetchone())['m']
        async with db.execute("SELECT SUM(gold_earned) as s FROM rpg_analytics") as c: total_gold = (await c.fetchone())['s']
        
        async with db.execute("SELECT killer_enemy, COUNT(killer_enemy) as c FROM rpg_analytics WHERE killer_enemy IS NOT NULL GROUP BY killer_enemy ORDER BY c DESC LIMIT 1") as cursor:
            killer = await cursor.fetchone()
            deadliest = f"{killer['killer_enemy']} ({killer['c']} kills)" if killer else "None"

        async with db.execute("SELECT COUNT(*) as c FROM rpg_analytics WHERE outcome = 'ESCAPED'") as c: wins = (await c.fetchone())['c']
        async with db.execute("SELECT COUNT(*) as c FROM rpg_analytics WHERE outcome = 'WIPED'") as c: wipes = (await c.fetchone())['c']
        async with db.execute("SELECT AVG(gold_earned) as a FROM rpg_analytics") as c: avg_gold = (await c.fetchone())['a']
        async with db.execute("SELECT AVG(party_size) as a FROM rpg_analytics") as c: avg_party = (await c.fetchone())['a']

        async with db.execute("SELECT party_classes, floor_reached, outcome, gold_earned FROM rpg_analytics") as cursor:
            rows = await cursor.fetchall()
            
        class_stats = {}
        for r in rows:
            classes = [c.strip() for c in r['party_classes'].split(",") if c.strip()]
            for cls in classes:
                if cls not in class_stats:
                    class_stats[cls] = {"picks": 0, "wins": 0, "total_floor": 0, "total_gold": 0.0}
                class_stats[cls]["picks"] += 1
                class_stats[cls]["total_floor"] += r['floor_reached']
                class_stats[cls]["total_gold"] += r['gold_earned']
                if r['outcome'] == "ESCAPED":
                    class_stats[cls]["wins"] += 1

        for cls in class_stats:
            s = class_stats[cls]
            s["avg_floor"] = round(s["total_floor"] / s["picks"], 1)
            s["avg_gold"] = round(s["total_gold"] / s["picks"], 1)
            s["win_rate"] = round(s["wins"] / s["picks"] * 100, 1) if s["picks"] > 0 else 0

        async with db.execute("SELECT killer_enemy, COUNT(*) as c FROM rpg_analytics WHERE killer_enemy IS NOT NULL GROUP BY killer_enemy ORDER BY c DESC LIMIT 5") as cursor:
            top5_killers = await cursor.fetchall()

        return {
            "total_runs": total_runs,
            "wins": wins,
            "wipes": wipes,
            "win_rate": round(wins / total_runs * 100, 1) if total_runs > 0 else 0,
            "avg_floor": round(avg_floor, 1),
            "max_floor": max_floor,
            "total_gold": total_gold,
            "avg_gold": round(avg_gold, 1) if avg_gold else 0,
            "avg_party": round(avg_party, 1) if avg_party else 0,
            "deadliest": deadliest,
            "top5_killers": [(k['killer_enemy'], k['c']) for k in top5_killers],
            "class_stats": class_stats
        }

async def execute_player(user_id, penalty_fraction=0.50):
    """VIVE LA REVOLUTION: Takes 50% of their money, fires them, and gives money to town."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user = await cursor.fetchone()
        
        if not user or user['balance'] <= 0:
            seized_funds = 0.0
        else:
            seized_funds = round(user['balance'] * penalty_fraction, 2)
        
        await db.execute("UPDATE users SET balance = balance - ?, job = 'Unemployed' WHERE user_id = ?", (seized_funds, user_id))
        await db.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id=1", (seized_funds,))
        await db.commit()
        return seized_funds


# --- DONATIONS (Top Donors) ---
async def record_donation(user_id, amount):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.execute("UPDATE users SET total_donated_gold = COALESCE(total_donated_gold, 0) + ? WHERE user_id = ?", (amount, user_id))
        await db.commit()

async def get_user_total_donated(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT total_donated_gold FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
        return float(row["total_donated_gold"] or 0) if row else 0.0

async def get_top_donors(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, total_donated_gold FROM users WHERE COALESCE(total_donated_gold, 0) > 0 ORDER BY total_donated_gold DESC LIMIT ?",
            (limit,)
        ) as c:
            return await c.fetchall()


# --- ACHIEVEMENTS ---
async def get_user_achievements(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT achievement_id, unlocked_at FROM user_achievements WHERE user_id = ?", (user_id,)) as c:
            return await c.fetchall()

async def unlock_achievement(user_id, achievement_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO user_achievements (user_id, achievement_id) VALUES (?, ?)",
            (user_id, achievement_id)
        )
        await db.commit()


# --- DAILY QUESTS ---
async def get_daily_quest_progress(user_id, date_str):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT quest_id, completed, claimed FROM daily_quest_progress WHERE user_id = ? AND date = ?",
            (user_id, date_str)
        ) as c:
            return {r["quest_id"]: dict(r) for r in await c.fetchall()}

async def set_quest_completed(user_id, quest_id, date_str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO daily_quest_progress (user_id, quest_id, date, completed) VALUES (?, ?, ?, 1) ON CONFLICT(user_id, quest_id, date) DO UPDATE SET completed = 1",
            (user_id, quest_id, date_str)
        )
        await db.commit()

async def get_daily_quest_progress_for_date(user_id, date_str):
    """Returns list of {quest_id, completed, claimed} for all quests for that date."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT quest_id, completed, claimed FROM daily_quest_progress WHERE user_id = ? AND date = ?",
            (user_id, date_str)
        ) as c:
            return [dict(r) for r in await c.fetchall()]

async def claim_quest_reward(user_id, quest_id, date_str, gold_reward):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT completed, claimed FROM daily_quest_progress WHERE user_id = ? AND quest_id = ? AND date = ?",
            (user_id, quest_id, date_str)
        ) as c:
            row = await c.fetchone()
        if not row:
            return False
        if not row["completed"] or row["claimed"]:
            return False
        await db.execute("UPDATE daily_quest_progress SET claimed = 1 WHERE user_id = ? AND quest_id = ? AND date = ?", (user_id, quest_id, date_str))
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?", (gold_reward, user_id))
        await db.commit()
        return True


# --- LOGIN STREAK ---
async def record_login_streak(user_id):
    """Returns (new_streak, gold_reward). Call on page load or daily."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        now = datetime.datetime.now()
        today = now.date().isoformat()
        async with db.execute("SELECT last_login_at, login_streak FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
        last = row["last_login_at"] if row and row["last_login_at"] else None
        last_date = (last.date().isoformat() if hasattr(last, "date") else (str(last)[:10] if last else None))
        streak = int(row["login_streak"] or 0)
        if last_date == today:
            await db.execute("UPDATE users SET last_login_at = ? WHERE user_id = ?", (now, user_id))
            await db.commit()
            return streak, 0.0
        try:
            last_d = datetime.date.fromisoformat(last_date) if last_date else None
            if last_d and (now.date() - last_d).days == 1:
                streak += 1
            else:
                streak = 1
        except Exception:
            streak = 1
        reward = min(35.0 + streak * 3.0, 120.0)
        await db.execute("UPDATE users SET last_login_at = ?, login_streak = ? WHERE user_id = ?", (now, streak, user_id))
        await db.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?", (reward, user_id))
        await db.commit()
        return streak, reward


# --- WORLD BOSS ---
async def get_world_boss():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT current_hp, max_hp, updated_at FROM world_boss WHERE id = 1") as c:
            row = await c.fetchone()
        return dict(row) if row else {"current_hp": 10000.0, "max_hp": 10000.0, "updated_at": None}

async def deal_world_boss_damage(amount):
    """Returns new current_hp. Resets to max_hp when <= 0."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT current_hp, max_hp FROM world_boss WHERE id = 1") as c:
            row = await c.fetchone()
        if not row:
            return 10000.0
        new_hp = max(0.0, (row["current_hp"] or 10000.0) - amount)
        if new_hp <= 0:
            new_hp = row["max_hp"] or 10000.0
        await db.execute("UPDATE world_boss SET current_hp = ?, updated_at = ? WHERE id = 1", (new_hp, datetime.datetime.now()))
        await db.commit()
        return new_hp


# --- GUILDS ---
async def create_guild(leader_id, name):
    """Create a guild. Leader joins as leader. Returns (guild_id, None) or (None, error_msg)."""
    name = (name or "").strip()
    if not name or len(name) < 2 or len(name) > 32:
        return None, "Guild name must be 2–32 characters."
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT guild_id FROM users WHERE user_id = ?", (leader_id,)) as c:
            row = await c.fetchone()
        if row and row["guild_id"] is not None:
            return None, "You are already in a guild. Leave it first."
        try:
            cursor = await db.execute(
                """INSERT INTO guilds (name, leader_id, treasury, level, food, tax_rate, famine, world_boss_hp, world_boss_max_hp)
                   VALUES (?, ?, 0.0, 1, 100, 0.10, 0, 10000.0, 10000.0)""",
                (name, leader_id),
            )
            guild_id = cursor.lastrowid
        except sqlite3.IntegrityError:
            return None, "A guild with that name already exists."
        await db.execute("UPDATE users SET guild_id = ?, guild_role = ? WHERE user_id = ?", (guild_id, "leader", leader_id))
        await db.commit()
        return guild_id, None

async def get_guild(guild_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)) as c:
            row = await c.fetchone()
        return dict(row) if row else None

async def get_guild_by_name(name):
    """Case-insensitive lookup by guild name. Returns guild dict or None."""
    if not name or not str(name).strip():
        return None
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM guilds WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))", (str(name),)) as c:
            row = await c.fetchone()
        return dict(row) if row else None

async def get_guild_state(guild_id):
    """Return guild as town-like state (level, treasury, food, tax_rate, famine, user_count, name, leader_id, world_boss)."""
    g = await get_guild(guild_id)
    if not g:
        return None
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT COUNT(*) as c FROM users WHERE guild_id = ?", (guild_id,)) as c:
            row = await c.fetchone()
    count = row["c"] if row and row["c"] else 0
    return {
        "id": g["id"],
        "name": g["name"],
        "leader_id": g["leader_id"],
        "level": g["level"] or 1,
        "treasury": g["treasury"] or 0.0,
        "food": g["food"] or 100,
        "tax_rate": g["tax_rate"] if g["tax_rate"] is not None else 0.10,
        "famine": g["famine"] or 0,
        "user_count": max(1, count),
        "world_boss_hp": g.get("world_boss_hp") or 10000.0,
        "world_boss_max_hp": g.get("world_boss_max_hp") or 10000.0,
    }

async def get_user_guild_info(user_id):
    """Return user's guild dict with guild_id, guild_role, or None if not in guild."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT guild_id, guild_role FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
    if not row or row["guild_id"] is None:
        return None
    guild = await get_guild(row["guild_id"])
    if not guild:
        return None
    return {"guild_id": row["guild_id"], "guild_role": row["guild_role"] or "member", "guild_name": guild["name"], "leader_id": guild["leader_id"]}

async def leave_guild(user_id):
    """Remove user from guild. If leader, assign new leader or disband if alone."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT guild_id, guild_role FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
        if not row or row["guild_id"] is None:
            return False, "You are not in a guild."
        gid = row["guild_id"]
        is_leader = (row["guild_role"] or "") == "leader"
        await db.execute("UPDATE users SET guild_id = NULL, guild_role = NULL WHERE user_id = ?", (user_id,))
        if is_leader:
            async with db.execute("SELECT user_id FROM users WHERE guild_id = ? AND user_id != ? LIMIT 1", (gid, user_id)) as c:
                next_leader = await c.fetchone()
            if next_leader:
                await db.execute("UPDATE users SET guild_role = 'leader' WHERE user_id = ?", (next_leader["user_id"],))
                await db.execute("UPDATE guilds SET leader_id = ? WHERE id = ?", (next_leader["user_id"], gid))
            else:
                await db.execute("DELETE FROM guilds WHERE id = ?", (gid,))
        await db.commit()
        return True, "Left the guild."

async def join_guild(user_id, guild_id):
    """User joins an existing guild as a member. Returns (True, None) or (False, error_msg)."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT guild_id FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
        if row and row["guild_id"] is not None:
            return False, "You are already in a guild. Leave it first."
        async with db.execute("SELECT id, name FROM guilds WHERE id = ?", (guild_id,)) as c:
            guild = await c.fetchone()
        if not guild:
            return False, "That guild does not exist."
        await db.execute("UPDATE users SET guild_id = ?, guild_role = ? WHERE user_id = ?", (guild_id, "member", user_id))
        await db.commit()
        return True, f"Joined {guild['name']}!"

async def disband_guild(leader_id):
    """Leader only. Deletes guild and clears everyone's guild_id."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT guild_id, guild_role FROM users WHERE user_id = ?", (leader_id,)) as c:
            row = await c.fetchone()
        if not row or row["guild_id"] is None:
            return False, "You are not in a guild."
        if (row["guild_role"] or "") != "leader":
            return False, "Only the guild leader can disband the guild."
        gid = row["guild_id"]
        await db.execute("UPDATE users SET guild_id = NULL, guild_role = NULL WHERE guild_id = ?", (gid,))
        await db.execute("DELETE FROM guilds WHERE id = ?", (gid,))
        await db.commit()
        return True, "Guild disbanded."

async def add_guild_treasury(guild_id, amount):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE guilds SET treasury = COALESCE(treasury, 0) + ? WHERE id = ?", (amount, guild_id))
        await db.commit()

async def add_guild_food(guild_id, amount):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE guilds SET food = COALESCE(food, 0) + ? WHERE id = ?", (amount, guild_id))
        await db.commit()

async def get_guild_world_boss(guild_id):
    g = await get_guild(guild_id)
    if not g:
        return {"current_hp": 10000.0, "max_hp": 10000.0}
    return {"current_hp": g.get("world_boss_hp") or 10000.0, "max_hp": g.get("world_boss_max_hp") or 10000.0}

async def deal_guild_world_boss_damage(guild_id, amount):
    """Returns new current_hp. Resets to max when <= 0."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT world_boss_hp, world_boss_max_hp FROM guilds WHERE id = ?", (guild_id,)) as c:
            row = await c.fetchone()
        if not row:
            return 10000.0
        cur = row["world_boss_hp"] or 10000.0
        mx = row["world_boss_max_hp"] or 10000.0
        new_hp = max(0.0, cur - amount)
        if new_hp <= 0:
            new_hp = mx
        await db.execute("UPDATE guilds SET world_boss_hp = ? WHERE id = ?", (new_hp, guild_id))
        await db.commit()
        return new_hp

async def get_top_guilds(limit=10):
    """Order by treasury + level*10000 (so level matters)."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT id, name, leader_id, treasury, level, 
                      (COALESCE(treasury, 0) + (COALESCE(level, 1) * 10000.0)) AS score
               FROM guilds ORDER BY score DESC LIMIT ?""",
            (limit,),
        ) as c:
            return await c.fetchall()


# --- GUILD WARS ---
GUILD_WAR_DURATION_DAYS = 7
GUILD_WAR_WINS_TO_VICTORY = 10
GUILD_WAR_WIN_BONUS = 1000.0  # Gold to winning guild treasury

async def create_guild_war(challenger_guild_id, defender_guild_id, challenger_leader_id):
    """Create a pending war. Only leader of challenger guild. Returns (war_id, None) or (None, error)."""
    if challenger_guild_id == defender_guild_id:
        return None, "You cannot declare war on your own guild."
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT leader_id FROM guilds WHERE id = ?", (challenger_guild_id,)) as c:
            row = await c.fetchone()
        if not row or row["leader_id"] != challenger_leader_id:
            return None, "Only your guild leader can declare war."
        async with db.execute("SELECT id FROM guilds WHERE id = ?", (defender_guild_id,)) as c:
            if not await c.fetchone():
                return None, "That guild does not exist."
        async with db.execute(
            """SELECT id FROM guild_wars WHERE status IN ('pending', 'active')
               AND (challenger_guild_id = ? OR defender_guild_id = ? OR challenger_guild_id = ? OR defender_guild_id = ?)""",
            (challenger_guild_id, challenger_guild_id, defender_guild_id, defender_guild_id),
        ) as c:
            if await c.fetchone():
                return None, "One of these guilds is already in a war."
        cursor = await db.execute(
            """INSERT INTO guild_wars (challenger_guild_id, defender_guild_id, status)
               VALUES (?, ?, 'pending')""",
            (challenger_guild_id, defender_guild_id),
        )
        war_id = cursor.lastrowid
        await db.commit()
        return war_id, None

async def get_war_by_id(war_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT w.*, c.name AS challenger_name, d.name AS defender_name
               FROM guild_wars w
               JOIN guilds c ON w.challenger_guild_id = c.id
               JOIN guilds d ON w.defender_guild_id = d.id
               WHERE w.id = ?""",
            (war_id,),
        ) as c:
            row = await c.fetchone()
        return dict(row) if row else None

async def get_active_war_for_guild(guild_id):
    """Return the current active or pending war involving this guild, or None."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT w.*, c.name AS challenger_name, d.name AS defender_name
               FROM guild_wars w
               JOIN guilds c ON w.challenger_guild_id = c.id
               JOIN guilds d ON w.defender_guild_id = d.id
               WHERE w.status IN ('pending', 'active') AND (w.challenger_guild_id = ? OR w.defender_guild_id = ?)
               ORDER BY w.id DESC LIMIT 1""",
            (guild_id, guild_id),
        ) as c:
            row = await c.fetchone()
        return dict(row) if row else None

async def accept_guild_war(war_id, accepter_user_id):
    """Defender guild leader accepts. Sets status to active and expires_at."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT defender_guild_id, status FROM guild_wars WHERE id = ?", (war_id,)) as c:
            row = await c.fetchone()
        if not row:
            return False, "War not found."
        if row["status"] != "pending":
            return False, "This war is no longer pending."
        async with db.execute("SELECT leader_id FROM guilds WHERE id = ?", (row["defender_guild_id"],)) as c:
            lead = await c.fetchone()
        if not lead or lead["leader_id"] != accepter_user_id:
            return False, "Only the defender guild leader can accept."
        now = datetime.datetime.now()
        expires = now + datetime.timedelta(days=GUILD_WAR_DURATION_DAYS)
        await db.execute(
            "UPDATE guild_wars SET status = 'active', accepted_at = ?, expires_at = ? WHERE id = ?",
            (now, expires, war_id),
        )
        await db.commit()
        return True, "War accepted! Battle until " + expires.strftime("%Y-%m-%d") + " or first to " + str(GUILD_WAR_WINS_TO_VICTORY) + " wins."

async def record_guild_war_battle(war_id, attacker_user_id, defender_user_id, winner_user_id):
    """Record a battle and increment the appropriate guild's win count. Returns (challenger_wins, defender_wins) after update."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT challenger_guild_id, defender_guild_id, challenger_wins, defender_wins, status FROM guild_wars WHERE id = ?", (war_id,)) as c:
            war = await c.fetchone()
        if not war or war["status"] != "active":
            return None, "War is not active."
        attacker_guild = await get_user_guild_info(attacker_user_id)
        defender_guild = await get_user_guild_info(defender_user_id)
        if not attacker_guild or not defender_guild:
            return None, "Both players must be in a guild."
        if attacker_guild["guild_id"] != war["challenger_guild_id"] and attacker_guild["guild_id"] != war["defender_guild_id"]:
            return None, "Attacker is not in this war."
        if defender_guild["guild_id"] != war["challenger_guild_id"] and defender_guild["guild_id"] != war["defender_guild_id"]:
            return None, "Defender is not in this war."
        if attacker_guild["guild_id"] == defender_guild["guild_id"]:
            return None, "You cannot fight a member of your own guild."
        await db.execute(
            "INSERT INTO guild_war_battles (war_id, attacker_user_id, defender_user_id, winner_user_id) VALUES (?, ?, ?, ?)",
            (war_id, attacker_user_id, defender_user_id, winner_user_id),
        )
        challenger_wins = war["challenger_wins"] or 0
        defender_wins = war["defender_wins"] or 0
        if winner_user_id == attacker_user_id:
            if attacker_guild["guild_id"] == war["challenger_guild_id"]:
                challenger_wins += 1
            else:
                defender_wins += 1
        elif winner_user_id == defender_user_id:
            if defender_guild["guild_id"] == war["challenger_guild_id"]:
                challenger_wins += 1
            else:
                defender_wins += 1
        await db.execute(
            "UPDATE guild_wars SET challenger_wins = ?, defender_wins = ? WHERE id = ?",
            (challenger_wins, defender_wins, war_id),
        )
        winner_guild_id = None
        if challenger_wins >= GUILD_WAR_WINS_TO_VICTORY:
            winner_guild_id = war["challenger_guild_id"]
        elif defender_wins >= GUILD_WAR_WINS_TO_VICTORY:
            winner_guild_id = war["defender_guild_id"]
        if winner_guild_id is not None:
            await db.execute(
                "UPDATE guild_wars SET status = 'ended', winner_guild_id = ? WHERE id = ?",
                (winner_guild_id, war_id),
            )
            await add_guild_treasury(winner_guild_id, GUILD_WAR_WIN_BONUS)
        await db.commit()
        return (challenger_wins, defender_wins), None

async def get_guild_members(guild_id):
    """Return list of dicts with user_id, username for guild members."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, guild_role FROM users WHERE guild_id = ? ORDER BY guild_role = 'leader' DESC, username",
            (guild_id,),
        ) as c:
            rows = await c.fetchall()
        return [dict(r) for r in rows]

async def get_other_guild_in_war(war, my_guild_id):
    """Return the guild_id of the opposing guild in this war."""
    if war["challenger_guild_id"] == my_guild_id:
        return war["defender_guild_id"]
    return war["challenger_guild_id"]
