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
            balance REAL DEFAULT 1000.00, 
            last_daily DATETIME
        )''')
        # 1. CORE TABLES
        await db.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, balance REAL DEFAULT 1000.00, last_daily DATETIME)''')
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
        ]
        
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
        await db.commit()


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
        net, tax = await process_town_payout(user_id, 500.0)
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
            pot = (count * 50) + 500
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
        pot = (count * 50) + 500
        
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

# --- JOBS & TOWN INCOME SYSTEM ---
async def process_town_payout(user_id, amount):
    """MASTER FUNCTION: Applies town multipliers, famines, and taxes to new currency generation."""
    if amount <= 0: return 0.0, 0.0
    
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        async with db.execute("SELECT level, tax_rate, famine FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
        
        level = town['level'] if town and town['level'] else 1
        tax_rate = town['tax_rate'] if town and town['tax_rate'] is not None else 0.05
        famine = town['famine'] if town and town['famine'] else 0
        
        multiplier = 1.0 + (level * 0.05)
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

async def process_chat_income(user_id, amount, daily_cap=500.0):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        
        async with db.execute("SELECT daily_chat_earnings, last_chat_reset FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user = await cursor.fetchone()
            
        async with db.execute("SELECT level, famine, tax_rate FROM town WHERE id=1") as cursor:
            town = await cursor.fetchone()
        
        level = town['level'] if town and town['level'] else 1
        famine = town['famine'] if town and town['famine'] else 0
        tax_rate = town['tax_rate'] if town and town['tax_rate'] is not None else 0.05
        
        multiplier = 1.0 + (level * 0.05)
        if famine == 1: multiplier *= 0.5 
        adjusted_amount = amount * multiplier

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
        
        for p in party_data:
            uid = p['user'].id
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (uid,))
            async with db.execute("SELECT max_floor FROM users WHERE user_id = ?", (uid,)) as cursor:
                user_row = await cursor.fetchone()
                current_max = user_row['max_floor'] if user_row and user_row['max_floor'] else 0
                
            if floor > current_max:
                await db.execute("UPDATE users SET max_floor = ? WHERE user_id = ?", (floor, uid))
                
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
