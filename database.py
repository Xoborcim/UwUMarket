import sqlite3
import datetime

DB_NAME = "market.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db():
    conn = get_connection()
    try:
        c = conn.cursor()
        # 1. CORE TABLES
        c.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, balance REAL DEFAULT 1000.00, last_daily DATETIME)''')
        c.execute('''CREATE TABLE IF NOT EXISTS markets (
            market_id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            creator_id INTEGER,
            status TEXT DEFAULT 'OPEN',
            created_at DATETIME,
            closes_at DATETIME,
            message_id INTEGER,
            volume REAL DEFAULT 0.0
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS outcomes (outcome_id INTEGER PRIMARY KEY AUTOINCREMENT, market_id INTEGER, label TEXT NOT NULL, pool_balance REAL DEFAULT 5000.0, FOREIGN KEY(market_id) REFERENCES markets(market_id))''')
        c.execute('''CREATE TABLE IF NOT EXISTS positions (position_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, market_id INTEGER, outcome_id INTEGER, shares_held REAL DEFAULT 0.0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS jury_votes (vote_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, market_id INTEGER, outcome_id INTEGER, UNIQUE(user_id, market_id))''')
        
        # 2. CASINO TABLES
        c.execute('''CREATE TABLE IF NOT EXISTS lottery_tickets (ticket_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER)''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_globals (key TEXT PRIMARY KEY, value REAL)''')

        # 3. INVENTORY & MARKET TABLES
        c.execute('''CREATE TABLE IF NOT EXISTS inventory (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            user_id INTEGER, 
            item_name TEXT, 
            tier TEXT, 
            is_listed INTEGER DEFAULT 0, 
            list_price REAL DEFAULT 0.0,
            set_name TEXT DEFAULT 'Base_Set'
        )''')
        
        # 4. TOWN SIMULATION TABLE
        c.execute('''CREATE TABLE IF NOT EXISTS town (
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
        c.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        # LIVE BOARD MIGRATIONS
        try: c.execute("ALTER TABLE town ADD COLUMN board_channel_id INTEGER")
        except: pass
        try: c.execute("ALTER TABLE town ADD COLUMN board_message_id INTEGER")
        except: pass
        # Safe Column Migrations
        try: c.execute("ALTER TABLE users ADD COLUMN job TEXT DEFAULT 'Unemployed'")
        except: pass
        try: c.execute("ALTER TABLE users ADD COLUMN last_work DATETIME")
        except: pass
        try: c.execute("ALTER TABLE users ADD COLUMN daily_chat_earnings REAL DEFAULT 0.0")
        except: pass
        try: c.execute("ALTER TABLE users ADD COLUMN last_chat_reset DATETIME")
        except: pass
        try: c.execute("ALTER TABLE users ADD COLUMN starter_weapon TEXT DEFAULT 'Rusty Dagger'")
        except: pass
        try: c.execute("ALTER TABLE users ADD COLUMN rpg_class TEXT DEFAULT 'Fighter'")
        except: pass
        try: c.execute("ALTER TABLE inventory ADD COLUMN set_name TEXT DEFAULT 'Base_Set'")
        except: pass
        
        # LIVE BOARD MIGRATIONS
        try: c.execute("ALTER TABLE town ADD COLUMN board_channel_id INTEGER")
        except: pass
        try: c.execute("ALTER TABLE town ADD COLUMN board_message_id INTEGER")
        except: pass
        
        c.execute("UPDATE users SET balance = ROUND(balance, 2)")
        conn.commit()
    finally:
        conn.close()

# --- MARKET CREATION ---
def create_market_custom_date(question, creator_id, close_time_obj, options=["YES", "NO"]):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute('''INSERT INTO markets (question, creator_id, status, created_at, closes_at, volume) 
                     VALUES (?, ?, 'OPEN', datetime('now'), ?, 0.0)''', 
                  (question, creator_id, close_time_obj))
        market_id = c.lastrowid
        for label in options:
            c.execute("INSERT INTO outcomes (market_id, label, pool_balance) VALUES (?, ?, ?)", (market_id, label, 5000.0))
        conn.commit()
        return market_id
    finally: conn.close()

def get_active_markets():
    conn = get_connection()
    try:
        conn.execute("DELETE FROM markets WHERE status = 'OPEN' AND closes_at IS NULL")
        conn.commit()
        markets = conn.execute("SELECT market_id, question, closes_at FROM markets WHERE status = 'OPEN'").fetchall()
        results = []
        for m in markets:
            try:
                close_time = datetime.datetime.strptime(m['closes_at'], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                try: close_time = datetime.datetime.strptime(m['closes_at'], "%Y-%m-%d %H:%M:%S")
                except: continue
            outcomes = conn.execute("SELECT label FROM outcomes WHERE market_id = ?", (m['market_id'],)).fetchall()
            results.append({'market_id': m['market_id'], 'question': m['question'], 'close_time': close_time, 'options': [o['label'] for o in outcomes]})
        return results
    finally: conn.close()

# --- TRADING LOGIC ---
def buy_shares(user_id, market_id, outcome_label, investment):
    if investment < 0.01: return False, "Minimum bet is $0.01"
    investment = round(investment, 2)

    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        bal = c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()['balance']
        if bal < investment: return False, "Insufficient funds."

        outcomes = c.execute("SELECT * FROM outcomes WHERE market_id = ?", (market_id,)).fetchall()
        target = next((o for o in outcomes if o['label'] == outcome_label), None)
        if not target: return False, "Invalid outcome."

        current_pool = sum(o['pool_balance'] for o in outcomes)
        target_bal = target['pool_balance']
        future_pool = current_pool + investment
        future_target = target_bal + investment
        probability = future_target / future_pool
        shares = investment / probability

        c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (investment, user_id))
        c.execute("UPDATE outcomes SET pool_balance = pool_balance + ? WHERE outcome_id = ?", (investment, target['outcome_id']))
        c.execute("UPDATE markets SET volume = volume + ? WHERE market_id = ?", (investment, market_id))

        existing = c.execute("SELECT position_id FROM positions WHERE user_id = ? AND outcome_id = ?", (user_id, target['outcome_id'])).fetchone()
        if existing:
            c.execute("UPDATE positions SET shares_held = shares_held + ? WHERE position_id = ?", (shares, existing['position_id']))
        else:
            c.execute("INSERT INTO positions (user_id, market_id, outcome_id, shares_held) VALUES (?, ?, ?, ?)", (user_id, market_id, target['outcome_id'], shares))
        conn.commit()
        return True, f"Bought {shares:.2f} shares @ {int(probability*100)}¢"
    except Exception as e: return False, str(e)
    finally: conn.close()

def resolve_market(market_id, winning_label):
    conn = get_connection()
    try:
        c = conn.cursor()
        m = c.execute("SELECT question FROM markets WHERE market_id = ?", (market_id,)).fetchone()
        question = m['question'] if m else "Unknown"
        winner = c.execute("SELECT outcome_id FROM outcomes WHERE market_id = ? AND label = ?", (market_id, winning_label)).fetchone()
        if not winner: return False, ["Invalid Outcome"], ""
        
        winners = c.execute("SELECT user_id, shares_held FROM positions WHERE market_id = ? AND outcome_id = ?", (market_id, winner['outcome_id'])).fetchall()
        c.execute("UPDATE markets SET status = 'RESOLVED' WHERE market_id = ?", (market_id,))
        logs = []
        for w in winners:
            payout = round(w['shares_held'] * 1.0, 2)
            c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (payout, w['user_id']))
            logs.append(f"<@{w['user_id']}> won ${payout:.2f}")
        conn.commit()
        return True, logs, question
    finally: conn.close()

# --- HELPERS ---
def get_balance(user_id):
    conn = get_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        return conn.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()['balance']
    finally: conn.close()

def update_balance(user_id, amount):
    amount = round(amount, 2)
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        if amount < 0:
            bal = c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()['balance']
            if bal + amount < 0: return False
        c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        conn.commit()
        return True
    finally: conn.close()

def get_odds(market_id):
    conn = get_connection()
    try:
        outcomes = conn.execute("SELECT * FROM outcomes WHERE market_id = ?", (market_id,)).fetchall()
        if not outcomes: return []
        total = sum(o['pool_balance'] for o in outcomes)
        if total == 0: return []
        return [(o['label'], o['pool_balance'] / total) for o in outcomes]
    finally: conn.close()

def get_market_volume(market_id):
    conn = get_connection()
    try: return conn.execute("SELECT volume FROM markets WHERE market_id = ?", (market_id,)).fetchone()['volume']
    except: return 0.0
    finally: conn.close()

def get_expired_markets():
    conn = get_connection()
    try: return conn.execute("SELECT market_id, question FROM markets WHERE status = 'OPEN' AND closes_at <= datetime('now')").fetchall()
    finally: conn.close()

def get_leaderboard(limit=10):
    conn = get_connection()
    try: return conn.execute("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT ?", (limit,)).fetchall()
    finally: conn.close()

def get_market_message_id(market_id):
    conn = get_connection()
    try: return conn.execute("SELECT message_id FROM markets WHERE market_id = ?", (market_id,)).fetchone()['message_id']
    except: return None
    finally: conn.close()

def set_market_message_id(market_id, message_id):
    conn = get_connection()
    try: 
        conn.execute("UPDATE markets SET message_id = ? WHERE market_id = ?", (message_id, market_id))
        conn.commit()
    finally: conn.close()

def close_market_betting(market_id):
    conn = get_connection()
    try: 
        conn.execute("UPDATE markets SET status = 'VOTING' WHERE market_id = ?", (market_id,))
        conn.commit()
    finally: conn.close()

def get_user_portfolio(user_id):
    conn = get_connection()
    try:
        return conn.execute("""
            SELECT m.question, o.label, p.shares_held, m.status, m.market_id
            FROM positions p
            JOIN outcomes o ON p.outcome_id = o.outcome_id
            JOIN markets m ON p.market_id = m.market_id
            WHERE p.user_id = ? AND p.shares_held > 0
            ORDER BY m.status DESC, m.market_id DESC
        """, (user_id,)).fetchall()
    finally: conn.close()

def cast_jury_vote(user_id, market_id, outcome_label):
    conn = get_connection()
    try:
        outcome = conn.execute("SELECT outcome_id FROM outcomes WHERE market_id = ? AND label = ?", (market_id, outcome_label)).fetchone()
        if not outcome: return False, "Invalid Outcome"
        try:
            conn.execute("INSERT INTO jury_votes (user_id, market_id, outcome_id) VALUES (?, ?, ?)", (user_id, market_id, outcome['outcome_id']))
            conn.commit()
            return True, "Vote Cast"
        except sqlite3.IntegrityError: return False, "Already voted."
    finally: conn.close()

def get_market_vote_tally(market_id):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT o.label, COUNT(j.vote_id) as count
            FROM jury_votes j JOIN outcomes o ON j.outcome_id = o.outcome_id
            WHERE j.market_id = ? GROUP BY o.label
        """, (market_id,)).fetchall()
        return {r['label']: r['count'] for r in rows}
    finally: conn.close()

# --- DAILY ---
def process_daily(user_id):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        user = c.execute("SELECT last_daily FROM users WHERE user_id = ?", (user_id,)).fetchone()
        
        if user and user['last_daily']:
            last_daily = datetime.datetime.strptime(user['last_daily'], "%Y-%m-%d %H:%M:%S.%f")
            next_daily = last_daily + datetime.timedelta(hours=24)
            if datetime.datetime.now() < next_daily:
                ts = int(next_daily.timestamp())
                return False, f"Daily already claimed! Try again <t:{ts}:R>."
        
        c.execute("UPDATE users SET balance = balance + 500, last_daily = ? WHERE user_id = ?", (datetime.datetime.now(), user_id))
        conn.commit()
        return True, "Daily $500 claimed! Come back in 24 hours."
    finally: conn.close()

# --- LOTTERY ---
def buy_lottery_ticket(user_id, cost=50):
    if update_balance(user_id, -cost):
        conn = get_connection()
        try:
            conn.execute("INSERT INTO lottery_tickets (user_id) VALUES (?)", (user_id,))
            conn.commit()
            return True
        finally: conn.close()
    return False

def get_lottery_stats():
    conn = get_connection()
    try:
        count = conn.execute("SELECT COUNT(*) as c FROM lottery_tickets").fetchone()['c']
        pot = (count * 50) + 500
        return count, pot
    finally: conn.close()

def draw_lottery_winner():
    conn = get_connection()
    try:
        tickets = conn.execute("SELECT user_id FROM lottery_tickets").fetchall()
        if not tickets: return None, 0
        import random
        winner_row = random.choice(tickets)
        winner_id = winner_row['user_id']
        count = len(tickets)
        pot = (count * 50) + 500
        c = conn.cursor()
        c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (pot, winner_id))
        c.execute("DELETE FROM lottery_tickets")
        conn.commit()
        return winner_id, pot
    finally: conn.close()

# --- INVENTORY & P2P MARKET ---
def add_item(user_id, item_name, tier, set_name):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO inventory (user_id, item_name, tier, set_name) VALUES (?, ?, ?, ?)", (user_id, item_name, tier, set_name))
        conn.commit()
    finally: conn.close()

def scrap_item(user_id, item_id, scrap_value):
    conn = get_connection()
    try:
        c = conn.cursor()
        item = c.execute("SELECT * FROM inventory WHERE item_id = ? AND user_id = ? AND is_listed = 0", (item_id, user_id)).fetchone()
        if not item: return False, "Item not found or currently listed on market."
        
        c.execute("DELETE FROM inventory WHERE item_id = ?", (item_id,))
        c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (scrap_value, user_id))
        conn.commit()
        return True, f"Scrapped **{item['item_name']}** for ${scrap_value:,.2f}!"
    finally: conn.close()

def get_inventory(user_id):
    conn = get_connection()
    try: return conn.execute("SELECT * FROM inventory WHERE user_id = ? AND is_listed = 0", (user_id,)).fetchall()
    finally: conn.close()

def get_item_by_id(item_id):
    conn = get_connection()
    try: return conn.execute("SELECT * FROM inventory WHERE item_id = ?", (item_id,)).fetchone()
    finally: conn.close()

def list_item_on_market(user_id, item_id, price):
    if price < 0.01: return False, "Price must be at least $0.01"
    conn = get_connection()
    try:
        item = conn.execute("SELECT * FROM inventory WHERE item_id = ? AND user_id = ? AND is_listed = 0", (item_id, user_id)).fetchone()
        if not item: return False, "Item not found or already listed."
        conn.execute("UPDATE inventory SET is_listed = 1, list_price = ? WHERE item_id = ?", (round(price, 2), item_id))
        conn.commit()
        return True, "Item listed successfully."
    finally: conn.close()

def get_market_listings():
    conn = get_connection()
    try: return conn.execute("SELECT * FROM inventory WHERE is_listed = 1").fetchall()
    finally: conn.close()

def buy_market_item(buyer_id, item_id):
    conn = get_connection()
    try:
        c = conn.cursor()
        item = c.execute("SELECT * FROM inventory WHERE item_id = ? AND is_listed = 1", (item_id,)).fetchone()
        if not item: return False, "Item no longer available."
        
        seller_id = item['user_id']
        price = item['list_price']
        if buyer_id == seller_id: return False, "You cannot buy your own item."
        
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (buyer_id,))
        buyer_bal = c.execute("SELECT balance FROM users WHERE user_id = ?", (buyer_id,)).fetchone()['balance']
        if buyer_bal < price: return False, "Insufficient funds."
        
        c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (price, buyer_id))
        c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (price, seller_id))
        c.execute("UPDATE inventory SET user_id = ?, is_listed = 0, list_price = 0.0 WHERE item_id = ?", (buyer_id, item_id))
        conn.commit()
        return True, f"You bought {item['item_name']} for ${price:,.2f}!"
    except Exception as e: return False, str(e)
    finally: conn.close()

# --- JOBS & TOWN INCOME SYSTEM ---
def get_job_profile(user_id):
    conn = get_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return conn.execute("SELECT job, last_work, daily_chat_earnings FROM users WHERE user_id = ?", (user_id,)).fetchone()
    finally: conn.close()

def set_job(user_id, job_name):
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET job = ? WHERE user_id = ?", (job_name, user_id))
        conn.commit()
    finally: conn.close()

def process_work(user_id, base_payout):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        town = c.execute("SELECT level, tax_rate, famine FROM town WHERE id=1").fetchone()
        
        # Safety fallbacks in case the table had empty rows
        if town:
            level = town['level'] if town['level'] else 1
            tax_rate = town['tax_rate'] if town['tax_rate'] is not None else 0.05
            famine = town['famine'] if town['famine'] else 0
        else:
            level, tax_rate, famine = 1, 0.05, 0
        
        multiplier = 1.0 + (level * 0.05)
        if famine == 1:
            multiplier *= 0.5 
            
        gross_pay = base_payout * multiplier
        tax_amount = round(gross_pay * tax_rate, 2)
        net_pay = round(gross_pay - tax_amount, 2)
        
        # COALESCE prevents the SQLite "NULL Math" bug!
        c.execute("UPDATE users SET balance = COALESCE(balance, 0.0) + ?, last_work = ? WHERE user_id = ?", (net_pay, datetime.datetime.now(), user_id))
        
        c.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        c.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id = 1", (tax_amount,))
        conn.commit()
        return net_pay, tax_amount
    finally: conn.close()

def process_chat_income(user_id, amount, daily_cap=500.0):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        user = c.execute("SELECT daily_chat_earnings, last_chat_reset FROM users WHERE user_id = ?", (user_id,)).fetchone()
        town = c.execute("SELECT level, famine FROM town WHERE id=1").fetchone()
        
        level = town['level'] if town and town['level'] else 1
        famine = town['famine'] if town and town['famine'] else 0
        
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
        
        c.execute("UPDATE users SET balance = COALESCE(balance, 0.0) + ?, daily_chat_earnings = ?, last_chat_reset = ? WHERE user_id = ?", 
                  (actual_amount, earnings + actual_amount, now, user_id))
        conn.commit()
        return True
    finally: conn.close()

# --- TOWN SIMULATION HELPERS ---
def get_town_state():
    conn = get_connection()
    try: return dict(conn.execute("SELECT * FROM town WHERE id=1").fetchone())
    except: return None
    finally: conn.close()

def add_town_resources(food=0, materials=0):
    conn = get_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        conn.execute("UPDATE town SET food = COALESCE(food, 0) + ?, materials = COALESCE(materials, 0) + ? WHERE id=1", (food, materials))
        conn.commit()
    finally: conn.close()

def set_tax_rate(rate):
    conn = get_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        conn.execute("UPDATE town SET tax_rate = ? WHERE id=1", (rate,))
        conn.commit()
    finally: conn.close()

def try_upgrade_town(mat_cost, gold_cost):
    conn = get_connection()
    try:
        c = conn.cursor()
        town = c.execute("SELECT materials, treasury FROM town WHERE id=1").fetchone()
        if not town: return False
        
        mats = town['materials'] or 0
        treasury = town['treasury'] or 0.0
        
        if mats < mat_cost or treasury < gold_cost:
            return False
        
        c.execute("UPDATE town SET materials = materials - ?, treasury = treasury - ?, level = COALESCE(level, 1) + 1 WHERE id=1", (mat_cost, gold_cost))
        conn.commit()
        return True
    finally: conn.close()
    
def embezzle_town_funds(user_id, amount):
    conn = get_connection()
    try:
        c = conn.cursor()
        town = c.execute("SELECT treasury FROM town WHERE id=1").fetchone()
        if not town: return False
        
        treasury = town['treasury'] or 0.0
        if treasury < amount: return False
            
        c.execute("UPDATE town SET treasury = treasury - ? WHERE id=1", (amount,))
        c.execute("UPDATE users SET balance = COALESCE(balance, 0.0) + ? WHERE user_id=?", (amount, user_id))
        conn.commit()
        return True
    finally: conn.close()
    
def run_town_daily_upkeep():
    conn = get_connection()
    try:
        c = conn.cursor()
        town = c.execute("SELECT level, food FROM town WHERE id=1").fetchone()
        if not town: return False, 0
        
        level = town['level'] or 1
        food = town['food'] or 0
        
        drain = level * 15
        new_food = food - drain
        
        famine = 1 if new_food < 0 else 0
        new_food = max(0, new_food)
        
        c.execute("UPDATE town SET food = ?, famine = ? WHERE id=1", (new_food, famine))
        conn.commit()
        return famine, drain
    finally: conn.close()

def set_town_board(channel_id, message_id):
    conn = get_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO town (id) VALUES (1)")
        conn.execute("UPDATE town SET board_channel_id=?, board_message_id=? WHERE id=1", (channel_id, message_id))
        conn.commit()
    finally: conn.close()

# --- RPG SHOP ---
def get_rpg_profile(user_id):
    conn = get_connection()
    try:
        conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        profile = conn.execute("SELECT starter_weapon, rpg_class FROM users WHERE user_id = ?", (user_id,)).fetchone()
        weapon = profile['starter_weapon'] if profile and profile['starter_weapon'] else "Rusty Dagger"
        rpg_class = profile['rpg_class'] if profile and profile['rpg_class'] else "Fighter"
        return weapon, rpg_class
    finally: conn.close()

def set_rpg_class(user_id, class_name):
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET rpg_class = ? WHERE user_id = ?", (class_name, user_id))
        conn.commit()
    finally: conn.close()

def set_town_board(channel_id, message_id):
    conn = get_connection()
    try:
        conn.execute("UPDATE town SET board_channel_id=?, board_message_id=? WHERE id=1", (channel_id, message_id))
        conn.commit()
    finally: conn.close()

def buy_starter_weapon(user_id, weapon_name, cost):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        user = c.execute("SELECT balance, starter_weapon FROM users WHERE user_id = ?", (user_id,)).fetchone()
        
        if user['balance'] < cost:
            return False, "Insufficient funds in your global balance."
        if user['starter_weapon'] == weapon_name:
            return False, "You already own this weapon and have it equipped!"
            
        c.execute("UPDATE users SET balance = balance - ?, starter_weapon = ? WHERE user_id = ?", (cost, weapon_name, user_id))
        conn.commit()
        return True, f"Successfully purchased and equipped the {weapon_name}!"
    finally: conn.close()