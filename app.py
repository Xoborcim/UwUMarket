import os
import random
import requests
import aiosqlite
import asyncio
from flask import Flask, render_template, request, session, redirect, url_for, send_from_directory
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv
import database as db  # Your local database.py file

# --- INITIALIZATION ---
load_dotenv()

app = Flask(__name__)

# Fetch the secrets from .env
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
DB_NAME = os.getenv('DB_NAME')

# Initialize SocketIO with eventlet for high-concurrency background tasks
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

TIER_WEIGHTS = {
    "Common": 50, "Uncommon": 25, "Rare": 15, 
    "Epic": 9, "Legendary": 5, "Mythic": 1
}

SCRAP_VALUES = {
    "Common": 10.0, "Uncommon": 25.0, "Rare": 50.0, 
    "Epic": 150.0, "Legendary": 500.0, "Mythic": 2000.0
}

# --- HELPER FUNCTIONS ---

def run_async(coro):
    """Utility to run async database functions in a synchronous Flask route."""
    return asyncio.run(coro)

async def get_player_stats(user_id):
    async with aiosqlite.connect(DB_NAME) as db_conn:
        db_conn.row_factory = aiosqlite.Row
        async with db_conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def get_market_listings():
    async with aiosqlite.connect(DB_NAME) as db_conn:
        db_conn.row_factory = aiosqlite.Row
        async with db_conn.execute("SELECT * FROM inventory WHERE is_listed = 1") as cursor:
            return await cursor.fetchall()

def get_random_item(set_name, tier):
    folder_path = f"lootboxes/{set_name}/{tier}"
    if not os.path.exists(folder_path): return None
    files = [f for f in os.listdir(folder_path) if f.endswith('.png')]
    if not files: return None
    chosen = random.choice(files)
    item_name = chosen.replace(".png", "").replace("_", " ").title()
    image_url = f"/images/{set_name}/{tier}/{chosen}"
    return item_name, image_url

# --- WEBSOCKET BROADCASTER ---

def broadcast_update(event_type, data):
    """Shouts an update to all connected users instantly."""
    socketio.emit(event_type, data)

# --- CORE ROUTES ---

@app.route('/')
def index():
    return redirect(url_for('market'))

@app.route('/images/<path:filename>')
def serve_image(filename):
    fixed_filename = filename.replace(" ", "_")
    if not os.path.exists(os.path.join('lootboxes', fixed_filename)):
        fixed_filename = filename
    return send_from_directory('lootboxes', fixed_filename)

@app.route('/api/gold')
def api_gold():
    if 'user_id' not in session: return {"success": False, "error": "Not logged in"}, 401
    player = run_async(get_player_stats(session['user_id']))
    if player: return {"success": True, "gold": player['balance']}
    return {"success": False, "error": "Player not found"}, 404

# --- AUTHENTICATION & PROFILE ---

@app.route('/login')
def login():
    discord_auth_url = f"https://discord.com/api/oauth2/authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code&scope=identify"
    return redirect(discord_auth_url)

@app.route('/callback')
def callback():
    code = request.args.get('code')
    data = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET, 'grant_type': 'authorization_code', 'code': code, 'redirect_uri': REDIRECT_URI}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    r = requests.post('https://discord.com/api/oauth2/token', data=data, headers=headers)
    token_json = r.json()
    if 'access_token' not in token_json: return f"Login Failed: {token_json.get('error_description', 'Unknown error')}"
    user_info = requests.get('https://discord.com/api/users/@me', headers={'Authorization': f"Bearer {token_json['access_token']}"}).json()
    
    session['user_id'] = int(user_info['id'])
    session['username'] = user_info['username']
    
    # Notify everyone that a player has entered
    broadcast_update('player_login', {'username': user_info['username']})
    
    user_info = requests.get('https://discord.com/api/users/@me', 
                             headers={'Authorization': f"Bearer {token_json['access_token']}"}).json()
    
    user_id = int(user_info['id'])
    username = user_info['username']
    
    # NEW: Sync the username to the DB immediately
    run_async(db.sync_user_data(user_id, username))
    
    session['user_id'] = user_id
    session['username'] = username
    return redirect(url_for('market'))
    
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('market'))

@app.route('/profile/<int:user_id>')
def profile(user_id):
    player = run_async(get_player_stats(user_id))
    if not player: return "<h1>404: Player not found in market.db</h1>", 404
    return render_template('profile.html', player=player, user_id=user_id)

# --- MARKET ROUTES ---

@app.route('/market')
def market():
    # 1. Check if the user is logged in
    if 'user_id' not in session:
        # We pass a message so the user knows why they were redirected
        return render_template('market.html', 
                               items=[], 
                               message="Please login with Discord to view the Marketplace.", 
                               success=False)

    # 2. If they are logged in, proceed as normal
    items = run_async(get_market_listings())
    return render_template('market.html', items=items)

@app.route('/buy/<int:item_id>', methods=['POST'])
def buy_item(item_id):
    if 'user_id' not in session: return redirect(url_for('login'))
    success, message = run_async(db.buy_market_item(session['user_id'], item_id))
    
    if success:
        # Notify browsers to remove the item from their lists without refreshing
        broadcast_update('item_sold', {'item_id': item_id, 'buyer': session['username']})
        
    return render_template('market.html', items=run_async(get_market_listings()), message=message, success=success)

# --- INVENTORY ROUTES ---

@app.route('/inventory')
def inventory():
    if 'user_id' not in session: return redirect(url_for('login'))
    raw_items = run_async(db.get_inventory(session['user_id']))
    return render_template('inventory.html', items=[dict(row) for row in raw_items])

@app.route('/api/sell_item', methods=['POST'])
def api_sell_item():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json()
    try: price = float(data.get('price'))
    except: return {"success": False, "message": "Invalid price."}
    
    success, msg = run_async(db.list_item_on_market(session['user_id'], data.get('item_id'), price))
    
    if success:
        # Shout that a new item is available!
        broadcast_update('new_listing', {'item_id': data.get('item_id'), 'price': price})
        
    return {"success": success, "message": msg}

@app.route('/api/scrap_item', methods=['POST'])
def api_scrap_item():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    user_id = session['user_id']
    item_id = request.get_json().get('item_id')
    item = run_async(db.get_item_by_id(item_id))
    if not item or item['user_id'] != user_id: return {"success": False, "message": "You don't own this item."}
    success, msg = run_async(db.scrap_item(user_id, item_id, SCRAP_VALUES.get(item['tier'], 5.0)))
    return {"success": success, "message": msg}

# --- LOOTBOX ROUTES ---

@app.route('/lootbox')
def lootbox_page():
    if 'user_id' not in session: return redirect(url_for('login'))
    available_sets = [d for d in os.listdir("lootboxes") if os.path.isdir(os.path.join("lootboxes", d))] if os.path.exists("lootboxes") else []
    return render_template('lootbox.html', available_sets=available_sets)

@app.route('/api/open_box', methods=['POST'])
def api_open_box():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    user_id = session['user_id']
    set_name = (request.get_json() or {}).get('set_name', 'Base_Set')
    cost = 500.0
    
    valid_tiers = [tier for tier in TIER_WEIGHTS.keys() if os.path.exists(f"lootboxes/{set_name}/{tier}") and any(f.endswith('.png') for f in os.listdir(f"lootboxes/{set_name}/{tier}"))]
    if not valid_tiers: return {"success": False, "message": f"Set '{set_name}' is empty."}

    bal = run_async(db.get_balance(user_id))
    if bal < cost: return {"success": False, "message": f"Not enough gold! Need ${cost:,.2f}."}
        
    run_async(db.update_balance(user_id, -cost))
    rolled_tier = random.choices(valid_tiers, weights=[TIER_WEIGHTS[t] for t in valid_tiers], k=1)[0]
    result = get_random_item(set_name, rolled_tier)
    
    if not result:
        run_async(db.update_balance(user_id, cost))
        return {"success": False, "message": "Item generation failed."}
        
    run_async(db.add_item(user_id, result[0], rolled_tier, set_name))
    
    # Shout to the world about the pull!
    if rolled_tier in ["Epic", "Legendary", "Mythic"]:
        broadcast_update('big_pull', {'username': session['username'], 'item': result[0], 'tier': rolled_tier})
        
    return {"success": True, "message": f"Unboxed a {rolled_tier} {result[0]}!", "item": {"name": result[0], "tier": rolled_tier, "image": result[1]}}

# --- TOWN HALL ROUTES ---

@app.route('/town')
def town_hall():
    town = run_async(db.get_town_state())
    if not town: town = {'level': 1, 'treasury': 0.0, 'food': 0, 'tax_rate': 0.05, 'famine': 0, 'user_count': 1}
    return render_template('town.html', town=town)

@app.route('/api/donate', methods=['POST'])
def api_donate():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json()
    user_id = session['user_id']
    try: amount = float(data.get('amount'))
    except: return {"success": False, "message": "Invalid amount."}
    if amount <= 0: return {"success": False, "message": "Amount must be positive."}
        
    bal = run_async(db.get_balance(user_id))
    
    if data.get('type') == 'gold':
        if bal < amount: return {"success": False, "message": "Not enough gold!"}
        run_async(db.update_balance(user_id, -amount))
        async def donate_gold_db(amt):
            async with aiosqlite.connect(DB_NAME) as db_conn:
                await db_conn.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id=1", (amt,))
                await db_conn.commit()
        run_async(donate_gold_db(amount))
        broadcast_update('town_update', {'message': f"{session['username']} donated ${amount:,.2f}!"})
        return {"success": True, "message": f"Donated ${amount:,.2f} to the Town Treasury!"}
        
    elif data.get('type') == 'food':
        cost = amount * 10
        if bal < cost: return {"success": False, "message": f"Not enough gold! {int(amount)} Food costs ${cost:,.2f}."}
        run_async(db.update_balance(user_id, -cost))
        run_async(db.add_town_resources(food=int(amount)))
        broadcast_update('town_update', {'message': f"{session['username']} bought {int(amount)} Food!"})
        return {"success": True, "message": f"Bought {int(amount)} Food for the town for ${cost:,.2f}!"}

# --- BETTING EXCHANGE ROUTES ---

@app.route('/exchange')
def exchange():
    raw_markets = run_async(db.get_active_markets())
    markets_data = []
    
    for m in raw_markets:
        market_id = m['market_id']
        async def get_market_details(m_id):
            async with aiosqlite.connect(DB_NAME) as db_conn:
                db_conn.row_factory = aiosqlite.Row
                async with db_conn.execute("SELECT * FROM outcomes WHERE market_id = ?", (m_id,)) as cursor:
                    return await cursor.fetchall()
        
        outcomes = run_async(get_market_details(market_id))
        total_pool = sum(o['pool_balance'] for o in outcomes)
        
        outcomes_data = []
        for o in outcomes:
            percentage = (o['pool_balance'] / total_pool * 100) if total_pool > 0 else 50
            outcomes_data.append({
                'label': o['label'],
                'pool': o['pool_balance'],
                'percent': percentage
            })
            
        volume = run_async(db.get_market_volume(market_id))
        markets_data.append({
            'market_id': market_id,
            'question': m['question'],
            'close_time': m['close_time'].strftime("%b %d, %Y %H:%M") if m['close_time'] else "N/A",
            'volume': volume,
            'outcomes': outcomes_data
        })
        
    return render_template('exchange.html', markets=markets_data)

@app.route('/api/buy_shares', methods=['POST'])
def api_buy_shares():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json()
    market_id = data.get('market_id')
    outcome_label = data.get('outcome_label')
    try:
        investment = float(data.get('investment'))
        if investment < 0.01: return {"success": False, "message": "Minimum bet is $0.01"}
    except: return {"success": False, "message": "Invalid investment amount."}
        
    success, msg = run_async(db.buy_shares(session['user_id'], market_id, outcome_label, investment))
    
    if success:
        broadcast_update('market_bet', {'market_id': market_id, 'amount': investment})
        
    return {"success": success, "message": msg}

# --- LEADERBOARD ROUTES ---

# --- LEADERBOARD ROUTES ---

@app.route('/leaderboard')
def leaderboard():
    async def get_leaders():
        async with aiosqlite.connect(DB_NAME) as db_conn:
            db_conn.row_factory = aiosqlite.Row
            
            # 1. Richest (Grabbing username directly from users table)
            async with db_conn.execute("""
                SELECT username, balance FROM users 
                WHERE username IS NOT NULL 
                ORDER BY balance DESC LIMIT 10
            """) as c:
                richest = await c.fetchall()
            
            # 2. Strongest (Grabbing username directly from users table)
            async with db_conn.execute("""
                SELECT username, max_floor FROM users 
                WHERE username IS NOT NULL 
                ORDER BY max_floor DESC LIMIT 10
            """) as c:
                strongest = await c.fetchall()
                
            # 3. Collectors (JOINING users and inventory to count items by name)
            async with db_conn.execute("""
                SELECT u.username, COUNT(i.item_id) as item_count 
                FROM users u
                JOIN inventory i ON u.user_id = i.user_id 
                WHERE u.username IS NOT NULL
                GROUP BY u.user_id 
                ORDER BY item_count DESC LIMIT 10
            """) as c:
                collectors = await c.fetchall()
                
            return richest, strongest, collectors

    richest, strongest, collectors = run_async(get_leaders())
    
    return render_template('leaderboard.html', 
                           richest=richest, 
                           strongest=strongest, 
                           collectors=collectors)
# --- START THE ENGINE ---

if __name__ == '__main__':
    # Using socketio.run instead of app.run is mandatory for real-time functionality
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)