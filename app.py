from flask import Flask, render_template, request, session, redirect, url_for, send_from_directory
import aiosqlite
import asyncio
import requests
import database as db
import os
import random
from dotenv import load_dotenv

# Load the variables from .env into the system environment
load_dotenv()

app = Flask(__name__)

# Fetch the secrets
app.secret_key = os.getenv('FLASK_SECRET_KEY')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
DB_NAME = os.getenv('DB_NAME')

TIER_WEIGHTS = {
    "Common": 50, "Uncommon": 25, "Rare": 15, 
    "Epic": 9, "Legendary": 5, "Mythic": 1
}

SCRAP_VALUES = {
    "Common": 10.0, "Uncommon": 25.0, "Rare": 50.0, 
    "Epic": 150.0, "Legendary": 500.0, "Mythic": 2000.0
}

# --- HELPER FUNCTIONS ---
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

# --- CORE ROUTES ---
@app.route('/')
def index():
    return redirect(url_for('market'))

@app.route('/images/<path:filename>')
def serve_image(filename):
    # This converts "A%20Passionate%20Salute.png" back to "A_Passionate_Salute.png"
    # only for the file lookup, so the browser doesn't get confused.
    fixed_filename = filename.replace(" ", "_")
    
    # We check if the underscore version exists; if not, we try the original
    if not os.path.exists(os.path.join('lootboxes', fixed_filename)):
        fixed_filename = filename

    return send_from_directory('lootboxes', fixed_filename)

@app.route('/api/gold')
def api_gold():
    if 'user_id' not in session: return {"success": False, "error": "Not logged in"}, 401
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    player = loop.run_until_complete(get_player_stats(session['user_id']))
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
    return redirect(url_for('market'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('market'))

@app.route('/profile/<int:user_id>')
def profile(user_id):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    player = loop.run_until_complete(get_player_stats(user_id))
    if not player: return "<h1>404: Player not found in market.db</h1>", 404
    return render_template('profile.html', player=player, user_id=user_id)

# --- MARKET ROUTES ---
@app.route('/market')
def market():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    items = loop.run_until_complete(get_market_listings())
    return render_template('market.html', items=items)

@app.route('/buy/<int:item_id>', methods=['POST'])
def buy_item(item_id):
    if 'user_id' not in session: return redirect(url_for('login'))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    success, message = loop.run_until_complete(db.buy_market_item(session['user_id'], item_id))
    return render_template('market.html', items=loop.run_until_complete(get_market_listings()), message=message, success=success)

# --- INVENTORY ROUTES ---
@app.route('/inventory')
def inventory():
    if 'user_id' not in session: return redirect(url_for('login'))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    raw_items = loop.run_until_complete(db.get_inventory(session['user_id']))
    return render_template('inventory.html', items=[dict(row) for row in raw_items])

@app.route('/api/sell_item', methods=['POST'])
def api_sell_item():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json()
    try: price = float(data.get('price'))
    except: return {"success": False, "message": "Invalid price."}
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    success, msg = loop.run_until_complete(db.list_item_on_market(session['user_id'], data.get('item_id'), price))
    return {"success": success, "message": msg}

@app.route('/api/scrap_item', methods=['POST'])
def api_scrap_item():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    user_id = session['user_id']
    item_id = request.get_json().get('item_id')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    item = loop.run_until_complete(db.get_item_by_id(item_id))
    if not item or item['user_id'] != user_id: return {"success": False, "message": "You don't own this item."}
    success, msg = loop.run_until_complete(db.scrap_item(user_id, item_id, SCRAP_VALUES.get(item['tier'], 5.0)))
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

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bal = loop.run_until_complete(db.get_balance(user_id))
    if bal < cost: return {"success": False, "message": f"Not enough gold! Need ${cost:,.2f}."}
        
    loop.run_until_complete(db.update_balance(user_id, -cost))
    rolled_tier = random.choices(valid_tiers, weights=[TIER_WEIGHTS[t] for t in valid_tiers], k=1)[0]
    result = get_random_item(set_name, rolled_tier)
    
    if not result:
        loop.run_until_complete(db.update_balance(user_id, cost))
        return {"success": False, "message": "Item generation failed."}
        
    loop.run_until_complete(db.add_item(user_id, result[0], rolled_tier, set_name))
    return {"success": True, "message": f"Unboxed a {rolled_tier} {result[0]}!", "item": {"name": result[0], "tier": rolled_tier, "image": result[1]}}

# --- TOWN HALL ROUTES ---
@app.route('/town')
def town_hall():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    town = loop.run_until_complete(db.get_town_state())
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
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bal = loop.run_until_complete(db.get_balance(user_id))
    
    if data.get('type') == 'gold':
        if bal < amount: return {"success": False, "message": "Not enough gold!"}
        loop.run_until_complete(db.update_balance(user_id, -amount))
        async def donate_gold(amt):
            async with aiosqlite.connect(DB_NAME) as db_conn:
                await db_conn.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id=1", (amt,))
                await db_conn.commit()
        loop.run_until_complete(donate_gold(amount))
        return {"success": True, "message": f"Donated ${amount:,.2f} to the Town Treasury!"}
        
    elif data.get('type') == 'food':
        cost = amount * 10
        if bal < cost: return {"success": False, "message": f"Not enough gold! {int(amount)} Food costs ${cost:,.2f}."}
        loop.run_until_complete(db.update_balance(user_id, -cost))
        loop.run_until_complete(db.add_town_resources(food=int(amount)))
        return {"success": True, "message": f"Bought {int(amount)} Food for the town for ${cost:,.2f}!"}


# --- BETTING EXCHANGE ROUTES ---
@app.route('/exchange')
def exchange():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Get active markets from the database
    raw_markets = loop.run_until_complete(db.get_active_markets())
    markets_data = []
    
    # Calculate the exact odds and pool sizes for the web UI
    for m in raw_markets:
        market_id = m['market_id']
        
        async def get_market_details(m_id):
            async with aiosqlite.connect(DB_NAME) as db_conn:
                db_conn.row_factory = aiosqlite.Row
                async with db_conn.execute("SELECT * FROM outcomes WHERE market_id = ?", (m_id,)) as cursor:
                    return await cursor.fetchall()
        
        outcomes = loop.run_until_complete(get_market_details(market_id))
        total_pool = sum(o['pool_balance'] for o in outcomes)
        
        outcomes_data = []
        for o in outcomes:
            # Calculate percentage (odds) for the progress bars
            percentage = (o['pool_balance'] / total_pool * 100) if total_pool > 0 else 50
            outcomes_data.append({
                'label': o['label'],
                'pool': o['pool_balance'],
                'percent': percentage
            })
            
        volume = loop.run_until_complete(db.get_market_volume(market_id))
        
        markets_data.append({
            'market_id': market_id,
            'question': m['question'],
            'close_time': m['close_time'].strftime("%b %d, %Y %H:%M"),
            'volume': volume,
            'outcomes': outcomes_data
        })
        
    return render_template('exchange.html', markets=markets_data)

@app.route('/api/buy_shares', methods=['POST'])
def api_buy_shares():
    if 'user_id' not in session: 
        return {"success": False, "message": "Not logged in"}, 401
    
    data = request.get_json()
    market_id = data.get('market_id')
    outcome_label = data.get('outcome_label')
    
    try:
        investment = float(data.get('investment'))
        if investment < 0.01:
            return {"success": False, "message": "Minimum bet is $0.01"}
    except:
        return {"success": False, "message": "Invalid investment amount."}
        
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run the exact same betting math your Discord bot uses!
    success, msg = loop.run_until_complete(db.buy_shares(session['user_id'], market_id, outcome_label, investment))
    
    return {"success": success, "message": msg}


# --- LEADERBOARD ROUTES ---
@app.route('/leaderboard')
def leaderboard():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def get_leaders():
        async with aiosqlite.connect(DB_NAME) as db_conn:
            db_conn.row_factory = aiosqlite.Row
            
            # 1. Richest (By Balance)
            async with db_conn.execute("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT 10") as c:
                richest = await c.fetchall()
            
            # 2. Strongest (By Max Floor)
            async with db_conn.execute("SELECT user_id, max_floor FROM users ORDER BY max_floor DESC LIMIT 10") as c:
                strongest = await c.fetchall()
                
            # 3. Collectors (By Item Count)
            async with db_conn.execute("""
                SELECT user_id, COUNT(*) as item_count 
                FROM inventory 
                GROUP BY user_id 
                ORDER BY item_count DESC LIMIT 10
            """) as c:
                collectors = await c.fetchall()
                
            return richest, strongest, collectors

    richest, strongest, collectors = loop.run_until_complete(get_leaders())
    
    return render_template('leaderboard.html', 
                           richest=richest, 
                           strongest=strongest, 
                           collectors=collectors)



if __name__ == '__main__':
    app.run(debug=False)