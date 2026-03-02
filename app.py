import os
import random
import re
import requests
import aiosqlite
import asyncio
import datetime
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
    "Common": 18.0, "Uncommon": 45.0, "Rare": 90.0, 
    "Epic": 270.0, "Legendary": 900.0, "Mythic": 3600.0
}

# --- INTERACTABLE NPCs (MMO-RPG flavor) ---
# Each NPC: id, name, emoji, dialogue (list of lines), action (optional {label, url})
NPCS_BY_PAGE = {
    "market": [
        {"id": "merchant", "name": "The Merchant", "emoji": "🧙", "dialogue": ["Adventurers trade here daily. List your wares from the Inventory, or buy what others offer.", "Gold makes the realm go round. Spend wisely."], "action": {"label": "Open Inventory", "url": "/inventory"}},
        {"id": "guard", "name": "Bazaar Guard", "emoji": "🛡️", "dialogue": ["I keep the peace. No pickpockets on my watch.", "If you see something suspicious, report it to the Council."], "action": None},
    ],
    "town": [
        {"id": "mayor", "name": "Council Elder", "emoji": "🏛️", "dialogue": ["Donations to the treasury strengthen the whole realm. We remember our benefactors.", "Food stocks keep the city safe. Consider buying supplies for the guild."], "action": None},
        {"id": "innkeeper", "name": "Innkeeper", "emoji": "🍺", "dialogue": ["Rest here before you head into the dungeons. The tavern games are downstairs if you're feeling lucky.", "Many an adventurer has left gold at the tables. Don't say I didn't warn you."], "action": {"label": "Tavern Games", "url": "/casino"}},
    ],
    "lootbox": [
        {"id": "crate_keeper", "name": "Crate Keeper", "emoji": "📦", "dialogue": ["Crates from across the realm. Crack one open — you might find legendary gear.", "Luck favors the bold. Or the well-funded. Your call."], "action": None},
        {"id": "collector", "name": "The Collector", "emoji": "👁️", "dialogue": ["I've seen Mythics pulled in this very room. Could be you next.", "Different sets, different loot. Choose your crate and take the plunge."], "action": None},
    ],
    "casino": [
        {"id": "dealer", "name": "House Dealer", "emoji": "🎲", "dialogue": ["Welcome to the tables. Slots, roulette, blackjack — pick your poison.", "The house always has a seat. Bet within your means, adventurer."], "action": None},
        {"id": "drunk", "name": "Tipsy Adventurer", "emoji": "🫃", "dialogue": ["*hic* I lost my shirt on Plinko. Still fun though.", "Don't be like me. Set a limit. *stumbles*"], "action": None},
    ],
}

def get_npcs_for_page(page_key):
    return NPCS_BY_PAGE.get(page_key, [])

# --- LOCATION AMBIANCE (RPG "you are here" feel per page) ---
LOCATION_AMBIANCE = {
    "market": ("The Bazaar", "Merchants haggle and adventurers browse the stalls. Your gold purse weighs at your belt."),
    "inventory": ("Your Armory", "Gear and crates from your travels. Equip, list for gold, or crack a crate."),
    "lootbox": ("The Crate Room", "Mysterious crates from across the realm. Fortune favors the bold."),
    "town": ("Guild Hall", "The heart of Polyville. Donate to the treasury, face the world boss, or rest at the inn."),
    "casino": ("Tavern Games", "Dice and cards. The innkeeper nods from the bar. Bet within your means."),
    "exchange": ("The Exchange", "Wagers on the realm's outcomes. Only the shrewd leave richer."),
    "leaderboard": ("Hall of Fame", "The realm's finest — richest adventurers, deepest delvers, top donors."),
    "quests": ("Quest Board", "Daily tasks from the guild. Complete them and claim your reward."),
    "rpg_stats": ("RPG Stats", "Records of every adventurer. The Hall of Legends honors the boldest."),
    "profile": ("Adventurer Profile", "Your renown, gear, and deeds. This is your legend."),
    "guild_wars": ("Guild Wars", "Challenge another guild and fight head-to-head for glory and gold."),
    "wordle": ("Wordle Arena", "Race other adventurers through shared Wordle boards. Speed and accuracy decide the victor."),
}

def get_location_ambiance(active_page):
    """Return location_name and location_flavor for a page (for RPG ambiance)."""
    t = LOCATION_AMBIANCE.get(active_page, ("Polyville", ""))
    return {"location_name": t[0], "location_flavor": t[1]}

# --- ACHIEVEMENTS (id -> {name, desc, emoji}) ---
ACHIEVEMENTS = {
    "first_lootbox": {"name": "First Crate", "desc": "Open your first lootbox", "emoji": "📦"},
    "floor_10": {"name": "Dungeon Diver", "desc": "Reach floor 10", "emoji": "⚔️"},
    "earn_1m": {"name": "Millionaire", "desc": "Earn $1,000,000 total", "emoji": "💰"},
    "donate_10k": {"name": "Guild Patron", "desc": "Donate $10,000 to the guild", "emoji": "🏛️"},
    "sell_one": {"name": "Merchant", "desc": "Sell an item on the Bazaar", "emoji": "🛒"},
}

# --- DAILY QUESTS (id -> {name, desc, reward_gold, check: async (user_id) -> bool or sync) ---
DAILY_QUESTS = [
    {"id": "sell_one", "name": "List an item", "desc": "List 1 item on the Bazaar", "reward": 60.0},
    {"id": "open_one", "name": "Crack a crate", "desc": "Open 1 lootbox", "reward": 35.0},
    {"id": "donate", "name": "Support the guild", "desc": "Donate any amount to the Guild Hall", "reward": 50.0},
    {"id": "dungeon_run", "name": "Brave the dungeon", "desc": "Complete a dungeon run (Discord)", "reward": 90.0},
]

# --- SET BONUSES (set_name -> {2: {atk: x}, 4: {def: x}, 6: {atk: x, def: x}}) ---
SET_BONUSES = {
    "RPG_Set_1": {2: {"atk": 2}, 4: {"def": 3}, 6: {"atk": 5, "def": 5}},
    "Base_Set": {2: {"atk": 1}, 4: {"def": 2}},
}

# --- WORDLE ARENA (multiplayer Wordle lobbies) ---

def _load_wordle_words():
    """
    Load 5-letter Wordle-valid words from docs.
    Falls back to a tiny built-in list if file missing/empty.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(base_dir, "docs", "valid_wordle_words.txt"),
        os.path.join(base_dir, "docs", "valid-wordle-words.txt"),
    ]
    chosen = next((p for p in candidates if os.path.exists(p)), None)

    fallback = [
        "aback", "cigar", "rebut", "sissy", "humph", "awake", "blush", "focal", "evade", "naval",
    ]

    if not chosen:
        print("[Wordle] No word list found in docs/. Using fallback list.")
        return fallback

    try:
        with open(chosen, "r", encoding="utf-8") as f:
            raw = [ln.strip().lower() for ln in f.readlines()]
    except Exception as e:
        print(f"[Wordle] Failed to read word list ({chosen}): {e}. Using fallback list.")
        return fallback

    words = []
    seen = set()
    for w in raw:
        if not w or w.startswith("#"):
            continue
        if len(w) != 5 or (not w.isalpha()):
            continue
        if w in seen:
            continue
        seen.add(w)
        words.append(w)

    if len(words) < 50:
        print(f"[Wordle] Word list too small ({len(words)}). Using fallback list.")
        return fallback

    print(f"[Wordle] Loaded {len(words)} valid 5-letter words from {os.path.basename(chosen)}.")
    return words


WORDLE_WORDS = _load_wordle_words()
WORDLE_WORDS_SET = set(WORDLE_WORDS)

# In-memory active lobbies: code -> lobby dict
ACTIVE_WORDLE_LOBBIES = {}


def _wordle_generate_lobby_code(length: int = 5) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(length))


def _wordle_pick_words(count: int = 5):
    pool = WORDLE_WORDS
    if count >= len(pool):
        # If asking for more than we have, just shuffle and slice
        words = list(pool)
        random.shuffle(words)
        return words[:count]
    return random.sample(pool, count)


def _wordle_eval(secret: str, guess: str):
    """
    Standard Wordle evaluation:
    - 'correct'  = right letter, right spot
    - 'present'  = right letter, wrong spot
    - 'absent'   = not in word (accounting for duplicates)
    """
    secret = secret.lower()
    guess = guess.lower()
    result = ["absent"] * len(secret)
    counts = {}

    # First pass: correct letters
    for i, ch in enumerate(secret):
        if guess[i] == ch:
            result[i] = "correct"
        else:
            counts[ch] = counts.get(ch, 0) + 1

    # Second pass: present letters
    for i, ch in enumerate(guess):
        if result[i] == "correct":
            continue
        if counts.get(ch, 0) > 0:
            result[i] = "present"
            counts[ch] -= 1

    return result


def _wordle_ensure_player(lobby, user_id: int, username: str):
    if user_id in lobby["players"]:
        lobby["players"][user_id]["username"] = username
        return lobby["players"][user_id]

    per_word = [
        {"guesses": [], "solved_at": None, "failed": False}
        for _ in lobby["word_list"]
    ]
    state = {
        "username": username,
        "joined_at": datetime.datetime.utcnow(),
        "current_index": 0,
        "per_word": per_word,
        "finished_at": None,
    }
    lobby["players"][user_id] = state
    return state


def _wordle_build_scoreboard(lobby, now=None):
    if now is None:
        now = datetime.datetime.utcnow()
    rows = []
    started_at = lobby.get("started_at")

    for uid, st in lobby["players"].items():
        solved = sum(1 for w in st["per_word"] if w["solved_at"])
        total_guesses = sum(len(w["guesses"]) for w in st["per_word"])
        if started_at:
            end_time = st.get("finished_at") or now
            total_secs = max(0, int((end_time - started_at).total_seconds()))
        else:
            total_secs = 0
        rows.append(
            {
                "user_id": uid,
                "username": st["username"],
                "solved": solved,
                "total_guesses": total_guesses,
                "total_time_seconds": total_secs,
            }
        )

    # Sort: most solved, then least time, then least guesses
    rows.sort(
        key=lambda r: (-r["solved"], r["total_time_seconds"], r["total_guesses"])
    )
    for i, row in enumerate(rows, start=1):
        row["rank"] = i
    return rows


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

def _inject_nav_rpg(context):
    """Inject nav RPG class and emoji for logged-in user (for base template)."""
    if "session" not in context or not context["session"].get("user_id"):
        return {}
    try:
        from cogs.rpg import CLASSES
        gear_data, class_name = run_async(db.get_rpg_profile(context["session"]["user_id"]))
        cls = CLASSES.get(class_name, CLASSES["Fighter"])
        return {"nav_rpg_class": class_name or "Fighter", "nav_class_emoji": cls.get("emoji", "⚔️")}
    except Exception:
        return {"nav_rpg_class": "Fighter", "nav_class_emoji": "⚔️"}

def _realm_prosperity():
    """Return {label, flavor} for current economy multiplier (for RPG economy feel)."""
    try:
        mult = run_async(db.get_economy_multiplier())
    except Exception:
        mult = 0.5
    if mult < 0.5:
        return {"label": "Struggling", "flavor": "The realm's coffers are thin. Rewards from jobs and dungeons are reduced."}
    if mult < 0.8:
        return {"label": "Stable", "flavor": "The realm runs as usual. Payouts are modest but steady."}
    return {"label": "Prosperous", "flavor": "Times are good. The council has boosted rewards across the realm."}

@app.context_processor
def inject_nav_context():
    out = {}
    if session.get("user_id"):
        out.update(_inject_nav_rpg({"session": session}))
    try:
        out["realm_prosperity"] = _realm_prosperity()
    except Exception:
        out["realm_prosperity"] = {"label": "Stable", "flavor": "The realm runs as usual."}
    return out

def broadcast_update(event_type, data):
    """Shouts an update to all connected users instantly."""
    socketio.emit(event_type, data)

# --- CORE ROUTES ---

@app.route('/')
def index():
    return redirect(url_for('market'))


@app.route('/wordle')
def wordle_page():
    """Competitive Wordle arena landing page."""
    if "user_id" not in session:
        return redirect(url_for("login"))
    code = request.args.get("code", "").strip().upper() or ""
    return render_template(
        "wordle.html",
        active_page="wordle",
        lobby_code=code,
        npcs=[],
        **get_location_ambiance("wordle"),
    )

def _lootbox_image_exists(set_name: str, tier: str, name: str) -> bool:
    """Case-insensitive existence check for lootbox PNGs."""
    rel_dir = os.path.join(set_name, tier)
    dir_path = os.path.join("lootboxes", rel_dir)
    if not os.path.isdir(dir_path):
        return False
    target = f"{name}.png"
    full_exact = os.path.join(dir_path, target)
    if os.path.exists(full_exact):
        return True
    # Normalize: ignore case, spaces, underscores and other punctuation
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", s.lower())
    norm_target = _norm(target)
    for candidate in os.listdir(dir_path):
        if _norm(candidate) == norm_target:
            return True
    return False


@app.route('/images/<path:filename>')
def serve_image(filename):
    # filename looks like "Base_Set/Common/My_Item.png"
    fixed = filename.replace(" ", "_")
    base_dir = "lootboxes"

    rel_dir, fname = os.path.split(fixed)
    search_dir = os.path.join(base_dir, rel_dir)
    chosen_rel = fixed

    if os.path.isdir(search_dir):
        full_exact = os.path.join(search_dir, fname)
        if not os.path.exists(full_exact):
            # Fallback: case- and punctuation-insensitive match
            def _norm(s: str) -> str:
                return re.sub(r"[^a-z0-9]", "", s.lower())
            target_norm = _norm(fname)
            for entry in os.listdir(search_dir):
                if _norm(entry) == target_norm:
                    chosen_rel = os.path.join(rel_dir, entry) if rel_dir else entry
                    break

    return send_from_directory(base_dir, chosen_rel)

@app.route('/api/gold')
def api_gold():
    if 'user_id' not in session: return {"success": False, "error": "Not logged in"}, 401
    player = run_async(get_player_stats(session['user_id']))
    if player: return {"success": True, "gold": player['balance']}
    return {"success": False, "error": "Player not found"}, 404


# --- WORDLE ARENA API ---

@app.route("/api/wordle/create", methods=["POST"])
def api_wordle_create():
    if "user_id" not in session:
        return {"success": False, "error": "Not logged in"}, 401
    user_id = session["user_id"]
    username = session.get("username") or "Player"

    # Generate unique lobby code
    code = _wordle_generate_lobby_code()
    while code in ACTIVE_WORDLE_LOBBIES:
        code = _wordle_generate_lobby_code()

    words = _wordle_pick_words(5)
    now = datetime.datetime.utcnow()
    lobby = {
        "code": code,
        "host_user_id": user_id,
        "host_username": username,
        "status": "waiting",  # waiting | in_progress | finished
        "created_at": now,
        "started_at": None,
        "finished_at": None,
        "word_list": words,
        "players": {},
    }
    ACTIVE_WORDLE_LOBBIES[code] = lobby
    _wordle_ensure_player(lobby, user_id, username)

    return {"success": True, "code": code}


@app.route("/api/wordle/join", methods=["POST"])
def api_wordle_join():
    if "user_id" not in session:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    code = str(data.get("code", "")).strip().upper()
    if not code:
        return {"success": False, "error": "Missing lobby code."}, 400

    lobby = ACTIVE_WORDLE_LOBBIES.get(code)
    if not lobby:
        return {"success": False, "error": "Lobby not found."}, 404
    if lobby.get("status") != "waiting":
        return {"success": False, "error": "Game already started."}, 409

    user_id = session["user_id"]
    username = session.get("username") or "Player"
    state = _wordle_ensure_player(lobby, user_id, username)

    scoreboard = _wordle_build_scoreboard(lobby)
    return {
        "success": True,
        "lobby": {
            "code": lobby["code"],
            "status": lobby["status"],
            "host_user_id": lobby["host_user_id"],
            "word_count": len(lobby["word_list"]),
            "player_count": len(lobby["players"]),
        },
        "you": {
            "user_id": user_id,
            "username": state["username"],
            "is_host": lobby["host_user_id"] == user_id,
        },
        "scoreboard": scoreboard,
    }


@app.route("/api/wordle/start", methods=["POST"])
def api_wordle_start():
    if "user_id" not in session:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    code = str(data.get("code", "")).strip().upper()
    lobby = ACTIVE_WORDLE_LOBBIES.get(code)
    if not lobby:
        return {"success": False, "error": "Lobby not found."}, 404

    user_id = session["user_id"]
    if lobby["host_user_id"] != user_id:
        return {"success": False, "error": "Only the lobby host can start the game."}, 403
    if lobby["status"] != "waiting":
        return {"success": False, "error": "Game already started."}, 409
    if not lobby["players"]:
        return {"success": False, "error": "No players in lobby."}, 400

    lobby["status"] = "in_progress"
    lobby["started_at"] = datetime.datetime.utcnow()
    return {"success": True}


@app.route("/api/wordle/state")
def api_wordle_state():
    if "user_id" not in session:
        return {"success": False, "error": "Not logged in"}, 401
    code = str(request.args.get("code", "")).strip().upper()
    lobby = ACTIVE_WORDLE_LOBBIES.get(code)
    if not lobby:
        return {"success": False, "error": "Lobby not found."}, 404

    user_id = session["user_id"]
    if user_id not in lobby["players"]:
        return {"success": False, "error": "You are not part of this lobby."}, 403

    state = lobby["players"][user_id]
    now = datetime.datetime.utcnow()
    scoreboard = _wordle_build_scoreboard(lobby, now=now)

    words_payload = []
    for idx, word_state in enumerate(state["per_word"]):
        secret = lobby["word_list"][idx]
        guesses_payload = []
        for g in word_state["guesses"]:
            guesses_payload.append(
                {"guess": g, "pattern": _wordle_eval(secret, g)}
            )
        words_payload.append(
            {
                "guesses": guesses_payload,
                "solved": bool(word_state["solved_at"]),
                "failed": bool(word_state["failed"]),
            }
        )

    you = {
        "user_id": user_id,
        "username": state["username"],
        "is_host": lobby["host_user_id"] == user_id,
        "current_index": state["current_index"],
        "finished": bool(state.get("finished_at")),
        "word_count": len(lobby["word_list"]),
        "words": words_payload,
    }

    lobby_info = {
        "code": lobby["code"],
        "status": lobby["status"],
        "host_user_id": lobby["host_user_id"],
        "word_count": len(lobby["word_list"]),
        "player_count": len(lobby["players"]),
    }

    return {"success": True, "lobby": lobby_info, "you": you, "scoreboard": scoreboard}


@app.route("/api/wordle/guess", methods=["POST"])
def api_wordle_guess():
    if "user_id" not in session:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    code = str(data.get("code", "")).strip().upper()
    raw_guess = str(data.get("guess", "")).strip().lower()

    if len(raw_guess) != 5 or not raw_guess.isalpha():
        return {"success": False, "error": "Guess must be a 5-letter word."}, 400
    if raw_guess not in WORDLE_WORDS_SET:
        return {"success": False, "error": "That word is not in the allowed Wordle dictionary."}, 400

    lobby = ACTIVE_WORDLE_LOBBIES.get(code)
    if not lobby:
        return {"success": False, "error": "Lobby not found."}, 404
    if lobby["status"] != "in_progress":
        return {"success": False, "error": "Game not in progress."}, 409

    user_id = session["user_id"]
    if user_id not in lobby["players"]:
        return {"success": False, "error": "You are not part of this lobby."}, 403

    state = lobby["players"][user_id]
    if state.get("finished_at"):
        return {"success": False, "error": "You have already finished this game."}, 409

    idx = state["current_index"]
    if idx >= len(lobby["word_list"]):
        state["finished_at"] = state.get("finished_at") or datetime.datetime.utcnow()
        return {"success": False, "error": "No more words remaining."}, 409

    word_state = state["per_word"][idx]
    # If somehow already solved/failed this index, advance once
    if word_state["solved_at"] or word_state["failed"]:
        state["current_index"] += 1
        return {"success": False, "error": "Advancing to next word, try again."}, 409

    # Apply guess
    secret = lobby["word_list"][idx]
    pattern = _wordle_eval(secret, raw_guess)
    word_state["guesses"].append(raw_guess)

    solved_this_word = False
    failed_this_word = False

    if raw_guess == secret:
        solved_this_word = True
        word_state["solved_at"] = datetime.datetime.utcnow()
        state["current_index"] += 1
    elif len(word_state["guesses"]) >= 6:
        failed_this_word = True
        word_state["failed"] = True
        state["current_index"] += 1

    # Check if player has finished all words
    if state["current_index"] >= len(lobby["word_list"]) and not state.get("finished_at"):
        state["finished_at"] = datetime.datetime.utcnow()

    # If every player finished, mark lobby as finished
    if lobby["status"] == "in_progress":
        all_done = all(
            (p.get("finished_at") is not None)
            or (p["current_index"] >= len(lobby["word_list"]))
            for p in lobby["players"].values()
        )
        if all_done:
            lobby["status"] = "finished"
            lobby["finished_at"] = datetime.datetime.utcnow()

    now = datetime.datetime.utcnow()
    scoreboard = _wordle_build_scoreboard(lobby, now=now)

    return {
        "success": True,
        "pattern": pattern,
        "word_index": idx,
        "solved_this_word": solved_this_word,
        "failed_this_word": failed_this_word,
        "current_index": state["current_index"],
        "lobby_status": lobby["status"],
        "scoreboard": scoreboard,
    }

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
    
    user_info = requests.get(
        'https://discord.com/api/users/@me',
        headers={'Authorization': f"Bearer {token_json['access_token']}"}
    ).json()
    
    user_id = int(user_info['id'])
    username = user_info['username']
    avatar_hash = user_info.get('avatar')
    
    # Sync the username and avatar to the DB immediately
    run_async(db.sync_user_data(user_id, username, avatar_hash))
    
    session['user_id'] = user_id
    session['username'] = username
    return redirect(url_for('market'))
    
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('market'))

@app.route('/profile/<identifier>')
def profile(identifier):
    async def get_user_smart(val):
        async with aiosqlite.connect(DB_NAME) as db_conn:
            db_conn.row_factory = aiosqlite.Row
            
            # 1. Try ID search if it's a number (Discord IDs are long strings of digits)
            if val.isdigit():
                async with db_conn.execute("SELECT * FROM users WHERE user_id = ?", (int(val),)) as c:
                    user = await c.fetchone()
                    if user: 
                        return user
            
            # 2. Try Username search (for the new text-based links)
            async with db_conn.execute("SELECT * FROM users WHERE username = ?", (val,)) as c:
                return await c.fetchone()

    # Run the smart search
    player = run_async(get_user_smart(identifier))
    if not player:
        return f"<h1>404: Resident '{identifier}' not found in Polyville</h1>", 404
    player = dict(player)
    
    # Fetch equipped RPG gear for this player
    equipped = run_async(db.get_equipped_gear(player['user_id']))
    equipped_list = [dict(row) for row in equipped] if equipped else []

    # Derive RPG stats similar to dungeon runs
    from cogs.rpg import CLASSES, SHOP_GEAR  # local import to avoid heavy globals at startup
    gear_data, class_name = run_async(db.get_rpg_profile(player['user_id']))
    gear_names = [g.strip() for g in (gear_data.split(",") if gear_data else ["Rusty Dagger"]) if g.strip()]
    base_cls = CLASSES.get(class_name, CLASSES["Fighter"])

    total_atk_bonus = 0
    total_def_bonus = 0
    total_int_bonus = 0

    for g in gear_names:
        g_item = SHOP_GEAR.get(g, SHOP_GEAR["Rusty Dagger"])
        total_atk_bonus += int(g_item.get("atk", 0) or 0)
        total_def_bonus += int(g_item.get("def", 0) or 0)
        total_int_bonus += int(g_item.get("int", 0) or 0)

    for it in equipped_list:
        total_atk_bonus += int(it.get("atk_bonus") or 0)
        total_def_bonus += int(it.get("def_bonus") or 0)
        total_int_bonus += int(it.get("int_bonus") or 0)

    rpg_stats = {
        "hp": base_cls["hp"],
        "atk": total_atk_bonus + base_cls["atk_mod"],
        "defense": 5 + total_def_bonus + base_cls["def_mod"],
        "intelligence": total_int_bonus + base_cls["spell_mod"],
    }

    # Adventurer rank (MMO progression feel)
    max_floor = int(player.get("max_floor") or 0)
    if max_floor >= 40:
        adventurer_rank = "Legend"
    elif max_floor >= 25:
        adventurer_rank = "Champion"
    elif max_floor >= 10:
        adventurer_rank = "Veteran"
    elif max_floor > 0 or (class_name and class_name not in (None, "", "Unassigned")):
        adventurer_rank = "Apprentice"
    else:
        adventurer_rank = "Novice"

    # Unlock achievements by progress (profile load)
    uid = player["user_id"]
    if max_floor >= 10:
        run_async(db.unlock_achievement(uid, "floor_10"))
    if float(player.get("balance") or 0) >= 1_000_000:
        run_async(db.unlock_achievement(uid, "earn_1m"))
    
    # Set bonuses from equipped gear (by set_name count)
    from collections import Counter
    set_counts = Counter(it.get("set_name") or "Base_Set" for it in equipped_list)
    set_bonus_lines = []
    for set_name, bonuses in SET_BONUSES.items():
        n = set_counts.get(set_name, 0)
        for pieces, stats in sorted(bonuses.items(), reverse=True):
            if n >= pieces:
                parts = [f"+{v} {k.upper()}" for k, v in stats.items()]
                set_bonus_lines.append(f"{pieces}-piece {set_name}: {', '.join(parts)}")
                break
    
    user_achievements = run_async(db.get_user_achievements(uid))
    achievements_unlocked = [ACHIEVEMENTS.get(a["achievement_id"], {"name": a["achievement_id"], "desc": "", "emoji": "🏅"}) for a in user_achievements]
    
    avatar_hash = player.get("avatar_hash")
    avatar_url = f"https://cdn.discordapp.com/avatars/{player['user_id']}/{avatar_hash}.png?size=256" if avatar_hash else None
    
    class_emoji = base_cls.get("emoji", "⚔️")
    return render_template(
        'profile.html',
        player=player,
        equipped_gear=equipped_list,
        rpg_stats=rpg_stats,
        avatar_url=avatar_url,
        class_emoji=class_emoji,
        adventurer_rank=adventurer_rank,
        achievements_unlocked=achievements_unlocked,
        set_bonus_lines=set_bonus_lines,
        active_page="profile",
        **get_location_ambiance("profile"),
    )

# --- MARKET ROUTES ---

@app.route('/market')
def market():
    login_streak_reward = None
    login_streak = 0
    if session.get("user_id"):
        login_streak, reward = run_async(db.record_login_streak(session["user_id"]))
        if reward > 0:
            login_streak_reward = reward
    # 1. Check if the user is logged in
    if 'user_id' not in session:
        # We pass a message so the user knows why they were redirected
        return render_template(
            'market.html',
            items=[],
            message="Please login with Discord to view the Marketplace.",
            success=False,
            active_page="market",
            npcs=get_npcs_for_page("market"),
            login_streak=0,
            login_streak_reward=None,
            **get_location_ambiance("market"),
        )

    # 2. If they are logged in, proceed as normal
    items = run_async(get_market_listings())
    items = [dict(row) for row in items]

    # Attach severed head avatar URLs and local has_image flag
    for it in items:
        # Per-head custom art (Discord avatar of the victim)
        if (
            it.get("set_name") == "Trophy"
            and "Severed Head" in str(it.get("item_name") or "")
            and it.get("head_owner_id")
        ):
            avatar_hash = run_async(db.get_user_avatar_hash(it["head_owner_id"]))
            if avatar_hash:
                it["head_avatar_url"] = f"https://cdn.discordapp.com/avatars/{it['head_owner_id']}/{avatar_hash}.png?size=256"

        # Check if a lootbox PNG actually exists for this item (case-insensitive)
        set_name = str(it.get("set_name") or "Base_Set")
        tier = str(it.get("tier") or "Common")
        name = str(it.get("item_name") or "")
        it["has_image"] = _lootbox_image_exists(set_name, tier, name)

    return render_template('market.html', items=items, active_page="market", npcs=get_npcs_for_page("market"), **get_location_ambiance("market"))

@app.route('/buy/<int:item_id>', methods=['POST'])
def buy_item(item_id):
    if 'user_id' not in session: return redirect(url_for('login'))
    success, message = run_async(db.buy_market_item(session['user_id'], item_id))
    
    if success:
        # Notify browsers to remove the item from their lists without refreshing
        broadcast_update('item_sold', {'item_id': item_id, 'buyer': session['username']})
        
    return render_template('market.html', items=run_async(get_market_listings()), message=message, success=success, active_page="market", npcs=get_npcs_for_page("market"), login_streak=0, login_streak_reward=None)

# --- INVENTORY ROUTES ---

@app.route('/inventory')
def inventory():
    if 'user_id' not in session: return redirect(url_for('login'))
    raw_items = run_async(db.get_inventory(session['user_id']))
    items = [dict(row) for row in raw_items]

    # Attach severed head avatar URLs and local has_image flag
    for it in items:
        if (
            it.get("set_name") == "Trophy"
            and "Severed Head" in str(it.get("item_name") or "")
            and it.get("head_owner_id")
        ):
            avatar_hash = run_async(db.get_user_avatar_hash(it["head_owner_id"]))
            if avatar_hash:
                it["head_avatar_url"] = f"https://cdn.discordapp.com/avatars/{it['head_owner_id']}/{avatar_hash}.png?size=256"

        set_name = str(it.get("set_name") or "Base_Set")
        tier = str(it.get("tier") or "Common")
        name = str(it.get("item_name") or "")
        it["has_image"] = _lootbox_image_exists(set_name, tier, name)

    return render_template('inventory.html', items=items, active_page="inventory", **get_location_ambiance("inventory"))

@app.route('/api/sell_item', methods=['POST'])
def api_sell_item():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json()
    try: price = float(data.get('price'))
    except: return {"success": False, "message": "Invalid price."}
    
    success, msg = run_async(db.list_item_on_market(session['user_id'], data.get('item_id'), price))
    
    if success:
        broadcast_update('new_listing', {'item_id': data.get('item_id'), 'price': price})
        run_async(db.unlock_achievement(session["user_id"], "sell_one"))
        run_async(db.set_quest_completed(session["user_id"], "sell_one", datetime.date.today().isoformat()))
    return {"success": success, "message": msg}


@app.route('/api/equip_item', methods=['POST'])
def api_equip_item():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    try:
        item_id = int(data.get('item_id'))
    except (TypeError, ValueError):
        return {"success": False, "message": "Invalid item id."}, 400

    success, msg = run_async(db.equip_inventory_item(session['user_id'], item_id))
    return {"success": success, "message": msg}


@app.route('/api/unequip_slot', methods=['POST'])
def api_unequip_slot():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    slot = (data.get('slot') or '').strip().lower()
    if slot not in ('weapon', 'armor', 'mage'):
        return {"success": False, "message": "Invalid slot."}, 400
    success, msg = run_async(db.unequip_slot(session['user_id'], slot))
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


@app.route('/api/scrap_duplicates', methods=['POST'])
def api_scrap_duplicates():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    try:
        count, total_net, msg = run_async(db.scrap_duplicates(session['user_id'], SCRAP_VALUES))
        return {"success": True, "count": count, "total_net": total_net, "message": msg}
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}, 500

# --- LOOTBOX ROUTES ---

@app.route('/lootbox')
def lootbox_page():
    if 'user_id' not in session: return redirect(url_for('login'))
    available_sets = [d for d in os.listdir("lootboxes") if os.path.isdir(os.path.join("lootboxes", d))] if os.path.exists("lootboxes") else []
    return render_template('lootbox.html', available_sets=available_sets, active_page="lootbox", npcs=get_npcs_for_page("lootbox"), **get_location_ambiance("lootbox"))

def _open_one_box(user_id, set_name, valid_tiers):
    """Roll one item and add to user inventory. Returns (item_dict, None) or (None, error_msg)."""
    rolled_tier = random.choices(valid_tiers, weights=[TIER_WEIGHTS[t] for t in valid_tiers], k=1)[0]
    result = get_random_item(set_name, rolled_tier)
    if not result:
        return None, "Item generation failed."
    item_name, image_url = result
    item_type = "Collectible"
    slot = None
    atk_bonus = def_bonus = int_bonus = 0
    meta_path = os.path.join("lootboxes", set_name, "meta.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                raw_meta = json.load(f)
            if isinstance(raw_meta, dict):
                meta = {str(k).lower(): v for k, v in raw_meta.items()}
                item_meta = meta.get(item_name.lower())
                if isinstance(item_meta, dict):
                    item_type = item_meta.get("item_type", item_type)
                    slot = item_meta.get("slot")
                    atk_bonus = int(item_meta.get("atk_bonus", 0) or 0)
                    def_bonus = int(item_meta.get("def_bonus", 0) or 0)
                    int_bonus = int(item_meta.get("int_bonus", 0) or 0)
        except Exception:
            pass
    run_async(
        db.add_item(
            user_id,
            item_name,
            rolled_tier,
            set_name,
            item_type=item_type,
            slot=slot,
            atk_bonus=atk_bonus,
            def_bonus=def_bonus,
            int_bonus=int_bonus,
        )
    )
    return {"name": item_name, "tier": rolled_tier, "image": image_url}, None


@app.route('/api/open_box', methods=['POST'])
def api_open_box():
    if 'user_id' not in session: return {"success": False, "message": "Not logged in"}, 401
    user_id = session['user_id']
    data = request.get_json() or {}
    set_name = data.get('set_name', 'Base_Set')
    count = max(1, min(10, int(data.get('count', 1))))
    cost_per = 10000.0
    total_cost = cost_per * count

    valid_tiers = [tier for tier in TIER_WEIGHTS.keys() if os.path.exists(f"lootboxes/{set_name}/{tier}") and any(f.endswith('.png') for f in os.listdir(f"lootboxes/{set_name}/{tier}"))]
    if not valid_tiers: return {"success": False, "message": f"Set '{set_name}' is empty."}

    bal = run_async(db.get_balance(user_id))
    if bal < total_cost: return {"success": False, "message": f"Not enough gold! Need ${total_cost:,.2f} for {count} box(es)."}

    run_async(db.update_balance(user_id, -total_cost))
    items = []
    for _ in range(count):
        item_dict, err = _open_one_box(user_id, set_name, valid_tiers)
        if err:
            run_async(db.update_balance(user_id, total_cost))
            for _ in items:
                pass  # already added; could try to remove if DB supported it
            return {"success": False, "message": err}
        items.append(item_dict)
        if item_dict["tier"] in ["Epic", "Legendary", "Mythic"]:
            broadcast_update('big_pull', {'username': session['username'], 'item': item_dict['name'], 'tier': item_dict['tier']})
        run_async(db.unlock_achievement(user_id, "first_lootbox"))
        run_async(db.set_quest_completed(user_id, "open_one", datetime.date.today().isoformat()))

    return {
        "success": True,
        "message": f"Unboxed {len(items)} item(s)!" if count > 1 else f"Unboxed a {items[0]['tier']} {items[0]['name']}!",
        "items": items,
        "item": items[0],
    }

# --- TOWN HALL ROUTES ---

def _guild_progress(town):
    """Return (progress_0_to_100, next_level_gold) for guild level bar. Purely cosmetic next-level goal."""
    level = int(town.get("level") or 1)
    treasury = float(town.get("treasury") or 0)
    next_gold = (level ** 2) * 15000  # e.g. L2=60k, L3=135k
    progress = min(100.0, 100.0 * treasury / next_gold) if next_gold else 0
    return round(progress, 1), next_gold

def _get_rumors(town, world_boss):
    """Return list of short 'notice board' / rumor strings for RPG world feel."""
    out = []
    if town.get("famine") == 1:
        out.append("⚠️ Rumor: The guild's stores are low. Donations of food are desperately needed.")
    try:
        mult = run_async(db.get_economy_multiplier())
        if mult >= 0.8:
            out.append("📜 Notice: The council has declared a season of prosperity. Rewards are boosted.")
        elif mult < 0.5:
            out.append("📜 Notice: Times are lean. Job and dungeon payouts are reduced until the realm recovers.")
    except Exception:
        pass
    tax = (town.get("tax_rate") or 0.10) * 100
    if tax > 12:
        out.append("📜 Notice: The council has raised taxes to fund the guild. Current rate: {:.0f}%.".format(tax))
    boss_hp = (world_boss or {}).get("current_hp") or 0
    boss_max = (world_boss or {}).get("max_hp") or 10000
    if boss_max and boss_hp / boss_max < 0.2:
        out.append("🐲 Rumor: The world boss is wounded! Adventurers are rallying for the kill.")
    if not out:
        out.append("📜 Notice: All quiet in Polyville. Check the quest board for daily tasks.")
    return out

@app.route('/town')
def town_hall():
    guild_info = None
    if session.get("user_id"):
        guild_info = run_async(db.get_user_guild_info(session["user_id"]))
    if guild_info:
        town = run_async(db.get_guild_state(guild_info["guild_id"]))
        if not town:
            guild_info = None
            town = run_async(db.get_town_state())
            if not town:
                town = {'level': 1, 'treasury': 0.0, 'food': 0, 'tax_rate': 0.05, 'famine': 0, 'user_count': 1}
            town = dict(town)
            world_boss = run_async(db.get_world_boss()) or {"current_hp": 10000.0, "max_hp": 10000.0}
        else:
            town = dict(town)
            world_boss = {"current_hp": town.get("world_boss_hp", 10000), "max_hp": town.get("world_boss_max_hp", 10000)}
    else:
        town = run_async(db.get_town_state())
        if not town:
            town = {'level': 1, 'treasury': 0.0, 'food': 0, 'tax_rate': 0.05, 'famine': 0, 'user_count': 1}
        town = dict(town)
        world_boss = run_async(db.get_world_boss()) or {"current_hp": 10000.0, "max_hp": 10000.0}
    town["guild_progress_pct"], town["guild_next_level_gold"] = _guild_progress(town)
    rumors = _get_rumors(town, world_boss)
    joinable_guilds = []
    if not guild_info:
        joinable_guilds = run_async(db.get_top_guilds(30))
        joinable_guilds = [dict(g) for g in joinable_guilds]
    return render_template('town.html', town=town, world_boss=world_boss, guild_info=guild_info, joinable_guilds=joinable_guilds, active_page="town", npcs=get_npcs_for_page("town"), rumors=rumors, **get_location_ambiance("town"))

# Flavor messages for "Rest at the Inn" (RPG world interaction)
REST_AT_INN_MESSAGES = [
    "You rest by the fire. The innkeeper nods. You feel ready for the road.",
    "A warm meal and a soft bed. The bustle of the tavern fades. You're rested.",
    "You raise a tankard with other adventurers. Tales of the dungeon echo in the hall.",
    "The inn's cat curls on your lap. For a moment, the realm can wait.",
    "You rest your feet. Tomorrow the Bazaar and the dungeons will still be there.",
]

@app.route("/api/rest_at_inn", methods=["POST"])
def api_rest_at_inn():
    """Return a random flavor message (RPG world interaction). No cost or cooldown."""
    if "user_id" not in session:
        return {"success": False, "message": "Not logged in."}, 401
    msg = random.choice(REST_AT_INN_MESSAGES)
    return {"success": True, "message": msg}

@app.route("/api/world_boss/damage", methods=["POST"])
def api_world_boss_damage():
    if "user_id" not in session:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    try:
        gold = float(data.get("gold", 0))
    except Exception:
        return {"success": False, "error": "Invalid amount."}, 400
    if gold < 100:
        return {"success": False, "error": "Minimum $100 per attack (10 damage)."}
    damage = gold / 10.0
    user_id = session["user_id"]
    if not run_async(db.update_balance(user_id, -gold)):
        return {"success": False, "error": "Insufficient funds."}
    guild_info = run_async(db.get_user_guild_info(user_id))
    if guild_info:
        new_hp = run_async(db.deal_guild_world_boss_damage(guild_info["guild_id"], damage))
    else:
        new_hp = run_async(db.deal_world_boss_damage(damage))
    return {"success": True, "new_hp": new_hp, "damage": damage}

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
        guild_info = run_async(db.get_user_guild_info(user_id))
        if guild_info:
            run_async(db.add_guild_treasury(guild_info["guild_id"], amount))
            run_async(db.record_donation(user_id, amount))
            broadcast_update('town_update', {'message': f"{session['username']} donated ${amount:,.2f} to {guild_info['guild_name']}!"})
        else:
            async def donate_gold_db(amt):
                async with aiosqlite.connect(DB_NAME) as db_conn:
                    await db_conn.execute("UPDATE town SET treasury = COALESCE(treasury, 0.0) + ? WHERE id=1", (amt,))
                    await db_conn.commit()
            run_async(donate_gold_db(amount))
            run_async(db.record_donation(user_id, amount))
            broadcast_update('town_update', {'message': f"{session['username']} donated ${amount:,.2f}!"})
        total_d = run_async(db.get_user_total_donated(user_id))
        if total_d and total_d >= 10000:
            run_async(db.unlock_achievement(user_id, "donate_10k"))
        run_async(db.set_quest_completed(user_id, "donate", datetime.date.today().isoformat()))
        dest = guild_info["guild_name"] if guild_info else "the Town Treasury"
        return {"success": True, "message": f"Donated ${amount:,.2f} to {dest}!"}
        
    elif data.get('type') == 'food':
        cost = amount * 10
        if bal < cost: return {"success": False, "message": f"Not enough gold! {int(amount)} Food costs ${cost:,.2f}."}
        run_async(db.update_balance(user_id, -cost))
        guild_info = run_async(db.get_user_guild_info(user_id))
        if guild_info:
            run_async(db.add_guild_food(guild_info["guild_id"], int(amount)))
            broadcast_update('town_update', {'message': f"{session['username']} bought {int(amount)} Food for {guild_info['guild_name']}!"})
        else:
            run_async(db.add_town_resources(food=int(amount)))
            broadcast_update('town_update', {'message': f"{session['username']} bought {int(amount)} Food!"})
        dest = guild_info["guild_name"] if guild_info else "the town"
        return {"success": True, "message": f"Bought {int(amount)} Food for {dest} for ${cost:,.2f}!"}


@app.route('/api/guild/create', methods=['POST'])
def api_guild_create():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    guild_id, err = run_async(db.create_guild(session['user_id'], name))
    if err:
        return {"success": False, "message": err}
    return {"success": True, "message": "Guild created!", "guild_id": guild_id}


@app.route('/api/guild/join', methods=['POST'])
def api_guild_join():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    guild_id = data.get('guild_id')
    if guild_id is None:
        return {"success": False, "message": "Missing guild_id"}, 400
    try:
        guild_id = int(guild_id)
    except (TypeError, ValueError):
        return {"success": False, "message": "Invalid guild_id"}, 400
    ok, msg = run_async(db.join_guild(session['user_id'], guild_id))
    return {"success": ok, "message": msg}


@app.route('/api/guild/leave', methods=['POST'])
def api_guild_leave():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    ok, msg = run_async(db.leave_guild(session['user_id']))
    return {"success": ok, "message": msg}


@app.route('/api/guild/disband', methods=['POST'])
def api_guild_disband():
    if 'user_id' not in session:
        return {"success": False, "message": "Not logged in"}, 401
    ok, msg = run_async(db.disband_guild(session['user_id']))
    return {"success": ok, "message": msg}


# --- GUILD WARS (PvP combat) ---
def get_player_combat_stats(user_id):
    """Return hp, max_hp, atk, def for a user (class + shop gear + equipped gear). Used for PvP."""
    from cogs.rpg import CLASSES, SHOP_GEAR
    gear_data, class_name = run_async(db.get_rpg_profile(user_id))
    gear_names = [g.strip() for g in (gear_data.split(",") if gear_data else ["Rusty Dagger"]) if g.strip()]
    equipped = run_async(db.get_equipped_gear(user_id))
    equipped_list = [dict(row) for row in equipped] if equipped else []
    cls = CLASSES.get(class_name, CLASSES["Fighter"])
    total_atk = total_def = total_int = 0
    for g in gear_names:
        item = SHOP_GEAR.get(g, SHOP_GEAR["Rusty Dagger"])
        total_atk += int(item.get("atk", 0) or 0)
        total_def += int(item.get("def", 0) or 0)
        total_int += int(item.get("int", 0) or 0)
    for it in equipped_list:
        total_atk += int(it.get("atk_bonus") or 0)
        total_def += int(it.get("def_bonus") or 0)
        total_int += int(it.get("int_bonus") or 0)
    hp = cls["hp"]
    atk = total_atk + cls["atk_mod"]
    defense = 5 + total_def + cls["def_mod"]
    return {"hp": hp, "max_hp": hp, "atk": max(1, atk), "def": max(0, defense)}

def run_pvp_combat(stats_a, stats_b):
    """Simulate turn-based PvP. A and B take turns attacking. Returns 1 if A wins, 2 if B wins, 0 draw (shouldn't happen)."""
    import random
    a_hp, b_hp = float(stats_a["hp"]), float(stats_b["hp"])
    a_atk, a_def = stats_a["atk"], stats_a["def"]
    b_atk, b_def = stats_b["atk"], stats_b["def"]
    while a_hp > 0 and b_hp > 0:
        dmg_a_to_b = max(1, a_atk - b_def + random.randint(-2, 2))
        b_hp -= dmg_a_to_b
        if b_hp <= 0:
            return 1
        dmg_b_to_a = max(1, b_atk - a_def + random.randint(-2, 2))
        a_hp -= dmg_b_to_a
        if a_hp <= 0:
            return 2
    return 1 if a_hp > 0 else 2


@app.route('/guild_wars')
def guild_wars_page():
    if "user_id" not in session:
        return redirect(url_for("login"))
    guild_info = run_async(db.get_user_guild_info(session["user_id"]))
    war = run_async(db.get_active_war_for_guild(guild_info["guild_id"])) if guild_info else None
    enemy_members = []
    if guild_info and war and war.get("status") == "active":
        other_guild_id = run_async(db.get_other_guild_in_war(war, guild_info["guild_id"]))
        enemy_members = run_async(db.get_guild_members(other_guild_id)) if other_guild_id else []
    all_guilds = run_async(db.get_top_guilds(50))
    all_guilds = [dict(g) for g in all_guilds]
    return render_template(
        "guild_wars.html",
        guild_info=guild_info,
        war=war,
        enemy_members=enemy_members,
        all_guilds=all_guilds,
        wins_to_victory=db.GUILD_WAR_WINS_TO_VICTORY,
        win_bonus=db.GUILD_WAR_WIN_BONUS,
        active_page="guild_wars",
        **get_location_ambiance("guild_wars"),
    )


@app.route("/api/guild_war/challenge", methods=["POST"])
def api_guild_war_challenge():
    if "user_id" not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    defender_guild_id = data.get("defender_guild_id")
    if not defender_guild_id:
        return {"success": False, "message": "Missing defender_guild_id"}, 400
    try:
        defender_guild_id = int(defender_guild_id)
    except (TypeError, ValueError):
        return {"success": False, "message": "Invalid defender_guild_id"}, 400
    guild_info = run_async(db.get_user_guild_info(session["user_id"]))
    if not guild_info:
        return {"success": False, "message": "You must be in a guild to declare war."}
    war_id, err = run_async(db.create_guild_war(guild_info["guild_id"], defender_guild_id, session["user_id"]))
    if err:
        return {"success": False, "message": err}
    return {"success": True, "message": "War declared! Waiting for the other guild's leader to accept.", "war_id": war_id}


@app.route("/api/guild_war/accept", methods=["POST"])
def api_guild_war_accept():
    if "user_id" not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    war_id = data.get("war_id")
    if not war_id:
        return {"success": False, "message": "Missing war_id"}, 400
    try:
        war_id = int(war_id)
    except (TypeError, ValueError):
        return {"success": False, "message": "Invalid war_id"}, 400
    ok, msg = run_async(db.accept_guild_war(war_id, session["user_id"]))
    return {"success": ok, "message": msg}


@app.route("/api/guild_war/fight", methods=["POST"])
def api_guild_war_fight():
    if "user_id" not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    war_id = data.get("war_id")
    defender_user_id = data.get("defender_user_id")
    if not war_id or defender_user_id is None:
        return {"success": False, "message": "Missing war_id or defender_user_id"}, 400
    try:
        war_id = int(war_id)
        defender_user_id = int(defender_user_id)
    except (TypeError, ValueError):
        return {"success": False, "message": "Invalid ids"}, 400
    attacker_id = session["user_id"]
    if attacker_id == defender_user_id:
        return {"success": False, "message": "You cannot fight yourself."}
    guild_info = run_async(db.get_user_guild_info(attacker_id))
    if not guild_info:
        return {"success": False, "message": "You must be in a guild to fight in a guild war."}
    war = run_async(db.get_war_by_id(war_id))
    if not war or war.get("status") != "active":
        return {"success": False, "message": "This war is not active."}
    defender_guild = run_async(db.get_user_guild_info(defender_user_id))
    if not defender_guild or defender_guild["guild_id"] not in (war["challenger_guild_id"], war["defender_guild_id"]):
        return {"success": False, "message": "That player is not in the enemy guild."}
    if defender_guild["guild_id"] == guild_info["guild_id"]:
        return {"success": False, "message": "You cannot fight a guildmate."}
    stats_attacker = get_player_combat_stats(attacker_id)
    stats_defender = get_player_combat_stats(defender_user_id)
    winner = run_pvp_combat(stats_attacker, stats_defender)
    winner_user_id = attacker_id if winner == 1 else defender_user_id
    result, err = run_async(db.record_guild_war_battle(war_id, attacker_id, defender_user_id, winner_user_id))
    if err:
        return {"success": False, "message": err}
    challenger_wins, defender_wins = result
    you_won = winner_user_id == attacker_id
    return {
        "success": True,
        "you_won": you_won,
        "challenger_wins": challenger_wins,
        "defender_wins": defender_wins,
        "message": "You won the battle!" if you_won else "You lost the battle.",
    }


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
        
    return render_template('exchange.html', markets=markets_data, active_page="exchange", **get_location_ambiance("exchange"))


@app.route('/rpg_stats')
def rpg_stats_page():
    async def get_rpg_stats():
        async with aiosqlite.connect(DB_NAME) as db_conn:
            db_conn.row_factory = aiosqlite.Row

            async with db_conn.execute(
                "SELECT COUNT(*) AS player_count, AVG(max_floor) AS avg_floor, MAX(max_floor) AS max_floor FROM users"
            ) as c:
                overall = await c.fetchone()

            async with db_conn.execute(
                """
                SELECT COALESCE(rpg_class, 'Unassigned') AS rpg_class,
                       COUNT(*) AS count,
                       AVG(max_floor) AS avg_floor
                FROM users
                GROUP BY COALESCE(rpg_class, 'Unassigned')
                ORDER BY count DESC
                """
            ) as c:
                classes = await c.fetchall()

            async with db_conn.execute(
                """
                SELECT user_id, username, max_floor, COALESCE(rpg_class, 'Fighter') AS rpg_class
                FROM users
                WHERE username IS NOT NULL AND max_floor > 0
                ORDER BY max_floor DESC
                LIMIT 3
                """
            ) as c:
                hall_of_legends = await c.fetchall()

        return overall, classes, hall_of_legends

    overall, classes, hall_of_legends = run_async(get_rpg_stats())
    from cogs.rpg import CLASSES
    legends_with_emoji = []
    for row in hall_of_legends:
        r = dict(row)
        cls = CLASSES.get(r.get("rpg_class"), CLASSES["Fighter"])
        r["class_emoji"] = cls.get("emoji", "⚔️")
        legends_with_emoji.append(r)
    return render_template('rpg_stats.html', overall=overall, classes=classes, hall_of_legends=legends_with_emoji, active_page="rpg_stats", **get_location_ambiance("rpg_stats"))

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

@app.route('/quests')
def quests_page():
    if "user_id" not in session:
        return redirect(url_for("login"))
    today = datetime.date.today().isoformat()
    progress_list = run_async(db.get_daily_quest_progress_for_date(session["user_id"], today))
    progress_by_id = {p["quest_id"]: p for p in progress_list}
    quests_with_status = []
    for q in DAILY_QUESTS:
        pid = q["id"]
        prog = progress_by_id.get(pid, {"completed": 0, "claimed": 0})
        quests_with_status.append({
            **q,
            "completed": bool(prog.get("completed")),
            "claimed": bool(prog.get("claimed")),
        })
    return render_template("quests.html", quests=quests_with_status, active_page="quests", **get_location_ambiance("quests"))

@app.route("/api/quests/claim", methods=["POST"])
def api_quests_claim():
    if "user_id" not in session:
        return {"success": False, "message": "Not logged in"}, 401
    data = request.get_json() or {}
    quest_id = (data.get("quest_id") or "").strip()
    if not quest_id or not any(q["id"] == quest_id for q in DAILY_QUESTS):
        return {"success": False, "message": "Invalid quest."}, 400
    today = datetime.date.today().isoformat()
    reward = next((q["reward"] for q in DAILY_QUESTS if q["id"] == quest_id), 0.0)
    ok = run_async(db.claim_quest_reward(session["user_id"], quest_id, today, reward))
    if not ok:
        return {"success": False, "message": "Complete the quest first or already claimed."}
    return {"success": True, "reward": reward}

@app.route("/api/quests/complete_dungeon", methods=["POST"])
def api_quests_complete_dungeon():
    """Honor system: mark 'dungeon_run' daily quest as complete (e.g. after a Discord run)."""
    if "user_id" not in session:
        return {"success": False, "message": "Not logged in"}, 401
    today = datetime.date.today().isoformat()
    run_async(db.set_quest_completed(session["user_id"], "dungeon_run", today))
    return {"success": True}

@app.route('/leaderboard')
def leaderboard():
    async def get_leaders():
        async with aiosqlite.connect(DB_NAME) as db_conn:
            db_conn.row_factory = aiosqlite.Row
            
            # 1. Richest
            async with db_conn.execute("""
                SELECT user_id, username, balance 
                FROM users 
                WHERE username IS NOT NULL 
                ORDER BY balance DESC LIMIT 10
            """) as c:
                richest = await c.fetchall()
            
            # 2. Strongest
            async with db_conn.execute("""
                SELECT user_id, username, max_floor 
                FROM users 
                WHERE username IS NOT NULL 
                ORDER BY max_floor DESC LIMIT 10
            """) as c:
                strongest = await c.fetchall()
                
            # 3. Collectors
            async with db_conn.execute("""
                SELECT u.user_id, u.username, COUNT(i.item_id) as item_count 
                FROM users u
                JOIN inventory i ON u.user_id = i.user_id 
                WHERE u.username IS NOT NULL
                GROUP BY u.user_id 
                ORDER BY item_count DESC LIMIT 10
            """) as c:
                collectors = await c.fetchall()
                
        return richest, strongest, collectors

    richest, strongest, collectors = run_async(get_leaders())
    top_donors = run_async(db.get_top_donors(10))
    top_donors = [dict(d) for d in top_donors]
    top_guilds = run_async(db.get_top_guilds(10))
    top_guilds = [dict(g) for g in top_guilds]
    
    return render_template('leaderboard.html',
                           richest=richest,
                           strongest=strongest,
                           collectors=collectors,
                           top_donors=top_donors,
                           top_guilds=top_guilds,
                           active_page="leaderboard",
                           **get_location_ambiance("leaderboard"))

# --- CASINO (mirrors cogs/casino.py logic for web) ---
CASINO_SUITS = {'H': '♥', 'D': '♦', 'C': '♣', 'S': '♠'}
CASINO_RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']

def _casino_deck():
    deck = [f"{r}{s}" for r in CASINO_RANKS for s in CASINO_SUITS.keys()]
    random.shuffle(deck)
    return deck

def _bj_score(hand):
    score, aces = 0, 0
    for card in hand:
        rank = card[:-1]
        if rank in ['J', 'Q', 'K']:
            score += 10
        elif rank == 'A':
            score += 11
            aces += 1
        else:
            score += int(rank)
    while score > 21 and aces > 0:
        score -= 10
        aces -= 1
    return score

@app.route('/casino')
def casino_page():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('casino.html', active_page='casino', npcs=get_npcs_for_page("casino"), **get_location_ambiance("casino"))

def _require_casino_user():
    if 'user_id' not in session:
        return None
    return session['user_id']

@app.route('/api/casino/slots', methods=['POST'])
def api_casino_slots():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    bet = float(data.get('bet', 0))
    if bet < 1.0:
        return {"success": False, "error": "Minimum bet is $1.00"}
    if not run_async(db.update_balance(user_id, -bet)):
        return {"success": False, "error": "Insufficient funds"}
    emojis = ["🍒", "🍋", "🍇", "🔔", "💎", "7️⃣"]
    result = [random.choice(emojis) for _ in range(3)]
    if result[0] == result[1] == result[2]:
        desired = bet * 10
        gross = run_async(db.gross_for_desired_net(desired))
        net, tax = run_async(db.process_town_payout(user_id, gross))
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "result": result, "win": "jackpot", "multiplier": 10, "net": net, "tax": tax, "new_balance": new_bal}
    if result[0] == result[1] or result[1] == result[2] or result[0] == result[2]:
        desired = bet * 2
        gross = run_async(db.gross_for_desired_net(desired))
        net, tax = run_async(db.process_town_payout(user_id, gross))
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "result": result, "win": "pair", "multiplier": 2, "net": net, "tax": tax, "new_balance": new_bal}
    new_bal = run_async(db.get_balance(user_id))
    return {"success": True, "result": result, "win": "loss", "net": 0, "tax": 0, "new_balance": new_bal}

@app.route('/api/casino/roulette', methods=['POST'])
def api_casino_roulette():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    bet = float(data.get('bet', 0))
    choice = (data.get('choice') or '').lower()
    if choice not in ('red', 'black', 'green'):
        return {"success": False, "error": "Choice must be red, black, or green"}
    if bet < 1.0:
        return {"success": False, "error": "Minimum bet is $1.00"}
    if not run_async(db.update_balance(user_id, -bet)):
        return {"success": False, "error": "Insufficient funds"}
    roll = random.randint(0, 36)
    if roll == 0:
        color = "green"
    elif roll % 2 == 0:
        color = "black"
    else:
        color = "red"
    if choice == color:
        mult = 14 if color == "green" else 2
        desired = bet * mult
        gross = run_async(db.gross_for_desired_net(desired))
        net, tax = run_async(db.process_town_payout(user_id, gross))
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "number": roll, "color": color, "won": True, "multiplier": mult, "net": net, "tax": tax, "new_balance": new_bal}
    new_bal = run_async(db.get_balance(user_id))
    return {"success": True, "number": roll, "color": color, "won": False, "new_balance": new_bal}

@app.route('/api/casino/blackjack/start', methods=['POST'])
def api_casino_blackjack_start():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    bet = float(data.get('bet', 0))
    if bet < 5.0:
        return {"success": False, "error": "Minimum bet is $5.00"}
    if not run_async(db.update_balance(user_id, -bet)):
        return {"success": False, "error": "Insufficient funds"}
    deck = _casino_deck()
    dealer = [deck.pop(), deck.pop()]
    hand = [deck.pop(), deck.pop()]
    p_score = _bj_score(hand)
    d_score = _bj_score(dealer)
    game_over = False
    status = "playing"
    net, tax = 0.0, 0.0
    if p_score == 21:
        game_over = True
        if d_score == 21:
            status = "push"
            run_async(db.update_balance(user_id, bet))
        else:
            status = "blackjack"
            desired = bet * 2.5
            gross = run_async(db.gross_for_desired_net(desired))
            net, tax = run_async(db.process_town_payout(user_id, gross))
    else:
        status = "Playing"
    session['casino_bj'] = {
        'deck': deck, 'dealer': dealer, 'hands': [hand], 'bets': [bet],
        'statuses': [status], 'active_idx': 0, 'game_over': game_over,
        'net_payouts': [net], 'taxes': [tax], 'initial_bet': bet
    }
    new_bal = run_async(db.get_balance(user_id))
    return {
        "success": True, "dealer": dealer, "dealer_hidden": True, "player_hands": [hand],
        "player_scores": [_bj_score(hand)], "statuses": [status], "game_over": game_over,
        "net": net, "tax": tax, "new_balance": new_bal
    }

@app.route('/api/casino/blackjack/action', methods=['POST'])
def api_casino_blackjack_action():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    state = session.get('casino_bj')
    if not state:
        return {"success": False, "error": "No active blackjack game"}
    data = request.get_json() or {}
    action = (data.get('action') or '').lower()
    if action not in ('hit', 'stand', 'double', 'split'):
        return {"success": False, "error": "Invalid action"}
    deck = state['deck']
    dealer = state['dealer']
    hands = state['hands']
    bets = state['bets']
    statuses = state['statuses']
    active_idx = state['active_idx']
    initial_bet = state['initial_bet']
    net_payouts = state.get('net_payouts', [0.0] * len(hands))
    taxes = state.get('taxes', [0.0] * len(hands))
    game_over = state['game_over']

    if game_over:
        session.pop('casino_bj', None)
        return {"success": False, "error": "Game already over"}

    idx = active_idx
    hand = hands[idx]
    bet = bets[idx]

    if action == "hit":
        hand.append(deck.pop())
        if _bj_score(hand) > 21:
            statuses[idx] = "lost"
            idx += 1
            while idx < len(hands) and statuses[idx] != "Playing":
                idx += 1
            if idx >= len(hands):
                game_over = True
                d_score = _bj_score(dealer)
                while d_score < 17:
                    dealer.append(deck.pop())
                    d_score = _bj_score(dealer)
                total_net, total_tax = 0.0, 0.0
                for i in range(len(hands)):
                    if statuses[i] == "Playing":
                        ps = _bj_score(hands[i])
                        if ps > 21:
                            statuses[i] = "lost"
                        elif d_score > 21 or ps > d_score:
                            statuses[i] = "won"
                            desired = bets[i] * 2
                            gross = run_async(db.gross_for_desired_net(desired))
                            n, t = run_async(db.process_town_payout(user_id, gross))
                            net_payouts[i], taxes[i] = n, t
                            total_net += n
                            total_tax += t
                        elif ps < d_score:
                            statuses[i] = "lost"
                        else:
                            statuses[i] = "push"
                            run_async(db.update_balance(user_id, bets[i]))
                state['game_over'] = True
                state['dealer'] = dealer
                state['statuses'] = statuses
                state['net_payouts'] = net_payouts
                state['taxes'] = taxes
                session['casino_bj'] = state
                new_bal = run_async(db.get_balance(user_id))
                return {"success": True, "dealer": dealer, "dealer_hidden": False, "player_hands": hands,
                        "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": True,
                        "net": sum(net_payouts), "tax": sum(taxes), "new_balance": new_bal}
            state['active_idx'] = idx
            state['hands'] = hands
            state['statuses'] = statuses
            session['casino_bj'] = state
            new_bal = run_async(db.get_balance(user_id))
            return {"success": True, "dealer": dealer, "dealer_hidden": True, "player_hands": hands,
                    "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": False, "new_balance": new_bal}
        state['deck'] = deck
        state['hands'] = hands
        session['casino_bj'] = state
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "dealer": dealer, "dealer_hidden": True, "player_hands": hands,
                "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": False, "new_balance": new_bal}

    if action == "stand":
        idx += 1
        while idx < len(hands) and statuses[idx] != "Playing":
            idx += 1
        if idx >= len(hands):
            game_over = True
            d_score = _bj_score(dealer)
            while d_score < 17:
                dealer.append(deck.pop())
                d_score = _bj_score(dealer)
            total_net, total_tax = 0.0, 0.0
            for i in range(len(hands)):
                if statuses[i] != "Playing":
                    continue
                ps = _bj_score(hands[i])
                if ps > 21:
                    statuses[i] = "lost"
                elif d_score > 21 or ps > d_score:
                    statuses[i] = "won"
                    desired = bets[i] * 2
                    gross = run_async(db.gross_for_desired_net(desired))
                    n, t = run_async(db.process_town_payout(user_id, gross))
                    net_payouts[i], taxes[i] = n, t
                    total_net += n
                    total_tax += t
                elif ps < d_score:
                    statuses[i] = "lost"
                else:
                    statuses[i] = "push"
                    run_async(db.update_balance(user_id, bets[i]))
            state['game_over'] = True
            state['dealer'] = dealer
            state['statuses'] = statuses
            state['net_payouts'] = net_payouts
            state['taxes'] = taxes
            session.pop('casino_bj', None)
            new_bal = run_async(db.get_balance(user_id))
            return {"success": True, "dealer": dealer, "dealer_hidden": False, "player_hands": hands,
                    "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": True,
                    "net": total_net, "tax": total_tax, "new_balance": new_bal}
        state['active_idx'] = idx
        session['casino_bj'] = state
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "dealer": dealer, "dealer_hidden": True, "player_hands": hands,
                "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": False, "new_balance": new_bal}

    if action == "double":
        if not run_async(db.update_balance(user_id, -bet)):
            return {"success": False, "error": "Insufficient funds to double"}
        bets[idx] *= 2
        hand.append(deck.pop())
        if _bj_score(hand) > 21:
            statuses[idx] = "lost"
        idx += 1
        while idx < len(hands) and statuses[idx] != "Playing":
            idx += 1
        if idx >= len(hands):
            game_over = True
            d_score = _bj_score(dealer)
            while d_score < 17:
                dealer.append(deck.pop())
                d_score = _bj_score(dealer)
            for i in range(len(hands)):
                if statuses[i] != "Playing":
                    continue
                ps = _bj_score(hands[i])
                if ps > 21:
                    statuses[i] = "lost"
                elif d_score > 21 or ps > d_score:
                    statuses[i] = "won"
                    desired = bets[i] * 2
                    gross = run_async(db.gross_for_desired_net(desired))
                    n, t = run_async(db.process_town_payout(user_id, gross))
                    net_payouts[i], taxes[i] = n, t
                elif ps < d_score:
                    statuses[i] = "lost"
                else:
                    statuses[i] = "push"
                    run_async(db.update_balance(user_id, bets[i]))
            state['game_over'] = True
            state['dealer'] = dealer
            state['statuses'] = statuses
            state['bets'] = bets
            state['net_payouts'] = net_payouts
            state['taxes'] = taxes
            session.pop('casino_bj', None)
            new_bal = run_async(db.get_balance(user_id))
            return {"success": True, "dealer": dealer, "dealer_hidden": False, "player_hands": hands,
                    "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": True,
                    "net": sum(net_payouts), "tax": sum(taxes), "new_balance": new_bal}
        state['deck'] = deck
        state['hands'] = hands
        state['bets'] = bets
        state['statuses'] = statuses
        state['active_idx'] = idx
        session['casino_bj'] = state
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "dealer": dealer, "dealer_hidden": True, "player_hands": hands,
                "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": False, "new_balance": new_bal}

    if action == "split":
        if len(hand) != 2 or hand[0][:-1] != hand[1][:-1]:
            return {"success": False, "error": "Can only split a pair"}
        if not run_async(db.update_balance(user_id, -initial_bet)):
            return {"success": False, "error": "Insufficient funds to split"}
        c2 = hand.pop()
        hand.append(deck.pop())
        new_hand = [c2, deck.pop()]
        hands.insert(idx + 1, new_hand)
        bets.insert(idx + 1, initial_bet)
        statuses.insert(idx + 1, "Playing")
        net_payouts.insert(idx + 1, 0.0)
        taxes.insert(idx + 1, 0.0)
        state['deck'] = deck
        state['hands'] = hands
        state['bets'] = bets
        state['statuses'] = statuses
        state['net_payouts'] = net_payouts
        state['taxes'] = taxes
        session['casino_bj'] = state
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "dealer": dealer, "dealer_hidden": True, "player_hands": hands,
                "player_scores": [_bj_score(h) for h in hands], "statuses": statuses, "game_over": False, "new_balance": new_bal}

    return {"success": False, "error": "Unknown action"}

@app.route('/api/casino/slap/start', methods=['POST'])
def api_casino_slap_start():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    bet = float(data.get('bet', 0))
    if bet <= 0:
        return {"success": False, "error": "Bet must be positive"}
    if not run_async(db.update_balance(user_id, -bet)):
        return {"success": False, "error": "Insufficient funds"}
    session['casino_slap'] = {'bet': bet, 'multiplier': 1.0, 'slaps': 0, 'base_risk': 0.05, 'risk_increase': 0.07}
    new_bal = run_async(db.get_balance(user_id))
    return {"success": True, "bet": bet, "multiplier": 1.0, "slaps": 0, "risk_pct": 5, "new_balance": new_bal}

def _slap_risk(slap_state):
    return min(0.95, slap_state['base_risk'] + slap_state['slaps'] * slap_state['risk_increase'])

@app.route('/api/casino/slap/action', methods=['POST'])
def api_casino_slap_action():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    state = session.get('casino_slap')
    if not state:
        return {"success": False, "error": "No active slap game. Start one first."}
    data = request.get_json() or {}
    action = (data.get('action') or '').lower()
    if action == "slap":
        risk = _slap_risk(state)
        if random.random() < risk:
            session.pop('casino_slap', None)
            new_bal = run_async(db.get_balance(user_id))
            return {"success": True, "action": "slap", "farted": True, "multiplier": state['multiplier'], "slaps": state['slaps'], "new_balance": new_bal}
        state['slaps'] += 1
        state['multiplier'] *= 1.15
        risk_pct = int(_slap_risk(state) * 100)
        session['casino_slap'] = state
        return {"success": True, "action": "slap", "farted": False, "multiplier": state['multiplier'], "slaps": state['slaps'], "risk_pct": risk_pct}
    if action == "cashout":
        if state['slaps'] == 0:
            return {"success": False, "error": "Slap at least once before cashing out"}
        desired = state['bet'] * state['multiplier']
        gross = run_async(db.gross_for_desired_net(desired))
        net, tax = run_async(db.process_town_payout(user_id, gross))
        session.pop('casino_slap', None)
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "action": "cashout", "net": net, "tax": tax, "slaps": state['slaps'], "multiplier": state['multiplier'], "new_balance": new_bal}
    return {"success": False, "error": "Invalid action"}

@app.route('/api/casino/mines/start', methods=['POST'])
def api_casino_mines_start():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    bet = float(data.get('bet', 0))
    mines = int(data.get('mines', 3))
    if bet <= 0:
        return {"success": False, "error": "Bet must be positive"}
    if mines < 1 or mines > 19:
        return {"success": False, "error": "Mines must be 1–19"}
    if not run_async(db.update_balance(user_id, -bet)):
        return {"success": False, "error": "Insufficient funds"}
    total_tiles = 20
    safe = total_tiles - mines
    grid = [True] * mines + [False] * safe
    random.shuffle(grid)
    session['casino_mines'] = {
        'bet': bet, 'mines': mines, 'grid': grid, 'revealed': [False] * 20,
        'multiplier': 1.0, 'safe_revealed': 0, 'ended': False
    }
    new_bal = run_async(db.get_balance(user_id))
    return {"success": True, "multiplier": 1.0, "safe_revealed": 0, "new_balance": new_bal}

def _mines_next_mult(revealed_safe, mine_count, total_tiles=20):
    remaining = total_tiles - revealed_safe
    remaining_safe = (total_tiles - mine_count) - revealed_safe
    if remaining_safe <= 0:
        return None
    odds = remaining / remaining_safe
    return 1.0 * (odds * 0.98)  # initial mult 1.0, then chain

@app.route('/api/casino/mines/reveal', methods=['POST'])
def api_casino_mines_reveal():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    state = session.get('casino_mines')
    if not state or state['ended']:
        return {"success": False, "error": "No active mines game"}
    data = request.get_json() or {}
    tile = int(data.get('tile'))
    if tile < 0 or tile >= 20 or state['revealed'][tile]:
        return {"success": False, "error": "Invalid tile"}
    state['revealed'][tile] = True
    if state['grid'][tile]:
        state['ended'] = True
        session.pop('casino_mines', None)
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "mine": True, "revealed": state['revealed'], "game_over": True, "new_balance": new_bal}
    state['safe_revealed'] += 1
    safe_count = 20 - state['mines']
    prev_mult = state['multiplier']
    state['multiplier'] = prev_mult * ((20 - state['safe_revealed']) / (safe_count - state['safe_revealed']) * 0.98) if (safe_count - state['safe_revealed']) > 0 else prev_mult
    session['casino_mines'] = state
    new_bal = run_async(db.get_balance(user_id))
    if state['safe_revealed'] >= safe_count:
        desired = state['bet'] * state['multiplier']
        gross = run_async(db.gross_for_desired_net(desired))
        net, tax = run_async(db.process_town_payout(user_id, gross))
        session.pop('casino_mines', None)
        new_bal = run_async(db.get_balance(user_id))
        return {"success": True, "mine": False, "tile": tile, "multiplier": state['multiplier'], "perfect": True, "net": net, "tax": tax, "game_over": True, "new_balance": new_bal}
    return {"success": True, "mine": False, "tile": tile, "multiplier": state['multiplier'], "safe_revealed": state['safe_revealed'],
            "gems_left": safe_count - state['safe_revealed'], "game_over": False, "new_balance": new_bal}

@app.route('/api/casino/mines/cashout', methods=['POST'])
def api_casino_mines_cashout():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    state = session.get('casino_mines')
    if not state or state['ended']:
        return {"success": False, "error": "No active mines game"}
    if state['safe_revealed'] == 0:
        return {"success": False, "error": "Reveal at least one gem first"}
    desired = state['bet'] * state['multiplier']
    gross = run_async(db.gross_for_desired_net(desired))
    net, tax = run_async(db.process_town_payout(user_id, gross))
    session.pop('casino_mines', None)
    new_bal = run_async(db.get_balance(user_id))
    return {"success": True, "net": net, "tax": tax, "multiplier": state['multiplier'], "game_over": True, "new_balance": new_bal}

@app.route('/api/casino/plinko', methods=['POST'])
def api_casino_plinko():
    user_id = _require_casino_user()
    if not user_id:
        return {"success": False, "error": "Not logged in"}, 401
    data = request.get_json() or {}
    bet = float(data.get('bet', 0))
    if bet <= 0:
        return {"success": False, "error": "Bet must be positive"}
    if not run_async(db.update_balance(user_id, -bet)):
        return {"success": False, "error": "Insufficient funds"}
    multipliers = [2.0, 1.2, 1.0, 0.6, 0.2, 0.6, 1.0, 1.2, 2.0]
    center = len(multipliers) // 2
    idx = center
    moves = []
    for _ in range(10):
        step = random.choice((-1, 1))
        idx = max(0, min(len(multipliers) - 1, idx + step))
        moves.append("L" if step < 0 else "R")
    mult = multipliers[idx]
    desired = bet * mult
    gross = run_async(db.gross_for_desired_net(desired))
    net, tax = run_async(db.process_town_payout(user_id, gross))
    new_bal = run_async(db.get_balance(user_id))
    return {"success": True, "landed_idx": idx, "multiplier": mult, "moves": moves, "gross": gross, "net": net, "tax": tax, "new_balance": new_bal}

# --- START THE ENGINE ---

if __name__ == '__main__':
    # Using socketio.run instead of app.run is mandatory for real-time functionality
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)