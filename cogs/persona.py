import discord
from discord import app_commands
from discord.ext import commands
import random
import asyncio
from collections import defaultdict

# ─────────────────────────────────────────────
#  ELEMENTS & AFFINITIES
# ─────────────────────────────────────────────
ELEMENT_EMOJI = {
    "fire": "🔥", "ice": "❄️", "elec": "⚡", "wind": "🌪️",
    "light": "✨", "dark": "🌑", "phys": "⚔️", "almighty": "💫",
    "heal": "💚", "status": "🌀",
}

# ─────────────────────────────────────────────
#  SKILL DEFINITIONS
# ─────────────────────────────────────────────
SKILLS = {
    # Physical
    "Slash":        {"type": "phys",     "power": 80,   "hits": 1, "cost": 0,  "target": "one",   "desc": "Light phys dmg to one foe."},
    "Heavy Slash":  {"type": "phys",     "power": 140,  "hits": 1, "cost": 10, "target": "one",   "desc": "Medium phys dmg to one foe."},
    "Cleave":       {"type": "phys",     "power": 70,   "hits": 1, "cost": 8,  "target": "all",   "desc": "Light phys dmg to all foes."},
    "Rampage":      {"type": "phys",     "power": 55,   "hits": 3, "cost": 18, "target": "random","desc": "3 hits of phys dmg, random targets."},
    "God's Hand":   {"type": "phys",     "power": 260,  "hits": 1, "cost": 28, "target": "one",   "desc": "Massive phys dmg to one foe."},
    # Fire
    "Agi":          {"type": "fire",     "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light fire dmg to one foe."},
    "Agilao":       {"type": "fire",     "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium fire dmg to one foe."},
    "Agidyne":      {"type": "fire",     "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy fire dmg to one foe."},
    "Maragi":       {"type": "fire",     "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light fire dmg to all foes."},
    "Maragidyne":   {"type": "fire",     "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy fire dmg to all foes."},
    # Ice
    "Bufu":         {"type": "ice",      "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light ice dmg to one foe."},
    "Bufula":       {"type": "ice",      "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium ice dmg to one foe."},
    "Bufudyne":     {"type": "ice",      "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy ice dmg to one foe."},
    "Mabufu":       {"type": "ice",      "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light ice dmg to all foes."},
    "Mabufudyne":   {"type": "ice",      "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy ice dmg to all foes."},
    # Electric
    "Zio":          {"type": "elec",     "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light elec dmg to one foe."},
    "Zionga":       {"type": "elec",     "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium elec dmg to one foe."},
    "Ziodyne":      {"type": "elec",     "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy elec dmg to one foe."},
    "Mazio":        {"type": "elec",     "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light elec dmg to all foes."},
    "Maziodyne":    {"type": "elec",     "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy elec dmg to all foes."},
    # Wind
    "Garu":         {"type": "wind",     "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light wind dmg to one foe."},
    "Garula":       {"type": "wind",     "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium wind dmg to one foe."},
    "Garudyne":     {"type": "wind",     "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy wind dmg to one foe."},
    "Magaru":       {"type": "wind",     "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light wind dmg to all foes."},
    "Magarudyne":   {"type": "wind",     "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy wind dmg to all foes."},
    # Light
    "Hama":         {"type": "light",    "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Low chance instant kill (light)."},
    "Hamaon":       {"type": "light",    "power": 0,    "hits": 1, "cost": 14, "target": "one",   "desc": "Med chance instant kill (light)."},
    "Mahama":       {"type": "light",    "power": 0,    "hits": 1, "cost": 16, "target": "all",   "desc": "Low chance instant kill all (light)."},
    "Kougaon":      {"type": "light",    "power": 200,  "hits": 1, "cost": 18, "target": "one",   "desc": "Heavy light dmg + instakill chance."},
    # Dark
    "Mudo":         {"type": "dark",     "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Low chance instant kill (dark)."},
    "Mudoon":       {"type": "dark",     "power": 0,    "hits": 1, "cost": 14, "target": "one",   "desc": "Med chance instant kill (dark)."},
    "Mamudo":       {"type": "dark",     "power": 0,    "hits": 1, "cost": 16, "target": "all",   "desc": "Low chance instant kill all (dark)."},
    "Eigaon":       {"type": "dark",     "power": 200,  "hits": 1, "cost": 18, "target": "one",   "desc": "Heavy dark dmg + instakill chance."},
    # Almighty
    "Megidola":     {"type": "almighty", "power": 160,  "hits": 1, "cost": 24, "target": "all",   "desc": "Heavy almighty dmg to all foes."},
    "Megidolaon":   {"type": "almighty", "power": 260,  "hits": 1, "cost": 36, "target": "all",   "desc": "Severe almighty dmg to all foes."},
    # Healing
    "Dia":          {"type": "heal",     "power": 60,   "hits": 1, "cost": 4,  "target": "self",  "desc": "Restore small HP (self)."},
    "Diarama":      {"type": "heal",     "power": 120,  "hits": 1, "cost": 8,  "target": "self",  "desc": "Restore moderate HP (self)."},
    "Diarahan":     {"type": "heal",     "power": 9999, "hits": 1, "cost": 18, "target": "ally",  "desc": "Fully restore one ally's HP."},
    "Media":        {"type": "heal",     "power": 50,   "hits": 1, "cost": 10, "target": "party", "desc": "Restore HP to all party members."},
    "Mediarahan":   {"type": "heal",     "power": 9999, "hits": 1, "cost": 28, "target": "party", "desc": "Fully restore all party members' HP."},
    # Status
    "Tarukaja":     {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "self",  "desc": "Raise own Attack."},
    "Rakukaja":     {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "self",  "desc": "Raise own Defense."},
    "Sukukaja":     {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "self",  "desc": "Raise own Agility."},
    "Tarunda":      {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Lower one foe's Attack."},
    "Rakunda":      {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Lower one foe's Defense."},
    "Sukunda":      {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Lower one foe's Agility."},
}

# ─────────────────────────────────────────────
#  PERSONA CLASSES
# ─────────────────────────────────────────────
PERSONA_CLASSES = {
    "Fool":      {"emoji": "🃏", "hp": 120, "sp": 80,  "atk": 12, "mag": 8,  "end": 8,  "agi": 8,  "skills": ["Slash", "Agi", "Bufu", "Dia"],                                  "desc": "Balanced beginner. Fire & ice."},
    "Magician":  {"emoji": "🔮", "hp": 90,  "sp": 120, "atk": 6,  "mag": 18, "end": 6,  "agi": 10, "skills": ["Agi", "Agilao", "Zio", "Maragi", "Media"],                      "desc": "High magic. Fire & electric."},
    "Chariot":   {"emoji": "⚔️", "hp": 150, "sp": 60,  "atk": 20, "mag": 4,  "end": 12, "agi": 14, "skills": ["Slash", "Heavy Slash", "Rampage", "God's Hand", "Rakukaja"],    "desc": "Physical powerhouse."},
    "Priestess": {"emoji": "🌙", "hp": 100, "sp": 130, "atk": 6,  "mag": 14, "end": 10, "agi": 8,  "skills": ["Bufu", "Bufula", "Mabufu", "Dia", "Media", "Mediarahan"],       "desc": "Ice & healer. Support specialist."},
    "Emperor":   {"emoji": "👑", "hp": 130, "sp": 90,  "atk": 14, "mag": 10, "end": 14, "agi": 8,  "skills": ["Heavy Slash", "Zionga", "Tarukaja", "Rakukaja", "Tarunda"],     "desc": "Melee & buffs. Tactical fighter."},
    "Tower":     {"emoji": "🌩️", "hp": 100, "sp": 110, "atk": 8,  "mag": 16, "end": 8,  "agi": 12, "skills": ["Zio", "Zionga", "Ziodyne", "Mazio", "Maziodyne", "Sukunda"],   "desc": "Electric specialist."},
    "Hermit":    {"emoji": "🌿", "hp": 100, "sp": 110, "atk": 8,  "mag": 15, "end": 8,  "agi": 14, "skills": ["Garu", "Garula", "Garudyne", "Magaru", "Sukukaja", "Sukunda"],  "desc": "Wind specialist. Evasion & speed."},
    "Death":     {"emoji": "💀", "hp": 95,  "sp": 120, "atk": 10, "mag": 16, "end": 6,  "agi": 10, "skills": ["Mudo", "Mudoon", "Mamudo", "Eigaon", "Tarunda", "Rakunda"],     "desc": "Dark arts. Instant kill specialist."},
    "Judgement": {"emoji": "☀️", "hp": 95,  "sp": 120, "atk": 10, "mag": 16, "end": 6,  "agi": 10, "skills": ["Hama", "Hamaon", "Mahama", "Kougaon", "Rakukaja", "Media"],    "desc": "Light arts. Instant kill specialist."},
    "World":     {"emoji": "🌌", "hp": 110, "sp": 130, "atk": 10, "mag": 18, "end": 8,  "agi": 10, "skills": ["Megidola", "Megidolaon", "Diarahan", "Mediarahan", "Tarukaja"], "desc": "Almighty magic & full heals."},
}

# ─────────────────────────────────────────────
#  ENEMIES
# ─────────────────────────────────────────────
PERSONA_ENEMIES = [
    {"name": "Pyro Jack",       "emoji": "🎃", "hp": 60,  "atk": 14, "def": 6,  "mag": 10, "affinities": {"fire": "null", "ice": "weak"},                    "skills": ["Agi", "Maragi"],                  "xp": 30, "gold": 80},
    {"name": "Frost",           "emoji": "🧊", "hp": 60,  "atk": 10, "def": 8,  "mag": 12, "affinities": {"ice": "drain", "fire": "weak"},                   "skills": ["Bufu", "Mabufu"],                 "xp": 30, "gold": 80},
    {"name": "Zionga Wisp",     "emoji": "⚡", "hp": 55,  "atk": 10, "def": 5,  "mag": 14, "affinities": {"elec": "null", "wind": "weak"},                   "skills": ["Zio", "Zionga"],                  "xp": 28, "gold": 75},
    {"name": "Shadow Knight",   "emoji": "🛡️", "hp": 90,  "atk": 18, "def": 14, "mag": 4,  "affinities": {"phys": "resist", "light": "weak", "dark": "null"},"skills": ["Slash", "Heavy Slash", "Rakukaja"],"xp": 40, "gold": 100},
    {"name": "Mist Wisp",       "emoji": "🌫️", "hp": 50,  "atk": 8,  "def": 4,  "mag": 12, "affinities": {"wind": "null", "elec": "weak", "phys": "resist"}, "skills": ["Garu", "Garula", "Sukunda"],      "xp": 25, "gold": 65},
    {"name": "Undead Soldier",  "emoji": "💀", "hp": 75,  "atk": 15, "def": 8,  "mag": 4,  "affinities": {"light": "weak", "dark": "drain"},                 "skills": ["Slash", "Heavy Slash"],           "xp": 35, "gold": 90},
    {"name": "Succubus",        "emoji": "😈", "hp": 65,  "atk": 10, "def": 6,  "mag": 16, "affinities": {"dark": "null", "light": "weak"},                  "skills": ["Eigaon", "Tarunda", "Rakunda"],   "xp": 38, "gold": 95},
    {"name": "Seraph Fragment", "emoji": "🕊️", "hp": 65,  "atk": 8,  "def": 8,  "mag": 14, "affinities": {"light": "drain", "dark": "weak"},                 "skills": ["Kougaon", "Hamaon"],              "xp": 38, "gold": 95},
    {"name": "Ose",             "emoji": "🦁", "hp": 80,  "atk": 20, "def": 10, "mag": 8,  "affinities": {},                                                  "skills": ["Slash", "Rampage", "Tarukaja"],   "xp": 42, "gold": 110},
    {"name": "Nue",             "emoji": "🐉", "hp": 70,  "atk": 12, "def": 6,  "mag": 16, "affinities": {"elec": "weak", "wind": "resist"},                  "skills": ["Agi", "Bufu", "Mudo"],            "xp": 42, "gold": 110},
]

PERSONA_BOSSES = [
    {"name": "Arcana Magician", "emoji": "🔮", "hp": 400, "atk": 20, "def": 10, "mag": 24, "boss": True,
     "affinities": {"fire": "drain", "ice": "weak", "elec": "null"},
     "skills": ["Agilao", "Agidyne", "Maragidyne", "Tarunda"],
     "xp": 200, "gold": 600, "phase2_hp_thresh": 0.4,
     "phase2_skills": ["Megidola", "Agidyne", "Maragidyne"],
     "phase2_msg": "🔥 Arcana Magician ENTERS RAGE MODE!"},
    {"name": "Shadow Self",     "emoji": "🌑", "hp": 500, "atk": 22, "def": 12, "mag": 20, "boss": True,
     "affinities": {"almighty": "resist"},
     "skills": ["Eigaon", "Mudoon", "Tarunda", "Megidola"],
     "xp": 250, "gold": 800, "phase2_hp_thresh": 0.5,
     "phase2_skills": ["Megidolaon", "Mamudo", "Mahama"],
     "phase2_msg": "🌑 Shadow Self awakens its true power!"},
    {"name": "Pale Rider",      "emoji": "🐎", "hp": 450, "atk": 30, "def": 8,  "mag": 18, "boss": True,
     "affinities": {"phys": "resist", "light": "weak", "dark": "null"},
     "skills": ["God's Hand", "Rampage", "Eigaon", "Tarunda"],
     "xp": 220, "gold": 700, "phase2_hp_thresh": 0.35,
     "phase2_skills": ["God's Hand", "Megidola", "Rampage"],
     "phase2_msg": "🐎 Pale Rider charges! Death follows!"},
]

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
AFFINITY_MULT   = {"weak": 2.0, None: 1.0, "resist": 0.5, "null": 0.0, "repel": -1.0, "drain": -1.0}
INSTAKILL_BASE  = {"Hama": 0.15, "Hamaon": 0.35, "Mahama": 0.20, "Mudo": 0.15, "Mudoon": 0.35, "Mamudo": 0.20}

def spawn_enemy(template, floor=1, party_size=1):
    scale = 1.0 + (floor * 0.06) + ((floor ** 2) * 0.001)
    e = dict(template)
    e["hp"]        = max(1, int(e["hp"]  * scale * (0.7 + 0.3 * party_size)))
    e["max_hp"]    = e["hp"]
    e["atk"]       = max(1, int(e["atk"] * scale))
    e["def"]       = max(0, int(e["def"] * scale))
    e["mag"]       = max(1, int(e["mag"] * scale))
    e["downed"]    = False
    e["buffs"]     = {}
    e["phase2"]    = False
    e["skills"]    = list(template["skills"])
    e["_rewarded"] = False
    return e

def calc_damage(skill_name, atk_stats, target_affinities):
    s    = SKILLS[skill_name]
    aff  = target_affinities.get(s["type"]) if isinstance(target_affinities, dict) else None
    mult = AFFINITY_MULT.get(aff, 1.0)
    is_magic  = s["type"] not in ("phys", "almighty")
    base_stat = atk_stats["mag"] if is_magic else atk_stats["atk"]
    raw  = int((s["power"] / 100) * base_stat * 4 * random.uniform(0.9, 1.1))
    dmg  = max(1, int(raw * abs(mult)))
    return (-dmg if mult < 0 else dmg), aff

def aff_label(aff):
    return {"weak": "WEAK‼", "resist": "Resist", "null": "Null!", "repel": "Repel!", "drain": "Drain!"}.get(aff, "")

# ─────────────────────────────────────────────
active_persona_runs: set = set()

# ─────────────────────────────────────────────
#  SESSION
# ─────────────────────────────────────────────
class PersonaSession(discord.ui.View):
    def __init__(self, party_members, classes):
        super().__init__(timeout=600)
        self.lock    = asyncio.Lock()
        self.sid     = str(random.randint(1000, 9999))
        self.message = None
        self.floor   = 1
        self.gold    = 0
        self.log     = []

        self.party = {}
        for user in party_members:
            cls_name = classes[user.id]
            cls = PERSONA_CLASSES[cls_name]
            self.party[user.id] = {
                "user": user, "class": cls_name, "emoji": cls["emoji"],
                "hp": cls["hp"], "max_hp": cls["hp"],
                "sp": cls["sp"], "max_sp": cls["sp"],
                "atk": cls["atk"], "mag": cls["mag"],
                "end": cls["end"], "agi": cls["agi"],
                "skills": list(cls["skills"]),
                "buffs": {}, "status": None, "status_dur": 0,
                "level": 1, "xp": 0, "max_xp": 60,
                "pending_level": 0,
                "one_more": False, "downed": False,
            }

        self.enemies       = []
        self.state         = "EXPLORE"   # EXPLORE / COMBAT / ALL_OUT / WIPED / ESCAPED
        self.pending_moves = {}          # uid -> move dict

        self.generate_floor()
        self._build_ui()

    # ── floor / log helpers ────────────────────
    def generate_floor(self):
        self.enemies = []
        if self.floor % 10 == 0:
            tmpl = random.choice(PERSONA_BOSSES)
            self.enemies.append(spawn_enemy(tmpl, self.floor, len(self.party)))
            self._log(f"⚠️ **BOSS — Floor {self.floor}!** {self.enemies[0]['emoji']} {self.enemies[0]['name']}!")
        else:
            count = random.randint(1, min(3, 1 + self.floor // 5))
            for t in random.choices(PERSONA_ENEMIES, k=count):
                self.enemies.append(spawn_enemy(t, self.floor, len(self.party)))
            self._log(f"🗺️ Floor {self.floor} — " + ", ".join(f"{e['emoji']} {e['name']}" for e in self.enemies))

    def _log(self, line):
        self.log.append(line)
        if len(self.log) > 12:
            self.log = self.log[-12:]

    def alive_players(self):
        return [uid for uid, p in self.party.items() if p["hp"] > 0]

    def alive_enemies(self):
        return [i for i, e in enumerate(self.enemies) if e["hp"] > 0]

    def all_downed(self):
        ae = self.alive_enemies()
        return bool(ae) and all(self.enemies[i]["downed"] for i in ae)

    # ── embed ──────────────────────────────────
    def _bar(self, cur, mx, fill="🟥", empty="⬛", n=5):
        f = round(max(0, cur / mx) * n) if mx else 0
        return fill * f + empty * (n - f)

    def _buf(self, buffs):
        icons = {"atk": "⚔️", "def": "🛡️", "agi": "💨", "mag": "🔮"}
        parts = [f"{icons.get(k,k)}{'+' if v>0 else ''}{v}" for k, v in buffs.items() if v]
        return (" " + " ".join(parts)) if parts else ""

    def get_embed(self):
        colors = {"COMBAT": 0xe74c3c, "ALL_OUT": 0xff6f00, "EXPLORE": 0x2c3e50, "WIPED": 0x111111, "ESCAPED": 0x27ae60}
        is_boss = any(e.get("boss") for e in self.enemies)
        embed = discord.Embed(
            title=f"{'☠️ BOSS' if is_boss else '⚔️ Floor'} {self.floor}  |  💰 ${self.gold:,}",
            description="\n".join(self.log[-6:]) or "…",
            color=colors.get(self.state, 0x2c3e50)
        )
        for uid, p in self.party.items():
            st = {"poison": "☠️", "burn": "🔥", "shock": "⚡"}.get(p["status"], "")
            tags = ("💢DOWN " if p["downed"] else "") + ("✨1MORE " if p["one_more"] else "")
            rdy  = (" ✅" if uid in self.pending_moves else " ⏳") if self.state == "COMBAT" else ""
            embed.add_field(
                name=f"{p['emoji']} {p['user'].display_name} {tags}{st}{rdy}",
                value=(f"❤️{self._bar(p['hp'],p['max_hp'])} {p['hp']}/{p['max_hp']}\n"
                       f"💙{self._bar(p['sp'],p['max_sp'],'🟦')} {p['sp']}/{p['max_sp']}\n"
                       f"Lv.{p['level']} {p['class']}{self._buf(p['buffs'])}"),
                inline=True
            )
        for i, e in enumerate(self.enemies):
            if e["hp"] <= 0:
                embed.add_field(name=f"~~{e['emoji']} {e['name']}~~", value="💀 Defeated", inline=True)
            else:
                aff_str = " ".join(f"{ELEMENT_EMOJI.get(el,'?')}{v[0].upper()}" for el, v in e.get("affinities", {}).items() if v)
                embed.add_field(
                    name=f"[{i+1}] {e['emoji']} {e['name']}{' 💢DOWN' if e['downed'] else ''}",
                    value=(f"❤️{self._bar(e['hp'],e['max_hp'])} {e['hp']}/{e['max_hp']}\n"
                           f"ATK:{e['atk']} DEF:{e['def']} MAG:{e['mag']}{self._buf(e.get('buffs',{}))}\n"
                           f"{aff_str or 'No known affinities'}"),
                    inline=True
                )
        return embed

    # ── UI ─────────────────────────────────────
    def _build_ui(self):
        self.clear_items()
        sid = self.sid

        if self.state == "EXPLORE":
            b = discord.ui.Button(label="Advance", style=discord.ButtonStyle.primary, emoji="➡️", custom_id=f"adv_{sid}", row=0)
            b.callback = self._cb_advance
            self.add_item(b)
            b2 = discord.ui.Button(label="Escape with Loot", style=discord.ButtonStyle.success, emoji="💰", custom_id=f"esc_{sid}", row=0)
            b2.callback = self._cb_escape
            self.add_item(b2)

        elif self.state == "COMBAT":
            b = discord.ui.Button(label="Choose Action", style=discord.ButtonStyle.danger, emoji="✨", custom_id=f"act_{sid}", row=0)
            b.callback = self._cb_pick_skill
            self.add_item(b)
            b2 = discord.ui.Button(label="Guard", style=discord.ButtonStyle.secondary, emoji="🛡️", custom_id=f"grd_{sid}", row=0)
            b2.callback = self._cb_guard
            self.add_item(b2)
            boss = any(e.get("boss") for e in self.enemies if e["hp"] > 0)
            b3 = discord.ui.Button(label="Cannot Flee Boss!" if boss else "Flee (−$100)",
                                   style=discord.ButtonStyle.secondary, emoji="🏃",
                                   custom_id=f"fle_{sid}", row=1, disabled=boss)
            b3.callback = self._cb_flee
            self.add_item(b3)

        elif self.state == "ALL_OUT":
            b = discord.ui.Button(label="ALL-OUT ATTACK!", style=discord.ButtonStyle.danger, emoji="💥", custom_id=f"aoa_{sid}", row=0)
            b.callback = self._cb_all_out
            self.add_item(b)

        if any(p.get("pending_level", 0) > 0 for p in self.party.values()):
            blv = discord.ui.Button(label="Level Up!", style=discord.ButtonStyle.success, emoji="🌟", custom_id=f"lvl_{sid}", row=2)
            blv.callback = self._cb_levelup
            self.add_item(blv)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id not in self.party:
            await interaction.response.send_message("❌ You are not in this battle!", ephemeral=True)
            return False
        return True

    # The ONE method that edits the main board — always uses self.message directly
    async def _refresh(self):
        if self.state in ("WIPED", "ESCAPED"):
            self.stop()
            for uid in self.party:
                active_persona_runs.discard(uid)
        self._build_ui()
        if self.message:
            try:
                await self.message.edit(embed=self.get_embed(), view=self)
            except Exception:
                pass

    # ── EXPLORE buttons ────────────────────────
    async def _cb_advance(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            self.state = "COMBAT"
            self.pending_moves.clear()
            for uid in self.alive_players():
                self.party[uid]["downed"] = False
                self.party[uid]["one_more"] = False
            self._log(f"🗡️ {interaction.user.display_name} leads the charge!")
            await self._refresh()

    async def _cb_escape(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            self.state = "ESCAPED"
            self._log(f"🏆 Escaped with **${self.gold:,}**!")
            await self._refresh()

    # ── COMBAT buttons ─────────────────────────
    async def _cb_pick_skill(self, interaction: discord.Interaction):
        uid = interaction.user.id
        if self.party[uid]["hp"] <= 0:
            return await interaction.response.send_message("💀 You are KO'd!", ephemeral=True)
        if uid in self.pending_moves:
            return await interaction.response.send_message("✅ Move already locked!", ephemeral=True)
        await interaction.response.send_message("**Choose a skill:**", view=SkillPickerView(self, uid), ephemeral=True)

    async def _cb_guard(self, interaction: discord.Interaction):
        uid = interaction.user.id
        if self.party[uid]["hp"] <= 0:
            return await interaction.response.send_message("💀 You are KO'd!", ephemeral=True)
        if uid in self.pending_moves:
            return await interaction.response.send_message("✅ Move already locked!", ephemeral=True)
        await interaction.response.defer()
        await self.submit_move(uid, {"action": "guard", "skill": None, "target_idx": None}, interaction)

    async def _cb_flee(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if random.random() < 0.55:
                penalty = min(100, self.gold)
                self.gold -= penalty
                self._log(f"🏃 Fled! Lost ${penalty}.")
                self.floor += 1
                self.generate_floor()
                self.state = "EXPLORE"
                self.pending_moves.clear()
            else:
                self._log("🚫 Flee failed! Enemy attacks!")
                self._enemy_turn()
                if not self.alive_players():
                    self.state = "WIPED"
                    self._log("💀 **THE PARTY WAS WIPED OUT.**")
            await self._refresh()

    async def _cb_all_out(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state != "ALL_OUT":
                return await self._refresh()
            total_atk = sum(self.party[uid]["atk"] for uid in self.alive_players())
            for ei in self.alive_enemies():
                e = self.enemies[ei]
                dmg = int(total_atk * random.uniform(2.5, 3.5))
                e["hp"] = max(0, e["hp"] - dmg)
                self._log(f"💥 ALL-OUT on {e['name']}! {dmg} dmg!")
                if e["hp"] <= 0 and not e["_rewarded"]:
                    self._reward(e)
            for e in self.enemies:
                e["downed"] = False
            for p in self.party.values():
                p["one_more"] = False
            if not self.alive_enemies():
                self._on_victory()
            else:
                self.state = "COMBAT"
            await self._refresh()

    async def _cb_levelup(self, interaction: discord.Interaction):
        uid = interaction.user.id
        if self.party[uid].get("pending_level", 0) <= 0:
            return await interaction.response.send_message("❌ No pending level ups.", ephemeral=True)
        await interaction.response.send_message("🌟 **Level Up!** Choose a stat:", view=PersonaLevelUpView(self, uid), ephemeral=True)

    # ──────────────────────────────────────────
    #  MOVE SUBMISSION (called by child views — no lock here)
    #  This is the single entry point. It acquires the lock itself.
    # ──────────────────────────────────────────
    async def submit_move(self, uid: int, move: dict, interaction: discord.Interaction):
        """
        Register uid's move. If all alive players have submitted, resolve the round.
        The interaction is the child ephemeral interaction — we ONLY use it to send
        a quiet confirmation. All board updates go through self._refresh().
        """
        async with self.lock:
            if self.state != "COMBAT":
                return

            if uid in self.pending_moves:
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("✅ Already locked in!", ephemeral=True)
                except Exception:
                    pass
                return

            self.pending_moves[uid] = move
            waiting = [u for u in self.alive_players() if u not in self.pending_moves]

            if waiting:
                # Not everyone ready — send a quiet ACK, then update the board's ✅ indicators
                skill_name = move.get("skill") or "Guard"
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message(
                            f"✅ **{skill_name}** locked in! Waiting for {len(waiting)} more…", ephemeral=True
                        )
                except Exception:
                    pass
                await self._refresh()
            else:
                # Everyone ready — resolve (synchronous, lock already held)
                try:
                    if not interaction.response.is_done():
                        await interaction.response.defer()
                except Exception:
                    pass
                self._resolve_round()
                await self._refresh()

    # ──────────────────────────────────────────
    #  ROUND RESOLUTION  (sync, called while lock is held)
    # ──────────────────────────────────────────
    def _resolve_round(self):
        self._log("─── Round ───")

        # Status ticks on players
        stunned_players = set()
        for uid in self.alive_players():
            p = self.party[uid]
            if p["status"] == "poison":
                tick = max(1, p["max_hp"] // 10)
                p["hp"] = max(0, p["hp"] - tick)
                self._log(f"☠️ {p['user'].display_name} −{tick} (poison)")
            elif p["status"] == "burn":
                tick = max(1, p["max_hp"] // 8)
                p["hp"] = max(0, p["hp"] - tick)
                self._log(f"🔥 {p['user'].display_name} −{tick} (burn)")
            elif p["status"] == "shock":
                stunned_players.add(uid)
                self._log(f"⚡ {p['user'].display_name} is shocked and can't act!")
            if p["status_dur"] > 0:
                p["status_dur"] -= 1
                if p["status_dur"] <= 0:
                    p["status"] = None

        # Player actions
        for uid, move in list(self.pending_moves.items()):
            p = self.party[uid]
            if p["hp"] <= 0:
                continue
            if uid in stunned_players:
                continue
            self._execute_player_move(uid, p, move)

        self.pending_moves.clear()

        # Reward dead enemies
        for e in self.enemies:
            if e["hp"] <= 0 and not e["_rewarded"]:
                self._reward(e)

        if not self.alive_enemies():
            self._on_victory()
            return

        if self.all_downed():
            self.state = "ALL_OUT"
            self._log("💥 ALL enemies downed! **ALL-OUT ATTACK** available!")
            return

        self._enemy_turn()

        if not self.alive_players():
            self.state = "WIPED"
            self._log("💀 **THE PARTY WAS WIPED OUT.**")

    def _execute_player_move(self, uid, p, move):
        if move["action"] == "guard":
            p["buffs"]["def"] = min(2, p["buffs"].get("def", 0) + 1)
            heal = max(5, p["max_hp"] // 12)
            p["hp"] = min(p["max_hp"], p["hp"] + heal)
            self._log(f"🛡️ {p['user'].display_name} guards (+1 DEF, +{heal} HP)")
            return

        skill_name = move["skill"]
        s = SKILLS.get(skill_name, SKILLS["Slash"])
        if p["sp"] < s["cost"]:
            self._log(f"😓 {p['user'].display_name}: no SP — falls back to Slash")
            skill_name = "Slash"
            s = SKILLS["Slash"]
        p["sp"] = max(0, p["sp"] - s["cost"])
        target_idx = move.get("target_idx", 0)
        earned_one_more = False

        # ── Heal ──
        if s["type"] == "heal":
            if s["target"] == "party":
                targets = list(self.party.values())
            elif s["target"] == "ally":
                ap = self.alive_players()
                tuid = ap[target_idx % len(ap)] if ap else uid
                targets = [self.party[tuid]]
            else:  # self
                targets = [p]
            for t in targets:
                t["hp"] = min(t["max_hp"], t["hp"] + s["power"])
                t["downed"] = False
            self._log(f"💚 {p['user'].display_name} → {skill_name}")
            return

        # ── Status kaja/nda ──
        if s["type"] == "status":
            skl = skill_name.lower()
            if "kaja" in skl:
                stat = {"tarukaja": "atk", "rakukaja": "def", "sukukaja": "agi"}.get(skl, "atk")
                p["buffs"][stat] = min(2, p["buffs"].get(stat, 0) + 1)
                self._log(f"⬆️ {p['user'].display_name} → {skill_name} → own {stat}+1")
            elif "nda" in skl:
                stat = {"tarunda": "atk", "rakunda": "def", "sukunda": "agi"}.get(skl, "atk")
                ae = self.alive_enemies()
                if ae:
                    e = self.enemies[ae[target_idx % len(ae)]]
                    e["buffs"][stat] = max(-2, e["buffs"].get(stat, 0) - 1)
                    self._log(f"⬇️ {p['user'].display_name} → {skill_name} → {e['name']} {stat}−1")
            return

        # ── Instakill (light/dark, power=0) ──
        if s["type"] in ("light", "dark") and s["power"] == 0:
            ae = self.alive_enemies()
            if not ae:
                return
            base_chance = INSTAKILL_BASE.get(skill_name, 0.20)
            kill_tgts = ae if s["target"] == "all" else [ae[target_idx % len(ae)]]
            for ei in kill_tgts:
                e = self.enemies[ei]
                aff = e.get("affinities", {}).get(s["type"])
                if aff in ("null", "drain"):
                    self._log(f"🚫 {skill_name} nullified by {e['name']}!")
                    continue
                if aff == "repel":
                    p["hp"] = 0
                    self._log(f"↩️ {skill_name} REPELLED! {p['user'].display_name} KO'd!")
                    continue
                chance = base_chance * (2.0 if aff == "weak" else 1.0)
                if random.random() < chance:
                    self._log(f"☠️ {skill_name} INSTAKILLS {e['name']}!")
                    e["hp"] = 0
                    earned_one_more = True
                else:
                    self._log(f"🎲 {skill_name} missed {e['name']}…")
            if earned_one_more:
                p["one_more"] = True
                self._log(f"✨ {p['user'].display_name} earns ONE MORE!")
            return

        # ── Damage ──
        ae = self.alive_enemies()
        if not ae:
            return
        if s["target"] == "all":
            tgts = list(ae)
        elif s["target"] == "random":
            tgts = [random.choice(ae) for _ in range(s["hits"])]
        else:
            tgts = [ae[target_idx % len(ae)]]

        for ei in tgts:
            e = self.enemies[ei]
            if e["hp"] <= 0:
                continue
            atk_stats = {
                "atk": max(1, p["atk"] + p["buffs"].get("atk", 0) * 3),
                "mag": max(1, p["mag"] + p["buffs"].get("mag", 0) * 3),
            }
            dmg, aff = calc_damage(skill_name, atk_stats, e.get("affinities", {}))
            # Apply enemy def debuff
            def_buf = e["buffs"].get("def", 0)
            if def_buf < 0:
                dmg = int(dmg * (1 + abs(def_buf) * 0.25))

            if aff == "null":
                self._log(f"🚫 {e['name']} nullifies {skill_name}!")
                continue
            if dmg < 0:
                p["hp"] = max(0, p["hp"] - abs(dmg))
                if aff == "drain":
                    e["hp"] = min(e["max_hp"], e["hp"] + abs(dmg))
                    self._log(f"🔁 {e['name']} DRAINS {skill_name}! healed {abs(dmg)}")
                else:
                    self._log(f"↩️ REPELLED! {p['user'].display_name} −{abs(dmg)}")
                continue

            e["hp"] = max(0, e["hp"] - dmg)
            lbl = aff_label(aff)
            self._log(f"✨ {p['user'].display_name}→{skill_name}→{e['name']}: {dmg}" + (f" **{lbl}**" if lbl else ""))

            if aff == "weak" and e["hp"] > 0 and not e["downed"]:
                e["downed"] = True
                self._log(f"💢 {e['name']} knocked DOWN!")
                earned_one_more = True

            # Boss phase 2
            thresh = e.get("phase2_hp_thresh", 0)
            if not e.get("phase2") and thresh and e["max_hp"] and e["hp"] / e["max_hp"] < thresh:
                e["phase2"] = True
                e["skills"] = list(e.get("phase2_skills", e["skills"]))
                self._log(e.get("phase2_msg", f"⚠️ {e['name']} powers up!"))

        if earned_one_more:
            p["one_more"] = True
            self._log(f"✨ {p['user'].display_name} earns ONE MORE!")

    def _enemy_turn(self):
        for ei in self.alive_enemies():
            e = self.enemies[ei]
            if not self.alive_players():
                break
            if e["downed"]:
                e["downed"] = False
                self._log(f"🔄 {e['name']} recovers.")
                continue
            skill_name = random.choice(e["skills"])
            s = SKILLS.get(skill_name)
            if not s:
                continue
            if s["type"] == "heal":
                amt = int((s["power"] / 100) * e["mag"] * 3)
                e["hp"] = min(e["max_hp"], e["hp"] + amt)
                self._log(f"💚 {e['name']} uses {skill_name}, healed {amt}")
                continue
            if s["type"] == "status":
                continue
            atk_buf = e.get("buffs", {}).get("atk", 0)
            atk_stats = {"atk": max(1, e["atk"] + atk_buf * 3), "mag": max(1, e["mag"] + atk_buf * 3)}
            tgt_uids = self.alive_players() if s["target"] == "all" else [random.choice(self.alive_players())]
            for t_uid in tgt_uids:
                tp = self.party[t_uid]
                def_val = tp.get("buffs", {}).get("def", 0)
                dmg, _ = calc_damage(skill_name, atk_stats, {})
                if def_val > 0:
                    dmg = int(dmg * (1 - def_val * 0.20))
                dmg = max(1, dmg)
                tp["hp"] = max(0, tp["hp"] - dmg)
                self._log(f"⚔️ {e['name']}→{skill_name}→{tp['user'].display_name}: {dmg}")
                if s["type"] == "fire"  and random.random() < 0.15 and not tp["status"]:
                    tp["status"] = "burn";   tp["status_dur"] = 2; self._log(f"🔥 {tp['user'].display_name} burned!")
                elif s["type"] == "dark" and random.random() < 0.15 and not tp["status"]:
                    tp["status"] = "poison"; tp["status_dur"] = 3; self._log(f"☠️ {tp['user'].display_name} poisoned!")
                elif s["type"] == "elec" and random.random() < 0.20 and not tp["status"]:
                    tp["status"] = "shock";  tp["status_dur"] = 1; self._log(f"⚡ {tp['user'].display_name} shocked!")

    def _reward(self, e):
        e["_rewarded"] = True
        self.gold += e.get("gold", 50)
        xp = e.get("xp", 20)
        self._log(f"💀 {e['name']} defeated! +${e.get('gold',50)} +{xp}XP")
        for uid in self.alive_players():
            p = self.party[uid]
            p["xp"] += xp
            while p["xp"] >= p["max_xp"]:
                p["xp"] -= p["max_xp"]
                p["level"] += 1
                p["max_xp"] = int(p["max_xp"] * 1.5)
                p["pending_level"] = p.get("pending_level", 0) + 1
                self._log(f"🌟 {p['user'].display_name} → Lv.{p['level']}!")

    def _on_victory(self):
        self._log("🎉 All enemies defeated!")
        self.floor += 1
        self.state = "EXPLORE"
        self.generate_floor()
        for p in self.party.values():
            p["sp"]     = min(p["max_sp"], p["sp"] + max(5, p["max_sp"] // 8))
            p["buffs"]  = {}
            p["downed"] = False
            p["one_more"] = False


# ─────────────────────────────────────────────
#  SKILL PICKER (ephemeral)
# ─────────────────────────────────────────────
class SkillPickerView(discord.ui.View):
    def __init__(self, session: PersonaSession, uid: int):
        super().__init__(timeout=60)
        self.session = session
        self.uid     = uid
        p = session.party[uid]
        options = []
        for skill_name in p["skills"]:
            s = SKILLS.get(skill_name)
            if not s:
                continue
            no_sp = p["sp"] < s["cost"]
            options.append(discord.SelectOption(
                label=skill_name + (" [No SP]" if no_sp else ""),
                description=f"{s['desc']} (SP:{s['cost']})"[:100],
                emoji=ELEMENT_EMOJI.get(s["type"], "✨"),
                value=skill_name
            ))
        sel = discord.ui.Select(placeholder="Choose a skill…", options=options)
        sel.callback = self._on_skill
        self.add_item(sel)

    async def _on_skill(self, interaction: discord.Interaction):
        skill_name = self.children[0].values[0]
        s  = SKILLS[skill_name]
        ae = self.session.alive_enemies()

        needs_enemy = (s["target"] == "one" and s["type"] != "heal" and ae) or \
                      (s["type"] == "status" and "nda" in skill_name.lower() and ae)
        needs_ally  = s["target"] == "ally"

        if needs_ally:
            ap = self.session.alive_players()
            view = TargetPickerView(self.session, self.uid, skill_name, list(range(len(ap))), "ally")
            await interaction.response.edit_message(content="**Select ally:**", view=view)
        elif needs_enemy:
            view = TargetPickerView(self.session, self.uid, skill_name, ae, "enemy")
            await interaction.response.edit_message(content="**Select target:**", view=view)
        else:
            # No targeting needed — submit immediately
            await interaction.response.defer()
            await self.session.submit_move(
                self.uid,
                {"action": "skill", "skill": skill_name, "target_idx": 0},
                interaction
            )


# ─────────────────────────────────────────────
#  TARGET PICKER (ephemeral, replaces skill picker via edit_message)
# ─────────────────────────────────────────────
class TargetPickerView(discord.ui.View):
    def __init__(self, session: PersonaSession, uid: int, skill_name: str, indices, mode: str):
        super().__init__(timeout=60)
        self.session    = session
        self.uid        = uid
        self.skill_name = skill_name
        sk = SKILLS.get(skill_name)

        options = []
        if mode == "enemy":
            for i, ei in enumerate(indices):
                e   = session.enemies[ei]
                aff = e.get("affinities", {}).get(sk["type"]) if sk else None
                lbl = f" [{aff_label(aff)}]" if aff else ""
                options.append(discord.SelectOption(
                    label=f"[{i+1}] {e['name']} HP:{e['hp']}{lbl}",
                    value=str(i), emoji=e["emoji"]
                ))
        else:
            ap = session.alive_players()
            for i, puid in enumerate(ap):
                p2 = session.party[puid]
                options.append(discord.SelectOption(
                    label=f"{p2['user'].display_name} HP:{p2['hp']}/{p2['max_hp']}",
                    value=str(i), emoji=p2["emoji"]
                ))

        sel = discord.ui.Select(placeholder="Select target…", options=options)
        sel.callback = self._on_target
        self.add_item(sel)

    async def _on_target(self, interaction: discord.Interaction):
        idx = int(self.children[0].values[0])
        await interaction.response.defer()
        await self.session.submit_move(
            self.uid,
            {"action": "skill", "skill": self.skill_name, "target_idx": idx},
            interaction
        )


# ─────────────────────────────────────────────
#  LEVEL UP VIEW
# ─────────────────────────────────────────────
class PersonaLevelUpView(discord.ui.View):
    def __init__(self, session: PersonaSession, uid: int):
        super().__init__(timeout=60)
        self.session = session
        self.uid     = uid
        options = [
            discord.SelectOption(label="Raise HP",        emoji="❤️",  value="hp",  description="Max HP +20 (scales w/ level)"),
            discord.SelectOption(label="Raise SP",        emoji="💙",  value="sp",  description="Max SP +15"),
            discord.SelectOption(label="Raise Attack",    emoji="⚔️",  value="atk", description="Physical ATK boost"),
            discord.SelectOption(label="Raise Magic",     emoji="🔮",  value="mag", description="Spell MAG boost"),
            discord.SelectOption(label="Raise Endurance", emoji="🛡️",  value="end", description="Resilience"),
            discord.SelectOption(label="Raise Agility",   emoji="💨",  value="agi", description="Speed"),
        ]
        sel = discord.ui.Select(placeholder="Choose a stat…", options=options)
        sel.callback = self._on_choice
        self.add_item(sel)

    async def _on_choice(self, interaction: discord.Interaction):
        async with self.session.lock:
            p = self.session.party[self.uid]
            if p.get("pending_level", 0) <= 0:
                return await interaction.response.send_message("❌ No pending level ups.", ephemeral=True)
            p["pending_level"] -= 1
            lvl = p["level"]
            choice = self.children[0].values[0]
            if choice == "hp":
                g = 20 + lvl * 4; p["max_hp"] += g; p["hp"] = min(p["max_hp"], p["hp"] + g); msg = f"+{g} Max HP"
            elif choice == "sp":
                g = 15 + lvl * 2; p["max_sp"] += g; p["sp"] = min(p["max_sp"], p["sp"] + g); msg = f"+{g} Max SP"
            elif choice == "atk":
                g = 3 + lvl; p["atk"] += g; msg = f"+{g} ATK"
            elif choice == "mag":
                g = 3 + lvl; p["mag"] += g; msg = f"+{g} MAG"
            elif choice == "end":
                g = 2 + lvl // 2; p["end"] += g; msg = f"+{g} END"
            else:
                g = 2 + lvl // 2; p["agi"] += g; msg = f"+{g} AGI"
            await interaction.response.send_message(f"🌟 {msg}!", ephemeral=True)
            await self.session._refresh()


# ─────────────────────────────────────────────
#  LOBBY
# ─────────────────────────────────────────────
class PersonaLobby(discord.ui.View):
    def __init__(self, host):
        super().__init__(timeout=300)
        self.host    = host
        self.party   = [host]
        self.classes = {}

    @discord.ui.button(label="Join Party", style=discord.ButtonStyle.primary, emoji="✋", row=0)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in self.party:
            return await interaction.response.send_message("❌ Already in party.", ephemeral=True)
        if len(self.party) >= 4:
            return await interaction.response.send_message("❌ Party full (max 4).", ephemeral=True)
        if interaction.user.id in active_persona_runs:
            return await interaction.response.send_message("❌ Already in an active run.", ephemeral=True)
        self.party.append(interaction.user)
        embed = interaction.message.embeds[0]
        embed.description += f"\n✅ {interaction.user.mention} joined!"
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Pick My Class", style=discord.ButtonStyle.secondary, emoji="🃏", row=0)
    async def btn_class(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user not in self.party:
            return await interaction.response.send_message("❌ Join first!", ephemeral=True)
        await interaction.response.send_message("**Choose your Arcana:**", view=ClassSelectView(self, interaction.user.id), ephemeral=True)

    @discord.ui.button(label="Start Battle", style=discord.ButtonStyle.success, emoji="⚔️", row=1)
    async def btn_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.host.id:
            return await interaction.response.send_message("❌ Only the host can start.", ephemeral=True)
        not_ready = [u for u in self.party if u.id not in self.classes]
        if not_ready:
            return await interaction.response.send_message(
                f"❌ {', '.join(u.display_name for u in not_ready)} hasn't picked a class!", ephemeral=True)
        await interaction.response.defer()
        for user in self.party:
            active_persona_runs.add(user.id)
        session = PersonaSession(self.party, self.classes)
        await interaction.edit_original_response(embed=session.get_embed(), view=session)
        session.message = await interaction.original_response()
        self.stop()


class ClassSelectView(discord.ui.View):
    def __init__(self, lobby: PersonaLobby, uid: int):
        super().__init__(timeout=120)
        self.lobby = lobby
        self.uid   = uid
        options = [
            discord.SelectOption(label=name, description=data["desc"][:100], emoji=data["emoji"], value=name)
            for name, data in PERSONA_CLASSES.items()
        ]
        sel = discord.ui.Select(placeholder="Choose your Arcana…", options=options)
        sel.callback = self._on_select
        self.add_item(sel)

    async def _on_select(self, interaction: discord.Interaction):
        cls_name = self.children[0].values[0]
        self.lobby.classes[self.uid] = cls_name
        cls = PERSONA_CLASSES[cls_name]
        await interaction.response.send_message(
            f"✅ **{cls['emoji']} {cls_name}** selected!\nSkills: {', '.join(cls['skills'])}", ephemeral=True)


# ─────────────────────────────────────────────
#  COG
# ─────────────────────────────────────────────
class PersonaRPG(commands.GroupCog, group_name="persona", group_description="Persona-style co-op RPG (test, no DB)."):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="play", description="Open a Persona dungeon lobby.")
    async def play(self, interaction: discord.Interaction):
        if interaction.user.id in active_persona_runs:
            return await interaction.response.send_message("❌ Already in a run! Use /persona abort.", ephemeral=True)
        class_list = "\n".join(f"{d['emoji']} **{n}** — {d['desc']}" for n, d in PERSONA_CLASSES.items())
        embed = discord.Embed(
            title="🃏 Persona Dungeon — Lobby",
            description=(f"**Host:** {interaction.user.mention}\n\n"
                         "1️⃣ **Join Party** — up to 4 players\n"
                         "2️⃣ **Pick My Class** — choose your Arcana\n"
                         "3️⃣ Host clicks **Start Battle** when ready\n\n"
                         f"**Arcanas:**\n{class_list}"),
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=PersonaLobby(interaction.user))

    @app_commands.command(name="skills", description="Browse all skills.")
    async def skills(self, interaction: discord.Interaction):
        embed = discord.Embed(title="📖 Skill Compendium", color=0x9b59b6)
        by_type = defaultdict(list)
        for name, s in SKILLS.items():
            by_type[s["type"]].append((name, s))
        for etype, entries in sorted(by_type.items()):
            val = "\n".join(f"`{n}` SP:{s['cost']} — {s['desc']}" for n, s in entries)
            embed.add_field(name=f"{ELEMENT_EMOJI.get(etype,'✨')} {etype.title()}", value=val[:1024], inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="classes", description="View all Arcana classes.")
    async def classes(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🃏 Arcana Classes", color=0xe67e22)
        for name, data in PERSONA_CLASSES.items():
            embed.add_field(
                name=f"{data['emoji']} {name}",
                value=(f"{data['desc']}\n"
                       f"HP:{data['hp']} SP:{data['sp']} ATK:{data['atk']} MAG:{data['mag']} END:{data['end']} AGI:{data['agi']}\n"
                       f"Skills: {', '.join(data['skills'])}"),
                inline=False
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="abort", description="Force-exit an active run.")
    async def abort(self, interaction: discord.Interaction):
        if interaction.user.id in active_persona_runs:
            active_persona_runs.discard(interaction.user.id)
            await interaction.response.send_message("🚨 Aborted. You can start a new run.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Not in an active run.", ephemeral=True)


async def setup(bot):
    await bot.add_cog(PersonaRPG(bot))
