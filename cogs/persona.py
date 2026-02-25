import discord
from discord import app_commands
from discord.ext import commands
import random
import asyncio
from collections import defaultdict

# ─────────────────────────────────────────────
#  ELEMENTS & AFFINITIES
# ─────────────────────────────────────────────
ELEMENTS = ["fire", "ice", "elec", "wind", "light", "dark", "phys", "almighty"]

ELEMENT_EMOJI = {
    "fire":      "🔥",
    "ice":       "❄️",
    "elec":      "⚡",
    "wind":      "🌪️",
    "light":     "✨",
    "dark":      "🌑",
    "phys":      "⚔️",
    "almighty":  "💫",
    "heal":      "💚",
    "status":    "🌀",
}

# Affinity values per enemy: "weak", "resist", "null", "repel", "drain", or None (normal)
# ─────────────────────────────────────────────
#  SKILL DEFINITIONS
# ─────────────────────────────────────────────
SKILLS = {
    # ── Physical ──────────────────────────────
    "Slash":        {"type": "phys",     "power": 80,   "hits": 1, "cost": 0,  "target": "one",   "desc": "Light phys dmg to one foe."},
    "Heavy Slash":  {"type": "phys",     "power": 140,  "hits": 1, "cost": 10, "target": "one",   "desc": "Medium phys dmg to one foe."},
    "Cleave":       {"type": "phys",     "power": 70,   "hits": 1, "cost": 8,  "target": "all",   "desc": "Light phys dmg to all foes."},
    "Rampage":      {"type": "phys",     "power": 55,   "hits": 3, "cost": 18, "target": "random","desc": "3 hits of phys dmg, random targets."},
    "God's Hand":   {"type": "phys",     "power": 260,  "hits": 1, "cost": 28, "target": "one",   "desc": "Massive phys dmg to one foe."},
    # ── Fire ──────────────────────────────────
    "Agi":          {"type": "fire",     "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light fire dmg to one foe."},
    "Agilao":       {"type": "fire",     "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium fire dmg to one foe."},
    "Agidyne":      {"type": "fire",     "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy fire dmg to one foe."},
    "Maragi":       {"type": "fire",     "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light fire dmg to all foes."},
    "Maragidyne":   {"type": "fire",     "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy fire dmg to all foes."},
    # ── Ice ───────────────────────────────────
    "Bufu":         {"type": "ice",      "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light ice dmg to one foe."},
    "Bufula":       {"type": "ice",      "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium ice dmg to one foe."},
    "Bufudyne":     {"type": "ice",      "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy ice dmg to one foe."},
    "Mabufu":       {"type": "ice",      "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light ice dmg to all foes."},
    "Mabufudyne":   {"type": "ice",      "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy ice dmg to all foes."},
    # ── Electric ──────────────────────────────
    "Zio":          {"type": "elec",     "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light elec dmg to one foe."},
    "Zionga":       {"type": "elec",     "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium elec dmg to one foe."},
    "Ziodyne":      {"type": "elec",     "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy elec dmg to one foe."},
    "Mazio":        {"type": "elec",     "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light elec dmg to all foes."},
    "Maziodyne":    {"type": "elec",     "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy elec dmg to all foes."},
    # ── Wind ──────────────────────────────────
    "Garu":         {"type": "wind",     "power": 80,   "hits": 1, "cost": 4,  "target": "one",   "desc": "Light wind dmg to one foe."},
    "Garula":       {"type": "wind",     "power": 140,  "hits": 1, "cost": 8,  "target": "one",   "desc": "Medium wind dmg to one foe."},
    "Garudyne":     {"type": "wind",     "power": 210,  "hits": 1, "cost": 14, "target": "one",   "desc": "Heavy wind dmg to one foe."},
    "Magaru":       {"type": "wind",     "power": 70,   "hits": 1, "cost": 10, "target": "all",   "desc": "Light wind dmg to all foes."},
    "Magarudyne":   {"type": "wind",     "power": 170,  "hits": 1, "cost": 22, "target": "all",   "desc": "Heavy wind dmg to all foes."},
    # ── Light ─────────────────────────────────
    "Hama":         {"type": "light",    "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Low chance instant kill (light). Weak targets: high chance."},
    "Hamaon":       {"type": "light",    "power": 0,    "hits": 1, "cost": 14, "target": "one",   "desc": "Med chance instant kill (light)."},
    "Mahama":       {"type": "light",    "power": 0,    "hits": 1, "cost": 16, "target": "all",   "desc": "Low chance instant kill all (light)."},
    "Kougaon":      {"type": "light",    "power": 200,  "hits": 1, "cost": 18, "target": "one",   "desc": "Heavy light dmg + instakill chance."},
    # ── Dark ──────────────────────────────────
    "Mudo":         {"type": "dark",     "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Low chance instant kill (dark)."},
    "Mudoon":       {"type": "dark",     "power": 0,    "hits": 1, "cost": 14, "target": "one",   "desc": "Med chance instant kill (dark)."},
    "Mamudo":       {"type": "dark",     "power": 0,    "hits": 1, "cost": 16, "target": "all",   "desc": "Low chance instant kill all (dark)."},
    "Eigaon":       {"type": "dark",     "power": 200,  "hits": 1, "cost": 18, "target": "one",   "desc": "Heavy dark dmg + instakill chance."},
    # ── Almighty ──────────────────────────────
    "Megidola":     {"type": "almighty", "power": 160,  "hits": 1, "cost": 24, "target": "all",   "desc": "Heavy almighty dmg to all foes."},
    "Megidolaon":   {"type": "almighty", "power": 260,  "hits": 1, "cost": 36, "target": "all",   "desc": "Severe almighty dmg to all foes."},
    # ── Healing ───────────────────────────────
    "Dia":          {"type": "heal",     "power": 60,   "hits": 1, "cost": 4,  "target": "self",  "desc": "Restore a small amount of HP."},
    "Diarama":      {"type": "heal",     "power": 120,  "hits": 1, "cost": 8,  "target": "self",  "desc": "Restore a moderate amount of HP."},
    "Diarahan":     {"type": "heal",     "power": 9999, "hits": 1, "cost": 18, "target": "self",  "desc": "Fully restore one ally's HP."},
    "Media":        {"type": "heal",     "power": 50,   "hits": 1, "cost": 10, "target": "party", "desc": "Restore HP to all party members."},
    "Mediarahan":   {"type": "heal",     "power": 9999, "hits": 1, "cost": 28, "target": "party", "desc": "Fully restore all party members' HP."},
    # ── Status ────────────────────────────────
    "Tarukaja":     {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "self",  "desc": "Raise one ally's Attack."},
    "Rakukaja":     {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "self",  "desc": "Raise one ally's Defense."},
    "Sukukaja":     {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "self",  "desc": "Raise one ally's Accuracy/Evasion."},
    "Tarunda":      {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Lower one foe's Attack."},
    "Rakunda":      {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Lower one foe's Defense."},
    "Sukunda":      {"type": "status",   "power": 0,    "hits": 1, "cost": 6,  "target": "one",   "desc": "Lower one foe's Accuracy/Evasion."},
}

# ─────────────────────────────────────────────
#  PERSONA CLASSES (Arcana)
# ─────────────────────────────────────────────
PERSONA_CLASSES = {
    "Fool":       {
        "emoji": "🃏", "hp": 120, "sp": 80,
        "atk": 12, "mag": 8, "end": 8, "agi": 8,
        "skills": ["Slash", "Agi", "Bufu", "Dia"],
        "desc": "Balanced beginner. Wields fire & ice.",
    },
    "Magician":   {
        "emoji": "🔮", "hp": 90, "sp": 120,
        "atk": 6, "mag": 18, "end": 6, "agi": 10,
        "skills": ["Agi", "Agilao", "Zio", "Maragi", "Media"],
        "desc": "High magic. Fire & electric specialist.",
    },
    "Chariot":    {
        "emoji": "⚔️", "hp": 150, "sp": 60,
        "atk": 20, "mag": 4, "end": 12, "agi": 14,
        "skills": ["Slash", "Heavy Slash", "Rampage", "God's Hand", "Rakukaja"],
        "desc": "Physical powerhouse. Pure melee.",
    },
    "Priestess":  {
        "emoji": "🌙", "hp": 100, "sp": 130,
        "atk": 6, "mag": 14, "end": 10, "agi": 8,
        "skills": ["Bufu", "Bufula", "Mabufu", "Dia", "Diarama", "Media", "Mediarahan"],
        "desc": "Ice & healer. Support specialist.",
    },
    "Emperor":    {
        "emoji": "👑", "hp": 130, "sp": 90,
        "atk": 14, "mag": 10, "end": 14, "agi": 8,
        "skills": ["Heavy Slash", "Zionga", "Tarukaja", "Rakukaja", "Tarunda"],
        "desc": "Melee & buffs. Tactical fighter.",
    },
    "Tower":      {
        "emoji": "🌩️", "hp": 100, "sp": 110,
        "atk": 8, "mag": 16, "end": 8, "agi": 12,
        "skills": ["Zio", "Zionga", "Ziodyne", "Mazio", "Maziodyne", "Sukunda"],
        "desc": "Electric specialist. Debuff & shock.",
    },
    "Hermit":     {
        "emoji": "🌿", "hp": 100, "sp": 110,
        "atk": 8, "mag": 15, "end": 8, "agi": 14,
        "skills": ["Garu", "Garula", "Garudyne", "Magaru", "Sukukaja", "Sukunda"],
        "desc": "Wind specialist. Evasion & speed.",
    },
    "Death":      {
        "emoji": "💀", "hp": 95, "sp": 120,
        "atk": 10, "mag": 16, "end": 6, "agi": 10,
        "skills": ["Mudo", "Mudoon", "Mamudo", "Eigaon", "Tarunda", "Rakunda"],
        "desc": "Dark arts. Instant kill specialist.",
    },
    "Judgement":  {
        "emoji": "☀️", "hp": 95, "sp": 120,
        "atk": 10, "mag": 16, "end": 6, "agi": 10,
        "skills": ["Hama", "Hamaon", "Mahama", "Kougaon", "Rakukaja", "Media"],
        "desc": "Light arts. Instant kill specialist.",
    },
    "World":      {
        "emoji": "🌌", "hp": 110, "sp": 130,
        "atk": 10, "mag": 18, "end": 8, "agi": 10,
        "skills": ["Megidola", "Megidolaon", "Diarahan", "Mediarahan", "Tarukaja", "Rakukaja"],
        "desc": "Almighty magic & full heals.",
    },
}

# ─────────────────────────────────────────────
#  ENEMY DEFINITIONS
# ─────────────────────────────────────────────
# affinities: element -> "weak" / "resist" / "null" / "repel" / "drain"
PERSONA_ENEMIES = [
    {
        "name": "Pyro Jack",      "emoji": "🎃",
        "hp": 60,  "atk": 14, "def": 6,  "mag": 10,
        "affinities": {"fire": "null", "ice": "weak", "phys": None},
        "skills": ["Agi", "Maragi"],
        "xp": 30, "gold": 80,
    },
    {
        "name": "Frost",          "emoji": "🧊",
        "hp": 60,  "atk": 10, "def": 8,  "mag": 12,
        "affinities": {"ice": "drain", "fire": "weak", "elec": None},
        "skills": ["Bufu", "Mabufu"],
        "xp": 30, "gold": 80,
    },
    {
        "name": "Zionga Wisp",    "emoji": "⚡",
        "hp": 55,  "atk": 10, "def": 5,  "mag": 14,
        "affinities": {"elec": "null", "wind": "weak"},
        "skills": ["Zio", "Zionga"],
        "xp": 28, "gold": 75,
    },
    {
        "name": "Shadow Knight",  "emoji": "🛡️",
        "hp": 90,  "atk": 18, "def": 14, "mag": 4,
        "affinities": {"phys": "resist", "light": "weak", "dark": "null"},
        "skills": ["Slash", "Heavy Slash", "Rakukaja"],
        "xp": 40, "gold": 100,
    },
    {
        "name": "Mist Wisp",      "emoji": "🌫️",
        "hp": 50,  "atk": 8,  "def": 4,  "mag": 12,
        "affinities": {"wind": "null", "elec": "weak", "phys": "resist"},
        "skills": ["Garu", "Garula", "Sukunda"],
        "xp": 25, "gold": 65,
    },
    {
        "name": "Undead Soldier", "emoji": "💀",
        "hp": 75,  "atk": 15, "def": 8,  "mag": 4,
        "affinities": {"light": "weak", "dark": "drain", "phys": None},
        "skills": ["Slash", "Heavy Slash"],
        "xp": 35, "gold": 90,
    },
    {
        "name": "Succubus",       "emoji": "😈",
        "hp": 65,  "atk": 10, "def": 6,  "mag": 16,
        "affinities": {"dark": "null", "light": "weak", "fire": "resist"},
        "skills": ["Eigaon", "Tarunda", "Rakunda"],
        "xp": 38, "gold": 95,
    },
    {
        "name": "Seraph Fragment","emoji": "🕊️",
        "hp": 65,  "atk": 8,  "def": 8,  "mag": 14,
        "affinities": {"light": "drain", "dark": "weak", "phys": None},
        "skills": ["Kougaon", "Hamaon", "Diarama"],
        "xp": 38, "gold": 95,
    },
    {
        "name": "Ose",            "emoji": "🦁",
        "hp": 80,  "atk": 20, "def": 10, "mag": 8,
        "affinities": {"fire": None, "ice": None, "phys": None},
        "skills": ["Slash", "Rampage", "Tarukaja"],
        "xp": 42, "gold": 110,
    },
    {
        "name": "Nue",            "emoji": "🐉",
        "hp": 70,  "atk": 12, "def": 6,  "mag": 16,
        "affinities": {"elec": "weak", "wind": "resist"},
        "skills": ["Agi", "Bufu", "Mudo"],
        "xp": 42, "gold": 110,
    },
]

PERSONA_BOSSES = [
    {
        "name": "Arcana Magician", "emoji": "🔮",
        "hp": 400, "atk": 20, "def": 10, "mag": 24,
        "affinities": {"fire": "drain", "ice": "weak", "elec": "null"},
        "skills": ["Agilao", "Agidyne", "Maragidyne", "Tarunda", "Rakunda"],
        "xp": 200, "gold": 600, "boss": True,
        "phase2_hp_thresh": 0.4,
        "phase2_skills": ["Megidola", "Agidyne", "Maragidyne"],
        "phase2_msg": "🔥 Arcana Magician ENTERS RAGE MODE! Magic surges wildly!",
    },
    {
        "name": "Shadow Self",    "emoji": "🌑",
        "hp": 500, "atk": 22, "def": 12, "mag": 20,
        "affinities": {"almighty": "resist", "phys": None},
        "skills": ["Eigaon", "Mudoon", "Tarunda", "Rakunda", "Megidola"],
        "xp": 250, "gold": 800, "boss": True,
        "phase2_hp_thresh": 0.5,
        "phase2_skills": ["Megidolaon", "Mamudo", "Mahama"],
        "phase2_msg": "🌑 Shadow Self awakens its true power! ALL SKILLS UNLEASHED!",
    },
    {
        "name": "Pale Rider",     "emoji": "🐎",
        "hp": 450, "atk": 30, "def": 8,  "mag": 18,
        "affinities": {"phys": "resist", "light": "weak", "dark": "null"},
        "skills": ["God's Hand", "Rampage", "Eigaon", "Tarunda"],
        "xp": 220, "gold": 700, "boss": True,
        "phase2_hp_thresh": 0.35,
        "phase2_skills": ["God's Hand", "Megidola", "Rampage"],
        "phase2_msg": "🐎 Pale Rider charges! Death follows in its wake!",
    },
]

# ─────────────────────────────────────────────
#  HELPER: build a live enemy dict from template
# ─────────────────────────────────────────────
def spawn_enemy(template, floor=1, party_size=1):
    scale = 1.0 + (floor * 0.06) + ((floor ** 2) * 0.001)
    e = dict(template)
    e["hp"]      = max(1, int(e["hp"]  * scale * (0.7 + 0.3 * party_size)))
    e["max_hp"]  = e["hp"]
    e["atk"]     = max(1, int(e["atk"] * scale))
    e["def"]     = max(0, int(e["def"] * scale))
    e["mag"]     = max(1, int(e["mag"] * scale))
    e["downed"]  = False      # knockdown flag
    e["buffs"]   = {}         # stat modifications
    e["phase2"]  = False
    e["skills"]  = list(template["skills"])  # mutable copy
    return e

# ─────────────────────────────────────────────
#  DAMAGE CALCULATION
# ─────────────────────────────────────────────
INSTAKILL_BASE = {"Hama": 0.15, "Hamaon": 0.35, "Mahama": 0.20,
                   "Mudo": 0.15, "Mudoon": 0.35, "Mamudo": 0.20}
INSTAKILL_BONUS = {"Kougaon": 0.25, "Eigaon": 0.25}

AFFINITY_MULT = {
    "weak":   2.0,
    None:     1.0,
    "resist": 0.5,
    "null":   0.0,
    "repel":  -1.0,   # reflected back
    "drain":  -1.0,   # heals enemy (same sign for us)
}

def calc_damage(skill, attacker_stats, target_affinity):
    """
    Returns (dmg, affinity_result_str)
    dmg can be negative (repel / drain reflects back at caster).
    """
    s = SKILLS[skill]
    etype = s["type"]
    power = s["power"]

    aff = target_affinity.get(etype, None) if isinstance(target_affinity, dict) else None
    mult = AFFINITY_MULT.get(aff, 1.0)

    is_magic = etype not in ("phys", "almighty")
    base_stat = attacker_stats["mag"] if is_magic else attacker_stats["atk"]

    # damage formula (loosely Persona-inspired)
    raw = int((power / 100) * base_stat * 4 * (random.uniform(0.9, 1.1)))
    dmg = int(raw * abs(mult))

    # Apply buffs to attacker (atk_up / atk_down)
    if etype == "phys":
        buf = attacker_stats.get("buffs", {}).get("atk", 0)
    else:
        buf = attacker_stats.get("buffs", {}).get("mag", 0)
    dmg = max(1, int(dmg * (1 + buf * 0.25)))

    if mult < 0:
        return -dmg, aff  # repel/drain
    return dmg, aff


def affinity_label(aff):
    labels = {
        "weak":   "WEAK ‼",
        "resist": "Resist",
        "null":   "Null!",
        "repel":  "Repel!",
        "drain":  "Drain!",
        None:     "",
    }
    return labels.get(aff, "")


# ─────────────────────────────────────────────
#  ONE MORE ROUND / BATON PASS SYSTEM
# ─────────────────────────────────────────────
#  If a player hits a weakness or crits they earn an extra action ("One More").
#  If all enemies are downed, the party can do an All-Out Attack.

# ─────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────
active_persona_runs = set()


class PersonaSession(discord.ui.View):
    """Core battle engine."""

    def __init__(self, party_members, classes):
        super().__init__(timeout=600)
        self.lock = asyncio.Lock()
        self.sid = str(random.randint(1000, 9999))
        self.message = None
        self.floor = 1
        self.gold_earned = 0
        self.log_lines = []

        # Build party
        self.party = {}
        for user in party_members:
            cls_name = classes[user.id]
            cls = PERSONA_CLASSES[cls_name]
            self.party[user.id] = {
                "user": user,
                "class": cls_name,
                "emoji": cls["emoji"],
                "hp": cls["hp"],  "max_hp": cls["hp"],
                "sp": cls["sp"],  "max_sp": cls["sp"],
                "atk": cls["atk"], "mag": cls["mag"],
                "end": cls["end"], "agi": cls["agi"],
                "skills": list(cls["skills"]),
                "buffs": {},          # "atk", "def", "agi" -> int (-2..+2)
                "status": None,       # poison / burn / shock / dizzy
                "status_dur": 0,
                "level": 1, "xp": 0, "max_xp": 60,
                "pending_level": 0,
                "one_more": False,    # earned extra turn
                "downed": False,      # player knocked down
            }

        self.enemies = []            # list of live enemy dicts
        self.state = "EXPLORE"       # EXPLORE / COMBAT / ALL_OUT / LEVELUP / WIPED / ESCAPED
        self.pending_moves = {}      # uid -> {"skill": ..., "target_idx": ...}
        self.all_out_pending = False

        self.generate_floor()
        self._build_ui()

    # ── floor generation ───────────────────────────────────────────────
    def generate_floor(self):
        self.enemies = []
        if self.floor > 0 and self.floor % 10 == 0:
            boss_tmpl = random.choice(PERSONA_BOSSES)
            self.enemies.append(spawn_enemy(boss_tmpl, self.floor, len(self.party)))
            self.add_log(f"⚠️ **BOSS BATTLE — Floor {self.floor}!** The {self.enemies[0]['name']} {self.enemies[0]['emoji']} awaits!")
        else:
            count = random.randint(1, min(3, 1 + self.floor // 5))
            chosen = random.choices(PERSONA_ENEMIES, k=count)
            for tmpl in chosen:
                self.enemies.append(spawn_enemy(tmpl, self.floor, len(self.party)))
            names = ", ".join(f"{e['emoji']} {e['name']}" for e in self.enemies)
            self.add_log(f"🗺️ **Floor {self.floor}** — Encounter! {names}")

    # ── logging ────────────────────────────────────────────────────────
    def add_log(self, line):
        self.log_lines.append(line)
        if len(self.log_lines) > 12:
            self.log_lines = self.log_lines[-12:]

    # ── alive helpers ──────────────────────────────────────────────────
    def alive_players(self):
        return [uid for uid, p in self.party.items() if p["hp"] > 0]

    def alive_enemies(self):
        return [i for i, e in enumerate(self.enemies) if e["hp"] > 0]

    def all_enemies_downed(self):
        ae = self.alive_enemies()
        return len(ae) > 0 and all(self.enemies[i]["downed"] for i in ae)

    # ── embed ──────────────────────────────────────────────────────────
    def get_embed(self):
        color = {
            "COMBAT":   0xe74c3c,
            "ALL_OUT":  0xff6f00,
            "EXPLORE":  0x2c3e50,
            "WIPED":    0x000000,
            "ESCAPED":  0x27ae60,
        }.get(self.state, 0x2c3e50)

        embed = discord.Embed(
            title=f"{'☠️ BOSS' if any(e.get('boss') for e in self.enemies) else '⚔️ Floor'} {self.floor}",
            color=color
        )
        embed.description = "\n".join(self.log_lines[-6:]) or "…"
        embed.add_field(name="💰 Gold", value=f"${self.gold_earned:,}", inline=True)
        embed.add_field(name="🗺️ Floor", value=str(self.floor), inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)

        # Party
        for uid, p in self.party.items():
            hp_bar = self._bar(p["hp"], p["max_hp"])
            sp_bar = self._bar(p["sp"],  p["max_sp"], fill="🟦", empty="⬛")
            status_icon = {"poison": "☠️", "burn": "🔥", "shock": "⚡", "dizzy": "💫"}.get(p["status"], "")
            down_icon = " 💢DOWN" if p["downed"] else ""
            one_more_icon = " ✨ONE MORE" if p["one_more"] else ""
            buf_str = self._buff_str(p["buffs"])
            ready_icon = "✅" if uid in self.pending_moves else "⏳"
            embed.add_field(
                name=f"{p['emoji']} {p['user'].display_name} {status_icon}{down_icon}{one_more_icon} {ready_icon if self.state=='COMBAT' else ''}",
                value=(
                    f"❤️ {hp_bar} {p['hp']}/{p['max_hp']}\n"
                    f"💙 {sp_bar} {p['sp']}/{p['max_sp']}\n"
                    f"Lv.{p['level']} | {p['class']}{buf_str}"
                ),
                inline=True
            )

        # Enemies
        if self.enemies:
            for i, e in enumerate(self.enemies):
                if e["hp"] <= 0:
                    embed.add_field(name=f"~~{e['emoji']} {e['name']}~~", value="💀 Defeated", inline=True)
                else:
                    hp_bar = self._bar(e["hp"], e["max_hp"])
                    aff_str = self._affinity_str(e)
                    down_icon = " 💢DOWN" if e["downed"] else ""
                    buf_str = self._buff_str(e.get("buffs", {}))
                    embed.add_field(
                        name=f"[{i+1}] {e['emoji']} {e['name']}{down_icon}",
                        value=(
                            f"❤️ {hp_bar} {e['hp']}/{e['max_hp']}\n"
                            f"ATK:{e['atk']} DEF:{e['def']} MAG:{e['mag']}{buf_str}\n"
                            f"{aff_str}"
                        ),
                        inline=True
                    )

        return embed

    def _bar(self, cur, max_v, fill="🟥", empty="⬛", length=8):
        pct = max(0, cur / max_v) if max_v > 0 else 0
        filled = round(pct * length)
        return fill * filled + empty * (length - filled)

    def _buff_str(self, buffs):
        if not buffs:
            return ""
        parts = []
        icons = {"atk": "⚔️", "def": "🛡️", "agi": "💨", "mag": "🔮"}
        for k, v in buffs.items():
            if v > 0:
                parts.append(f"{icons.get(k,k)}+{v}")
            elif v < 0:
                parts.append(f"{icons.get(k,k)}{v}")
        return " " + " ".join(parts) if parts else ""

    def _affinity_str(self, e):
        affs = e.get("affinities", {})
        parts = []
        for el, val in affs.items():
            if val:
                emoji = ELEMENT_EMOJI.get(el, el)
                parts.append(f"{emoji}{val[0].upper()}")
        return " ".join(parts) if parts else "No known affinities"

    # ── UI builder ─────────────────────────────────────────────────────
    def _build_ui(self):
        self.clear_items()

        if self.state == "EXPLORE":
            b = discord.ui.Button(label="Advance to Next Room", style=discord.ButtonStyle.primary,
                                  emoji="➡️", custom_id=f"advance_{self.sid}", row=0)
            b.callback = self._action_advance
            self.add_item(b)
            b2 = discord.ui.Button(label="Escape with Loot", style=discord.ButtonStyle.success,
                                   emoji="💰", custom_id=f"escape_{self.sid}", row=0)
            b2.callback = self._action_escape
            self.add_item(b2)

        elif self.state == "COMBAT":
            b = discord.ui.Button(label="Choose Action", style=discord.ButtonStyle.danger,
                                  emoji="✨", custom_id=f"action_{self.sid}", row=0)
            b.callback = self._action_pick_skill
            self.add_item(b)

            b2 = discord.ui.Button(label="Guard (+50% def)", style=discord.ButtonStyle.secondary,
                                   emoji="🛡️", custom_id=f"guard_{self.sid}", row=0)
            b2.callback = self._action_guard
            self.add_item(b2)

            flee_disabled = any(e.get("boss") for e in self.enemies if e["hp"] > 0)
            b3 = discord.ui.Button(
                label="Flee (costs $100)" if not flee_disabled else "Cannot Flee Boss!",
                style=discord.ButtonStyle.secondary, emoji="🏃",
                custom_id=f"flee_{self.sid}", row=1,
                disabled=flee_disabled
            )
            b3.callback = self._action_flee
            self.add_item(b3)

        elif self.state == "ALL_OUT":
            b = discord.ui.Button(label="ALL-OUT ATTACK!", style=discord.ButtonStyle.danger,
                                  emoji="💥", custom_id=f"allout_{self.sid}", row=0)
            b.callback = self._action_all_out
            self.add_item(b)

        # Level-up button always available when pending
        if any(p.get("pending_level", 0) > 0 for p in self.party.values()):
            b_lv = discord.ui.Button(label="Level Up!", style=discord.ButtonStyle.success,
                                     emoji="🌟", custom_id=f"lvlup_{self.sid}", row=2)
            b_lv.callback = self._action_levelup
            self.add_item(b_lv)

    # ── interaction guard ──────────────────────────────────────────────
    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id not in self.party:
            await interaction.response.send_message("❌ You are not in this battle!", ephemeral=True)
            return False
        return True

    # ── message update helper ──────────────────────────────────────────
    async def _update(self, interaction):
        self._build_ui()
        if self.state in ("WIPED", "ESCAPED"):
            self.stop()
            for uid in self.party:
                active_persona_runs.discard(uid)
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=self.get_embed(), view=self)
            else:
                await interaction.message.edit(embed=self.get_embed(), view=self)
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════════
    #  EXPLORE ACTIONS
    # ══════════════════════════════════════════════════════════════════
    async def _action_advance(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            self.state = "COMBAT"
            self.pending_moves.clear()
            for uid in self.alive_players():
                self.party[uid]["downed"] = False
                self.party[uid]["one_more"] = False
            self.add_log(f"🗡️ {interaction.user.display_name} advances — battle starts!")
            await self._update(interaction)

    async def _action_escape(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            self.state = "ESCAPED"
            self.add_log(f"🏆 Party escaped with **${self.gold_earned:,}**!")
            await self._update(interaction)

    # ══════════════════════════════════════════════════════════════════
    #  COMBAT — SKILL / TARGET PICKER
    # ══════════════════════════════════════════════════════════════════
    async def _action_pick_skill(self, interaction: discord.Interaction):
        uid = interaction.user.id
        p = self.party[uid]
        if p["hp"] <= 0:
            return await interaction.response.send_message("💀 You are KO'd!", ephemeral=True)
        if uid in self.pending_moves:
            return await interaction.response.send_message("✅ You already locked in a move!", ephemeral=True)

        view = SkillPickerView(self, uid)
        await interaction.response.send_message("**Choose a skill:**", view=view, ephemeral=True)

    async def _action_guard(self, interaction: discord.Interaction):
        await interaction.response.defer()
        uid = interaction.user.id
        if self.party[uid]["hp"] <= 0:
            return await interaction.followup.send("💀 You are KO'd!", ephemeral=True)
        if uid in self.pending_moves:
            return await interaction.followup.send("✅ Move already locked!", ephemeral=True)
        await self._register_move(interaction, uid, {"action": "guard", "skill": None, "target_idx": None})

    async def _action_flee(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if random.random() < 0.55:
                penalty = min(100, self.gold_earned)
                self.gold_earned -= penalty
                self.add_log(f"🏃 Party fled! Lost ${penalty}.")
                self.floor += 1
                self.generate_floor()
                self.state = "EXPLORE"
                self.pending_moves.clear()
            else:
                self.add_log(f"🚫 Flee failed! Enemy punishes the party!")
                await self._run_enemy_phase()
            await self._update(interaction)

    # ══════════════════════════════════════════════════════════════════
    #  REGISTER MOVE → RESOLVE
    # ══════════════════════════════════════════════════════════════════
    async def _register_move(self, interaction, uid, move):
        async with self.lock:
            if self.state != "COMBAT":
                return
            self.pending_moves[uid] = move
            alive = self.alive_players()
            # Filter out one_more players who haven't moved yet in their bonus turn
            needed = [u for u in alive if u not in self.pending_moves]
            if not needed:
                await self._resolve_round(interaction)
            else:
                skill_name = move.get("skill") or "Guard"
                await interaction.followup.send(
                    f"✅ **{skill_name}** locked in! Waiting for {len(needed)} more…", ephemeral=True
                )
                await self._update(interaction)

    # ══════════════════════════════════════════════════════════════════
    #  ROUND RESOLUTION
    # ══════════════════════════════════════════════════════════════════
    async def _resolve_round(self, interaction):
        self.add_log("─── New Round ───")

        # Tick player statuses
        for uid in self.alive_players():
            p = self.party[uid]
            if p["status"] == "poison":
                tick = max(1, p["max_hp"] // 10)
                p["hp"] = max(0, p["hp"] - tick)
                self.add_log(f"☠️ {p['user'].display_name} poisoned −{tick} HP")
            elif p["status"] == "burn":
                tick = max(1, p["max_hp"] // 8)
                p["hp"] = max(0, p["hp"] - tick)
                self.add_log(f"🔥 {p['user'].display_name} burning −{tick} HP")
            if p["status_dur"] > 0:
                p["status_dur"] -= 1
                if p["status_dur"] <= 0:
                    p["status"] = None

        # Player moves
        for uid, move in list(self.pending_moves.items()):
            p = self.party[uid]
            if p["hp"] <= 0:
                continue

            if move["action"] == "guard":
                buf_amt = p["buffs"].get("def", 0)
                if buf_amt < 2:
                    p["buffs"]["def"] = buf_amt + 1
                heal = max(5, p["max_hp"] // 12)
                p["hp"] = min(p["max_hp"], p["hp"] + heal)
                self.add_log(f"🛡️ {p['user'].display_name} guards (+1 DEF, +{heal} HP)")
                continue

            if move["action"] == "skill":
                skill_name = move["skill"]
                s = SKILLS[skill_name]
                sp_cost = s["cost"]

                if p["sp"] < sp_cost:
                    self.add_log(f"😓 {p['user'].display_name} has no SP for {skill_name}! Attacks normally.")
                    skill_name = "Slash"
                    s = SKILLS["Slash"]
                    sp_cost = 0

                p["sp"] = max(0, p["sp"] - sp_cost)
                target_idx = move.get("target_idx")
                earned_one_more = False

                # ── Healing ─────────────────────────────
                if s["type"] == "heal":
                    targets = []
                    if s["target"] == "party":
                        targets = list(self.party.values())
                    elif s["target"] == "self":
                        targets = [p]
                    elif target_idx is not None:
                        target_uid = self.alive_players()[target_idx] if target_idx < len(self.alive_players()) else uid
                        targets = [self.party[target_uid]]

                    for t in targets:
                        amt = min(s["power"], t["max_hp"] - t["hp"])
                        t["hp"] = min(t["max_hp"], t["hp"] + s["power"])
                        t["downed"] = False
                    names = ", ".join(t["user"].display_name for t in targets)
                    self.add_log(f"💚 {p['user'].display_name} uses {skill_name} → heals {names}")

                # ── Status buffs/debuffs ─────────────────
                elif s["type"] == "status":
                    skill_name_l = skill_name.lower()
                    if "kaja" in skill_name_l:
                        stat_map = {"tarukaja": "atk", "rakukaja": "def", "sukukaja": "agi"}
                        stat = stat_map.get(skill_name_l, "atk")
                        p["buffs"][stat] = min(2, p["buffs"].get(stat, 0) + 1)
                        self.add_log(f"⬆️ {p['user'].display_name} uses {skill_name} → own {stat} +1")
                    elif "nda" in skill_name_l:
                        stat_map = {"tarunda": "atk", "rakunda": "def", "sukunda": "agi"}
                        stat = stat_map.get(skill_name_l, "atk")
                        if target_idx is not None and self.alive_enemies():
                            ei = self.alive_enemies()[target_idx % len(self.alive_enemies())]
                            e = self.enemies[ei]
                            e["buffs"][stat] = max(-2, e["buffs"].get(stat, 0) - 1)
                            self.add_log(f"⬇️ {p['user'].display_name} uses {skill_name} → {e['name']} {stat} −1")

                # ── Instant kill ─────────────────────────
                elif s["type"] in ("light", "dark") and s["power"] == 0:
                    if not self.alive_enemies():
                        continue
                    base_chance = INSTAKILL_BASE.get(skill_name, 0.20)
                    targets_list = (
                        [self.alive_enemies()[target_idx % len(self.alive_enemies())]]
                        if s["target"] == "one" and target_idx is not None
                        else self.alive_enemies()
                    )
                    for ei in targets_list:
                        e = self.enemies[ei]
                        aff = e.get("affinities", {}).get(s["type"], None)
                        if aff == "null" or aff == "drain":
                            self.add_log(f"🚫 {skill_name} has no effect on {e['name']}!")
                            continue
                        elif aff == "repel":
                            # kills caster instead
                            p["hp"] = 0
                            self.add_log(f"💥 {skill_name} REPELLED by {e['name']}! {p['user'].display_name} is KO'd!")
                            continue
                        chance = base_chance * (2.0 if aff == "weak" else 1.0)
                        if random.random() < chance:
                            self.add_log(f"☠️ {p['user'].display_name} {skill_name} INSTA-KILLS {e['name']}!")
                            e["hp"] = 0
                            earned_one_more = True
                        else:
                            self.add_log(f"🎲 {p['user'].display_name} {skill_name} missed {e['name']}…")

                # ── Damage spells ────────────────────────
                else:
                    if not self.alive_enemies():
                        continue
                    # Determine targets
                    if s["target"] == "all":
                        targets_list = list(self.alive_enemies())
                    elif s["target"] == "random":
                        targets_list = [random.choice(self.alive_enemies()) for _ in range(s["hits"])]
                    elif target_idx is not None:
                        ae = self.alive_enemies()
                        if ae:
                            targets_list = [ae[target_idx % len(ae)]]
                        else:
                            continue
                    else:
                        targets_list = [self.alive_enemies()[0]]

                    for hit_idx, ei in enumerate(targets_list):
                        e = self.enemies[ei]
                        if e["hp"] <= 0:
                            continue

                        # def buff mitigation
                        def_buf = e["buffs"].get("def", 0)
                        attacker = {
                            "atk": max(1, p["atk"] + p["buffs"].get("atk", 0) * 3),
                            "mag": max(1, p["mag"] + p["buffs"].get("mag", 0) * 3),
                            "buffs": p["buffs"],
                        }
                        dmg, aff = calc_damage(skill_name, attacker, e.get("affinities", {}))

                        # Apply def debuff to enemy side
                        if def_buf < 0:
                            dmg = int(dmg * (1 + abs(def_buf) * 0.25))

                        aff_lbl = affinity_label(aff)

                        if dmg < 0:  # repel / drain
                            p["hp"] = max(0, p["hp"] - abs(dmg))
                            if aff == "drain":
                                e["hp"] = min(e["max_hp"], e["hp"] + abs(dmg))
                                self.add_log(f"🔁 {e['name']} DRAINED {skill_name} — healed {abs(dmg)}!")
                            else:
                                self.add_log(f"↩️ {skill_name} REPELLED! {p['user'].display_name} takes {abs(dmg)} dmg!")
                            continue

                        if aff == "null":
                            self.add_log(f"🚫 {e['name']} NULLIFIES {skill_name}!")
                            continue

                        e["hp"] = max(0, e["hp"] - dmg)
                        suffix = f" **{aff_lbl}**" if aff_lbl else ""
                        self.add_log(f"✨ {p['user'].display_name} → {skill_name} → {e['name']}: {dmg}{suffix}")

                        # Knockdown on weakness
                        if aff == "weak" and e["hp"] > 0 and not e["downed"]:
                            e["downed"] = True
                            self.add_log(f"💢 {e['name']} is knocked DOWN!")
                            earned_one_more = True

                        # Check boss phase 2
                        thresh = e.get("phase2_hp_thresh", 0)
                        if not e.get("phase2") and thresh and e["hp"] / e["max_hp"] < thresh:
                            e["phase2"] = True
                            e["skills"] = e.get("phase2_skills", e["skills"])
                            self.add_log(e.get("phase2_msg", f"⚠️ {e['name']} powers up!"))

                if earned_one_more:
                    p["one_more"] = True
                    self.add_log(f"✨ **{p['user'].display_name} earns ONE MORE!**")

        self.pending_moves.clear()

        # Remove dead enemies
        for e in self.enemies:
            if e["hp"] <= 0 and not e.get("_rewarded"):
                e["_rewarded"] = True
                gold = e.get("gold", 50)
                xp = e.get("xp", 20)
                self.gold_earned += gold
                self.add_log(f"💀 {e['name']} defeated! +${gold} +{xp}XP")
                for uid in self.alive_players():
                    p2 = self.party[uid]
                    p2["xp"] += xp
                    while p2["xp"] >= p2["max_xp"]:
                        p2["xp"] -= p2["max_xp"]
                        p2["level"] += 1
                        p2["max_xp"] = int(p2["max_xp"] * 1.5)
                        p2["pending_level"] = p2.get("pending_level", 0) + 1
                        self.add_log(f"🌟 {p2['user'].display_name} leveled up → Lv.{p2['level']}!")

        # All enemies dead?
        if not self.alive_enemies():
            self.add_log("🎉 All enemies defeated!")
            self.floor += 1
            self.state = "EXPLORE"
            self.generate_floor()
            # SP regen between fights
            for p in self.party.values():
                regen = max(5, p["max_sp"] // 8)
                p["sp"] = min(p["max_sp"], p["sp"] + regen)
                p["buffs"] = {}
                p["downed"] = False
                p["one_more"] = False
            await self._update(interaction)
            return

        # All enemies downed? → All-Out Attack opportunity
        if self.all_enemies_downed():
            self.state = "ALL_OUT"
            self.add_log("💥 ALL enemies knocked down! **ALL-OUT ATTACK AVAILABLE!**")
            await self._update(interaction)
            return

        # Enemy phase
        if self.alive_players():
            await self._run_enemy_phase()

        if not self.alive_players():
            self.state = "WIPED"
            self.add_log("💀 **THE PARTY WAS WIPED OUT.**")

        await self._update(interaction)

    # ══════════════════════════════════════════════════════════════════
    #  ALL-OUT ATTACK
    # ══════════════════════════════════════════════════════════════════
    async def _action_all_out(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state != "ALL_OUT":
                return await self._update(interaction)

            total_atk = sum(self.party[uid]["atk"] for uid in self.alive_players())
            for ei in self.alive_enemies():
                e = self.enemies[ei]
                dmg = int(total_atk * random.uniform(2.5, 3.5))
                e["hp"] = max(0, e["hp"] - dmg)
                self.add_log(f"💥 **ALL-OUT ATTACK** on {e['name']}! {dmg} damage!")
                if e["hp"] <= 0 and not e.get("_rewarded"):
                    e["_rewarded"] = True
                    self.gold_earned += e.get("gold", 50)
                    xp = e.get("xp", 20)
                    for uid in self.alive_players():
                        self.party[uid]["xp"] += xp
                        while self.party[uid]["xp"] >= self.party[uid]["max_xp"]:
                            self.party[uid]["xp"] -= self.party[uid]["max_xp"]
                            self.party[uid]["level"] += 1
                            self.party[uid]["max_xp"] = int(self.party[uid]["max_xp"] * 1.5)
                            self.party[uid]["pending_level"] = self.party[uid].get("pending_level", 0) + 1
                            self.add_log(f"🌟 {self.party[uid]['user'].display_name} → Lv.{self.party[uid]['level']}!")

            # Reset down states
            for e in self.enemies:
                e["downed"] = False
            for p in self.party.values():
                p["one_more"] = False

            if not self.alive_enemies():
                self.add_log("🎉 Victory!")
                self.floor += 1
                self.state = "EXPLORE"
                self.generate_floor()
                for p in self.party.values():
                    regen = max(5, p["max_sp"] // 8)
                    p["sp"] = min(p["max_sp"], p["sp"] + regen)
                    p["buffs"] = {}
            else:
                self.state = "COMBAT"

            await self._update(interaction)

    # ══════════════════════════════════════════════════════════════════
    #  ENEMY PHASE
    # ══════════════════════════════════════════════════════════════════
    async def _run_enemy_phase(self):
        for ei in self.alive_enemies():
            e = self.enemies[ei]
            if not self.alive_players():
                break

            # Thaw / recover knockdown
            if e["downed"]:
                e["downed"] = False
                self.add_log(f"🔄 {e['name']} recovers from knockdown.")
                continue

            skill_name = random.choice(e["skills"])
            s = SKILLS.get(skill_name)
            if not s:
                continue

            # Pick target
            target_uid = random.choice(self.alive_players())
            t = self.party[target_uid]

            if s["type"] == "heal":
                amt = int((s["power"] / 100) * e["mag"] * 3)
                e["hp"] = min(e["max_hp"], e["hp"] + amt)
                self.add_log(f"💚 {e['name']} uses {skill_name} — healed {amt} HP")
                continue

            if s["type"] == "status":
                self.add_log(f"🌀 {e['name']} uses {skill_name} but nothing happens.")
                continue

            atk_buf = e.get("buffs", {}).get("atk", 0)
            attacker_stats = {
                "atk": max(1, e["atk"] + atk_buf * 3),
                "mag": max(1, e["mag"] + atk_buf * 3),
                "buffs": e.get("buffs", {}),
            }

            if s["target"] == "all":
                targets = [(u, self.party[u]) for u in self.alive_players()]
            else:
                targets = [(target_uid, t)]

            for (t_uid, tp) in targets:
                # Player defense
                def_val = tp.get("buffs", {}).get("def", 0)
                guarding = self.pending_moves.get(t_uid, {}).get("action") == "guard"

                dmg, aff = calc_damage(skill_name, attacker_stats, {})  # players have no affinities (simple)
                if def_val > 0:
                    dmg = int(dmg * (1 - def_val * 0.20))
                if guarding:
                    dmg = dmg // 2

                dmg = max(1, dmg)
                tp["hp"] = max(0, tp["hp"] - dmg)
                self.add_log(f"⚔️ {e['name']} → {skill_name} → {tp['user'].display_name}: {dmg}")

                # Status effects
                if s["type"] == "fire" and random.random() < 0.15 and tp["status"] is None:
                    tp["status"] = "burn"
                    tp["status_dur"] = 2
                    self.add_log(f"🔥 {tp['user'].display_name} is burning!")
                elif s["type"] == "dark" and random.random() < 0.15 and tp["status"] is None:
                    tp["status"] = "poison"
                    tp["status_dur"] = 3
                    self.add_log(f"☠️ {tp['user'].display_name} is poisoned!")
                elif s["type"] == "elec" and random.random() < 0.20 and tp["status"] is None:
                    tp["status"] = "shock"
                    tp["status_dur"] = 1
                    self.add_log(f"⚡ {tp['user'].display_name} is shocked!")

    # ══════════════════════════════════════════════════════════════════
    #  LEVEL UP
    # ══════════════════════════════════════════════════════════════════
    async def _action_levelup(self, interaction: discord.Interaction):
        uid = interaction.user.id
        p = self.party[uid]
        if p.get("pending_level", 0) <= 0:
            return await interaction.response.send_message("❌ No pending level ups.", ephemeral=True)
        view = PersonaLevelUpView(self, uid)
        await interaction.response.send_message("🌟 **Level Up!** Choose a stat:", view=view, ephemeral=True)


# ─────────────────────────────────────────────
#  SKILL PICKER UI
# ─────────────────────────────────────────────
class SkillPickerView(discord.ui.View):
    def __init__(self, session: PersonaSession, uid: int):
        super().__init__(timeout=60)
        self.session = session
        self.uid = uid
        self._add_skill_select()

    def _add_skill_select(self):
        p = self.session.party[self.uid]
        options = []
        for skill_name in p["skills"]:
            s = SKILLS.get(skill_name)
            if not s:
                continue
            cost_str = f"SP:{s['cost']}" if s["cost"] > 0 else "Free"
            label = f"{ELEMENT_EMOJI.get(s['type'],'✨')} {skill_name}"
            desc = f"{s['desc']} ({cost_str}) | {s['target']}"
            options.append(discord.SelectOption(
                label=skill_name,
                description=desc[:100],
                emoji=ELEMENT_EMOJI.get(s["type"], "✨"),
                value=skill_name
            ))

        sel = discord.ui.Select(
            placeholder="Pick a skill…",
            min_values=1, max_values=1,
            options=options
        )
        sel.callback = self._on_skill
        self.add_item(sel)

    async def _on_skill(self, interaction: discord.Interaction):
        skill_name = self.children[0].values[0]
        s = SKILLS[skill_name]

        # Decide if we need a target
        ae = self.session.alive_enemies()
        ap = self.session.alive_players()

        needs_target = (
            s["target"] == "one"
            and s["type"] not in ("status",)   # kaja/nda handled separately below
            and s["type"] != "heal"             # self/party heals don't need target
        )

        # Buffer skills that debuff enemies need target
        if s["type"] == "status" and "nda" in skill_name.lower():
            needs_target = True

        # Healing targeting allies
        needs_ally_target = (s["target"] in ("self",) and s["type"] == "heal")

        if needs_target and ae:
            # Show enemy target selector
            view = TargetPickerView(self.session, self.uid, skill_name, ae, "enemy")
            await interaction.response.send_message("**Select a target:**", view=view, ephemeral=True)
        elif needs_ally_target and len(ap) > 1 and s["target"] != "self":
            view = TargetPickerView(self.session, self.uid, skill_name, list(range(len(ap))), "ally")
            await interaction.response.send_message("**Select ally to heal:**", view=view, ephemeral=True)
        else:
            # No target needed or target = all/random/self
            await interaction.response.defer()
            await self.session._register_move(
                interaction, self.uid,
                {"action": "skill", "skill": skill_name, "target_idx": 0}
            )


class TargetPickerView(discord.ui.View):
    def __init__(self, session: PersonaSession, uid: int, skill_name: str, indices, mode: str):
        super().__init__(timeout=60)
        self.session = session
        self.uid = uid
        self.skill_name = skill_name

        options = []
        if mode == "enemy":
            for i, ei in enumerate(indices):
                e = session.enemies[ei]
                aff_str = ""
                sk = SKILLS.get(skill_name)
                if sk:
                    aff = e.get("affinities", {}).get(sk["type"])
                    aff_str = f" [{affinity_label(aff)}]" if aff_str == "" and aff else f" [{affinity_label(aff)}]"
                options.append(discord.SelectOption(
                    label=f"{e['emoji']} {e['name']} (HP:{e['hp']}){aff_str}",
                    value=str(i),
                    emoji=e["emoji"]
                ))
        else:
            ap = session.alive_players()
            for i, puid in enumerate(ap):
                p2 = session.party[puid]
                options.append(discord.SelectOption(
                    label=f"{p2['emoji']} {p2['user'].display_name} (HP:{p2['hp']})",
                    value=str(i),
                    emoji=p2["emoji"]
                ))

        sel = discord.ui.Select(placeholder="Select target…", options=options)
        sel.callback = self._on_target
        self.add_item(sel)

    async def _on_target(self, interaction: discord.Interaction):
        idx = int(self.children[0].values[0])
        await interaction.response.defer()
        await self.session._register_move(
            interaction, self.uid,
            {"action": "skill", "skill": self.skill_name, "target_idx": idx}
        )


# ─────────────────────────────────────────────
#  LEVEL UP VIEW
# ─────────────────────────────────────────────
class PersonaLevelUpView(discord.ui.View):
    def __init__(self, session: PersonaSession, uid: int):
        super().__init__(timeout=60)
        self.session = session
        self.uid = uid

        options = [
            discord.SelectOption(label="Raise HP",           emoji="❤️",  value="hp",  description="Max HP +20 (scales w/ level)"),
            discord.SelectOption(label="Raise SP",           emoji="💙",  value="sp",  description="Max SP +15 (scales w/ level)"),
            discord.SelectOption(label="Raise Attack",       emoji="⚔️",  value="atk", description="Physical ATK boost"),
            discord.SelectOption(label="Raise Magic",        emoji="🔮",  value="mag", description="Spell MAG boost"),
            discord.SelectOption(label="Raise Endurance",    emoji="🛡️",  value="end", description="Resilience & defense"),
            discord.SelectOption(label="Raise Agility",      emoji="💨",  value="agi", description="Speed & evasion"),
        ]
        sel = discord.ui.Select(placeholder="Choose a stat…", options=options)
        sel.callback = self._on_choice
        self.add_item(sel)

    async def _on_choice(self, interaction: discord.Interaction):
        async with self.session.lock:
            p = self.session.party[self.uid]
            if p.get("pending_level", 0) <= 0:
                return await interaction.response.send_message("❌ No pending level ups.", ephemeral=True)

            choice = self.children[0].values[0]
            p["pending_level"] -= 1
            lvl = p["level"]

            if choice == "hp":
                gain = 20 + lvl * 4
                p["max_hp"] += gain
                p["hp"] = min(p["max_hp"], p["hp"] + gain)
                msg = f"+{gain} Max HP"
            elif choice == "sp":
                gain = 15 + lvl * 2
                p["max_sp"] += gain
                p["sp"] = min(p["max_sp"], p["sp"] + gain)
                msg = f"+{gain} Max SP"
            elif choice == "atk":
                gain = 3 + lvl
                p["atk"] += gain
                msg = f"+{gain} ATK"
            elif choice == "mag":
                gain = 3 + lvl
                p["mag"] += gain
                msg = f"+{gain} MAG"
            elif choice == "end":
                gain = 2 + lvl // 2
                p["end"] += gain
                msg = f"+{gain} END"
            else:  # agi
                gain = 2 + lvl // 2
                p["agi"] += gain
                msg = f"+{gain} AGI"

            await interaction.response.send_message(f"🌟 {msg} gained!", ephemeral=True)
            if self.session.message:
                self.session._build_ui()
                await self.session.message.edit(embed=self.session.get_embed(), view=self.session)


# ─────────────────────────────────────────────
#  LOBBY
# ─────────────────────────────────────────────
class PersonaClassPickView(discord.ui.View):
    """Each player picks their Arcana class before starting."""
    def __init__(self, host):
        super().__init__(timeout=300)
        self.host = host
        self.party = [host]
        self.classes = {}   # uid -> class name

    @discord.ui.button(label="Join Party", style=discord.ButtonStyle.primary, emoji="✋", row=0)
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in self.party:
            return await interaction.response.send_message("❌ Already in party.", ephemeral=True)
        if len(self.party) >= 4:
            return await interaction.response.send_message("❌ Party full (max 4).", ephemeral=True)
        if interaction.user.id in active_persona_runs:
            return await interaction.response.send_message("❌ You're already in a run.", ephemeral=True)
        self.party.append(interaction.user)
        await interaction.response.send_message("✅ Joined! Now pick your Arcana with /persona class.", ephemeral=True)
        embed = interaction.message.embeds[0]
        embed.description += f"\n✅ {interaction.user.mention} joined!"
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Pick My Class", style=discord.ButtonStyle.secondary, emoji="🃏", row=0)
    async def btn_class(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user not in self.party:
            return await interaction.response.send_message("❌ Join the party first!", ephemeral=True)
        view = ClassSelectView(self, interaction.user.id)
        await interaction.response.send_message("**Choose your Arcana:**", view=view, ephemeral=True)

    @discord.ui.button(label="Start Battle", style=discord.ButtonStyle.success, emoji="⚔️", row=1)
    async def btn_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.host.id:
            return await interaction.response.send_message("❌ Only the host can start.", ephemeral=True)

        for user in self.party:
            if user.id not in self.classes:
                return await interaction.response.send_message(
                    "❌ Not everyone has picked a class yet!", ephemeral=True
                )

        await interaction.response.defer()
        for user in self.party:
            active_persona_runs.add(user.id)

        session = PersonaSession(self.party, self.classes)
        await interaction.edit_original_response(embed=session.get_embed(), view=session)
        session.message = await interaction.original_response()
        self.stop()


class ClassSelectView(discord.ui.View):
    def __init__(self, lobby: PersonaClassPickView, uid: int):
        super().__init__(timeout=120)
        self.lobby = lobby
        self.uid = uid

        options = [
            discord.SelectOption(
                label=name,
                description=data["desc"][:100],
                emoji=data["emoji"],
                value=name
            )
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
            f"✅ You chose **{cls['emoji']} {cls_name}**!\n"
            f"Skills: {', '.join(cls['skills'])}", ephemeral=True
        )


# ─────────────────────────────────────────────
#  THE COG
# ─────────────────────────────────────────────
class PersonaRPG(commands.GroupCog, group_name="persona", group_description="Persona-style co-op RPG (test cog, no DB)."):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="play", description="Open a Persona-style dungeon lobby.")
    async def play(self, interaction: discord.Interaction):
        if interaction.user.id in active_persona_runs:
            return await interaction.response.send_message(
                "❌ You're already in an active run! Use /persona abort to reset.", ephemeral=True
            )

        embed = discord.Embed(
            title="🃏 Persona Dungeon — Lobby",
            description=(
                f"**Host:** {interaction.user.mention}\n\n"
                "1️⃣ Click **Join Party** to join (up to 4 players)\n"
                "2️⃣ Click **Pick My Class** to choose your Arcana\n"
                "3️⃣ Host clicks **Start Battle** when everyone is ready\n\n"
                "**Arcanas Available:**\n" +
                "\n".join(f"{d['emoji']} **{n}** — {d['desc']}" for n, d in PERSONA_CLASSES.items())
            ),
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=PersonaClassPickView(interaction.user))

    @app_commands.command(name="skills", description="Browse all available skills.")
    async def skills(self, interaction: discord.Interaction):
        embed = discord.Embed(title="📖 Skill Compendium", color=0x9b59b6)
        by_type = defaultdict(list)
        for name, s in SKILLS.items():
            by_type[s["type"]].append((name, s))

        for etype, entries in sorted(by_type.items()):
            val = ""
            for name, s in entries:
                tgt = {"one": "1 foe", "all": "all foes", "party": "party", "self": "self", "random": "random"}[s["target"]]
                val += f"`{name}` SP:{s['cost']} | {s['desc']} [{tgt}]\n"
            embed.add_field(
                name=f"{ELEMENT_EMOJI.get(etype,'✨')} {etype.title()}",
                value=val[:1024],
                inline=False
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="classes", description="View all Arcana classes and their skills.")
    async def classes(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🃏 Arcana Classes", color=0xe67e22)
        for name, data in PERSONA_CLASSES.items():
            embed.add_field(
                name=f"{data['emoji']} {name}",
                value=(
                    f"{data['desc']}\n"
                    f"HP:{data['hp']} SP:{data['sp']} "
                    f"ATK:{data['atk']} MAG:{data['mag']} END:{data['end']} AGI:{data['agi']}\n"
                    f"Skills: {', '.join(data['skills'])}"
                ),
                inline=False
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="abort", description="Emergency: exit an active run.")
    async def abort(self, interaction: discord.Interaction):
        if interaction.user.id in active_persona_runs:
            active_persona_runs.discard(interaction.user.id)
            await interaction.response.send_message("🚨 Run aborted. You can start a new game.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ You're not in an active run.", ephemeral=True)


async def setup(bot):
    await bot.add_cog(PersonaRPG(bot))