import discord
from discord import app_commands
from discord.ext import commands
import database as db
import random
import asyncio

# --- RPG DATA ---
CLASSES = {
    "Fighter": {"hp": 100, "atk_mod": 0, "def_mod": 0, "spell_mod": 0, "emoji": "🗡️", "passive": 'unleashed rage', "spell": None},
    "Assassin": {"hp": 70, "atk_mod": 5, "def_mod": -2, "spell_mod": 0, "emoji": "🥷", "passive": "dodge", "spell": None},
    "Tank": {"hp": 150, "atk_mod": -3, "def_mod": 4, "spell_mod": 0, "emoji": "🛡️", "passive": "thorns", "spell": None},
    "Cleric": {"hp": 85, "atk_mod": -4, "def_mod": 0, "spell_mod": 0, "emoji": "🪄", "passive": 'divine interference', "spell": "party_heal"},
    "Mage": {"hp": 60, "atk_mod": -4, "def_mod": 0, "spell_mod": 3, "emoji": "🔮", "passive": 'dodge', "spell": "poison"},
    "Paladin": {"hp": 130, "atk_mod": -2, "def_mod": 3, "spell_mod": 1, "emoji": "⚜️", "passive": 'divine interference', "spell": None},
    "Berserker": {"hp": 110, "atk_mod": 8, "def_mod": -4, "spell_mod": 0, "emoji": "🪓", "passive": 'unleashed rage', "spell": None},
    "Warlock": {"hp": 75, "atk_mod": -2, "def_mod": 0, "spell_mod": 2, "emoji": "👿", "passive": 'thorns', "spell": "hellfire"},
    "Cryomancer": {"hp": 65, "atk_mod": -3, "def_mod": 1, "spell_mod": 2, "emoji": "❄️", "passive": "dodge", "spell": "frostbite"}
}

# --- SHOP GEAR ---
SHOP_GEAR = {
    "Rusty Dagger": {"cost": 0, "atk": 20, "def": 0, "int": 0, "emoji": "🗡️"},
    "Wooden Club": {"cost": 500, "atk": 22, "def": 0, "int": 0, "emoji": "🏏"},
    "Iron Longsword": {"cost": 2500, "atk": 25, "def": 0, "int": 0, "emoji": "⚔️"},
    "Steel Halberd": {"cost": 10000, "atk": 30, "def": 0, "int": 0, "emoji": "🪓"},
    "Mithril Rapier": {"cost": 20000, "atk": 32, "def": 0, "int": 0, "emoji": "🤺"},
    "Obsidian Scythe": {"cost": 35000, "atk": 35, "def": 0, "int": 0, "emoji": "🌙"},
    "Dragonbone Greatsword": {"cost": 100000, "atk": 55, "def": 0, "int": 0, "emoji": "🔥"},
    "Void Reaper": {"cost": 250000, "atk": 85, "def": 0, "int": 0, "emoji": "🌌"},
    "Celestial Spear": {"cost": 1000000, "atk": 120, "def": 0, "int": 0, "emoji": "☄️"},
    "Leather Tunic": {"cost": 1500, "atk": 0, "def": 8, "int": 0, "emoji": "🦺"},
    "Chainmail": {"cost": 8000, "atk": 0, "def": 18, "int": 0, "emoji": "⛓️"},
    "Plate Armor": {"cost": 30000, "atk": 0, "def": 30, "int": 0, "emoji": "🛡️"},
    "Aegis of the Gods": {"cost": 500000, "atk": 0, "def": 75, "int": 0, "emoji": "🌟"},
    "Apprentice Wand": {"cost": 2000, "atk": 0, "def": 0, "int": 4, "emoji": "🪄"},
    "Ruby Staff": {"cost": 15000, "atk": 0, "def": 0, "int": 8, "emoji": "🦯"},
    "Archmage Grimoire": {"cost": 80000, "atk": 0, "def": 0, "int": 16, "emoji": "📖"},
    "Staff of the Cosmos": {"cost": 800000, "atk": 0, "def": 0, "int": 35, "emoji": "🌌"},
    "Jad's Ascended Horseshoe": {"cost": 5000000, "atk": 250, "def": 100, "int": 50, "emoji": "🐴"}
}

ENEMIES = [
    {"name": "Goblin", "hp": 30, "atk": 8, "def": 2, "emoji": "👺"},
    {"name": "Skeleton", "hp": 40, "atk": 10, "def": 4, "emoji": "💀"},
    {"name": "Acid Slime", "hp": 50, "atk": 6, "def": 3, "emoji": "🦠", "effect": "poison", "chance": 0.3},
    {"name": "Ice Wraith", "hp": 40, "atk": 12, "def": 3, "emoji": "👻", "effect": "freeze", "chance": 0.2, "weakness": "hellfire"},
    {"name": "Jad's Goon", "hp": 50, "atk": 20, "def": -5, "emoji": "🐴"},
    {"name": "Orc Berserker", "hp": 45, "atk": 16, "def": 0, "emoji": "🧌"},
    {"name": "Stone Golem", "hp": 85, "atk": 6, "def": 12, "emoji": "🪨", "weakness": "poison"},
    {"name": "Fire Elemental", "hp": 35, "atk": 14, "def": 2, "emoji": "🔥", "effect": "burn", "chance": 0.3, "weakness": "frostbite"},
    {"name": "Venomous Spider", "hp": 25, "atk": 12, "def": 1, "emoji": "🕷️", "effect": "poison", "chance": 0.4},
    {"name": "Cult Sorcerer", "hp": 35, "atk": 15, "def": 2, "emoji": "🦹", "effect": "burn", "chance": 0.2}
]

BOSSES = [
    {"name": "Elder Dragon", "hp": 150, "atk": 10, "def": 8, "emoji": "🐉", "effect": "burn", "chance": 0.3, "weakness": "frostbite"},
    {"name": "Lich King", "hp": 120, "atk": 15, "def": -5, "emoji": "🧙‍♂️", "effect": "freeze", "chance": 0.25, "weakness": "hellfire"},
    {"name": "Jad", "hp": 500, "atk": 8, "def": -10, "emoji": "🐴", "effect": "freeze", "chance": 0.25},
    {"name": "Colossal Minotaur", "hp": 220, "atk": 18, "def": 5, "emoji": "🐂"},
    {"name": "Archdemon", "hp": 140, "atk": 16, "def": 5, "emoji": "👿", "effect": "burn", "chance": 0.25, "weakness": "frostbite"}
]

ITEMS = [
    {"name": "Bandage", "type": "heal", "val": 25, "emoji": "🩹", "desc": "Restores 25 HP", "min_floor": 1},
    {"name": "Health Potion", "type": "heal", "val": 50, "emoji": "🧪", "desc": "Restores 50 HP", "min_floor": 1},
    {"name": "Bag of Coins", "type": "gold", "val": 75, "emoji": "🪙", "desc": "Adds $75 to your run", "min_floor": 1},
    {"name": "Golden Relic", "type": "gold", "val": 200, "emoji": "🏆", "desc": "Adds $200 to your run", "min_floor": 1},
    {"name": "Iron Buckler", "type": "stat", "target": "def", "val": 4, "emoji": "🛡️", "desc": "+4 Defense", "min_floor": 1},
    {"name": "Titanium Broadsword", "type": "stat", "target": "atk", "val": 12, "emoji": "⚔️", "desc": "+12 Attack", "min_floor": 1},
    {"name": "Apprentice Wand", "type": "stat", "target": "intelligence", "val": 1, "emoji": "🪄", "desc": "+1 Spell Knowledge", "min_floor": 1},
    
    {"name": "Golden Idol", "type": "relic", "effect": "golden_idol", "emoji": "🗽", "desc": "Party Buff: Enemies drop 50% more gold.", "min_floor": 10},
    {"name": "Steel Claymore", "type": "stat", "target": "atk", "val": 18, "emoji": "🗡️", "desc": "+18 Attack", "min_floor": 10},
    {"name": "Enchanted Robes", "type": "stat", "target": "def", "val": 12, "emoji": "🥻", "desc": "+12 Defense", "min_floor": 10},
    {"name": "Mystic Amulet", "type": "stat", "target": "intelligence", "val": 2, "emoji": "📿", "desc": "+2 Spell Knowledge", "min_floor": 10},
    {"name": "Greater Health Potion", "type": "heal", "val": 100, "emoji": "🧪", "desc": "Restores 100 HP", "min_floor": 10},
    {"name": "Ruby Necklace", "type": "gold", "val": 400, "emoji": "📿", "desc": "Adds $400 to your run", "min_floor": 10},
    {"name": "Elise's Venom", "type": "spell", "effect": "poison", "emoji": "🕷️", "desc": "Equip Spell: Poison enemy.", "min_floor": 10},
    {"name": "Scroll of Hellfire", "type": "spell", "effect": "hellfire", "emoji": "🔥", "desc": "Equip Spell: Heavy Fire DMG.", "min_floor": 10},
    {"name": "Scroll of Frostbite", "type": "spell", "effect": "frostbite", "emoji": "❄️", "desc": "Equip Spell: Freeze enemy.", "min_floor": 10},
    {"name": "Tome of Knowledge", "type": "stat", "target": "intelligence", "val": 3, "emoji": "🧠", "desc": "+3 Spell Knowledge", "min_floor": 10},
    
    {"name": "Vampire Fangs", "type": "relic", "effect": "vampire", "emoji": "🧛", "desc": "Party Buff: Physical attacks heal for 10% DMG.", "min_floor": 20},
    {"name": "Hermes Boots", "type": "relic", "effect": "dodge", "emoji": "🪽", "desc": "Party Buff: +15% Dodge chance.", "min_floor": 20},
    {"name": "Demonbane Axe", "type": "stat", "target": "atk", "val": 25, "emoji": "🪓", "desc": "+25 Attack", "min_floor": 20},
    {"name": "Aegis Plate Armor", "type": "stat", "target": "def", "val": 18, "emoji": "🛡️", "desc": "+18 Defense", "min_floor": 20},
    {"name": "Sapphire Wand", "type": "stat", "target": "intelligence", "val": 6, "emoji": "🪄", "desc": "+6 Spell Knowledge", "min_floor": 20},
    {"name": "Elixir of Life", "type": "heal", "val": 150, "emoji": "🍾", "desc": "Restores 150 HP", "min_floor": 20},
    {"name": "Diamond Cluster", "type": "gold", "val": 600, "emoji": "💎", "desc": "Adds $600 to your run", "min_floor": 20},
    {"name": "Scroll of Restoration", "type": "spell", "effect": "party_heal", "emoji": "📖", "desc": "Equip Spell: Party Heal.", "min_floor": 20},
    
    {"name": "Excalibur", "type": "stat", "target": "atk", "val": 45, "emoji": "⚔️", "desc": "+45 Attack", "min_floor": 30},
    {"name": "Obsidian Bulwark", "type": "stat", "target": "def", "val": 30, "emoji": "🛡️", "desc": "+30 Defense", "min_floor": 30},
    {"name": "Tome of the Cosmos", "type": "stat", "target": "intelligence", "val": 12, "emoji": "🌌", "desc": "+12 Spell Knowledge", "min_floor": 30},
    {"name": "Panacea", "type": "heal", "val": 500, "emoji": "💖", "desc": "Restores 500 HP", "min_floor": 30},
    {"name": "King's Crown", "type": "gold", "val": 1500, "emoji": "👑", "desc": "Adds $1,500 to your run", "min_floor": 30},

    {"name": "Jad's Almighty Hoof", "type": "stat", "target": "atk", "val": 85, "emoji": "🐴", "desc": "+85 Attack", "min_floor": 45},
    {"name": "Jad's Saddle of Invincibility", "type": "stat", "target": "def", "val": 50, "emoji": "🐎", "desc": "+50 Defense", "min_floor": 45},
    {"name": "Jad's Golden Carrot", "type": "heal", "val": 9999, "emoji": "🥕", "desc": "Fully Restores HP", "min_floor": 45},
    {"name": "Jad's Secret Stash", "type": "gold", "val": 5000, "emoji": "💰", "desc": "Adds $5,000 to your run", "min_floor": 45}
]

active_runs = set()

# --- UI CLASSES ---
class RPGSpellSelectView(discord.ui.View):
    def __init__(self, session, user_id):
        super().__init__(timeout=60)
        self.session = session
        self.user_id = user_id
        p = session.party[user_id]
        
        options = []
        for spell in p['active_spells']:
            cd = p['spell_cds'].get(spell, 0)
            desc = f"Cooldown: {cd} turns" if cd > 0 else "Ready to cast!"
            options.append(discord.SelectOption(
                label=spell.replace('_', ' ').title(), 
                value=spell,
                description=desc,
                emoji="✨"
            ))
        
        select = discord.ui.Select(placeholder="Select a spell to cast...", min_values=1, max_values=1, options=options)
        select.callback = self.callback
        self.add_item(select)
        
    async def callback(self, interaction: discord.Interaction):
        spell = self.children[0].values[0]
        p = self.session.party[self.user_id]
        
        if p['spell_cds'].get(spell, 0) > 0:
            return await interaction.response.send_message(f"⏳ That spell is on cooldown for {p['spell_cds'][spell]} turns.", ephemeral=True)
        
        await interaction.response.defer()
        await self.session.register_move(interaction, f"spell_{spell}")
        await interaction.delete_original_response()

class RPGInventoryDropdown(discord.ui.Select):
    def __init__(self, session, user_id):
        self.session = session
        self.user_id = user_id
        options = []
        for i, item in enumerate(session.party[user_id]['inventory']):
            options.append(discord.SelectOption(
                label=item['name'], 
                description=item.get('desc', ''), 
                emoji=item.get('emoji', '🎒'), 
                value=str(i) 
            ))
        super().__init__(placeholder="Choose an item to use...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        async with self.session.lock:
            idx = int(self.values[0])
            p = self.session.party[self.user_id]
            
            if idx >= len(p['inventory']):
                return await interaction.response.send_message("❌ Item not found!", ephemeral=True)
                
            item = p['inventory'].pop(idx)
            t_type = item.get('type')
            
            if t_type == 'stat':
                if item.get('target') == 'atk': p['atk'] += item.get('val', 0)
                elif item.get('target') == 'def': p['def'] += item.get('val', 0)
                elif item.get('target') == 'intelligence': p['intelligence'] += item.get('val', 0)

            elif t_type == 'heal':
                p['hp'] = min(p['max_hp'], p['hp'] + item.get('val', 0))
            elif t_type == 'spell':
                effect = item.get('effect')
                max_spells = max(1, 1 + (p['intelligence'] // 4))
                
                if effect in p['active_spells']:
                    p['intelligence'] += 1
                    self.session.log += f"\n🎒 {interaction.user.display_name} studies the {item['name']}. (+1 INT)"
                else:
                    if len(p['active_spells']) >= max_spells:
                        removed = p['active_spells'].pop(0)
                        self.session.log += f"\n🎒 {interaction.user.display_name} forgot {removed.replace('_', ' ').title()} to equip {item['name']}!"
                    p['active_spells'].append(effect)
                    p['spell_cds'][effect] = 0
                    self.session.log += f"\n🎒 {interaction.user.display_name} equipped {item['name']}!"
                
            if t_type != 'spell':
                self.session.log += f"\n🎒 {interaction.user.display_name} used the {item['name']}!"

            # --- JAD SET ASCENSION EVENT ---
            if "Jad" in item['name'] and t_type == 'stat':
                p['jad_pieces'].add(item['name'])
                
                stat_pieces = [x for x in p['jad_pieces'] if x in ["Jad's Ascended Horseshoe", "Jad's Almighty Hoof", "Jad's Saddle of Invincibility"]]
                
                if len(stat_pieces) >= 2 and not p['is_jad']:
                    p['is_jad'] = True
                    p['class'] = "Jad's Avatar"
                    p['emoji'] = "🐎"
                    
                    p['max_hp'] += 1000
                    p['hp'] = p['max_hp']
                    p['atk'] += 200
                    p['def'] += 150
                    p['intelligence'] += 50
                    
                    for passive in ['dodge', 'thorns', 'unleashed rage', 'divine interference']:
                        if passive not in p['passives']:
                            p['passives'].append(passive)
                            
                    self.session.log += f"\n\n🌟 **DIVINE ASCENSION!** {interaction.user.display_name} combined the artifacts of Jad! They transformed into JAD'S AVATAR! Massive stat boost & ALL PASSIVES unlocked!"
                
            await interaction.response.send_message(f"✅ You used the **{item['name']}**!", ephemeral=True)
            
            if self.session.message:
                self.session.build_ui()
                await self.session.message.edit(embed=self.session.get_embed(), view=self.session)

class RPGInventoryView(discord.ui.View):
    def __init__(self, session, user_id):
        super().__init__(timeout=60)
        self.add_item(RPGInventoryDropdown(session, user_id))

class RPGLevelUpView(discord.ui.View):
    def __init__(self, session, user_id):
        super().__init__(timeout=60)
        self.session = session
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label="Train Body", description="+25 Max HP, Heals 25 HP", emoji="❤️", value="hp"),
            discord.SelectOption(label="Train Arms", description="+8 Attack", emoji="⚔️", value="atk"),
            discord.SelectOption(label="Study Magic", description="+3 Intelligence", emoji="🧠", value="int"),
            discord.SelectOption(label="Harden Armor", description="+5 Defense", emoji="🛡️", value="def")
        ]
        select = discord.ui.Select(placeholder="Choose your stat upgrade...", min_values=1, max_values=1, options=options)
        select.callback = self.callback
        self.add_item(select)
        
    async def callback(self, interaction: discord.Interaction):
        async with self.session.lock:
            p = self.session.party[self.user_id]
            if p.get('pending_level', 0) <= 0:
                return await interaction.response.send_message("❌ You have no pending level ups.", ephemeral=True)
                
            choice = self.children[0].values[0]
            p['pending_level'] -= 1
            
            if choice == "hp":
                p['max_hp'] += 25
                p['hp'] += 25
                msg = "+25 Max HP"
            elif choice == "atk":
                p['atk'] += 8
                msg = "+8 Attack"
            elif choice == "int":
                p['intelligence'] += 3
                msg = "+3 Intelligence"
            elif choice == "def":
                p['def'] += 5
                msg = "+5 Defense"
                
            await interaction.response.send_message(f"🌟 You leveled up and selected **{msg}**!", ephemeral=True)
            if self.session.message:
                self.session.build_ui()
                await self.session.message.edit(embed=self.session.get_embed(), view=self.session)


# --- THE CO-OP ENGINE ---
class RPGSession(discord.ui.View):
    def __init__(self, party_members):
        super().__init__(timeout=600)
        self.lock = asyncio.Lock()
        self.session_id = str(random.randint(1000, 9999)) 
        self.message = None 
        self.party = {}
        
        for user in party_members:
            gear_data, class_name = db.get_rpg_profile(user.id)
            gear_names = gear_data.split(',') if gear_data else ["Rusty Dagger"]
            cls = CLASSES.get(class_name, CLASSES["Fighter"])
            passives = [cls['passive']] if cls['passive'] else []
            
            total_atk_bonus = 0
            total_def_bonus = 0
            total_int_bonus = 0
            jad_pieces = set()
            
            for g in gear_names:
                g = g.strip()
                g_item = SHOP_GEAR.get(g, SHOP_GEAR["Rusty Dagger"])
                total_atk_bonus += g_item['atk']
                total_def_bonus += g_item['def']
                total_int_bonus += g_item['int']
                if "Jad" in g:
                    jad_pieces.add(g)
            
            self.party[user.id] = {
                "user": user, "class": class_name, "emoji": cls['emoji'],
                "max_hp": cls['hp'], "hp": cls['hp'],
                "atk": total_atk_bonus + cls['atk_mod'], 
                "def": 5 + total_def_bonus + cls['def_mod'],
                "intelligence": total_int_bonus + cls['spell_mod'],
                "passives": passives, 
                "active_spells": [cls['spell']] if cls['spell'] else [], 
                "spell_cds": {cls['spell']: 0} if cls['spell'] else {},
                "status": None, "status_dur": 0,
                "inventory": [],
                "level": 1,         
                "xp": 0,
                "max_xp": 50,
                "pending_level": 0,
                "jad_pieces": jad_pieces,
                "is_jad": False
            }
            
        self.gold_earned = 0
        self.floor = 1
        self.paths = []
        self.relics = [] 
        
        self.state = "EXPLORE"
        self.log = "Your party enters the dungeon..."
        self.enemy = None
        self.treasure = None
        
        self.enemy_status = None 
        self.status_duration = 0
        self.poison_dmg = 0 
        self.combat_moves = {} 
        
        self.generate_paths()
        self.build_ui()

    def generate_paths(self):
        self.paths = []
        
        if self.floor > 0 and self.floor % 10 == 0:
            base = random.choice(BOSSES)
            multiplier = 1.0 + (self.floor * 0.1)
            party_size = len(self.party)
            enemy = {
                "name": base['name'], "emoji": base['emoji'],
                "hp": int(base['hp'] * multiplier * (party_size * 0.85)), 
                "max_hp": int(base['hp'] * multiplier * (party_size * 0.85)),
                "atk": int(base['atk'] * multiplier), "def": int(base['def'] * multiplier),
                "effect": base.get('effect'), "chance": base.get('chance', 0),
                "weakness": base.get('weakness'), "boss": True
            }
            self.paths.append({"type": "boss", "label": "Enter Boss Room", "emoji": "☠️", "data": enemy})
            return

        base1 = random.choice(ENEMIES)
        multiplier = 1.0 + (self.floor * 0.1)
        enemy1 = {
            "name": base1['name'], "emoji": base1['emoji'],
            "hp": int(base1['hp'] * multiplier * len(self.party)), 
            "max_hp": int(base1['hp'] * multiplier * len(self.party)), 
            "atk": int(base1['atk'] * multiplier), "def": int(base1['def'] * multiplier),
            "effect": base1.get('effect'), "chance": base1.get('chance', 0),
            "weakness": base1.get('weakness'), "boss": False
        }
        self.paths.append({"type": "combat", "label": f"Fight {enemy1['name']}", "emoji": "⚔️", "data": enemy1})
        
        roll = random.random()
        if roll < 0.35: 
            self.paths.append({"type": "treasure", "label": "Mysterious Door", "emoji": "🚪"})
        elif roll < 0.50: 
            self.paths.append({"type": "shrine", "label": "Angel Shrine", "emoji": "👼"})
        elif roll < 0.70: 
            self.paths.append({"type": "event", "label": "Strange Altar", "emoji": "🔮"})
        else: 
            base2 = random.choice(ENEMIES)
            enemy2 = {
                "name": base2['name'], "emoji": base2['emoji'],
                "hp": int(base2['hp'] * multiplier * len(self.party)), 
                "max_hp": int(base2['hp'] * multiplier * len(self.party)), 
                "atk": int(base2['atk'] * multiplier), "def": int(base2['def'] * multiplier),
                "effect": base2.get('effect'), "chance": base2.get('chance', 0),
                "weakness": base2.get('weakness'), "boss": False
            }
            self.paths.append({"type": "combat", "label": f"Fight {enemy2['name']}", "emoji": "⚔️", "data": enemy2})

    def get_alive_players(self):
        return [uid for uid, p in self.party.items() if p['hp'] > 0]

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id not in self.party:
            await interaction.response.send_message("❌ You are not in this party!", ephemeral=True)
            return False
        if self.party[interaction.user.id]['hp'] <= 0 and self.state != "SHRINE":
            await interaction.response.send_message("💀 You are dead! Wait for your party to revive you or escape.", ephemeral=True)
            return False
        return True

    def build_ui(self):
        self.clear_items()
        
        if self.state == "EXPLORE":
            for i, path in enumerate(self.paths):
                btn_style = discord.ButtonStyle.danger if path['type'] == 'boss' else discord.ButtonStyle.primary
                btn = discord.ui.Button(label=path['label'], style=btn_style, emoji=path['emoji'], custom_id=f"path_{i}_{self.session_id}", row=0)
                btn.callback = self.action_choose_path
                self.add_item(btn)
                
            btn_escape = discord.ui.Button(label="Escape with Loot", style=discord.ButtonStyle.success, emoji="💰", custom_id=f"escape_{self.session_id}", row=1)
            btn_escape.callback = self.action_escape
            self.add_item(btn_escape)
            
        elif self.state == "COMBAT":
            btn_attack = discord.ui.Button(label="Attack", style=discord.ButtonStyle.danger, emoji="⚔️", custom_id=f"attack_{self.session_id}", row=0)
            btn_attack.callback = self.action_attack
            btn_defend = discord.ui.Button(label="Defend", style=discord.ButtonStyle.secondary, emoji="🛡️", custom_id=f"defend_{self.session_id}", row=0)
            btn_defend.callback = self.action_defend
            btn_spell = discord.ui.Button(label="Cast Spell", style=discord.ButtonStyle.success, emoji="✨", custom_id=f"spell_{self.session_id}", row=0)
            btn_spell.callback = self.action_spell
            self.add_item(btn_attack)
            self.add_item(btn_defend)
            self.add_item(btn_spell)
            
            btn_flee = discord.ui.Button(label="Flee (50%)", style=discord.ButtonStyle.secondary, emoji="🏃", custom_id=f"flee_{self.session_id}", row=1)
            btn_flee.callback = self.action_flee
            self.add_item(btn_flee)
            
        elif self.state == "TREASURE":
            btn_take = discord.ui.Button(label="Take Item", style=discord.ButtonStyle.success, emoji="✋", custom_id=f"take_{self.session_id}", row=0)
            btn_take.callback = self.action_take_treasure
            btn_ignore = discord.ui.Button(label="Ignore", style=discord.ButtonStyle.secondary, emoji="⏭️", custom_id=f"ignore_{self.session_id}", row=0)
            btn_ignore.callback = self.action_ignore_treasure
            self.add_item(btn_take)
            self.add_item(btn_ignore)
            
        elif self.state == "SHRINE":
            btn_revive = discord.ui.Button(label="Sacrifice 50% HP to Revive", style=discord.ButtonStyle.success, emoji="👼", custom_id=f"shrine_{self.session_id}", row=0)
            btn_revive.callback = self.action_shrine
            btn_leave = discord.ui.Button(label="Leave Shrine", style=discord.ButtonStyle.secondary, emoji="🚶", custom_id=f"leave_{self.session_id}", row=0)
            btn_leave.callback = self.action_leave_room
            self.add_item(btn_revive)
            self.add_item(btn_leave)
            
        elif self.state == "EVENT":
            btn_drink = discord.ui.Button(label="Drink Potion", style=discord.ButtonStyle.danger, emoji="🧪", custom_id=f"drink_{self.session_id}", row=0)
            btn_drink.callback = self.action_event_drink
            btn_leave = discord.ui.Button(label="Walk Away", style=discord.ButtonStyle.secondary, emoji="🚶", custom_id=f"leave_{self.session_id}", row=0)
            btn_leave.callback = self.action_leave_room
            self.add_item(btn_drink)
            self.add_item(btn_leave)

        if self.state in ["EXPLORE", "COMBAT", "SHRINE", "EVENT"]:
            btn_inv = discord.ui.Button(label="Inventory", style=discord.ButtonStyle.secondary, emoji="🎒", custom_id=f"inv_{self.session_id}", row=2)
            btn_inv.callback = self.action_inventory
            self.add_item(btn_inv)
            
            if any(p.get('pending_level', 0) > 0 for p in self.party.values()):
                btn_lvl = discord.ui.Button(label="Level Up!", style=discord.ButtonStyle.success, emoji="🌟", custom_id=f"lvl_{self.session_id}", row=2)
                btn_lvl.callback = self.action_levelup
                self.add_item(btn_lvl)

    def get_embed(self):
        color = 0xe74c3c if self.state == "COMBAT" else 0x2c3e50
        embed = discord.Embed(title=f"🗡️ Floor {self.floor}", description=f"```{self.log}```\n💰 **Party Gold:** ${self.gold_earned:,.2f}", color=color)
        
        if self.relics:
            embed.add_field(name="🏛️ Party Relics", value=", ".join(self.relics).title().replace('_', ' '), inline=False)
        
        for uid, p in self.party.items():
            status = ""
            if p['hp'] <= 0: status = "💀 DEAD"
            elif p['status'] == 'poison': status = "🤢 Poisoned"
            elif p['status'] == 'burn': status = "🔥 Burning"
            elif p['status'] == 'freeze': status = "🥶 Frozen"
            
            action_status = ""
            if self.state == "COMBAT" and p['hp'] > 0:
                action_status = "✅ Ready" if uid in self.combat_moves else "⏳ Waiting..."
            
            lvl_text = f" (🌟 {p['pending_level']} Pending)" if p.get('pending_level', 0) > 0 else ""
            spell_str = ", ".join([s.replace('_', ' ').title() for s in p['active_spells']]) if p['active_spells'] else "None"
            
            embed.add_field(name=f"Lv.{p['level']} {p['emoji']} {p['user'].display_name} {action_status}{lvl_text}", 
                            value=f"**HP:** {p['hp']}/{p['max_hp']} {status}\n**ATK:** {p['atk']} | **DEF:** {p['def']} | **INT:** {p['intelligence']}\n**XP:** {p['xp']}/{p['max_xp']} | 🎒 **Items:** {len(p['inventory'])}/5\n✨ **Spells:** {spell_str}", 
                            inline=True)
            
        if self.state == "COMBAT" and self.enemy:
            e_status = ""
            if self.enemy_status == 'poison': e_status = "🕷️ Poisoned"
            elif self.enemy_status == 'freeze': e_status = "❄️ Frozen"
            
            embed.add_field(name=f"\n⚔️ ENEMY: {self.enemy['emoji']} {self.enemy['name']} {e_status}", 
                            value=f"**HP:** {self.enemy['hp']}/{self.enemy['max_hp']} | **ATK:** {self.enemy['atk']}", inline=False)
            
        if self.state == "TREASURE" and self.treasure:
            embed.add_field(name="\n🎁 Treasure Found!", value=f"**{self.treasure['emoji']} {self.treasure['name']}**\n*{self.treasure['desc']}*", inline=False)

        return embed

    async def update_message(self, interaction: discord.Interaction):
        self.build_ui()
        if self.state in ["ESCAPED", "WIPED"]:
            self.stop()
            for uid in self.party.keys(): active_runs.discard(uid)
            
            # STAT TRACKING
            db.log_rpg_run(self.floor, self.state, self.gold_earned, list(self.party.values()), killer=self.enemy['name'] if self.state == "WIPED" and self.enemy else None)
            
        if interaction.message:
            self.message = interaction.message
            
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=self.get_embed(), view=self)
            else:
                await interaction.message.edit(embed=self.get_embed(), view=self)
        except Exception as e:
            pass 

    async def action_inventory(self, interaction: discord.Interaction):
        p = self.party[interaction.user.id]
        if not p['inventory']:
            return await interaction.response.send_message("🎒 Your inventory is completely empty.", ephemeral=True)
        view = RPGInventoryView(self, interaction.user.id)
        await interaction.response.send_message("🎒 **Your Backpack:**\nSelect an item to use it instantly.", view=view, ephemeral=True)

    async def action_levelup(self, interaction: discord.Interaction):
        p = self.party[interaction.user.id]
        if p.get('pending_level', 0) <= 0:
            return await interaction.response.send_message("❌ You don't have any pending level ups.", ephemeral=True)
        view = RPGLevelUpView(self, interaction.user.id)
        await interaction.response.send_message("🌟 **Level Up!** Choose a stat to increase:", view=view, ephemeral=True)

    async def action_choose_path(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state != "EXPLORE": return await self.update_message(interaction)
            
            idx = int(interaction.data['custom_id'].split('_')[1])
            path = self.paths[idx]
            
            if path['type'] in ['combat', 'boss']:
                self.state = "COMBAT"
                self.enemy = path['data']
                if path['type'] == 'boss':
                    self.log = f"⚠️ BOSS ENCOUNTER! {self.enemy['name']} blocks your path!"
                else:
                    self.log = f"The party engages the {self.enemy['name']}!"
                    
            elif path['type'] == 'treasure':
                self.state = "TREASURE"
                avail_items = [i for i in ITEMS if i.get("min_floor", 1) <= self.floor]
                self.treasure = random.choice(avail_items)
                self.log = "The party entered a quiet room and found an abandoned chest!"
                
            elif path['type'] == 'camp':
                for uid, p in self.party.items():
                    if p['hp'] > 0:
                        p['hp'] = min(p['max_hp'], p['hp'] + int(p['max_hp'] * 0.3))
                self.log = "⛺ The party rested safely at a campfire and recovered 30% HP."
                self.floor += 1
                self.generate_paths()
                
            elif path['type'] == 'shrine':
                self.state = "SHRINE"
                self.log = "👼 The party finds a Holy Shrine. You may sacrifice 50% of your current HP to revive a fallen ally."
                
            elif path['type'] == 'event':
                self.state = "EVENT"
                self.log = "🔮 A mysterious hooded figure offers you a bubbling crimson potion... Do you drink it?"
                
            self.combat_moves.clear()
            await self.update_message(interaction)

    async def action_leave_room(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            self.log = f"🚶 The party walked away safely."
            self.floor += 1
            self.generate_paths()
            self.state = "EXPLORE"
            await self.update_message(interaction)

    async def action_shrine(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            p = self.party[interaction.user.id]
            if p['hp'] <= 0: return await self.update_message(interaction)
            
            dead_allies = [uid for uid, ally in self.party.items() if ally['hp'] <= 0]
            if not dead_allies:
                return await interaction.followup.send("❌ No dead allies to revive!", ephemeral=True)
            if p['hp'] <= 1:
                return await interaction.followup.send("❌ You don't have enough HP to sacrifice!", ephemeral=True)
            
            target_id = random.choice(dead_allies)
            self.party[target_id]['hp'] = self.party[target_id]['max_hp'] // 2
            p['hp'] = p['hp'] // 2
            self.log = f"👼 {interaction.user.display_name} sacrificed half their HP to revive {self.party[target_id]['user'].display_name}!"
            
            self.state = "EXPLORE"
            self.floor += 1
            self.generate_paths()
            await self.update_message(interaction)

    async def action_event_drink(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            p = self.party[interaction.user.id]
            if random.random() < 0.5:
                p['atk'] += 10
                self.log = f"💪 The potion surges with power! {interaction.user.display_name} gains +10 ATK!"
            else:
                p['status'] = 'poison'
                p['status_dur'] = 3
                self.log = f"🤢 The potion was toxic! {interaction.user.display_name} is poisoned!"
                
            self.state = "EXPLORE"
            self.floor += 1
            self.generate_paths()
            await self.update_message(interaction)

    async def register_move(self, interaction, move_type):
        async with self.lock:
            if self.state != "COMBAT" or self.enemy is None:
                return await self.update_message(interaction)
            if interaction.user.id in self.combat_moves:
                return await self.update_message(interaction) 
                
            self.combat_moves[interaction.user.id] = move_type
            
            alive = self.get_alive_players()
            if len(self.combat_moves) >= len(alive):
                await self.resolve_round(interaction)
            else:
                await self.update_message(interaction)

    async def action_attack(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.register_move(interaction, "attack")

    async def action_defend(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.register_move(interaction, "defend")

    async def action_spell(self, interaction: discord.Interaction):
        p = self.party[interaction.user.id]
        if not p['active_spells']:
            return await interaction.response.send_message("❌ You don't have any spells equipped!", ephemeral=True)
            
        if len(p['active_spells']) == 1:
            spell = p['active_spells'][0]
            if p['spell_cds'].get(spell, 0) > 0:
                return await interaction.response.send_message(f"⏳ That spell is on cooldown for {p['spell_cds'][spell]} turns.", ephemeral=True)
            await interaction.response.defer()
            await self.register_move(interaction, f"spell_{spell}")
        else:
            view = RPGSpellSelectView(self, interaction.user.id)
            await interaction.response.send_message("✨ **Select a spell to cast:**", view=view, ephemeral=True)

    async def action_flee(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state != "COMBAT" or self.enemy is None: return await self.update_message(interaction)
            
            if random.random() < 0.5:
                self.gold_earned //= 2
                self.log = f"🏃 {interaction.user.display_name} yells 'RUN!' The party escapes, but drops half their gold..."
                self.combat_moves.clear()
                self.floor += 1
                self.generate_paths()
                self.state = "EXPLORE"
                self.enemy = None
            else:
                self.log = f"🚫 {interaction.user.display_name} tries to flee, but the {self.enemy['name']} blocks the path!\n"
                self.combat_moves.clear()
                await self._enemy_attack_phase()
            await self.update_message(interaction)

    async def _enemy_attack_phase(self):
        if self.enemy_status == 'freeze':
            self.enemy_status = None 
            self.log += f"❄️ {self.enemy['name']} is frozen and skips its turn!\n"
            return

        if self.enemy['hp'] <= self.enemy['max_hp'] * 0.3 and not self.enemy.get('enraged') and self.enemy.get('boss'):
            self.enemy['enraged'] = True
            self.enemy['atk'] = int(self.enemy['atk'] * 1.1) 
            self.log += f"⚠️ THE {self.enemy['name'].upper()} BECOMES ENRAGED! ATK INCREASED! IT ATTACKS TWICE!\n"
            
        attacks = 2 if self.enemy.get('enraged') else 1
        
        for _ in range(attacks):
            target_pool = []
            taunters = [uid for uid, p in self.party.items() if self.combat_moves.get(uid) == "defend" and p['class'] == 'Tank' and p['hp'] > 0]
            
            if taunters:
                target_pool = taunters
            else:
                for uid in self.get_alive_players():
                    weight = 3 if self.party[uid]['class'] == 'Tank' else 1
                    target_pool.extend([uid] * weight)
                    
            if target_pool:
                target_id = random.choice(target_pool)
                t_player = self.party[target_id]
                
                divine_cleric = any('divine interference' in self.party[uid]['passives'] for uid in self.get_alive_players())
                dodge_chance = 0.20 + (0.15 if 'dodge' in self.relics else 0.0)
                
                if divine_cleric and random.random() < 0.15:
                    self.log += f"✨ Divine Interference protected {t_player['user'].display_name} from all damage!\n"
                elif 'dodge' in t_player['passives'] and random.random() < dodge_chance:
                    self.log += f"💨 {self.enemy['name']} attacks {t_player['user'].display_name}, but they DODGED!\n"
                else:
                    is_defending = self.combat_moves.get(target_id) == "defend"
                    dmg = max(1, self.enemy['atk'] - t_player['def'] + random.randint(-2, 2))
                    if is_defending: dmg = max(0, dmg // 2)
                    
                    t_player['hp'] -= dmg
                    self.log += f"💥 {self.enemy['name']} hits {t_player['user'].display_name} for {dmg} DMG!\n"
                    
                    if 'thorns' in t_player['passives'] and dmg > 0:
                        thorn_dmg = max(1, dmg // 3) 
                        self.enemy['hp'] -= thorn_dmg
                        self.log += f" 🌵 Thorns reflected {thorn_dmg} DMG!\n"
                    
                    if self.enemy.get('effect') and random.random() < self.enemy['chance'] and t_player['status'] is None:
                        t_player['status'] = self.enemy['effect']
                        t_player['status_dur'] = 2
                        self.log += f" They were {self.enemy['effect']}ed!\n"

    async def resolve_round(self, interaction):
        self.log = "-- COMBAT ROUND --\n"
        alive_players = self.get_alive_players()
        
        for uid in alive_players:
            p = self.party[uid]
            for s in p['spell_cds']:
                if p['spell_cds'][s] > 0: p['spell_cds'][s] -= 1

        if self.enemy_status == 'poison' and self.status_duration > 0:
            self.enemy['hp'] -= self.poison_dmg
            self.status_duration -= 1
            self.log += f"🕷️ Venom ticks for {self.poison_dmg} DMG!\n"
            if self.status_duration <= 0: self.enemy_status = None

        for uid in alive_players:
            p = self.party[uid]
            move = self.combat_moves.get(uid, "defend")
            
            if p['status'] == 'poison': p['hp'] -= 5
            elif p['status'] == 'burn': p['hp'] -= 8
            if p['status_dur'] > 0: p['status_dur'] -= 1
            if p['status_dur'] <= 0: p['status'] = None
            
            if p['hp'] <= 0:
                self.log += f"💀 {p['user'].display_name} succumbed to wounds!\n"
                continue
            if p['status'] == 'freeze':
                self.log += f"🥶 {p['user'].display_name} is frozen solid!\n"
                p['status'] = None
                continue

            if move == "attack":
                dmg = max(1, p['atk'] - self.enemy['def'] + random.randint(-2, 2))
                if 'unleashed rage' in p['passives'] and p['hp'] <= (p['max_hp'] * 0.6):
                    dmg = int(dmg * 1.5)
                    self.log += f"💢 {p['user'].display_name}'s Unleashed Rage triggers (+50% DMG)!\n"
                
                self.enemy['hp'] -= dmg
                self.log += f"⚔️ {p['user'].display_name} attacks for {dmg}!\n"
                
                if 'vampire' in self.relics:
                    heal = max(1, dmg // 10)
                    p['hp'] = min(p['max_hp'], p['hp'] + heal)
                    self.log += f"🦇 Vampire Fangs heal {p['user'].display_name} for {heal} HP!\n"
                
            elif move == "defend":
                base_heal = max(1, p['def'] // 2)
                max_heal = max(base_heal + 1, p['def'])
                heal = random.randint(base_heal, max_heal)
                p['hp'] = min(p['max_hp'], p['hp'] + heal)
                self.log += f"🛡️ {p['user'].display_name} defends and heals {heal} HP.\n"
                
            elif move.startswith("spell_"):
                spell_name = move.split("spell_")[1]
                p['spell_cds'][spell_name] = max(1, 3 - (p['intelligence'] // 5))
                
                if spell_name == 'party_heal':
                    heal_amt = 30 + (p['intelligence'] * 5)
                    for p_uid in alive_players:
                        self.party[p_uid]['hp'] = min(self.party[p_uid]['max_hp'], self.party[p_uid]['hp'] + heal_amt)
                    self.log += f"🪄 {p['user'].display_name} casts Party Heal (+{heal_amt} HP to all)!\n"
                    
                elif spell_name == 'poison':
                    self.enemy_status = 'poison'
                    self.status_duration = 3 + (p['intelligence'] // 2)
                    dmg_tick = 10 + (p['intelligence'] * 3)
                    if self.enemy.get('weakness') == 'poison':
                        dmg_tick *= 2
                        self.log += "🕷️ IT'S SUPER EFFECTIVE!\n"
                    self.poison_dmg = dmg_tick
                    self.log += f"🕷️ {p['user'].display_name} poisoned the enemy! ({dmg_tick} DMG/turn)\n"
                    
                elif spell_name == 'hellfire':
                    p['hp'] -= 10
                    fire_dmg = 50 + (p['intelligence'] * 6)
                    if self.enemy.get('weakness') == 'hellfire':
                        fire_dmg *= 2
                        self.log += "🔥 IT'S SUPER EFFECTIVE!\n"
                    self.enemy['hp'] -= fire_dmg
                    self.log += f"🔥 {p['user'].display_name} casts Hellfire (-10 HP, {fire_dmg} DMG)!\n"
                    
                elif spell_name == 'frostbite':
                    dmg = max(1, p['atk']) + (p['intelligence'] * 4)
                    if self.enemy.get('weakness') == 'frostbite':
                        dmg *= 2
                        self.log += "❄️ IT'S SUPER EFFECTIVE!\n"
                    self.enemy['hp'] -= dmg
                    self.enemy_status = 'freeze'
                    self.status_duration = 1 + (p['intelligence'] // 4)
                    self.log += f"❄️ {p['user'].display_name} casts Frostbite for {dmg} DMG and froze the enemy for {self.status_duration} turn(s)!\n"

        if self.enemy['hp'] <= 0:
            reward = int((random.randint(20, 50) + (self.floor * 5)) * len(self.party) + (self.enemy['max_hp'] * 0.6))
            if 'golden_idol' in self.relics:
                reward = int(reward * 1.5)
            self.gold_earned += reward
            self.log += f"\n💥 ENEMY DEFEATED! Party found ${reward}!\n"

            base_xp = random.randint(15, 25) + (self.floor * 4)
            xp_yield = base_xp * 2 if self.enemy.get('boss') else base_xp

            for uid in alive_players:
                p = self.party[uid]
                p['xp'] += xp_yield
                self.log += f"✨ {p['user'].display_name} gained {xp_yield} XP.\n"
                
                while p['xp'] >= p['max_xp']:
                    p['xp'] -= p['max_xp']
                    p['level'] += 1
                    p['max_xp'] = int(p['max_xp'] * 1.5) 
                    p['pending_level'] = p.get('pending_level', 0) + 1
                    self.log += f"🌟 **LEVEL UP!** {p['user'].display_name} reached Lvl {p['level']}! (Use the Level Up button to pick stats!)\n"

            self.floor += 1
            self.generate_paths()
            self.state = "EXPLORE"
            self.enemy = None
            self.enemy_status = None
            for p in self.party.values(): p['status'] = None 
            self.combat_moves.clear()
            return await self.update_message(interaction)

        await self._enemy_attack_phase()

        if len(self.get_alive_players()) == 0:
            self.state = "WIPED"
            self.log += "\n\n💀 THE PARTY WAS WIPED OUT. All gold lost."
            
        self.combat_moves.clear()
        await self.update_message(interaction)

    async def action_take_treasure(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state != "TREASURE" or not self.treasure: 
                self.state = "EXPLORE"
                return await self.update_message(interaction)
                
            p = self.party[interaction.user.id]
            t = self.treasure
            t_type = t.get('type')
            t_name = t.get('name')
            
            if random.random() < 0.15 and t_type not in ['gold', 'relic']:
                self.state = "COMBAT"
                self.enemy = {
                    "name": "Mimic", "emoji": "📦", 
                    "max_hp": 50 * len(self.party), 
                    "hp": 50 * len(self.party), 
                    "atk": 10 + self.floor, 
                    "def": 2 + (self.floor // 2), 
                    "chance": 0, "boss": False
                }
                self.log = f"⚠️ THE CHEST WAS A MIMIC! It bites {interaction.user.display_name}!\n"
                self.treasure = None
                self.combat_moves.clear()
                return await self.update_message(interaction)
            
            if t_type == 'gold': 
                self.gold_earned += t.get('val', 0)
                self.log = f"💰 {interaction.user.display_name} grabbed the {t.get('name', 'gold')}!"
            elif t_type == 'relic':
                self.relics.append(t['effect'])
                self.log = f"🏛️ {interaction.user.display_name} claimed the Relic: {t_name}! The whole party is buffed!"
            elif t_type == 'spell' and t.get('effect') in p['active_spells']: 
                self.log = f"🎒 {interaction.user.display_name} studies the {t_name}. (+1 INT)"
                p['intelligence'] += 1
            else:
                if len(p['inventory']) >= 5:
                    return await interaction.followup.send("❌ Your backpack is full! You can only carry 5 items.", ephemeral=True)
                p['inventory'].append(t)
                self.log = f"🎒 {interaction.user.display_name} pocketed the {t_name}!"
                
            self.floor += 1
            self.generate_paths()
            self.state = "EXPLORE"
            self.treasure = None
            await self.update_message(interaction)

    async def action_ignore_treasure(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state != "TREASURE": return await self.update_message(interaction)
            self.log = "The party leaves the item behind."
            self.floor += 1
            self.generate_paths()
            self.state = "EXPLORE"
            self.treasure = None
            await self.update_message(interaction)

    async def action_escape(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            if self.state not in ["EXPLORE", "TREASURE"]: return await self.update_message(interaction)
            
            self.state = "ESCAPED"
            split = self.gold_earned / len(self.party)
            self.log = f"The party escaped!"
            
            if self.gold_earned > 0:
                for uid in self.party.keys():
                    net, tax = db.process_town_payout(uid, split)
                self.log += f" Everyone gets ${net:,.2f} after Town Taxes!"
            else:
                self.log += " The party leaves empty-handed."
                    
            await self.update_message(interaction)

# --- RPG SHOP UI ---
class RPGShopDropdown(discord.ui.Select):
    def __init__(self):
        options = []
        for name, data in SHOP_GEAR.items():
            if data['cost'] > 0:
                stats = []
                if data['atk'] > 0: stats.append(f"ATK: +{data['atk']}")
                if data['def'] > 0: stats.append(f"DEF: +{data['def']}")
                if data['int'] > 0: stats.append(f"INT: +{data['int']}")
                options.append(discord.SelectOption(
                    label=name, description=f"{' | '.join(stats)} | Cost: ${data['cost']:,.2f}", emoji=data['emoji'], value=name
                ))
        super().__init__(placeholder="Select gear to purchase...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        gear_name = self.values[0]
        cost = SHOP_GEAR[gear_name]['cost']
        
        success, msg = db.buy_starter_weapon(interaction.user.id, gear_name, cost)
        if success:
            await interaction.response.send_message(f"🎉 **Success!** {msg}", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ {msg}", ephemeral=True)

class RPGShopUI(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(RPGShopDropdown())

# --- PARTY LOBBY ---
class RPGLobby(discord.ui.View):
    def __init__(self, host):
        super().__init__(timeout=300)
        self.host = host
        self.party = [host]

    @discord.ui.button(label="Join Party", style=discord.ButtonStyle.primary, emoji="✋")
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in self.party:
            return await interaction.response.send_message("❌ You are already in the party.", ephemeral=True)
        if len(self.party) >= 3:
            return await interaction.response.send_message("❌ The party is full! (Max 3)", ephemeral=True)
        if interaction.user.id in active_runs:
            return await interaction.response.send_message("❌ You are currently in another dungeon run.", ephemeral=True)
            
        await interaction.response.defer()
        self.party.append(interaction.user)
        embed = interaction.message.embeds[0]
        embed.description += f"\n✅ {interaction.user.mention} joined the party!"
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Start Dungeon", style=discord.ButtonStyle.success, emoji="⚔️")
    async def btn_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.host.id:
            return await interaction.response.send_message("❌ Only the host can start the run.", ephemeral=True)
            
        await interaction.response.defer()
        for user in self.party: active_runs.add(user.id)
        
        session = RPGSession(self.party)
        await interaction.edit_original_response(embed=session.get_embed(), view=session)
        session.message = await interaction.original_response() 
        self.stop()

# --- THE COG ---
class RPG(commands.GroupCog, group_name="rpg", group_description="Co-op Endless Dungeon."):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="class", description="Choose your RPG Class.")
    @app_commands.choices(class_name=[app_commands.Choice(name=c, value=c) for c in CLASSES.keys()])
    async def set_class(self, interaction: discord.Interaction, class_name: app_commands.Choice[str]):
        db.set_rpg_class(interaction.user.id, class_name.value)
        cls = CLASSES[class_name.value]
        await interaction.response.send_message(f"✅ You are now a **{class_name.value} {cls['emoji']}**!\n*(HP: {cls['hp']} | ATK: {cls['atk_mod']} | DEF: +{cls['def_mod']})*", ephemeral=True)

    @app_commands.command(name="shop", description="Buy permanent gear! All purchased gear stacks together.")
    async def shop(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="⚔️ The Blacksmith's Shop", 
            description="Buy permanent gear for your `/rpg play` runs! **All items you purchase permanently stack their stats together!**",
            color=0xe67e22
        )
        for name, data in SHOP_GEAR.items():
            if data['cost'] > 0:
                stats = []
                if data['atk'] > 0: stats.append(f"ATK: +{data['atk']}")
                if data['def'] > 0: stats.append(f"DEF: +{data['def']}")
                if data['int'] > 0: stats.append(f"INT: +{data['int']}")
                embed.add_field(name=f"{data['emoji']} {name}", value=f"*{', '.join(stats)}*\n**Cost:** ${data['cost']:,.2f}", inline=True)
        await interaction.response.send_message(embed=embed, view=RPGShopUI())

    @app_commands.command(name="profile", description="View your RPG Class, Stats, and Equipped Gear.")
    async def profile(self, interaction: discord.Interaction):
        gear_data, class_name = db.get_rpg_profile(interaction.user.id)
        gear_names = gear_data.split(',') if gear_data else ["Rusty Dagger"]
        cls = CLASSES.get(class_name, CLASSES["Fighter"])
        
        total_gear_atk, total_gear_def, total_gear_int = 0, 0, 0
        gear_display = []
        
        for g in gear_names:
            g = g.strip()
            g_item = SHOP_GEAR.get(g, SHOP_GEAR["Rusty Dagger"])
            total_gear_atk += g_item['atk']
            total_gear_def += g_item['def']
            total_gear_int += g_item['int']
            gear_display.append(f"{g_item['emoji']} {g}")
        
        total_hp = cls['hp']
        total_atk = total_gear_atk + cls['atk_mod']
        total_def = 5 + total_gear_def + cls['def_mod']
        total_int = total_gear_int + cls['spell_mod']
        
        embed = discord.Embed(title=f"🛡️ {interaction.user.display_name}'s RPG Profile", color=0x3498db)
        embed.add_field(
            name="Class", 
            value=f"**{cls['emoji']} {class_name}**\n*Passive: {cls['passive'].title().replace('_', ' ') if cls['passive'] else 'None'}*\n*Starter Spell: {cls['spell'].title().replace('_', ' ') if cls['spell'] else 'None'}*", inline=True
        )
        
        gear_str = "\n".join(gear_display) if gear_display else "None"
        embed.add_field(name="Stacked Armory", value=gear_str, inline=True)
        
        embed.add_field(name="Starting Dungeon Stats", value=f"**❤️ HP:** {total_hp}\n**⚔️ ATK:** {total_atk}\n**🛡️ DEF:** {total_def}\n**🧠 INT:** {total_int}", inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="play", description="Open a lobby for the Endless Dungeon.")
    async def play(self, interaction: discord.Interaction):
        if interaction.user.id in active_runs:
            return await interaction.response.send_message("❌ You are already in the dungeon! Finish or die in your current run first.", ephemeral=True)
            
        embed = discord.Embed(
            title="🏰 Dungeon Lobby", 
            description=f"Host: {interaction.user.mention}\n\nClick **Join Party** to play co-op, or the Host can click **Start** to begin immediately!", color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=RPGLobby(interaction.user))
        
    @app_commands.command(name="abort", description="Emergency button: Force-quit an active dungeon run.")
    async def abort(self, interaction: discord.Interaction):
        if interaction.user.id in active_runs:
            active_runs.discard(interaction.user.id)
            await interaction.response.send_message("🚨 **Run Aborted.** You have been forcefully removed from the active dungeon roster and can start a new game.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ You are not currently locked in an active dungeon run.", ephemeral=True)

    @app_commands.command(name="analytics", description="Admin: View RPG balance stats (avg floors, deadliest enemy, etc).")
    @app_commands.checks.has_permissions(administrator=True)
    async def analytics(self, interaction: discord.Interaction):
        stats = db.get_rpg_analytics()
        if not stats:
            return await interaction.response.send_message("📊 No runs recorded yet.", ephemeral=True)
            
        embed = discord.Embed(title="📊 RPG Analytics", color=0x9b59b6)
        embed.add_field(name="Runs Completed", value=str(stats['total_runs']), inline=True)
        embed.add_field(name="Avg Floor", value=str(stats['avg_floor']), inline=True)
        embed.add_field(name="Highest Floor", value=str(stats['max_floor']), inline=True)
        embed.add_field(name="Total Gold Earned", value=f"${stats['total_gold']:,.2f}", inline=True)
        embed.add_field(name="Deadliest Enemy", value=stats['deadliest'], inline=False)
        
        await interaction.response.send_message(embed=embed)

async def setup(bot):
    await bot.add_cog(RPG(bot))
