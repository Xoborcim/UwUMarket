import discord
from discord import app_commands
from discord.ext import commands, tasks
import database as db
import datetime
import random
import asyncio
import aiosqlite

# --- JOB DEFINITIONS ---
JOBS = {
    "Farmer": {
        "cooldown": 3, "payout": 90.0, "emoji": "🌾", "type": "farmer",
        "desc": "Grow food. If the town starves, EVERYONE'S pay is cut in half!"
    },
    "Miner": {
        "cooldown": 3, "payout": 90.0, "emoji": "⛏️", "type": "miner",
        "desc": "Mine town materials and personal ores. Don't cause a cave-in!"
    },
    "Builder": {
        "cooldown": 4, "payout": 180.0, "emoji": "🏗️", "type": "builder",
        "desc": "Earn steady pay. Perk: Use /career upgrade to level up the town!"
    },
    "Blacksmith": {
        "cooldown": 4, "payout": 150.0, "emoji": "🔨", "type": "blacksmith",
        "desc": "Earn steady pay. Perk: Use /career craft to forge RPG weapons!"
    },
    "Hacker": {
        "cooldown": 5, "payout": 0.0, "emoji": "💻", "type": "hacker",
        "desc": "Hack mainframes. Perk: Use /career hack to steal from players!"
    },
    "Politician": {
        "cooldown": 6, "payout": 240.0, "emoji": "🏛️", "type": "politician",
        "desc": "Earn steady pay. Perk: Use /career set_tax or /career embezzle!"
    }
}

# --- BLACKSMITH CRAFTING RECIPES ---
RECIPES = {
    "Iron Longsword": {"Iron Ore": 2},
    "Steel Halberd": {"Iron Ore": 4},
    "Golden Relic": {"Gold Ore": 3},
    "Aegis Plate Armor": {"Iron Ore": 2, "Gold Ore": 2},
    "Obsidian Scythe": {"Diamond": 2, "Gold Ore": 1},
    "Excalibur": {"Diamond": 3, "Gold Ore": 3, "Iron Ore": 5}
}

# --- HACKING ---
HEX_POOL   = ["E9", "CB", "2A", "7D", "1C", "55", "FF", "BD", "3E", "A7", "8F", "C3"]
SIPHON_TABLE = {0: 0.05, 1: 0.10, 2: 0.15, 3: 0.20}

# Crafting outputs become equippable RPG gear (weapon/armor/mage slot).
# Stats mirror RPG shop gear so crafted items are meaningful in `/rpg play`.
CRAFTED_GEAR = {
    "Iron Longsword": {"slot": "weapon", "atk_bonus": 25, "def_bonus": 0, "int_bonus": 0},
    "Steel Halberd": {"slot": "weapon", "atk_bonus": 30, "def_bonus": 0, "int_bonus": 0},
    "Golden Relic": {"slot": "mage", "atk_bonus": 0, "def_bonus": 0, "int_bonus": 0},
    "Aegis Plate Armor": {"slot": "armor", "atk_bonus": 0, "def_bonus": 18, "int_bonus": 0},
    "Obsidian Scythe": {"slot": "weapon", "atk_bonus": 35, "def_bonus": 0, "int_bonus": 0},
    "Excalibur": {"slot": "weapon", "atk_bonus": 45, "def_bonus": 0, "int_bonus": 0},
}

# Added Async/Aiosqlite Support here!
async def consume_materials(user_id, materials):
    async with aiosqlite.connect(db.DB_NAME) as conn:
        conn.row_factory = aiosqlite.Row
        
        # 1. Check if they have enough of everything first
        for item_name, count in materials.items():
            async with conn.execute("SELECT item_id FROM inventory WHERE user_id = ? AND item_name = ? AND is_listed = 0 LIMIT ?", (user_id, item_name, count)) as cursor:
                items = await cursor.fetchall()
            if len(items) < count:
                return False, f"Missing materials. You need {count}x **{item_name}**."
                
        # 2. If they have enough, consume them
        for item_name, count in materials.items():
            async with conn.execute("SELECT item_id FROM inventory WHERE user_id = ? AND item_name = ? AND is_listed = 0 LIMIT ?", (user_id, item_name, count)) as cursor:
                items = await cursor.fetchall()
            for i in items:
                await conn.execute("DELETE FROM inventory WHERE item_id = ?", (i['item_id'],))
                
        await conn.commit()
        return True, "Success"

# --- EMOJI TOWN MAP GENERATOR ---
def generate_town_map(level, famine):
    grass = "🟩" if not famine else "🟫"
    crop = "🌾" if not famine else "🥀"
    
    if level < 5: bldg, road, decor = "⛺", "🟫", "🌲"
    elif level < 10: bldg, road, decor = "🛖", "🪨", "🪵"
    elif level < 20: bldg, road, decor = "🏘️", "🛣️", "🌳"
    else: bldg, road, decor = "🏰", "🛣️", "🗽"
        
    grid = [[grass for _ in range(7)] for _ in range(5)]
    
    for i in range(5):
        grid[i][3] = road
        if level >= 10:
            grid[2][i] = road
            if i+2 < 7: grid[2][i+2] = road
            
    grid[0][0] = crop; grid[0][1] = crop
    grid[0][5] = crop; grid[0][6] = crop
    grid[4][0] = crop; grid[4][1] = crop
    grid[4][5] = crop; grid[4][6] = crop
    
    slots = [(1,2), (1,4), (3,2), (3,4), (2,1), (2,5), (0,2), (0,4), (4,2), (4,4), (1,1), (1,5), (3,1), (3,5)]
    num_buildings = min(len(slots), 1 + (level // 2))
    
    for i in range(num_buildings):
        r, c = slots[i]
        grid[r][c] = bldg
        
    for r in range(5):
        for c in range(7):
            if grid[r][c] == grass and random.random() < 0.2:
                grid[r][c] = decor
                
    return "\n".join("".join(row) for row in grid)

def build_town_embed(t):
    status_str = "🔴 **FAMINE** (-50% Global Income)" if t['famine'] else "🟢 **Prospering**"
    tax_pct = int(t['tax_rate'] * 100)
    wealth_tax_pct = t['tax_rate'] * 10 # 10% of the income tax rate
    boost_pct = int((t['level'] * 0.05) * 100)
    drain_amt = t.get('user_count', 1) * 2
    
    map_str = generate_town_map(t['level'], t['famine'])
    
    embed = discord.Embed(
        title=f"🏙️ Polyville (Level {t['level']})", 
        description=f"**Status:** {status_str}\n*(+{boost_pct}% to all paychecks)*\n\n{map_str}", 
        color=0x3498db if not t['famine'] else 0xe74c3c
    )
    embed.add_field(name="💰 Treasury", value=f"**${t['treasury']:,.2f}**\n*(Income Tax: {tax_pct}% | Daily Wealth Tax: {wealth_tax_pct:.1f}%)*", inline=True)
    embed.add_field(name="🍲 Food", value=f"**{t['food']}** *(Drains {drain_amt}/day)*", inline=True)
    embed.add_field(name="🧱 Materials", value=f"**{t['materials']}**", inline=True)
    embed.set_footer(text="The map updates automatically in real-time.")
    return embed

# --- FORCE BOARD UPDATE LOGIC ---
async def force_board_update(bot):
    """Instantly updates the live town board message."""
    t = await db.get_town_state() # Added Await!
    if not t or not t.get('board_channel_id') or not t.get('board_message_id'): return
    try:
        channel = bot.get_channel(t['board_channel_id']) or await bot.fetch_channel(t['board_channel_id'])
        if channel:
            msg = await channel.fetch_message(t['board_message_id'])
            await msg.edit(embed=build_town_embed(t))
    except Exception as e:
        print(f"[Town Board] Could not update board: {e}")

# ==========================================
#             MINI-GAMES
# ==========================================
class FarmerGame(discord.ui.View):
    def __init__(self, user, base_pay):
        super().__init__(timeout=30)
        self.user = user
        self.base_pay = base_pay
        
    @discord.ui.button(label="Harvest Crops", style=discord.ButtonStyle.success, emoji="🌾")
    async def harvest(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        self.clear_items()
        
        food_yield = random.randint(15, 30)
        await db.add_town_resources(food=food_yield) # Added Await!
        net, tax = await db.process_work(self.user.id, self.base_pay) # Added Await!
        
        embed = discord.Embed(
            title="🌾 Bountiful Harvest", 
            description=f"You worked the fields and generated **{food_yield} Food** for the town!\n\n**Paycheck:** ${net:,.2f}\n*(Taxes paid to town: ${tax:,.2f})*", 
            color=0x2ecc71
        )
        await interaction.response.edit_message(embed=embed, view=self)
        await force_board_update(interaction.client) 
        self.stop()

class MinerGame(discord.ui.View):
    def __init__(self, user, base_pay):
        super().__init__(timeout=60)
        self.user = user
        self.base_pay = base_pay
        self.risk = 0.10
        self.mats = 0
        self.inventory = {}
        self.ended = False
    
    def get_embed(self, status="playing"):
        if status == "playing":
            embed = discord.Embed(title="⛏️ The Deep Mines", description=f"Swing your pickaxe to find ores!\n**Risk of Cave-in:** {int(self.risk*100)}%", color=0x95a5a6)
        elif status == "cavein":
            embed = discord.Embed(title="💥 CAVE-IN!", description="The roof collapsed! You dropped all your materials and fled. No paycheck today.", color=0xe74c3c)
        elif status == "left":
            embed = discord.Embed(title="🚪 Safe Escape", description="You safely left the mine with your haul!", color=0x2ecc71)
        
        embed.add_field(name="🏗️ Town Materials", value=f"{self.mats} Bricks", inline=False)
        loot_str = "\n".join([f"**{v}x** {k}" for k, v in self.inventory.items()])
        embed.add_field(name="🎒 Personal Ores", value=loot_str if loot_str else "Empty Cart", inline=False)
        return embed
        
    @discord.ui.button(label="Swing Pickaxe", style=discord.ButtonStyle.primary, emoji="⛏️")
    async def swing(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        if random.random() < self.risk:
            self.ended = True
            self.clear_items()
            await db.process_work(self.user.id, 0) # Added Await!
            await interaction.response.edit_message(embed=self.get_embed("cavein"), view=self)
            self.stop()
        else:
            self.risk += 0.08
            self.mats += random.randint(2, 6)
            
            roll = random.random()
            if roll < 0.05: drop = "Diamond"
            elif roll < 0.25: drop = "Gold Ore"
            else: drop = "Iron Ore"
            self.inventory[drop] = self.inventory.get(drop, 0) + 1
            
            await interaction.response.edit_message(embed=self.get_embed("playing"), view=self)

    @discord.ui.button(label="Leave Mine", style=discord.ButtonStyle.success, emoji="🚪")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        self.ended = True
        self.clear_items()
        
        await db.add_town_resources(materials=self.mats) # Added Await!
        for item, count in self.inventory.items():
            for _ in range(count):
                await db.add_item(self.user.id, item, "Resource", "Materials") # Added Await!
                
        net, tax = await db.process_work(self.user.id, self.base_pay) # Added Await!
        
        embed = self.get_embed("left")
        embed.description += f"\n\n**Paycheck:** ${net:,.2f} *(Tax: ${tax:,.2f})*"
        await interaction.response.edit_message(embed=embed, view=self)
        await force_board_update(interaction.client) 
        self.stop()
        
class GuillotineVoteView(discord.ui.View):
    def __init__(self, target, initiator, bot):
        super().__init__(timeout=120) # 2 minutes to vote
        self.target = target
        self.initiator = initiator
        self.bot = bot
        self.votes = set()
        self.required_votes = 5 # ⚠️ Change this number if you want more or fewer people to vote!
        
    @discord.ui.button(label="Guilty! (0/5)", style=discord.ButtonStyle.danger, emoji="🔪")
    async def vote_guilty(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.votes:
            return await interaction.response.send_message("❌ You already cast your vote!", ephemeral=True)
        if interaction.user.id == self.target.id:
            return await interaction.response.send_message("❌ You can't vote at your own execution!", ephemeral=True)
            
        self.votes.add(interaction.user.id)
        
        if len(self.votes) >= self.required_votes:
            self.stop()
            seized = await db.execute_player(self.target.id, 0.50) # Added Await!
            
            head_name = f"{self.target.display_name}'s Severed Head"
            # Store whose head this is so the website can show their avatar.
            await db.add_item(
                self.initiator.id,
                head_name,
                "Mythic",
                "Trophy",
                item_type="Collectible",
                head_owner_id=self.target.id,
            )
            
            embed = discord.Embed(
                title="🩸 PUBLIC EXECUTION 🩸", 
                description=f"### VIVE LA RÉVOLUTION!\n\nThe mob has spoken! **{self.target.mention}** was executed for crimes against Polyville.\n\n**50% of their wealth (${seized:,.2f})** has been seized and returned to the Town Treasury!\n\n*{self.initiator.display_name} kept the head as a trophy.*", 
                color=0x8a0303
            )
            embed.set_thumbnail(url=self.target.display_avatar.url)
            await interaction.response.edit_message(embed=embed, view=None)
            await force_board_update(self.bot)
        else:
            button.label = f"Guilty! ({len(self.votes)}/{self.required_votes})"
            await interaction.response.edit_message(view=self)
            
    async def on_timeout(self):
        try:
            self.clear_items()
            embed = discord.Embed(title="🕊️ Execution Failed", description=f"The mob lost interest. Not enough votes were cast. **{self.target.display_name}** survives another day.", color=0x95a5a6)
            await self.message.edit(embed=embed, view=self)
        except: pass

class ElectionVoteView(discord.ui.View):
    def __init__(self, candidate, bot):
        super().__init__(timeout=600)
        self.candidate = candidate
        self.bot = bot
        self.votes = set()
        self.required_votes = 4
        self.message = None

    @discord.ui.button(label="Vote Yes (0/4)", style=discord.ButtonStyle.success, emoji="🗳️")
    async def vote_yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in self.votes:
            return await interaction.response.send_message("❌ You already voted!", ephemeral=True)
        if interaction.user.id == self.candidate.id:
            return await interaction.response.send_message("❌ You can't vote in your own election!", ephemeral=True)

        self.votes.add(interaction.user.id)

        if len(self.votes) >= self.required_votes:
            self.stop()
            await db.set_job(self.candidate.id, "Politician") # Added Await!
            embed = discord.Embed(
                title="🏛️ ELECTION WON",
                description=f"### The people have spoken!\n\n{self.candidate.mention} has been elected as **Politician**!\n\nThey now have the power to set taxes and access the treasury.",
                color=0x2ecc71
            )
            embed.set_thumbnail(url=self.candidate.display_avatar.url)
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            button.label = f"Vote Yes ({len(self.votes)}/{self.required_votes})"
            await interaction.response.edit_message(view=self)

    async def on_timeout(self):
        try:
            self.clear_items()
            embed = discord.Embed(
                title="🏛️ Election Failed",
                description=f"Not enough votes were cast. **{self.candidate.display_name}**'s campaign has ended.",
                color=0x95a5a6
            )
            await self.message.edit(embed=embed, view=self)
        except: pass


class BreachProtocolGame(discord.ui.View):
    """
    Reusable 5x5 breach protocol grid.

    grid_size        : always 5 (Discord button row limit)
    sequences        : how many sequences must be solved (chained)
    seq_length       : codes per sequence
    timeout_seconds  : timer
    on_complete(interaction, sequences_done: int)
                     : called when all done OR wrong code OR timeout
                       sequences_done = how many full sequences were completed
    """
    def __init__(self, user, grid_size, sequences, seq_length, timeout_seconds, on_complete):
        super().__init__(timeout=timeout_seconds)
        self.user            = user
        self.grid_size       = grid_size
        self.total_sequences = sequences
        self.seq_length      = seq_length
        self.on_complete     = on_complete
        self.ended           = False
        self.message         = None
        self.sequences_done  = 0

        # Build grid
        self.grid = [
            [random.choice(HEX_POOL) for _ in range(grid_size)]
            for _ in range(grid_size)
        ]

        # Generate all sequences upfront as guaranteed-solvable paths
        self.all_targets = []
        for _ in range(sequences):
            self.all_targets.append(self._generate_sequence())

        self.current_seq = 0
        self.entered     = []

        # Selection state — always start locked to row 0
        self.axis        = "row"
        self.locked_idx  = 0
        self.used_cells  = set()

        self._build_grid()

    def _generate_sequence(self):
        seq    = []
        axis   = "row"
        locked = 0
        used   = set(self.used_cells)
        for _ in range(self.seq_length):
            candidates = (
                [(locked, c) for c in range(self.grid_size) if (locked, c) not in used]
                if axis == "row"
                else [(r, locked) for r in range(self.grid_size) if (r, locked) not in used]
            )
            if not candidates:
                break
            r, c = random.choice(candidates)
            seq.append(self.grid[r][c])
            used.add((r, c))
            locked = c if axis == "row" else r
            axis   = "col" if axis == "row" else "row"
        return seq

    @property
    def target(self):
        return self.all_targets[self.current_seq]

    def _is_selectable(self, r, c):
        return (r == self.locked_idx) if self.axis == "row" else (c == self.locked_idx)

    def _build_grid(self):
        self.clear_items()
        for r in range(self.grid_size):
            for c in range(self.grid_size):
                is_used    = (r, c) in self.used_cells
                selectable = self._is_selectable(r, c) and not is_used and not self.ended
                btn = discord.ui.Button(
                    label     = self.grid[r][c] if not is_used else "██",
                    style     = discord.ButtonStyle.primary if selectable else discord.ButtonStyle.secondary,
                    custom_id = f"bp_{r}_{c}",
                    row       = r,
                    disabled  = not selectable
                )
                btn.callback = self._make_callback(r, c)
                self.add_item(btn)

    def _make_callback(self, r, c):
        async def cb(interaction: discord.Interaction):
            try:
                if interaction.user.id != self.user.id:
                    return await interaction.response.defer()
                if self.ended:
                    return await interaction.response.defer()
                if not self._is_selectable(r, c):
                    return await interaction.response.defer()
                if (r, c) in self.used_cells:
                    return await interaction.response.defer()

                chosen   = self.grid[r][c]
                expected = self.target[len(self.entered)]

                self.used_cells.add((r, c))
                self.locked_idx = c if self.axis == "row" else r
                self.axis       = "col" if self.axis == "row" else "row"

                if chosen == expected:
                    self.entered.append(chosen)

                    if len(self.entered) == len(self.target):
                        self.sequences_done += 1
                        self.entered = []

                        if self.sequences_done == self.total_sequences:
                            # All sequences done — success
                            self.ended = True
                            self.clear_items()
                            await self.on_complete(interaction, self.sequences_done)
                            self.stop()
                        else:
                            # Advance to next sequence, reset axis but keep used_cells
                            self.current_seq += 1
                            self.axis        = "row"
                            self.locked_idx  = 0
                            self._build_grid()
                            await interaction.response.edit_message(
                                embed=self.get_embed(
                                    extra_desc=f"✅ Sequence {self.sequences_done} complete! Now load sequence {self.current_seq + 1}."
                                ),
                                view=self
                            )
                    else:
                        self._build_grid()
                        await interaction.response.edit_message(embed=self.get_embed(), view=self)
                else:
                    # Wrong code — report whatever was completed so far
                    self.ended = True
                    self.clear_items()
                    await self.on_complete(interaction, self.sequences_done)
                    self.stop()
            except Exception as e:
                # Failsafe so the interaction is always acknowledged and doesn't time out
                print(f\"[BreachProtocolGame] Button error: {e}\")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message(
                            \"❌ Something went wrong handling this hack action. Please try again.\",
                            ephemeral=True
                        )
                    else:
                        await interaction.followup.send(
                            \"❌ Something went wrong handling this hack action. Please try again.\",
                            ephemeral=True
                        )
                except Exception:
                    pass

        return cb

    def get_embed(self, title="💻 BREACH PROTOCOL", color=0x00ff9f, extra_desc=""):
        progress = []
        for i, code in enumerate(self.target):
            if i < len(self.entered):    progress.append(f"~~`{code}`~~")
            elif i == len(self.entered): progress.append(f"**`{code}`**")
            else:                        progress.append(f"`{code}`")
        progress_str = "  →  ".join(progress)

        axis_hint = (
            f"Select from **ROW {self.locked_idx + 1}** →"
            if self.axis == "row"
            else f"↓ Select from **COLUMN {self.locked_idx + 1}**"
        )

        seq_counter = f"Sequence **{self.current_seq + 1}** of **{self.total_sequences}**"

        desc = "\n\n".join(filter(None, [
            extra_desc,
            f"{seq_counter}\n**Progress:** {progress_str}",
            axis_hint
        ]))

        embed = discord.Embed(title=title, description=desc, color=color)
        embed.set_footer(text="Highlighted = valid selections  |  Wrong code = instant fail")
        return embed

    async def on_timeout(self):
        if not self.ended:
            self.ended = True
            self.clear_items()
            await self.on_complete(None, self.sequences_done)

class HackerGame:
    """
    Factory — wires BreachProtocolGame for /career work.
    5x5, 1 sequence of 4 codes, 25 seconds.
    """
    def __init__(self, user, payout_range=(360, 720)):
        self.user         = user
        self.payout_range = payout_range
        self._view        = None

    def build(self):
        payout_range = self.payout_range
        user         = self.user
        factory      = self

        async def on_complete(interaction, sequences_done):
            if sequences_done == 1:
                payout = random.randint(*payout_range)
                net, tax = await db.process_work(user.id, payout)
                embed = discord.Embed(
                    title="💻 ACCESS GRANTED",
                    description=(
                        f"Mainframe breached. Funds siphoned.\n\n"
                        f"**Payout:** ${net:,.2f}\n"
                        f"*(Laundered Tax: ${tax:,.2f})*"
                    ),
                    color=0x00ff9f
                )
            else:
                await db.process_work(user.id, 0)
                embed = discord.Embed(
                    title="🚨 BREACH FAILED",
                    description="ICE detected you. No payout this shift.",
                    color=0xe74c3c
                )

            if interaction:
                await interaction.response.edit_message(embed=embed, view=None)
                await force_board_update(interaction.client)
            else:
                try: await factory._view.message.edit(embed=embed, view=None)
                except Exception: pass

        game = BreachProtocolGame(
            user            = user,
            grid_size       = 5,
            sequences       = 1,
            seq_length      = 4,
            timeout_seconds = 25,
            on_complete     = on_complete
        )
        self._view = game
        return game

    def get_embed(self):
        return self._view.get_embed(
            title      = "💻 BREACH PROTOCOL — WORK SHIFT",
            color      = 0x00ff9f,
            extra_desc = "Breach the mainframe to earn your paycheck. **25 seconds.**"
        )
    
class HackExecutionGame:
    """
    Factory — wires BreachProtocolGame for /career hack execution.
    5x5, 3 sequences of 5 codes, 60 seconds.
    Siphon: 0 seqs=5%, 1=10%, 2=15%, 3=20%
    """
    def __init__(self, user, on_result):
        self.user      = user
        self.on_result = on_result
        self._view     = None

    def build(self):
        user      = self.user
        on_result = self.on_result

        async def on_complete(interaction, sequences_done):
            pct = SIPHON_TABLE.get(sequences_done, 0.05)
            await on_result(interaction, pct)

        game = BreachProtocolGame(
            user            = user,
            grid_size       = 5,
            sequences       = 3,
            seq_length      = 5,
            timeout_seconds = 60,
            on_complete     = on_complete
        )
        self._view = game
        return game

    def get_embed(self):
        return self._view.get_embed(
            title      = "💻 HACK EXECUTION — BREACH PROTOCOL",
            color      = 0xf39c12,
            extra_desc = (
                "Complete sequences to maximise your siphon:\n"
                "**0 sequences → 5%  |  1 → 10%  |  2 → 15%  |  3 → 20%**\n"
                "You have **60 seconds.**"
            )
        )
    
class FirewallBolsterGame:
    """
    Factory — wires BreachProtocolGame for /career bolster.
    5x5, 3 sequences of 4 codes, 60 seconds.
    Each sequence completed = +10% hack fail chance (max 3 bolsters).
    """
    def __init__(self, user, on_result):
        self.user      = user
        self.on_result = on_result
        self._view     = None

    def build(self):
        user      = self.user
        on_result = self.on_result

        async def on_complete(interaction, sequences_done):
            await on_result(interaction, sequences_done)

        game = BreachProtocolGame(
            user            = user,
            grid_size       = 5,
            sequences       = 3,
            seq_length      = 4,
            timeout_seconds = 60,
            on_complete     = on_complete
        )
        self._view = game
        return game

    def get_embed(self):
        return self._view.get_embed(
            title      = "🛡️ FIREWALL BOLSTER — BREACH PROTOCOL",
            color      = 0x3498db,
            extra_desc = (
                "Each sequence completed adds **+10% hack fail chance** (max 3 bolsters).\n"
                "**60 seconds.**"
            )
        )

class FirewallRebootGame:
    """
    Factory — wires BreachProtocolGame for /career reboot.
    5x5, 1 sequence of 5 codes, 45 seconds.
    Success = firewall rebooted, hacker ID stored for identify attempt.
    """
    def __init__(self, user, on_result):
        self.user      = user
        self.on_result = on_result
        self._view     = None

    def build(self):
        user      = self.user
        on_result = self.on_result

        async def on_complete(interaction, sequences_done):
            await on_result(interaction, sequences_done >= 1)

        game = BreachProtocolGame(
            user            = user,
            grid_size       = 5,
            sequences       = 1,
            seq_length      = 5,
            timeout_seconds = 45,
            on_complete     = on_complete
        )
        self._view = game
        return game

    def get_embed(self):
        return self._view.get_embed(
            title      = "🔄 FIREWALL REBOOT — BREACH PROTOCOL",
            color      = 0xe74c3c,
            extra_desc = (
                "Your firewall is **COMPROMISED**. Solve the sequence to reboot it.\n"
                "On success you can attempt to **identify** who hacked you.\n"
                "**45 seconds.**"
            )
        )

class IdentifyGuessView(discord.ui.View):
    """
    After rebooting, victim gets ONE guess at who hacked them.
    Correct = hacker pays $500 fine to victim.
    """
    def __init__(self, victim, actual_hacker_id, guild):
        super().__init__(timeout=120)
        self.victim           = victim
        self.actual_hacker_id = actual_hacker_id
        self.guild            = guild
        self.message          = None

    def build_select(self, hacker_candidates):
        """Pass a list of discord.Member. Returns False if empty."""
        if not hacker_candidates:
            return False
        options = [
            discord.SelectOption(label=m.display_name, value=str(m.id))
            for m in hacker_candidates[:25]
        ]
        select = discord.ui.Select(
            placeholder = "Who hacked you?",
            options     = options,
            custom_id   = "identify_select"
        )
        select.callback = self._select_callback
        self.add_item(select)
        return True

    async def _select_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.victim.id:
            return await interaction.response.defer()

        self.clear_items()
        guessed_id = int(interaction.data["values"][0])

        if guessed_id == self.actual_hacker_id:
            fine = 500.0
            await db.update_balance(self.actual_hacker_id, -fine)
            await db.update_balance(self.victim.id, fine)
            hacker_member = interaction.guild.get_member(self.actual_hacker_id)
            hacker_name   = hacker_member.mention if hacker_member else f"<@{self.actual_hacker_id}>"
            embed = discord.Embed(
                title       = "🎯 CORRECT IDENTIFICATION",
                description = (
                    f"You correctly identified **{hacker_name}** as the culprit!\n\n"
                    f"They were fined **$500.00** which was sent directly to your account."
                ),
                color = 0x2ecc71
            )
        else:
            embed = discord.Embed(
                title       = "❌ WRONG TARGET",
                description = "Incorrect. The real hacker slipped away.",
                color       = 0xe74c3c
            )

        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    async def on_timeout(self):
        self.clear_items()
        try:
            await self.message.edit(
                embed=discord.Embed(
                    title       = "⏰ Identify Window Expired",
                    description = "You didn't make a guess in time.",
                    color       = 0x95a5a6
                ),
                view=self
            )
        except Exception:
            pass
# ==========================================
#               THE COG
# ==========================================
class Jobs(commands.GroupCog, group_name="career", group_description="Make money and manage the town."):
    def __init__(self, bot):
        self.bot = bot
        if not self.town_upkeep_task.is_running():
            self.town_upkeep_task.start()

    def cog_unload(self):
        self.town_upkeep_task.cancel()

    # --- RELIABLE BACKGROUND TASK (Checks every 10 minutes) ---
    @tasks.loop(minutes=10)
    async def town_upkeep_task(self):
        await self.bot.wait_until_ready()
        try:
            ran_upkeep, is_famine, drain, tax_collected = await db.run_town_daily_upkeep() # Added Await!
            
            if ran_upkeep:
                # ⚠️ IMPORTANT: REPLACE THIS NUMBER WITH YOUR ANNOUNCEMENT CHANNEL ID!
                channel = self.bot.get_channel(123456789012345678) 
                if channel:
                    if is_famine:
                        embed = discord.Embed(title="🚨 TOWN FAMINE 🚨", description=f"The town ran out of food! The citizens are starving.\n\n**All incomes and paychecks across the server are cut by 50% until Farmers restore the food supply!**", color=0xe74c3c)
                        embed.add_field(name="📉 Daily Wealth Tax", value=f"**${tax_collected:,.2f}** was taxed from citizen bank accounts.")
                        await channel.send(embed=embed)
                    else:
                        embed = discord.Embed(title="🍲 Daily Town Upkeep", description=f"The town consumed **{drain} Food** today to stay healthy.", color=0x3498db)
                        embed.add_field(name="📈 Daily Wealth Tax", value=f"**${tax_collected:,.2f}** was taxed from citizen bank accounts and added to the Treasury.")
                        await channel.send(embed=embed)
                await force_board_update(self.bot)
        except Exception as e:
            print(f"Town Upkeep Error: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or len(message.content) < 5: return
        if random.random() < 0.50:
            amount = round(random.uniform(1.0, 3.0), 2)
            await db.process_chat_income(message.author.id, amount, daily_cap=200.0) # Added Await!

    # --- TOWN UI COMMANDS ---
    @app_commands.command(name="setup_board", description="Admin: Setup the live updating town board in this channel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup_board(self, interaction: discord.Interaction):
        t = await db.get_town_state() # Added Await!
        embed = build_town_embed(t)
        await interaction.response.send_message("Initializing town board...", ephemeral=True)
        msg = await interaction.channel.send(embed=embed)
        await db.set_town_board(interaction.channel.id, msg.id) # Added Await!
        await interaction.followup.send("✅ Board setup complete! It will update automatically.", ephemeral=True)

    @app_commands.command(name="town", description="View the current status of the server's Town!")
    async def view_town(self, interaction: discord.Interaction):
        t = await db.get_town_state() # Added Await!
        embed = build_town_embed(t)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="jobs", description="View the list of available jobs.")
    async def jobs_list(self, interaction: discord.Interaction):
        embed = discord.Embed(title="🏢 Job Center", description="Use `/career apply [Job Name]` to get hired!\n\n", color=0x3498db)
        for job_name, info in JOBS.items():
            embed.description += f"### {info['emoji']} {job_name}\n**Shift Length:** {info['cooldown']} Hours\n*{info['desc']}*\n\n"
        await interaction.response.send_message(embed=embed)

    async def job_autocomplete(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=job, value=job) for job in JOBS.keys() if current.lower() in job.lower()]

    @app_commands.command(name="apply", description="Apply for a new job.")
    @app_commands.autocomplete(job_name=job_autocomplete)
    async def apply(self, interaction: discord.Interaction, job_name: str):
        if job_name not in JOBS:
            return await interaction.response.send_message("❌ That job doesn't exist. Check `/career jobs`.", ephemeral=True)
        if job_name == "Politician":
            return await interaction.response.send_message(
                "🏛️ Politicians must be **elected**! Use `/career run_for_office` to start a campaign.",
                ephemeral=True)
        await db.set_job(interaction.user.id, job_name) # Added Await!
        await interaction.response.send_message(f"🎉 Congratulations! You are now a **{job_name}**!\nUse `/career work` to start your shift.")

    @app_commands.command(name="run_for_office", description="Start a public election to become the town Politician.")
    @app_commands.checks.cooldown(1, 3600, key=lambda i: i.user.id)
    async def run_for_office(self, interaction: discord.Interaction):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        if profile and profile['job'] == "Politician":
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ You are already a Politician!", ephemeral=True)

        embed = discord.Embed(
            title="🏛️ ELECTION: VOTE NOW",
            description=f"{interaction.user.mention} is running for **Politician**!\n\n**If 4 citizens vote YES, they will be elected into office.**\n\nYou have 10 minutes to cast your vote.",
            color=0x3498db
        )
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        view = ElectionVoteView(interaction.user, self.bot)
        await interaction.response.send_message(embed=embed, view=view)
        view.message = await interaction.original_response()

    @run_for_office.error
    async def run_for_office_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CommandOnCooldown):
            mins = error.retry_after / 60
            await interaction.response.send_message(f"⏳ You must wait **{mins:.0f} minutes** before running for office again.", ephemeral=True)

    @app_commands.command(name="work", description="Go to work and earn your paycheck.")
    async def work(self, interaction: discord.Interaction):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        current_job = profile['job'] if profile else "Unemployed"
        
        if current_job == "Unemployed" or current_job not in JOBS:
            return await interaction.response.send_message("❌ You don't have a job! Use `/career apply` to find one.", ephemeral=True)
            
        job_data = JOBS[current_job]
        
        if profile and profile['last_work']:
            try: last_work = datetime.datetime.strptime(profile['last_work'], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError: last_work = datetime.datetime.strptime(profile['last_work'], "%Y-%m-%d %H:%M:%S")
            
            next_work = last_work + datetime.timedelta(hours=job_data['cooldown'])
            if datetime.datetime.now() < next_work:
                ts = int(next_work.timestamp())
                return await interaction.response.send_message(f"⏳ You are off shift. You can work again <t:{ts}:R>.", ephemeral=True)
                
        if job_data['type'] == "farmer":
            view = FarmerGame(interaction.user, job_data['payout'])
            await interaction.response.send_message("🌾 **The Fields**\nThe town needs food to survive. Get to work!", view=view)
            
        elif job_data['type'] == "miner":
            view = MinerGame(interaction.user, job_data['payout'])
            await interaction.response.send_message(embed=view.get_embed(), view=view)
            
        elif job_data['type'] == "hacker":
            factory = HackerGame(interaction.user)
            view    = factory.build()
            await interaction.response.send_message(embed=factory.get_embed(), view=view)
            view.message = await interaction.original_response()
            
        else: 
            net, tax = await db.process_work(interaction.user.id, job_data['payout']) # Added Await!
            embed = discord.Embed(title="💼 Shift Complete", description=f"You worked hard as a {current_job} {job_data['emoji']}.\n\n**Paycheck:** ${net:,.2f}\n*(Taxes paid to town: ${tax:,.2f})*", color=0x2ecc71)
            await interaction.response.send_message(embed=embed)
            await force_board_update(self.bot) 

    # --- JOB PERKS ---

    @app_commands.command(name="upgrade_town", description="[Builder] Spend Materials and Treasury money to level up the town!")
    async def upgrade(self, interaction: discord.Interaction):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        if not profile or profile['job'] != "Builder":
            return await interaction.response.send_message("❌ Only **Builders** know how to construct town upgrades!", ephemeral=True)
            
        t = await db.get_town_state() # Added Await!
        mat_cost = t['level'] * 50
        gold_cost = t['level'] * 2000.0
        
        if await db.try_upgrade_town(mat_cost, gold_cost): # Added Await!
            embed = discord.Embed(
                title="🏗️ Town Upgraded!", 
                description=f"**{interaction.user.display_name}** just led a massive construction project!\n\nThe town is now **Level {t['level']+1}**!\nEveryone on the server now gets a permanent +{int((t['level']+1)*5)}% bonus to all paychecks!",
                color=0xf1c40f
            )
            await interaction.response.send_message(embed=embed)
            await force_board_update(self.bot) 
        else:
            await interaction.response.send_message(f"❌ The town doesn't have enough resources to upgrade to Level {t['level']+1}. You need **{mat_cost} Materials** and **${gold_cost:,.2f} in the Treasury**.", ephemeral=True)

    @app_commands.command(name="set_tax", description="[Politician] Set the town's income tax rate (0% to 15%).")
    async def set_tax(self, interaction: discord.Interaction, percentage: int):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        if not profile or profile['job'] != "Politician":
            return await interaction.response.send_message("❌ Only **Politicians** can pass tax laws!", ephemeral=True)
            
        if percentage < 0 or percentage > 15:
            return await interaction.response.send_message("❌ Tax rate must be between 0 and 15.", ephemeral=True)
            
        rate = percentage / 100.0
        await db.set_tax_rate(rate) # Added Await!
        await interaction.response.send_message(f"🏛️ **New Legislation Passed:** {interaction.user.mention} has set the town tax rate to **{percentage}%**.")
        await force_board_update(self.bot) 

    @app_commands.command(name="embezzle", description="[Politician] Risk everything to steal from the town treasury.")
    @app_commands.checks.cooldown(1, 86400, key=lambda i: i.user.id) # 24 Hour Cooldown
    async def embezzle(self, interaction: discord.Interaction, amount: float):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        if not profile or profile['job'] != "Politician":
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ Only corrupt **Politicians** have access to the treasury!", ephemeral=True)
            
        if amount <= 0:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ Invalid amount.", ephemeral=True)
            
        t = await db.get_town_state() # Added Await!
        if t['treasury'] < amount:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ The town doesn't have that much money to steal.", ephemeral=True)

        if random.random() < 0.50:
            await db.embezzle_town_funds(interaction.user.id, amount) # Added Await!
            await interaction.response.send_message(f"🤫 **Success.** You quietly slipped **${amount:,.2f}** from the treasury into your personal account.")
            await force_board_update(self.bot) 
        else:
            await db.set_job(interaction.user.id, "Unemployed") # Added Await!
            user_bal = await db.get_balance(interaction.user.id) # Added Await!
            fine = user_bal * 0.50 
            await db.update_balance(interaction.user.id, -fine) # Added Await!
            await interaction.response.send_message(f"🚨 **BUSTED!** The auditors caught you attempting to embezzle town funds. You have been fired, stripped of your office, and fined **${fine:,.2f}**.")

    async def craft_autocomplete(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=r, value=r) for r in RECIPES.keys() if current.lower() in r.lower()]
    
    
    @app_commands.command(name="guillotine", description="Start a public vote to execute a citizen and seize 50% of their wealth.")
    @app_commands.checks.cooldown(1, 43200, key=lambda i: i.user.id) # 12 Hour cooldown
    async def guillotine(self, interaction: discord.Interaction, target: discord.Member):
        if target.bot:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ You cannot execute a machine.", ephemeral=True)
        if target.id == interaction.user.id:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ You cannot execute yourself.", ephemeral=True)
            
        embed = discord.Embed(
            title="⚖️ TRIBUNAL: VIVE LA RÉVOLUTION",
            description=f"{interaction.user.mention} has accused {target.mention} of corruption and dragged them to the Guillotine!\n\n**If 5 citizens vote GUILTY, 50% of their bank account will be seized by the town!**\n\nYou have 2 minutes to cast your vote.",
            color=0xe67e22
        )
        view = GuillotineVoteView(target, interaction.user, self.bot)
        await interaction.response.send_message(embed=embed, view=view)
        view.message = await interaction.original_response()

    @guillotine.error
    async def guillotine_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CommandOnCooldown):
            hours = error.retry_after / 3600
            await interaction.response.send_message(f"⏳ You must wait **{hours:.1f} hours** before starting another riot.", ephemeral=True)

    @app_commands.command(name="craft", description="[Blacksmith] Craft RPG items out of ores to sell on the market.")
    @app_commands.autocomplete(recipe_name=craft_autocomplete)
    async def craft(self, interaction: discord.Interaction, recipe_name: str):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        if not profile or profile['job'] != "Blacksmith":
            return await interaction.response.send_message("❌ Only **Blacksmiths** know how to forge weapons!", ephemeral=True)
            
        if recipe_name not in RECIPES:
            return await interaction.response.send_message("❌ Unknown recipe.", ephemeral=True)
            
        materials = RECIPES[recipe_name]
        success, msg = await consume_materials(interaction.user.id, materials) # Added Await!
        
        if success:
            gear = CRAFTED_GEAR.get(recipe_name, {})
            await db.add_item(
                interaction.user.id,
                recipe_name,
                "Gear",
                "RPG_Crafted",
                item_type="Gear",
                slot=gear.get("slot"),
                atk_bonus=gear.get("atk_bonus", 0),
                def_bonus=gear.get("def_bonus", 0),
                int_bonus=gear.get("int_bonus", 0),
            ) # Added Await!
            await interaction.response.send_message(f"🔨 **Success!** You forged the **{recipe_name}**! It is now in your inventory.")
        else:
            await interaction.response.send_message(f"❌ {msg}", ephemeral=True)

    # @app_commands.command(name="hack", description="[Hacker] Attempt to siphon funds from another player's bank account.")
    # @app_commands.checks.cooldown(1, 10800, key=lambda i: i.user.id)  # 3 hour cooldown 
    # async def hack(self, interaction: discord.Interaction, target: discord.Member):
    #     profile = await db.get_job_profile(interaction.user.id) # Added Await!
    #     job_name = ""
    #     if profile and "job" in profile.keys():
    #         job_name = profile["job"] or ""
    #     if not profile or str(job_name).strip().lower() != "hacker":
    #         interaction.command.reset_cooldown(interaction)
    #         return await interaction.response.send_message("❌ Only **Hackers** have the software to do this!", ephemeral=True)
            
    #     if target.id == interaction.user.id or target.bot:
    #         interaction.command.reset_cooldown(interaction)
    #         return await interaction.response.send_message("❌ Invalid target.", ephemeral=True)

    #     target_bal = await db.get_balance(target.id) # Added Await!
    #     if target_bal < 500:
    #         interaction.command.reset_cooldown(interaction)
    #         return await interaction.response.send_message("❌ Their firewall is garbage, but they are too poor to be worth hacking.", ephemeral=True)
            
    #     if random.random() < 0.60:
    #         stolen = round(target_bal * random.uniform(0.02, 0.05), 2)
    #         await db.update_balance(target.id, -stolen) # Added Await!
    #         await db.update_balance(interaction.user.id, stolen) # Added Await!
    #         await interaction.response.send_message(f"💻 **Hack Successful!** You bypassed {target.mention}'s security and siphoned **${stolen:,.2f}** into your account!")
    #     else:
    #         fine = 300.0
    #         await db.update_balance(interaction.user.id, -fine) # Added Await!
    #         await interaction.response.send_message(f"🚨 **Hack Traced!** {target.mention}'s security caught you. You were fined **${fine:,.2f}**.")

    # ==========================================
    #   /career firewall — buy a firewall
    # ==========================================
    @app_commands.command(name="firewall", description="Purchase a firewall to protect your account ($2,500 / 72 hours).")
    async def buy_firewall_cmd(self, interaction: discord.Interaction):
        COST  = 2500.0
        HOURS = 72
        success = await db.buy_firewall(interaction.user.id, COST, HOURS)
        if not success:
            return await interaction.response.send_message(
                f"❌ You need **${COST:,.2f}** to purchase a firewall.", ephemeral=True
            )
        expires_ts = int((datetime.datetime.now() + datetime.timedelta(hours=HOURS)).timestamp())
        embed = discord.Embed(
            title       = "🛡️ Firewall Activated",
            description = (
                f"Your firewall is online and active until <t:{expires_ts}:F>.\n\n"
                f"Use `/career bolster` to run a breach protocol and add up to **+30% hack resistance**."
            ),
            color = 0x3498db
        )
        await interaction.response.send_message(embed=embed)

    # ==========================================
    #   /career bolster — strengthen firewall
    # ==========================================
    @app_commands.command(name="bolster", description="Run a breach protocol to strengthen your active firewall.")
    async def bolster_firewall_cmd(self, interaction: discord.Interaction):
        fw = await db.get_firewall(interaction.user.id)
        if not fw:
            return await interaction.response.send_message(
                "❌ You don't have an active firewall. Buy one with `/career firewall`.", ephemeral=True
            )
        expires = datetime.datetime.strptime(fw["expires_at"], "%Y-%m-%d %H:%M:%S")
        if datetime.datetime.now() > expires:
            return await interaction.response.send_message(
                "❌ Your firewall has expired. Buy a new one with `/career firewall`.", ephemeral=True
            )
        if fw["bolster_count"] >= 3:
            return await interaction.response.send_message(
                "✅ Your firewall is already at max bolster (**+30% resistance**). No further upgrades possible.",
                ephemeral=True
            )

        async def on_result(inner_interaction, sequences_done):
            if sequences_done == 0:
                embed = discord.Embed(
                    title       = "🛡️ Bolster Failed",
                    description = "No sequences completed — firewall strength unchanged.",
                    color       = 0xe74c3c
                )
            else:
                new_count = await db.bolster_firewall(interaction.user.id, sequences_done)
                bonus_pct = (new_count or 0) * 10
                embed = discord.Embed(
                    title       = "🛡️ Firewall Bolstered",
                    description = (
                        f"**{sequences_done}** sequence(s) completed.\n\n"
                        f"Your firewall now has **+{bonus_pct}% hack resistance** "
                        f"({new_count}/3 bolsters)."
                    ),
                    color = 0x2ecc71
                )
            if inner_interaction:
                await inner_interaction.response.edit_message(embed=embed, view=None)
            else:
                try: await factory._view.message.edit(embed=embed, view=None)
                except Exception: pass

        factory = FirewallBolsterGame(interaction.user, on_result)
        view    = factory.build()
        await interaction.response.send_message(embed=factory.get_embed(), view=view)
        view.message = await interaction.original_response()

    # ==========================================
    #   /career reboot — reboot compromised firewall
    # ==========================================
    @app_commands.command(name="reboot", description="Reboot your compromised firewall and attempt to identify the hacker.")
    async def reboot_firewall_cmd(self, interaction: discord.Interaction):
        fw = await db.get_firewall(interaction.user.id)
        if not fw or not fw["compromised_at"]:
            return await interaction.response.send_message(
                "✅ Your firewall is not currently compromised.", ephemeral=True
            )
        compromised_at = datetime.datetime.strptime(fw["compromised_at"], "%Y-%m-%d %H:%M:%S")
        deadline       = compromised_at + datetime.timedelta(hours=2)
        if datetime.datetime.now() > deadline:
            return await interaction.response.send_message(
                "❌ The 2-hour reboot window has expired. Buy a new firewall with `/career firewall`.",
                ephemeral=True
            )

        async def on_result(inner_interaction, success: bool):
            if not success:
                embed = discord.Embed(
                    title       = "🔄 Reboot Failed",
                    description = "Sequence failed. Firewall still compromised — try again before the window expires.",
                    color       = 0xe74c3c
                )
                if inner_interaction:
                    await inner_interaction.response.edit_message(embed=embed, view=None)
                return

            hacker_id = await db.reboot_firewall(interaction.user.id)
            embed = discord.Embed(
                title       = "✅ Firewall Rebooted",
                description = (
                    "Firewall successfully rebooted!\n\n"
                    "The breach left a partial signature. You have **one guess** — "
                    "identify the hacker correctly and they pay **$500** to you."
                ),
                color = 0x2ecc71
            )
            if inner_interaction:
                await inner_interaction.response.edit_message(embed=embed, view=None)

            if not hacker_id:
                return

            # Build candidate list — all server Hackers + actual hacker regardless of job
            hacker_members = []
            for member in interaction.guild.members:
                if member.bot or member.id == interaction.user.id:
                    continue
                profile = await db.get_job_profile(member.id)
                if profile and profile.get("job") == "Hacker":
                    hacker_members.append(member)

            actual = interaction.guild.get_member(hacker_id)
            if actual and actual not in hacker_members:
                hacker_members.append(actual)

            if not hacker_members:
                return

            random.shuffle(hacker_members)

            guess_view = IdentifyGuessView(interaction.user, hacker_id, interaction.guild)
            if not guess_view.build_select(hacker_members):
                return

            guess_embed = discord.Embed(
                title       = "🔍 Identify the Hacker",
                description = (
                    f"The breach left a partial trace. You have **one guess**.\n"
                    f"Correct = hacker pays **$500** directly to you.\n\n"
                    f"There are no hints. Choose carefully."
                ),
                color = 0xf39c12
            )
            msg = await interaction.followup.send(embed=guess_embed, view=guess_view)
            guess_view.message = msg

        factory = FirewallRebootGame(interaction.user, on_result)
        view    = factory.build()
        await interaction.response.send_message(embed=factory.get_embed(), view=view)
        view.message = await interaction.original_response()

    # ==========================================
    #   /career hack — full flow
    # ==========================================
    @app_commands.command(name="hack", description="[Hacker] Attempt to breach another player's account.")
    @app_commands.checks.cooldown(1, 10800, key=lambda i: i.user.id)
    async def hack(self, interaction: discord.Interaction, target: discord.Member):
        # Job check
        profile  = await db.get_job_profile(interaction.user.id)
        job_name = (profile.get("job") or "") if profile else ""
        if str(job_name).strip().lower() != "hacker":
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message(
                "❌ Only **Hackers** have the software to do this!", ephemeral=True
            )

        if target.id == interaction.user.id or target.bot:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ Invalid target.", ephemeral=True)

        target_bal = await db.get_balance(target.id)
        if target_bal < 500:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message(
                "❌ They're too poor to be worth hacking.", ephemeral=True
            )

        # Firewall check
        fw           = await db.get_firewall(target.id)
        block_chance = 0.0
        fw_active    = False

        if fw:
            expires = datetime.datetime.strptime(fw["expires_at"], "%Y-%m-%d %H:%M:%S")
            if datetime.datetime.now() < expires and not fw["compromised_at"]:
                fw_active    = True
                block_chance = fw["bolster_count"] * 0.10

        # Random block roll — 15% base + firewall bonus
        base_block  = 0.15
        total_block = base_block + block_chance

        if random.random() < total_block:
            fw_note = (
                f" *(Firewall: +{int(block_chance*100)}% | Base: 15%)*"
                if fw_active
                else " *(Lucky block — 15% base chance)*"
            )
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title       = "🛡️ Hack Blocked",
                    description = f"The target's defenses rejected your intrusion.{fw_note}\nCooldown not consumed.",
                    color       = 0xe74c3c
                ),
                ephemeral=True
            )

        # Hacker runs breach puzzle to determine siphon %
        async def on_hack_result(inner_interaction, siphon_pct: float):
            current_bal = await db.get_balance(target.id)
            if current_bal < 500:
                embed = discord.Embed(
                    title       = "💻 Hack Fizzled",
                    description = f"{target.mention} spent their money before you could steal it.",
                    color       = 0x95a5a6
                )
                if inner_interaction:
                    await inner_interaction.response.edit_message(embed=embed, view=None)
                return

            stolen = round(current_bal * siphon_pct, 2)
            await db.update_balance(target.id, -stolen)
            await db.update_balance(interaction.user.id, stolen)
            await db.compromise_firewall(target.id, interaction.user.id)

            result_embed = discord.Embed(
                title       = "💻 HACK COMPLETE",
                description = (
                    f"**Siphon rate:** {int(siphon_pct * 100)}%\n"
                    f"**Stolen:** ${stolen:,.2f} from {target.mention}\n\n"
                    f"Their firewall is now **COMPROMISED**. "
                    f"They have **2 hours** to reboot it."
                ),
                color = 0x00ff9f
            )
            if inner_interaction:
                await inner_interaction.response.edit_message(embed=result_embed, view=None)

            # DM the victim
            try:
                await target.send(embed=discord.Embed(
                    title       = "🚨 YOUR ACCOUNT WAS HACKED",
                    description = (
                        f"Someone breached your account and stole **${stolen:,.2f}**!\n\n"
                        f"Your firewall is **COMPROMISED**.\n"
                        f"Use `/career reboot` within **2 hours** to reboot it "
                        f"and attempt to identify the hacker.\n"
                        f"After 2 hours the window expires."
                    ),
                    color = 0xe74c3c
                ))
            except discord.Forbidden:
                pass

            await force_board_update(interaction.client)

        factory = HackExecutionGame(interaction.user, on_hack_result)
        view    = factory.build()
        await interaction.response.send_message(embed=factory.get_embed(), view=view)
        view.message = await interaction.original_response()

    @hack.error
    async def hack_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        # Cooldown handling
        if isinstance(error, app_commands.CommandOnCooldown):
            hours = error.retry_after / 3600
            try:
                await interaction.response.send_message(
                    f"⏳ Laying low. Try again in **{hours:.1f} hours**.",
                    ephemeral=True,
                )
            except Exception:
                pass
            return

        # Generic fallback so the interaction never silently times out
        print(f"[hack] Unexpected error: {repr(error)}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ Something went wrong running this hack. Please try again later.",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(
                    "❌ Something went wrong running this hack. Please try again later.",
                    ephemeral=True,
                )
        except Exception:
            pass

    @embezzle.error
    async def embezzle_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CommandOnCooldown):
            hours = error.retry_after / 3600
            try:
                await interaction.response.send_message(
                    f"⏳ You are laying low. You must wait **{hours:.1f} hours** before doing this again.",
                    ephemeral=True,
                )
            except Exception:
                pass

async def setup(bot):
    await bot.add_cog(Jobs(bot))
