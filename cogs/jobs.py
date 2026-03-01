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

class HackerGame(discord.ui.View):
    def __init__(self, user):
        super().__init__(timeout=15) 
        self.user = user
        self.words = ["firewall", "encryption", "database", "algorithm", "network", "password", "security", "mainframe"]
        self.target = random.choice(self.words)
        
        chars = list(self.target)
        random.shuffle(chars)
        self.scrambled = ''.join(chars)
        
        options = random.sample([w for w in self.words if w != self.target], 3)
        options.append(self.target)
        random.shuffle(options)
        
        for i, word in enumerate(options):
            btn = discord.ui.Button(label=word, style=discord.ButtonStyle.secondary, custom_id=f"hack_{i}")
            btn.callback = self.make_callback(word)
            self.add_item(btn)
            
    def get_embed(self):
        return discord.Embed(
            title="💻 Mainframe Hack", 
            description=f"Bypass the firewall! Unscramble the security key:\n\n# `{self.scrambled}`\n\n*You have 15 seconds!*", 
            color=0x2ecc71
        )
        
    def make_callback(self, word):
        async def cb(interaction: discord.Interaction):
            if interaction.user.id != self.user.id: return await interaction.response.defer()
            self.clear_items()
            
            if word == self.target:
                payout = random.randint(360, 720)
                net, tax = await db.process_work(self.user.id, payout) # Added Await!
                embed = discord.Embed(title="💻 Access Granted", description=f"Firewall bypassed! You stole **${net:,.2f}**.\n*(Laundered Tax: ${tax:,.2f})*", color=0x2ecc71)
            else:
                await db.process_work(self.user.id, 0) # Added Await!
                embed = discord.Embed(title="🚨 Access Denied", description="Incorrect key. You were locked out and traced!", color=0xe74c3c)
                
            await interaction.response.edit_message(embed=embed, view=self)
            await force_board_update(interaction.client) 
            self.stop()
        return cb
        
    async def on_timeout(self):
        self.clear_items()
        await db.process_work(self.user.id, 0) # Added Await!
        embed = discord.Embed(title="🚨 Timeout", description="You took too long. Security locked you out.", color=0xe74c3c)
        try: await self.message.edit(embed=embed, view=self)
        except: pass

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
            view = HackerGame(interaction.user)
            await interaction.response.send_message(embed=view.get_embed(), view=view)
            
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

    @app_commands.command(name="hack", description="[Hacker] Attempt to siphon funds from another player's bank account.")
    @app_commands.checks.cooldown(1, 43200, key=lambda i: i.user.id) 
    async def hack(self, interaction: discord.Interaction, target: discord.Member):
        profile = await db.get_job_profile(interaction.user.id) # Added Await!
        job_name = ""
        if profile and "job" in profile.keys():
            job_name = profile["job"] or ""
        if not profile or str(job_name).strip().lower() != "hacker":
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ Only **Hackers** have the software to do this!", ephemeral=True)
            
        if target.id == interaction.user.id or target.bot:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ Invalid target.", ephemeral=True)

        target_bal = await db.get_balance(target.id) # Added Await!
        if target_bal < 500:
            interaction.command.reset_cooldown(interaction)
            return await interaction.response.send_message("❌ Their firewall is garbage, but they are too poor to be worth hacking.", ephemeral=True)
            
        if random.random() < 0.60:
            stolen = round(target_bal * random.uniform(0.02, 0.05), 2)
            await db.update_balance(target.id, -stolen) # Added Await!
            await db.update_balance(interaction.user.id, stolen) # Added Await!
            await interaction.response.send_message(f"💻 **Hack Successful!** You bypassed {target.mention}'s security and siphoned **${stolen:,.2f}** into your account!")
        else:
            fine = 300.0
            await db.update_balance(interaction.user.id, -fine) # Added Await!
            await interaction.response.send_message(f"🚨 **Hack Traced!** {target.mention}'s security caught you. You were fined **${fine:,.2f}**.")

    @embezzle.error
    @hack.error
    async def on_perk_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CommandOnCooldown):
            hours = error.retry_after / 3600
            await interaction.response.send_message(f"⏳ You are laying low. You must wait **{hours:.1f} hours** before doing this again.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Jobs(bot))
