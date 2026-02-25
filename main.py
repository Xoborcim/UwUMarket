import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord import ui
import os
from dotenv import load_dotenv
import database as db
import math
import datetime

# 1. Setup
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
MARKET_CHANNEL_ID = int(os.getenv('MARKET_CHANNEL_ID')) 
CLOSED_CHANNEL_ID = int(os.getenv('CLOSED_CHANNEL_ID')) 
LEADERBOARD_CHANNEL_ID = int(os.getenv('LEADERBOARD_CHANNEL_ID'))
BOTSPAM_CHANNEL_ID = int(os.getenv('BOTSPAM_CHANNEL_ID'))

class MarketBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True 
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        import shutil
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), db.DB_NAME)
        if os.path.exists(db_path):
            backup_dir = os.path.join(os.path.dirname(db_path), "backups")
            os.makedirs(backup_dir, exist_ok=True)
            stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            shutil.copy2(db_path, os.path.join(backup_dir, f"market_{stamp}.db"))
            print(f"--- Database checkpoint saved: market_{stamp}.db ---")

        db.initialize_db()
        
        # Load Casino Cog (Ensure folder cogs/casino.py exists!)
        # Load items
        try:
            await self.load_extension("cogs.casino")
            await self.load_extension("cogs.items")
            await self.load_extension("cogs.jobs")
            await self.load_extension("cogs.rpg")
            await self.load_extension("cogs.utilities")
            await self.load_extension("cogs.tournaments")
        except Exception as e:
            print(f"⚠️ Could not load cog: {e}")

        # Re-register Views
        active_markets = db.get_active_markets()
        print(f"--- Restoring {len(active_markets)} Active Markets ---")
        for m in active_markets:
            view = MarketView(m['market_id'], m['options'], m['question'], m['close_time'])
            self.add_view(view)

        await self.tree.sync()
        self.update_leaderboard_task.start()
        self.check_expired_markets_task.start()
        print("--- System Online ---")

    @tasks.loop(minutes=1)
    async def update_leaderboard_task(self):
        channel = self.get_channel(LEADERBOARD_CHANNEL_ID)
        if not channel: return
        leaders = db.get_leaderboard(10)
        embed = discord.Embed(title="🏆 Live Wealth Leaderboard", color=0xf1c40f)
        description = "No data yet." if not leaders else ""
        for i, row in enumerate(leaders):
            rank = i + 1
            icon = "👑" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"**#{rank}**"
            description += f"{icon} <@{row['user_id']}>: **${row['balance']:,.2f}**\n"
        embed.description = description
        embed.set_footer(text=f"Updates every minute • {datetime.datetime.now().strftime('%H:%M:%S')}")
        try:
            history = [msg async for msg in channel.history(limit=5) if msg.author == self.user]
            if history: await history[0].edit(embed=embed)
            else: await channel.send(embed=embed)
        except Exception: pass

    @tasks.loop(minutes=1)
    async def check_expired_markets_task(self):
        expired = db.get_expired_markets()
        if not expired: return
        botspam = self.get_channel(BOTSPAM_CHANNEL_ID)
        jury_role = None
        if botspam: jury_role = discord.utils.get(botspam.guild.roles, name="Jury")
        for market in expired:
            mid = market['market_id']
            db.close_market_betting(mid)
            if botspam:
                ping = jury_role.mention if jury_role else "@Jury"
                await botspam.send(f"⏰ **Market {mid} Expired!**\n{ping}, please cast your votes with `/vote {mid} [OUTCOME]`\n> {market['question']}")

    @update_leaderboard_task.before_loop
    async def before_tasks(self): await self.wait_until_ready()
    @check_expired_markets_task.before_loop
    async def before_tasks_2(self): await self.wait_until_ready()

bot = MarketBot()

# --- HELPER: RESOLUTION LOGIC ---
async def perform_resolution(interaction: discord.Interaction, market_id: int, winner: str):
    success, logs, question = db.resolve_market(market_id, winner)
    if not success:
        if interaction.response.is_done(): await interaction.followup.send(f"❌ Error: {logs[0]}")
        else: await interaction.response.send_message(f"❌ Error: {logs[0]}", ephemeral=True)
        return
    msg_id = db.get_market_message_id(market_id)
    market_channel = bot.get_channel(MARKET_CHANNEL_ID)
    if market_channel and msg_id:
        try:
            old_msg = await market_channel.fetch_message(msg_id)
            await old_msg.delete()
        except: pass 
    closed_channel = bot.get_channel(CLOSED_CHANNEL_ID)
    volume = db.get_market_volume(market_id)
    if closed_channel:
        embed = discord.Embed(title=f"🔒 Market Resolved: {winner.upper()}", color=0xf1c40f)
        embed.description = f"**Question:** {question}\n**Winner:** {winner.upper()}"
        embed.add_field(name="💰 Total Volume", value=f"${volume:,.2f}", inline=False)
        await closed_channel.send(embed=embed)
    botspam = bot.get_channel(BOTSPAM_CHANNEL_ID)
    msg = "\n".join(logs) if logs else "No winners."
    if len(msg) > 1900: msg = msg[:1900] + "... (truncated)"
    if botspam: await botspam.send(f"💸 **Payouts for Market {market_id}:**\n{msg}")
    if not interaction.response.is_done(): await interaction.response.send_message(f"✅ Resolved Market {market_id}.", ephemeral=True)
    else: await interaction.followup.send(f"✅ Resolved Market {market_id}.", ephemeral=True)

# --- UI COMPONENTS ---
def create_market_embed(market_id, question, close_time):
    odds = db.get_odds(market_id)
    volume = db.get_market_volume(market_id)
    
    embed = discord.Embed(title=f"🆔 {market_id}: {question}", color=0x3498db)
    embed.add_field(name="💰 Volume", value=f"${volume:,.2f}", inline=True)
    embed.add_field(name="⏳ Closes", value=f"<t:{int(close_time.timestamp())}:R>", inline=True)
    
    odds_text = ""
    for label, prob in odds:
        percent = int(prob * 100)
        bar = "▓" * int(prob * 10) + "░" * (10 - int(prob * 10))
        odds_text += f"**{label}**: {percent}%\n`{bar}` ¢{int(prob*100)}\n\n"
        
    embed.add_field(name="📊 Current Odds", value=odds_text, inline=False)
    embed.set_footer(text="Prices update live! Click a button to bet.")
    return embed

class BetModal(ui.Modal, title="Place Your Bet"):
    amount = ui.TextInput(label="Amount ($)", placeholder="e.g. 100")
    def __init__(self, market_id, outcome_label, market_question, close_time):
        super().__init__()
        self.market_id = market_id; self.outcome_label = outcome_label; self.market_question = market_question; self.close_time = close_time
    async def on_submit(self, interaction: discord.Interaction):
        try: amt = float(self.amount.value)
        except: return await interaction.response.send_message("❌ Invalid number.", ephemeral=True)
        success, msg = db.buy_shares(interaction.user.id, self.market_id, self.outcome_label, amt)
        if success:
            await interaction.response.send_message(f"✅ **Bet Placed!** {msg}", ephemeral=True)
            try: await interaction.message.edit(embed=create_market_embed(self.market_id, self.market_question, self.close_time))
            except: pass
        else: await interaction.response.send_message(f"❌ {msg}", ephemeral=True)

class BetButton(ui.Button):
    def __init__(self, label, market_id, question, close_time):
        style = discord.ButtonStyle.success if label.upper() == "YES" else discord.ButtonStyle.danger if label.upper() == "NO" else discord.ButtonStyle.secondary
        cid = f"bet:{market_id}:{label}"
        super().__init__(label=f"Bet {label}", style=style, custom_id=cid)
        self.market_id = market_id; self.question = question; self.close_time = close_time; self.outcome_label = label
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(BetModal(self.market_id, self.outcome_label, self.question, self.close_time))

class MarketView(ui.View):
    def __init__(self, market_id, options, question, close_time):
        super().__init__(timeout=None)
        for label in options: self.add_item(BetButton(label, market_id, question, close_time))

# --- COMMANDS ---

@bot.tree.command(name="create")
@app_commands.describe(question="The question", end_date="YYYY-MM-DD", end_time="HH:MM (24h)", duration_hours="Or just hours (e.g. 24)")
async def create(interaction: discord.Interaction, question: str, end_date: str = None, end_time: str = None, duration_hours: int = None, option1: str = "YES", option2: str = "NO"):
    
    # Check for overflow first
    if duration_hours and duration_hours > 8760:
        return await interaction.response.send_message("❌ Duration too long. Max is 1 year.", ephemeral=True)

    close_dt = None
    if end_date:
        try:
            time_str = end_time if end_time else "23:59"
            full_str = f"{end_date} {time_str}"
            close_dt = datetime.datetime.strptime(full_str, "%Y-%m-%d %H:%M")
        except ValueError:
            return await interaction.response.send_message("❌ Invalid Date Format. Use YYYY-MM-DD and HH:MM.", ephemeral=True)
    elif duration_hours:
        close_dt = datetime.datetime.now() + datetime.timedelta(hours=duration_hours)
    else:
        return await interaction.response.send_message("❌ You must specify either `end_date` OR `duration_hours`.", ephemeral=True)

    if close_dt < datetime.datetime.now():
         return await interaction.response.send_message("❌ Closing time cannot be in the past.", ephemeral=True)

    # CALLING THE NEW FUNCTION CORRECTLY
    market_id = db.create_market_custom_date(question, interaction.user.id, close_dt, [option1, option2])
    
    embed = create_market_embed(market_id, question, close_dt)
    view = MarketView(market_id, [option1, option2], question, close_dt)
    
    channel = bot.get_channel(MARKET_CHANNEL_ID)
    await interaction.response.send_message(f"✅ Market created in {channel.mention}!", ephemeral=True)
    if channel:
        msg = await channel.send(embed=embed, view=view)
        db.set_market_message_id(market_id, msg.id)

@bot.tree.command(name="resolve")
async def resolve(interaction: discord.Interaction, market_id: int, winner: str):
    has_perm = interaction.user.guild_permissions.administrator or discord.utils.get(interaction.user.roles, name="Grand Juror")
    if not has_perm: return await interaction.response.send_message("⛔ Access Denied.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    await perform_resolution(interaction, market_id, winner)

@bot.tree.command(name="vote")
async def vote(interaction: discord.Interaction, market_id: int, outcome: str):
    jury_role = discord.utils.get(interaction.user.roles, name="Jury")
    if not jury_role: return await interaction.response.send_message("⛔ Need 'Jury' role.", ephemeral=True)
    success, msg = db.cast_jury_vote(interaction.user.id, market_id, outcome)
    if not success: return await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
    total_jurors = len(jury_role.members)
    majority = math.floor(total_jurors / 2) + 1
    tally = db.get_market_vote_tally(market_id)
    votes = tally.get(outcome, 0)
    await interaction.response.send_message(f"🗳️ Voted! {outcome}: {votes}/{total_jurors}", ephemeral=True)
    if votes >= majority:
        await interaction.channel.send(f"🚨 **Majority Reached!** Resolving...")
        await perform_resolution(interaction, market_id, outcome)

@bot.tree.command(name="portfolio")
async def portfolio(interaction: discord.Interaction):
    bets = db.get_user_portfolio(interaction.user.id)
    if not bets: return await interaction.response.send_message("📉 You have no active bets.", ephemeral=True)
    embed = discord.Embed(title="📂 Your Portfolio", color=0x9b59b6)
    for row in bets:
        status_icon = "🟢" if row['status'] == 'OPEN' else "🔒"
        embed.add_field(name=f"{status_icon} Market {row['market_id']}: {row['label']}", value=f"**{row['shares_held']:.2f} Shares**\n*{row['question']}*", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="balance")
async def balance(interaction: discord.Interaction):
    bal = db.get_balance(interaction.user.id)
    await interaction.response.send_message(f"💳 Balance: **${bal:,.2f}**")

@bot.tree.command(name="daily")
async def daily(interaction: discord.Interaction):
    success, msg = db.process_daily(interaction.user.id)
    await interaction.response.send_message(f"{'✅' if success else '⏳'} {msg}", ephemeral=not success)

@bot.tree.command(name="close")
async def close(interaction: discord.Interaction, market_id: int):
    db.close_market_betting(market_id)
    await interaction.response.send_message(f"🔒 Market {market_id} closed.", ephemeral=True)

@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    leaders = db.get_leaderboard(10)
    embed = discord.Embed(title="🏆 Wealth Leaderboard", color=0xf1c40f)
    desc = ""
    for i, r in enumerate(leaders):
        desc += f"#{i+1} <@{r['user_id']}>: **${r['balance']:,.2f}**\n"
    embed.description = desc
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="transfer", description="Send money to another user")
async def transfer(interaction: discord.Interaction, recipient: discord.Member, amount: float):
    if amount < 0.01:
        return await interaction.response.send_message("❌ Minimum transfer is $0.01", ephemeral=True)
    if interaction.user.id == recipient.id:
        return await interaction.response.send_message("❌ You cannot send money to yourself.", ephemeral=True)
    
    amount = round(amount, 2)
    
    # Deduct from sender
    if not db.update_balance(interaction.user.id, -amount):
        return await interaction.response.send_message("❌ Insufficient funds.", ephemeral=True)
    
    # Add to recipient
    db.update_balance(recipient.id, amount)
    
    await interaction.response.send_message(f"💸 **Transfer Complete!** You sent **${amount:,.2f}** to {recipient.mention}.")

@bot.command()
async def sync(ctx):
    print("Syncing commands...")
    try:
        synced = await bot.tree.sync()
        await ctx.send(f"✅ Synced {len(synced)} commands globally!")
    except Exception as e:
        await ctx.send(f"❌ Sync failed: {e}")

if __name__ == "__main__":
    bot.run(TOKEN)
