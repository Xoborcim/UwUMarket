import discord
from discord import app_commands
from discord.ext import commands
import database as db
import random
import asyncio

# --- HELPER: CARD DECK ---
SUITS = ['♠', '♥', '♦', '♣']
RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
VALUES = {'2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9, '10':10, 'J':10, 'Q':10, 'K':10, 'A':11}

def get_deck():
    return [{'rank': r, 'suit': s} for s in SUITS for r in RANKS]

def card_str(card):
    return f"`{card['rank']}{card['suit']}`"

def calculate_bj_hand(hand):
    val = sum(VALUES[c['rank']] for c in hand)
    aces = sum(1 for c in hand if c['rank'] == 'A')
    while val > 21 and aces > 0:
        val -= 10
        aces -= 1
    return val

def evaluate_poker_hand(hand):
    ranks = [c['rank'] for c in hand]
    suits = [c['suit'] for c in hand]
    
    # Map ranks to numerical values for straight checking
    rank_vals = []
    for r in ranks:
        if r == 'A': rank_vals.append(14)
        elif r == 'K': rank_vals.append(13)
        elif r == 'Q': rank_vals.append(12)
        elif r == 'J': rank_vals.append(11)
        else: rank_vals.append(int(r))
    rank_vals.sort()

    is_flush = len(set(suits)) == 1
    is_straight = len(set(rank_vals)) == 5 and (rank_vals[-1] - rank_vals[0] == 4)
    # Special A-2-3-4-5 straight check
    if set(rank_vals) == {14, 2, 3, 4, 5}: is_straight = True

    counts = {r: ranks.count(r) for r in set(ranks)}
    freqs = list(counts.values())

    if is_flush and is_straight:
        if 14 in rank_vals and 13 in rank_vals: return "Royal Flush", 250
        return "Straight Flush", 50
    if 4 in freqs: return "Four of a Kind", 25
    if 3 in freqs and 2 in freqs: return "Full House", 9
    if is_flush: return "Flush", 6
    if is_straight: return "Straight", 4
    if 3 in freqs: return "Three of a Kind", 3
    if freqs.count(2) == 2: return "Two Pair", 2
    
    # Jacks or Better
    if 2 in freqs:
        pair_rank = [r for r, c in counts.items() if c == 2][0]
        if pair_rank in ['J', 'Q', 'K', 'A']:
            return "Jacks or Better", 1

    return "High Card", 0

# --- TOURNAMENT CASINO GAMES ---

class TournamentBlackjack(discord.ui.View):
    def __init__(self, cog, guild_id, user, wager):
        super().__init__(timeout=60)
        self.cog = cog
        self.guild_id = guild_id
        self.user = user
        self.wager = wager
        
        self.deck = get_deck()
        random.shuffle(self.deck)
        self.player_hand = [self.deck.pop(), self.deck.pop()]
        self.dealer_hand = [self.deck.pop(), self.deck.pop()]

    def get_embed(self, status="playing"):
        p_val = calculate_bj_hand(self.player_hand)
        p_str = " ".join([card_str(c) for c in self.player_hand])
        
        embed = discord.Embed(title="🃏 Tournament Blackjack", color=0x3498db)
        embed.add_field(name=f"Your Hand ({p_val})", value=p_str, inline=False)
        
        if status == "playing":
            d_str = f"{card_str(self.dealer_hand[0])} `?`"
            embed.add_field(name="Dealer's Hand", value=d_str, inline=False)
        else:
            d_val = calculate_bj_hand(self.dealer_hand)
            d_str = " ".join([card_str(c) for c in self.dealer_hand])
            embed.add_field(name=f"Dealer's Hand ({d_val})", value=d_str, inline=False)
            
            if status == "win":
                embed.color = 0x2ecc71
                embed.description = f"**You Win!** +{self.wager} Chips"
            elif status == "lose" or status == "bust":
                embed.color = 0xe74c3c
                embed.description = f"**You Lost.** -{self.wager} Chips"
            elif status == "push":
                embed.color = 0xf1c40f
                embed.description = "**Push.** Wager returned."
            elif status == "bj":
                embed.color = 0x9b59b6
                payout = int(self.wager * 1.5)
                embed.description = f"**BLACKJACK!** +{payout} Chips"
                
        return embed

    async def end_game(self, interaction, status):
        self.clear_items()
        t_data = self.cog.tournaments.get(self.guild_id)
        
        if t_data:
            if status == "win":
                t_data['players'][self.user.id] += self.wager
            elif status == "bj":
                t_data['players'][self.user.id] += int(self.wager * 1.5)
            elif status in ["lose", "bust"]:
                t_data['players'][self.user.id] -= self.wager
                
        await interaction.response.edit_message(embed=self.get_embed(status), view=self)

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.success)
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        self.player_hand.append(self.deck.pop())
        if calculate_bj_hand(self.player_hand) > 21:
            await self.end_game(interaction, "bust")
        else:
            await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.danger)
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        d_val = calculate_bj_hand(self.dealer_hand)
        while d_val < 17:
            self.dealer_hand.append(self.deck.pop())
            d_val = calculate_bj_hand(self.dealer_hand)
            
        p_val = calculate_bj_hand(self.player_hand)
        
        if d_val > 21 or p_val > d_val: await self.end_game(interaction, "win")
        elif d_val > p_val: await self.end_game(interaction, "lose")
        else: await self.end_game(interaction, "push")

class TournamentVideoPoker(discord.ui.View):
    def __init__(self, cog, guild_id, user, wager):
        super().__init__(timeout=60)
        self.cog = cog
        self.guild_id = guild_id
        self.user = user
        self.wager = wager
        
        self.deck = get_deck()
        random.shuffle(self.deck)
        self.hand = [self.deck.pop() for _ in range(5)]
        self.held = [False, False, False, False, False]
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()
        for i in range(5):
            btn = discord.ui.Button(
                label=f"Hold {i+1}" if not self.held[i] else f"HELD {i+1}", 
                style=discord.ButtonStyle.success if self.held[i] else discord.ButtonStyle.secondary,
                custom_id=f"hold_{i}"
            )
            btn.callback = self.make_callback(i)
            self.add_item(btn)
            
        draw_btn = discord.ui.Button(label="DRAW", style=discord.ButtonStyle.primary, row=1)
        draw_btn.callback = self.draw_action
        self.add_item(draw_btn)

    def make_callback(self, index):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.user.id: return await interaction.response.defer()
            self.held[index] = not self.held[index]
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        return callback

    def get_embed(self, final=False):
        hand_str = "   ".join([card_str(c) for c in self.hand])
        embed = discord.Embed(title="🎰 Tournament Video Poker", description=f"**Wager:** {self.wager} Chips\n\n### {hand_str}", color=0x9b59b6)
        
        if final:
            hand_name, mult = evaluate_poker_hand(self.hand)
            payout = self.wager * mult
            
            if mult > 0:
                embed.color = 0x2ecc71
                embed.add_field(name="Result", value=f"**{hand_name}!**\nWon **{payout} Chips**!")
            else:
                embed.color = 0xe74c3c
                embed.add_field(name="Result", value="**Nothing.**\nLost Wager.")
                
        return embed

    async def draw_action(self, interaction: discord.Interaction):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        for i in range(5):
            if not self.held[i]:
                self.hand[i] = self.deck.pop()
                
        self.clear_items()
        
        hand_name, mult = evaluate_poker_hand(self.hand)
        t_data = self.cog.tournaments.get(self.guild_id)
        
        if t_data:
            if mult > 0:
                # Give back original wager + winnings
                t_data['players'][self.user.id] += (self.wager * mult)
            else:
                t_data['players'][self.user.id] -= self.wager

        await interaction.response.edit_message(embed=self.get_embed(final=True), view=self)

# --- PVP DUEL VIEW ---
class DuelView(discord.ui.View):
    def __init__(self, cog, guild_id, challenger, target, wager):
        super().__init__(timeout=60)
        self.cog = cog
        self.guild_id = guild_id
        self.challenger = challenger
        self.target = target
        self.wager = wager

    @discord.ui.button(label="Accept Duel", style=discord.ButtonStyle.success, emoji="⚔️")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target.id:
            return await interaction.response.send_message("❌ Only the challenged player can accept!", ephemeral=True)

        t_data = self.cog.tournaments.get(self.guild_id)
        if not t_data or t_data['state'] != "ACTIVE":
            return await interaction.response.edit_message(content="❌ Tournament is no longer active.", view=None, embed=None)

        p1_chips = t_data['players'].get(self.challenger.id, 0)
        p2_chips = t_data['players'].get(self.target.id, 0)
        
        if p1_chips < self.wager or p2_chips < self.wager:
            return await interaction.response.edit_message(content="❌ Someone no longer has enough chips for this wager!", view=None, embed=None)

        roll1 = random.randint(1, 100)
        roll2 = random.randint(1, 100)
        
        while roll1 == roll2:
            roll2 = random.randint(1, 100)

        if roll1 > roll2:
            winner, loser = self.challenger, self.target
        else:
            winner, loser = self.target, self.challenger

        t_data['players'][winner.id] += self.wager
        t_data['players'][loser.id] -= self.wager

        embed = discord.Embed(
            title="🎲 Tournament Duel Results",
            description=f"**{self.challenger.display_name}** vs **{self.target.display_name}** for **{self.wager} Chips**!\n\n"
                        f"**{self.challenger.display_name}** rolled: `{roll1}`\n"
                        f"**{self.target.display_name}** rolled: `{roll2}`\n\n"
                        f"🏆 **{winner.mention} wins {self.wager} Chips!**",
            color=0x2ecc71
        )
        await interaction.response.edit_message(embed=embed, view=None)
        
        if t_data['players'][loser.id] <= 0:
            await interaction.channel.send(f"💀 **ELIMINATED!** {loser.mention} has lost all their chips and is out of the tournament!")

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target.id:
            return await interaction.response.send_message("❌ Only the challenged player can decline.", ephemeral=True)
        await interaction.response.edit_message(content=f"❌ {self.target.mention} declined the duel.", view=None, embed=None)

# --- TOURNAMENT LOBBY VIEW ---
class TournamentLobby(discord.ui.View):
    def __init__(self, cog, guild_id):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="Join Tournament", style=discord.ButtonStyle.primary, emoji="✋", custom_id="join_tourney")
    async def join_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        t_data = self.cog.tournaments.get(self.guild_id)
        if not t_data or t_data['state'] != "LOBBY":
            return await interaction.response.send_message("❌ This tournament is no longer taking players.", ephemeral=True)
            
        if interaction.user.id in t_data['players']:
            return await interaction.response.send_message("❌ You are already in the tournament!", ephemeral=True)
            
        t_data['players'][interaction.user.id] = t_data['starting_chips']
        
        embed = interaction.message.embeds[0]
        embed.description += f"\n✅ {interaction.user.mention} joined! ({t_data['starting_chips']} Chips)"
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Start Tournament", style=discord.ButtonStyle.success, emoji="▶️", custom_id="start_tourney")
    async def start_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != interaction.guild.owner_id:
            return await interaction.response.send_message("❌ Only the Server Owner can start the tournament.", ephemeral=True)
            
        t_data = self.cog.tournaments.get(self.guild_id)
        if len(t_data['players']) < 2:
            return await interaction.response.send_message("❌ You need at least 2 players to start!", ephemeral=True)
            
        t_data['state'] = "ACTIVE"
        self.clear_items()
        
        # Format the description based on the type
        instructions = ""
        if t_data['type'] == "casino":
            instructions = "Players must use the **`/t_casino`** commands to run up their stack!"
        elif t_data['type'] == "duel":
            instructions = "Players must use **`/tournament duel`** to steal chips from each other!"
        elif t_data['type'] == "external":
            instructions = "This is an external game (like PokerNow). The Host will use `/tournament admin_set_chips` to update scores!"
        else:
            instructions = "All Bot Games (`/t_casino` and `/tournament duel`) are enabled!"

        embed = interaction.message.embeds[0]
        embed.title = f"🏆 {t_data['name']} has STARTED!"
        embed.description = f"**Players:** {len(t_data['players'])}\n**Prize Pool:** ${t_data['prize']:,.2f}\n\n**Rules:** {instructions}"
        embed.color = 0xe74c3c
        
        await interaction.response.edit_message(embed=embed, view=self)
        await interaction.channel.send("🎺 **THE TOURNAMENT HAS BEGUN!** Good luck, everyone!")


# --- TOURNAMENT COG ---
class Tournament(commands.GroupCog, group_name="tournament", group_description="Server-wide special events and games."):
    def __init__(self, bot):
        self.bot = bot
        self.tournaments = {}

    @app_commands.command(name="open", description="Server Owner: Open a new tournament lobby.")
    @app_commands.choices(tournament_type=[
        app_commands.Choice(name="🎰 Casino Royale (Bot Games Only)", value="casino"),
        app_commands.Choice(name="⚔️ PvP Duels (Dice Rolls Only)", value="duel"),
        app_commands.Choice(name="🃏 External Game (PokerNow, Custom)", value="external"),
        app_commands.Choice(name="🔥 Mixed (All Bot Games)", value="mixed")
    ])
    async def open_tourney(self, interaction: discord.Interaction, tournament_name: str, tournament_type: app_commands.Choice[str], starting_chips: int, prize_money: float):
        if interaction.user.id != interaction.guild.owner_id:
            return await interaction.response.send_message("❌ Only the Server Owner can open a tournament.", ephemeral=True)
            
        if interaction.guild_id in self.tournaments:
            return await interaction.response.send_message("❌ A tournament is already running! Use `/tournament close` to end it first.", ephemeral=True)
            
        if starting_chips <= 0 or prize_money <= 0:
            return await interaction.response.send_message("❌ Chips and Prize must be greater than 0.", ephemeral=True)

        self.tournaments[interaction.guild_id] = {
            "host_id": interaction.user.id,
            "name": tournament_name,
            "type": tournament_type.value,
            "prize": prize_money,
            "starting_chips": starting_chips,
            "state": "LOBBY",
            "players": {}
        }

        embed = discord.Embed(
            title=f"🏆 NEW TOURNAMENT: {tournament_name}",
            description=f"The Server Owner is hosting a tournament!\n\n**Type:** {tournament_type.name}\n**Starting Chips:** {starting_chips}\n**Grand Prize:** ${prize_money:,.2f}\n\nClick below to join the lobby!",
            color=0xf1c40f
        )
        await interaction.response.send_message(embed=embed, view=TournamentLobby(self, interaction.guild_id))

    @app_commands.command(name="status", description="View the current tournament chip leaderboard.")
    async def status(self, interaction: discord.Interaction):
        t_data = self.tournaments.get(interaction.guild_id)
        if not t_data:
            return await interaction.response.send_message("❌ No tournament is currently running.", ephemeral=True)

        if not t_data['players']:
            return await interaction.response.send_message("📊 The lobby is empty right now.", ephemeral=True)

        sorted_players = sorted(t_data['players'].items(), key=lambda x: x[1], reverse=True)
        
        embed = discord.Embed(title=f"📊 {t_data['name']} Leaderboard", color=0x3498db)
        desc = ""
        for i, (uid, chips) in enumerate(sorted_players):
            medal = "🥇" if i == 0 else "💀" if chips <= 0 else "🔹"
            desc += f"{medal} <@{uid}>: **{chips} Chips**\n"
            
        embed.description = desc
        embed.set_footer(text=f"Grand Prize: ${t_data['prize']:,.2f}")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="duel", description="PvP: Wager your tournament chips against another player!")
    async def duel(self, interaction: discord.Interaction, target: discord.Member, wager: int):
        t_data = self.tournaments.get(interaction.guild_id)
        
        if not t_data or t_data['state'] != "ACTIVE":
            return await interaction.response.send_message("❌ There is no active tournament to duel in.", ephemeral=True)
            
        if t_data['type'] not in ['duel', 'mixed']:
            return await interaction.response.send_message("❌ This tournament is restricted and does not allow PvP Duels!", ephemeral=True)
            
        if target.bot or target.id == interaction.user.id:
            return await interaction.response.send_message("❌ Invalid target.", ephemeral=True)
            
        p1_chips = t_data['players'].get(interaction.user.id, 0)
        p2_chips = t_data['players'].get(target.id, 0)
        
        if p1_chips <= 0: return await interaction.response.send_message("❌ You are eliminated or not in the tournament!", ephemeral=True)
        if p2_chips <= 0: return await interaction.response.send_message("❌ That player is eliminated or not in the tournament.", ephemeral=True)
        if wager <= 0: return await interaction.response.send_message("❌ Wager must be greater than 0.", ephemeral=True)
        if wager > p1_chips: return await interaction.response.send_message(f"❌ You only have {p1_chips} chips to wager.", ephemeral=True)
        if wager > p2_chips: return await interaction.response.send_message(f"❌ They only have {p2_chips} chips. Lower your wager.", ephemeral=True)

        embed = discord.Embed(
            title="⚔️ Tournament Duel Challenge!",
            description=f"{target.mention}, you have been challenged by {interaction.user.mention} for **{wager} Chips**!\n\nDo you accept?",
            color=0xe67e22
        )
        view = DuelView(self, interaction.guild_id, interaction.user, target, wager)
        await interaction.response.send_message(f"{target.mention}", embed=embed, view=view)

    @app_commands.command(name="admin_set_chips", description="Server Owner: Manually update a player's chips.")
    async def admin_set_chips(self, interaction: discord.Interaction, target: discord.Member, amount: int):
        if interaction.user.id != interaction.guild.owner_id:
            return await interaction.response.send_message("❌ Only the Server Owner can do this.", ephemeral=True)
            
        t_data = self.tournaments.get(interaction.guild_id)
        if not t_data:
            return await interaction.response.send_message("❌ No tournament is running.", ephemeral=True)
            
        if target.id not in t_data['players']:
            return await interaction.response.send_message("❌ That player is not in the tournament.", ephemeral=True)
            
        t_data['players'][target.id] = amount
        await interaction.response.send_message(f"✅ Set {target.mention}'s chips to **{amount}**.")
        if amount <= 0:
            await interaction.channel.send(f"💀 **ELIMINATED!** The host has eliminated {target.mention} from the tournament!")

    @app_commands.command(name="crown_winner", description="Server Owner: End the tournament and award the grand prize to the chip leader!")
    async def crown_winner(self, interaction: discord.Interaction):
        if interaction.user.id != interaction.guild.owner_id:
            return await interaction.response.send_message("❌ Only the Server Owner can end the tournament.", ephemeral=True)
            
        t_data = self.tournaments.get(interaction.guild_id)
        if not t_data:
            return await interaction.response.send_message("❌ No tournament is running.", ephemeral=True)
            
        sorted_players = sorted(t_data['players'].items(), key=lambda x: x[1], reverse=True)
        if not sorted_players:
            return await interaction.response.send_message("❌ No one joined the tournament.", ephemeral=True)
            
        winner_id, winning_chips = sorted_players[0]
        winner = interaction.guild.get_member(winner_id)
        
        prize = t_data['prize']
        game_name = t_data['name']
        
        db.update_balance(winner_id, prize)
        del self.tournaments[interaction.guild_id]
        
        embed = discord.Embed(
            title=f"🎉 TOURNAMENT CHAMPION 🎉",
            description=f"The **{game_name}** Tournament has officially concluded!\n\n🏆 **{winner.mention if winner else f'<@{winner_id}>'} IS THE CHAMPION!** 🏆\nThey finished with an incredible **{winning_chips} Chips**!\n\nThey have been awarded the Grand Prize of **${prize:,.2f}** into their bank account!",
            color=0xf1c40f
        )
        if winner: embed.set_thumbnail(url=winner.display_avatar.url)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="close", description="Server Owner: Cancel a tournament without a winner.")
    async def close_tourney(self, interaction: discord.Interaction):
        if interaction.user.id != interaction.guild.owner_id:
            return await interaction.response.send_message("❌ Only the Server Owner can do this.", ephemeral=True)
            
        if interaction.guild_id in self.tournaments:
            del self.tournaments[interaction.guild_id]
            await interaction.response.send_message("🛑 The tournament has been forcefully cancelled. No prizes were awarded.")
        else:
            await interaction.response.send_message("❌ No tournament is currently running.", ephemeral=True)

# --- THE TOURNAMENT CASINO GROUP ---
class TournamentCasino(app_commands.Group, name="t_casino", description="Gamble your tournament chips!"):
    def __init__(self, tourney_cog):
        super().__init__()
        self.tourney_cog = tourney_cog

    def check_valid_bet(self, interaction, wager):
        t_data = self.tourney_cog.tournaments.get(interaction.guild_id)
        
        if not t_data or t_data['state'] != "ACTIVE":
            return False, "❌ There is no active tournament right now!"
            
        # --- NEW RESTRICTION CHECK ---
        if t_data['type'] not in ['casino', 'mixed']:
            return False, "❌ This tournament's rules do not allow Casino games!"
        
        chips = t_data['players'].get(interaction.user.id, 0)
        if chips <= 0:
            return False, "💀 You are eliminated! You have 0 chips left."
            
        if wager <= 0:
            return False, "❌ Wager must be greater than 0."
            
        if wager > chips:
            return False, f"❌ You only have {chips} chips."
            
        return True, ""

    @app_commands.command(name="blackjack", description="Play a hand of Blackjack using Tournament Chips.")
    async def t_blackjack(self, interaction: discord.Interaction, wager: int):
        valid, msg = self.check_valid_bet(interaction, wager)
        if not valid: return await interaction.response.send_message(msg, ephemeral=True)
        
        view = TournamentBlackjack(self.tourney_cog, interaction.guild_id, interaction.user, wager)
        
        if calculate_bj_hand(view.player_hand) == 21:
            await view.end_game(interaction, "bj")
        else:
            await interaction.response.send_message(embed=view.get_embed(), view=view)

    @app_commands.command(name="poker", description="Play Video Poker using Tournament Chips. (Jacks or Better)")
    async def t_poker(self, interaction: discord.Interaction, wager: int):
        valid, msg = self.check_valid_bet(interaction, wager)
        if not valid: return await interaction.response.send_message(msg, ephemeral=True)
        
        view = TournamentVideoPoker(self.tourney_cog, interaction.guild_id, interaction.user, wager)
        await interaction.response.send_message(embed=view.get_embed(), view=view)

    @app_commands.command(name="roulette", description="Spin the Roulette wheel!")
    @app_commands.choices(choice=[
        app_commands.Choice(name="Red (2x)", value="red"),
        app_commands.Choice(name="Black (2x)", value="black"),
        app_commands.Choice(name="Green (14x)", value="green")
    ])
    async def t_roulette(self, interaction: discord.Interaction, wager: int, choice: app_commands.Choice[str]):
        valid, msg = self.check_valid_bet(interaction, wager)
        if not valid: return await interaction.response.send_message(msg, ephemeral=True)
        
        t_data = self.tourney_cog.tournaments[interaction.guild_id]
        
        roll = random.randint(0, 14)
        if roll == 0: color = "green"
        elif roll % 2 == 0: color = "red"
        else: color = "black"
        
        if choice.value == color:
            mult = 14 if color == "green" else 2
            winnings = wager * mult
            t_data['players'][interaction.user.id] += winnings
            embed = discord.Embed(title="🎡 Tournament Roulette", description=f"The ball landed on **{color.title()}**!\n\n🎉 You won **{winnings} Chips**!", color=0x2ecc71)
        else:
            t_data['players'][interaction.user.id] -= wager
            embed = discord.Embed(title="🎡 Tournament Roulette", description=f"The ball landed on **{color.title()}**.\n\n❌ You lost **{wager} Chips**.", color=0xe74c3c)
            
        await interaction.response.send_message(embed=embed)


async def setup(bot):
    cog = Tournament(bot)
    bot.tree.add_command(TournamentCasino(cog))
    await bot.add_cog(cog)