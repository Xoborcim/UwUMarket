import discord
from discord import app_commands
from discord.ext import commands, tasks
import database as db
import random
import asyncio
import itertools

# ==========================================
#             POKER ENGINE
# ==========================================
SUITS = {'H': '♥️', 'D': '♦️', 'C': '♣️', 'S': '♠️'}
RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']

def get_deck():
    deck = [f"{r}{s}" for r in RANKS for s in SUITS.keys()]
    random.shuffle(deck)
    return deck

def format_cards(cards):
    return " ".join([f"**{c[:-1]}**{SUITS[c[-1]]}" for c in cards])

def eval_5(cards):
    vals = {'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,'10':10,'J':11,'Q':12,'K':13,'A':14}
    ranks = sorted([vals[c[:-1]] for c in cards], reverse=True)
    suits = [c[-1] for c in cards]
    
    is_flush = len(set(suits)) == 1
    is_st = False
    if len(set(ranks)) == 5:
        if ranks[0] - ranks[4] == 4: is_st = True
        elif ranks == [14, 5, 4, 3, 2]: 
            is_st = True
            ranks = [5, 4, 3, 2, 1] 
            
    counts = sorted([(ranks.count(r), r) for r in set(ranks)], reverse=True)
    
    if is_st and is_flush: return (8, ranks[0])
    if counts[0][0] == 4: return (7, counts[0][1], counts[1][1])
    if counts[0][0] == 3 and counts[1][0] == 2: return (6, counts[0][1], counts[1][1])
    if is_flush: return (5, ranks)
    if is_st: return (4, ranks[0])
    if counts[0][0] == 3: return (3, counts[0][1], [r for r in ranks if r != counts[0][1]])
    if counts[0][0] == 2 and counts[1][0] == 2: return (2, counts[0][1], counts[1][1], counts[2][1])
    if counts[0][0] == 2: return (1, counts[0][1], [r for r in ranks if r != counts[0][1]])
    return (0, ranks)

def get_best_hand(hole, board):
    best = max(eval_5(combo) for combo in itertools.combinations(hole + board, 5))
    names = ["High Card", "Pair", "Two Pair", "3 of a Kind", "Straight", "Flush", "Full House", "4 of a Kind", "Straight Flush"]
    return best, names[best[0]]

class MultiplayerPokerGame(discord.ui.View):
    def __init__(self, party, ante):
        super().__init__(timeout=600)
        self.lock = asyncio.Lock()
        self.session_id = str(random.randint(1000, 9999))
        self.players = party
        self.active_players = [p.id for p in party]
        self.ante = ante
        
        self.pot = ante * len(party)
        self.deck = get_deck()
        self.board = []
        self.hands = {p.id: [self.deck.pop(), self.deck.pop()] for p in party}
        
        # Betting Variables
        self.stage = 0 
        self.turn_idx = 0
        self.current_bet = 0
        self.player_bets = {p.id: 0 for p in party}
        self.acted_this_round = set()
        
        self.log = "Cards dealt! Pre-flop betting begins."
        self.build_ui()

    def build_ui(self):
        self.clear_items()
        if self.stage < 5:
            curr_uid = self.active_players[self.turn_idx]
            amt_to_call = self.current_bet - self.player_bets[curr_uid]
            
            call_label = "Check" if amt_to_call == 0 else f"Call ${amt_to_call:,.2f}"
            
            btn_fold = discord.ui.Button(label="Fold", style=discord.ButtonStyle.danger, emoji="🏳️", custom_id=f"pfold_{self.session_id}")
            btn_fold.callback = self.action_fold
            
            btn_call = discord.ui.Button(label=call_label, style=discord.ButtonStyle.success, emoji="✅", custom_id=f"pcall_{self.session_id}")
            btn_call.callback = self.action_call
            
            btn_raise = discord.ui.Button(label=f"Raise ${self.ante:,.2f}", style=discord.ButtonStyle.primary, emoji="📈", custom_id=f"praise_{self.session_id}")
            btn_raise.callback = self.action_raise
            
            btn_view = discord.ui.Button(label="View My Cards", style=discord.ButtonStyle.secondary, emoji="👁️", custom_id=f"pview_{self.session_id}")
            btn_view.callback = self.action_view
            
            self.add_item(btn_fold)
            self.add_item(btn_call)
            self.add_item(btn_raise)
            self.add_item(btn_view)

    def get_embed(self):
        embed = discord.Embed(title="🃏 Multiplayer Texas Hold'em", color=0xf1c40f)
        embed.description = f"### 💰 Pot: ${self.pot:,.2f}\n```{self.log}```"
        
        if self.stage == 0: board_str = "🎴 🎴 🎴 🎴 🎴"
        elif self.stage == 1: board_str = format_cards(self.board) + " 🎴 🎴"
        elif self.stage == 2: board_str = format_cards(self.board) + " 🎴"
        else: board_str = format_cards(self.board)
        
        embed.add_field(name="Community Board", value=board_str, inline=False)
        
        for p in self.players:
            uid = p.id
            if uid not in self.active_players:
                status, cards = "🏳️ Folded", "🎴 🎴"
            elif self.stage == 5:
                cards = format_cards(self.hands[uid])
                _, h_name = get_best_hand(self.hands[uid], self.board)
                status = f"*{h_name}*"
            else:
                cards = "🎴 🎴"
                if self.active_players[self.turn_idx] == uid:
                    status = "🔄 **Their Turn!**"
                elif uid in self.acted_this_round:
                    status = f"✅ In (${self.player_bets[uid]:,.2f})"
                else:
                    status = "⏳ Waiting"
                    
            embed.add_field(name=p.display_name, value=f"{cards}\n{status}", inline=True)
            
        return embed

    async def check_advance(self, interaction):
        if len(self.active_players) == 1:
            winner_id = self.active_players[0]
            db.update_balance(winner_id, self.pot)
            winner = next(p for p in self.players if p.id == winner_id)
            self.log = f"Everyone else folded! {winner.display_name} wins **${self.pot:,.2f}**!"
            self.stage = 5
            self.build_ui()
            return await interaction.edit_original_response(embed=self.get_embed(), view=self)

        all_matched = all(self.player_bets[uid] == self.current_bet for uid in self.active_players)
        all_acted = all(uid in self.acted_this_round for uid in self.active_players)

        if all_matched and all_acted:
            self.stage += 1
            self.current_bet = 0
            for uid in self.active_players: self.player_bets[uid] = 0
            self.acted_this_round.clear()
            self.turn_idx = 0

            if self.stage == 1:
                self.board.extend([self.deck.pop() for _ in range(3)])
                self.log = "**The Flop** is dealt!"
            elif self.stage == 2:
                self.board.append(self.deck.pop())
                self.log = "**The Turn** is dealt!"
            elif self.stage == 3:
                self.board.append(self.deck.pop())
                self.log = "**The River** is dealt!"
            elif self.stage == 4:
                # SHOWDOWN!
                best_score = (-1, [])
                winner_id, hand_name = None, ""

                for uid in self.active_players:
                    score, h_name = get_best_hand(self.hands[uid], self.board)
                    if score > best_score:
                        best_score, winner_id, hand_name = score, uid, h_name

                db.update_balance(winner_id, self.pot)
                winner = next(p for p in self.players if p.id == winner_id)
                self.log = f"SHOWDOWN!\n{winner.display_name} wins **${self.pot:,.2f}** with a {hand_name}!"
                self.stage = 5
        else:
            self.turn_idx = (self.turn_idx + 1) % len(self.active_players)

        self.build_ui()
        await interaction.edit_original_response(embed=self.get_embed(), view=self)

    async def action_view(self, interaction: discord.Interaction):
        if interaction.user.id not in self.hands:
            return await interaction.response.send_message("❌ You are not at this table!", ephemeral=True)
        await interaction.response.send_message(f"Your Hole Cards: {format_cards(self.hands[interaction.user.id])}", ephemeral=True)

    async def action_call(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            uid = interaction.user.id
            if self.active_players[self.turn_idx] != uid:
                return await interaction.followup.send("❌ It is not your turn!", ephemeral=True)

            amt = self.current_bet - self.player_bets[uid]
            if amt > 0:
                if not db.update_balance(uid, -amt):
                    return await interaction.followup.send("❌ You don't have enough money to Call! You must Fold.", ephemeral=True)
                self.log = f"✅ {interaction.user.display_name} calls ${amt:,.2f}."
            else:
                self.log = f"✅ {interaction.user.display_name} checks."

            self.pot += amt
            self.player_bets[uid] = self.current_bet
            self.acted_this_round.add(uid)
            
            await self.check_advance(interaction)

    async def action_raise(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            uid = interaction.user.id
            if self.active_players[self.turn_idx] != uid:
                return await interaction.followup.send("❌ It is not your turn!", ephemeral=True)

            new_bet = self.current_bet + self.ante
            amt = new_bet - self.player_bets[uid]

            if not db.update_balance(uid, -amt):
                return await interaction.followup.send(f"❌ You need ${amt:,.2f} to Raise!", ephemeral=True)

            self.current_bet = new_bet
            self.pot += amt
            self.player_bets[uid] = new_bet
            self.acted_this_round.clear() 
            self.acted_this_round.add(uid)

            self.log = f"📈 {interaction.user.display_name} RAISES to ${new_bet:,.2f}!"
            await self.check_advance(interaction)

    async def action_fold(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with self.lock:
            uid = interaction.user.id
            if self.active_players[self.turn_idx] != uid:
                return await interaction.followup.send("❌ It is not your turn!", ephemeral=True)

            self.active_players.remove(uid)
            self.log = f"🏳️ {interaction.user.display_name} folded."
            
            if self.turn_idx >= len(self.active_players):
                self.turn_idx = 0
            await self.check_advance(interaction)


class PokerLobby(discord.ui.View):
    def __init__(self, host, ante):
        super().__init__(timeout=300)
        self.host = host
        self.ante = ante
        self.party = [host]
        self.pot = ante

    @discord.ui.button(label="Join Table", style=discord.ButtonStyle.primary, emoji="💸")
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in self.party:
            return await interaction.response.send_message("❌ You are already at the table.", ephemeral=True)
        if len(self.party) >= 6:
            return await interaction.response.send_message("❌ The table is full! (Max 6)", ephemeral=True)
            
        if not db.update_balance(interaction.user.id, -self.ante):
            return await interaction.response.send_message(f"❌ You need ${self.ante:,.2f} to sit at this table!", ephemeral=True)
            
        await interaction.response.defer()
        self.party.append(interaction.user)
        self.pot += self.ante
        
        embed = interaction.message.embeds[0]
        embed.description += f"\n✅ {interaction.user.mention} bought in!"
        embed.title = f"🃏 Texas Hold'em Lobby (Pot: ${self.pot:,.2f})"
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.success, emoji="🃏")
    async def btn_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.host.id:
            return await interaction.response.send_message("❌ Only the host can deal the cards.", ephemeral=True)
        if len(self.party) < 2:
            return await interaction.response.send_message("❌ You need at least 2 players to start a poker game!", ephemeral=True)
            
        await interaction.response.defer()
        game = MultiplayerPokerGame(self.party, self.ante)
        await interaction.edit_original_response(embed=game.get_embed(), view=game)
        self.stop()


# ==========================================
#        ADVANCED BLACKJACK ENGINE
# ==========================================
def get_bj_score(hand):
    score = 0
    aces = 0
    for card in hand:
        rank = card[:-1]
        if rank in ['J', 'Q', 'K']: score += 10
        elif rank == 'A': 
            score += 11
            aces += 1
        else: score += int(rank)
        
    while score > 21 and aces > 0:
        score -= 10
        aces -= 1
    return score

class AdvancedBlackjackGame(discord.ui.View):
    def __init__(self, user, bet):
        super().__init__(timeout=120)
        self.user = user
        self.initial_bet = bet
        self.deck = get_deck()
        self.session_id = str(random.randint(1000, 9999))
        
        self.dealer_hand = [self.deck.pop(), self.deck.pop()]
        
        # We use lists to support Splitting into multiple hands!
        self.hands = [[self.deck.pop(), self.deck.pop()]]
        self.bets = [bet]
        self.statuses = ["Playing"] # Playing, Won, Lost, Push, Blackjack
        
        self.active_idx = 0
        self.game_over = False

        # Instant Blackjack Check
        p_score = get_bj_score(self.hands[0])
        d_score = get_bj_score(self.dealer_hand)
        
        if p_score == 21:
            self.game_over = True
            if d_score == 21:
                self.statuses[0] = "Push"
                db.update_balance(self.user.id, self.bets[0])
            else:
                self.statuses[0] = "Blackjack"
                db.update_balance(self.user.id, self.bets[0] * 2.5) # 3:2 payout
                
        self.update_buttons()

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("❌ This is not your game!", ephemeral=True)
            return False
        return True

    def update_buttons(self):
        self.clear_items()
        if self.game_over: return

        btn_hit = discord.ui.Button(label="Hit", style=discord.ButtonStyle.primary, custom_id=f"hit_{self.session_id}")
        btn_hit.callback = self.action_hit
        btn_stand = discord.ui.Button(label="Stand", style=discord.ButtonStyle.secondary, custom_id=f"stand_{self.session_id}")
        btn_stand.callback = self.action_stand
        self.add_item(btn_hit)
        self.add_item(btn_stand)

        current_hand = self.hands[self.active_idx]

        # DOUBLE DOWN (Only on first 2 cards of a hand)
        if len(current_hand) == 2:
            btn_double = discord.ui.Button(label="Double Down", style=discord.ButtonStyle.danger, custom_id=f"double_{self.session_id}")
            btn_double.callback = self.action_double
            self.add_item(btn_double)

        # SPLIT (Only on 2 cards of exact same rank, max 4 splits to prevent spam)
        if len(current_hand) == 2 and len(self.hands) < 4:
            if current_hand[0][:-1] == current_hand[1][:-1]:
                btn_split = discord.ui.Button(label="Split", style=discord.ButtonStyle.success, custom_id=f"split_{self.session_id}")
                btn_split.callback = self.action_split
                self.add_item(btn_split)

    def get_embed(self):
        color = 0x3498db
        if self.game_over:
            if any(s in ["Won", "Blackjack"] for s in self.statuses): color = 0x2ecc71
            elif all(s == "Lost" for s in self.statuses): color = 0xe74c3c

        embed = discord.Embed(title="♠️ Blackjack", color=color)
        
        # Dealer Hand
        if not self.game_over:
            embed.add_field(name="Dealer's Hand", value=f"{format_cards([self.dealer_hand[0]])} 🎴", inline=False)
        else:
            d_score = get_bj_score(self.dealer_hand)
            embed.add_field(name=f"Dealer's Hand ({d_score})", value=format_cards(self.dealer_hand), inline=False)
            
        # Player Hands (Supports multiple splits!)
        for i, (hand, bet, status) in enumerate(zip(self.hands, self.bets, self.statuses)):
            pointer = "👉 " if (i == self.active_idx and not self.game_over) else ""
            hand_name = f"{pointer}Hand {i+1} (${bet:,.2f}) [{get_bj_score(hand)}]"
            
            val = format_cards(hand)
            if status != "Playing": 
                val += f"\n*{status}*"
                
            embed.add_field(name=hand_name, value=val, inline=True)
            
        return embed

    async def advance_hand(self, interaction):
        self.active_idx += 1
        if self.active_idx >= len(self.hands):
            await self.resolve_dealer(interaction)
        else:
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)

    async def resolve_dealer(self, interaction):
        self.game_over = True
        self.clear_items()
        
        # Dealer only hits if there is at least one hand that hasn't busted
        needs_dealer = any(s == "Playing" for s in self.statuses)
        if needs_dealer:
            while get_bj_score(self.dealer_hand) < 17:
                self.dealer_hand.append(self.deck.pop())
                
        d_score = get_bj_score(self.dealer_hand)
        
        for i in range(len(self.hands)):
            if self.statuses[i] != "Playing": continue # Skip busted/BJ hands
            
            p_score = get_bj_score(self.hands[i])
            if p_score > 21: 
                self.statuses[i] = "Lost"
            elif d_score > 21 or p_score > d_score:
                self.statuses[i] = "Won"
                db.update_balance(self.user.id, self.bets[i] * 2)
            elif p_score < d_score:
                self.statuses[i] = "Lost"
            else:
                self.statuses[i] = "Push"
                db.update_balance(self.user.id, self.bets[i])
                
        self.stop()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    async def action_hit(self, interaction: discord.Interaction):
        self.hands[self.active_idx].append(self.deck.pop())
        if get_bj_score(self.hands[self.active_idx]) > 21:
            self.statuses[self.active_idx] = "Lost"
            await self.advance_hand(interaction)
        else:
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_embed(), view=self)

    async def action_stand(self, interaction: discord.Interaction):
        await self.advance_hand(interaction)

    async def action_double(self, interaction: discord.Interaction):
        if not db.update_balance(self.user.id, -self.bets[self.active_idx]):
            return await interaction.response.send_message("❌ Insufficient funds to Double Down.", ephemeral=True)
            
        self.bets[self.active_idx] *= 2
        self.hands[self.active_idx].append(self.deck.pop())
        
        if get_bj_score(self.hands[self.active_idx]) > 21:
            self.statuses[self.active_idx] = "Lost"
            
        # Double Down forces a stand
        await self.advance_hand(interaction)

    async def action_split(self, interaction: discord.Interaction):
        if not db.update_balance(self.user.id, -self.initial_bet):
            return await interaction.response.send_message("❌ Insufficient funds to Split.", ephemeral=True)
            
        # Separate the two identical cards
        card1 = self.hands[self.active_idx][0]
        card2 = self.hands[self.active_idx].pop()
        
        # Give Hand 1 a new second card
        self.hands[self.active_idx].append(self.deck.pop())
        
        # Create Hand 2 with card2 and a new card
        self.hands.insert(self.active_idx + 1, [card2, self.deck.pop()])
        self.bets.insert(self.active_idx + 1, self.initial_bet)
        self.statuses.insert(self.active_idx + 1, "Playing")
        
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)


# ==========================================
#          RUSSIAN ROULETTE ENGINE
# ==========================================
class RouletteGameUI(discord.ui.View):
    def __init__(self, party, bet):
        super().__init__(timeout=300)
        self.party = party
        self.alive = party.copy()
        self.bet = bet
        self.pot = bet * len(party)
        self.chamber_with_bullet = random.randint(0, 5)
        self.current_chamber = 0
        self.turn_idx = 0

    def get_embed(self):
        embed = discord.Embed(title="🔫 Russian Roulette", color=0xe74c3c)
        embed.description = f"### 💰 Pot: ${self.pot:,.2f}\nIt is currently **{self.alive[self.turn_idx].display_name}'s** turn to pull the trigger."
        alive_list = "\n".join([f"👤 {p.display_name}" for p in self.alive])
        embed.add_field(name="Survivors", value=alive_list if alive_list else "None", inline=False)
        return embed

    @discord.ui.button(label="Pull Trigger", style=discord.ButtonStyle.danger, emoji="🔫")
    async def btn_pull(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.alive[self.turn_idx].id:
            return await interaction.response.send_message("❌ It is not your turn!", ephemeral=True)
            
        await interaction.response.defer()
        
        if self.current_chamber == self.chamber_with_bullet:
            dead_player = self.alive.pop(self.turn_idx)
            if len(self.alive) == 1:
                winner = self.alive[0]
                db.update_balance(winner.id, self.pot)
                embed = discord.Embed(
                    title="🔫 BANG!", 
                    description=f"💀 **{dead_player.display_name}** was eliminated!\n\n🎉 **{winner.display_name}** is the last one standing and wins the **${self.pot:,.2f}** pot!", 
                    color=0x2ecc71
                )
                self.stop()
                return await interaction.edit_original_response(embed=embed, view=None)
            
            self.chamber_with_bullet = random.randint(0, 5)
            self.current_chamber = 0
            if self.turn_idx >= len(self.alive): self.turn_idx = 0
            
            embed = self.get_embed()
            embed.description = f"💥 **BANG!** {dead_player.display_name} was eliminated!\n\nThe gun is reloaded and spun. It's **{self.alive[self.turn_idx].display_name}'s** turn."
            await interaction.edit_original_response(embed=embed, view=self)
            
        else:
            self.current_chamber += 1
            self.turn_idx = (self.turn_idx + 1) % len(self.alive)
            embed = self.get_embed()
            embed.description = f"💨 *Click.* {interaction.user.display_name} survives.\n\nIt is now **{self.alive[self.turn_idx].display_name}'s** turn."
            await interaction.edit_original_response(embed=embed, view=self)

class RouletteLobbyUI(discord.ui.View):
    def __init__(self, host, bet):
        super().__init__(timeout=300)
        self.host = host
        self.bet = bet
        self.party = [host]
        self.pot = bet

    @discord.ui.button(label="Join Game", style=discord.ButtonStyle.primary, emoji="🩸")
    async def btn_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in self.party:
            return await interaction.response.send_message("❌ You are already in.", ephemeral=True)
        if len(self.party) >= 6:
            return await interaction.response.send_message("❌ Lobby is full (Max 6).", ephemeral=True)
            
        if not db.update_balance(interaction.user.id, -self.bet):
            return await interaction.response.send_message(f"❌ You need ${self.bet:,.2f} to join!", ephemeral=True)
            
        await interaction.response.defer()
        self.party.append(interaction.user)
        self.pot += self.bet
        
        embed = interaction.message.embeds[0]
        embed.description += f"\n✅ {interaction.user.mention} joined the circle."
        embed.title = f"🔫 Russian Roulette Lobby (Pot: ${self.pot:,.2f})"
        await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.success)
    async def btn_start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.host.id:
            return await interaction.response.send_message("❌ Only the host can start the game.", ephemeral=True)
        if len(self.party) < 2:
            return await interaction.response.send_message("❌ You need at least 2 people to play!", ephemeral=True)
            
        await interaction.response.defer()
        game = RouletteGameUI(self.party, self.bet)
        await interaction.edit_original_response(embed=game.get_embed(), view=game)
        self.stop()


# ==========================================
#          JADD'S BELLY SLAP ENGINE
# ==========================================
class BellySlapGame(discord.ui.View):
    def __init__(self, user, bet):
        super().__init__(timeout=60)
        self.user = user
        self.bet = bet
        self.multiplier = 1.0
        self.slaps = 0
        # Risk starts at 5% and increases by 7% per slap
        self.base_risk = 0.05 
        self.risk_increase = 0.07
        self.ended = False

    def get_current_risk(self):
        return min(0.95, self.base_risk + (self.slaps * self.risk_increase))

    def get_embed(self, status="playing"):
        risk_pct = int(self.get_current_risk() * 100)
        potential_win = self.bet * self.multiplier

        if status == "playing":
            color = 0xe67e22 # Orange
            title = "🫃 Slap Jad's Belly!"
            desc = ("Slap his belly to increase the multiplier!\n"
                    "But careful... every slap increases the chance he farts.")
        elif status == "won":
            color = 0x2ecc71 # Green
            title = "🎉 Cashed Out!"
            desc = f"You slapped {self.slaps} times and walked away safely."
        elif status == "lost":
            color = 0xe74c3c # Red
            title = "💨 Jad Farted! "
            desc = "Oh no! Your money blended into the fart and mysteriously dissapeared!"

        embed = discord.Embed(title=title, description=desc, color=color)
        embed.add_field(name="💵 Initial Bet", value=f"${self.bet:,.2f}", inline=True)
        embed.add_field(name="📈 Multiplier", value=f"**{self.multiplier:.2f}x**", inline=True)
        
        if status == "playing":
             embed.add_field(name="💰 Potential Win", value=f"${potential_win:,.2f}", inline=True)
             embed.add_field(name="⚠️ Fart Risk", value=f"{risk_pct}%", inline=True)
        elif status == "won":
             embed.add_field(name="💰 Winnings", value=f"**${potential_win:,.2f}**", inline=True)
        
        embed.set_footer(text=f"Player: {self.user.display_name}")
        return embed

    @discord.ui.button(label="👋 Slap Belly", style=discord.ButtonStyle.primary)
    async def slap_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        current_risk = self.get_current_risk()
        roll = random.random()

        if roll < current_risk:
            # LOST
            self.ended = True
            self.clear_items()
            await interaction.response.edit_message(embed=self.get_embed(status="lost"), view=self)
            self.stop()
        else:
            # SURVIVED
            self.slaps += 1
            self.multiplier *= 1.15 # Multiplier compounds exponentially 
            await interaction.response.edit_message(embed=self.get_embed(status="playing"), view=self)

    @discord.ui.button(label="💰 Cash Out", style=discord.ButtonStyle.success)
    async def cash_out_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        
        if self.slaps == 0:
             return await interaction.response.send_message("You have to slap at least once!", ephemeral=True)

        self.ended = True
        winnings = self.bet * self.multiplier
        db.update_balance(self.user.id, winnings)
        
        self.clear_items()
        await interaction.response.edit_message(embed=self.get_embed(status="won"), view=self)
        self.stop()

    async def on_timeout(self):
        if not self.ended:
            self.clear_items()
            await self.message.edit(content="⏳ Game timed out. Bet lost.", embed=self.get_embed(status="lost"), view=self)

# ==========================================
#             MINES ENGINE
# ==========================================
class MinesGame(discord.ui.View):
    def __init__(self, user, bet, mine_count):
        super().__init__(timeout=120)
        self.user = user
        self.bet = bet
        self.mine_count = mine_count
        # Discord only allows 5 rows of 5 buttons max. A 20-tile grid leaves room for the Cash Out button on the bottom row.
        self.total_tiles = 20 
        self.safe_count = self.total_tiles - mine_count
        self.revealed_safe = 0
        self.ended = False
        self.multiplier = 1.0
        
        self.grid = [True] * mine_count + [False] * self.safe_count
        random.shuffle(self.grid)
        
        self.create_grid()
        self.update_cashout_button()

    def calculate_next_multiplier(self):
        # Calculate odds based on the board state BEFORE the click that just happened
        previous_revealed = self.revealed_safe
        remaining_tiles = self.total_tiles - previous_revealed
        remaining_safe = self.safe_count - previous_revealed
        
        if remaining_safe <= 0: return self.multiplier 

        odds = remaining_tiles / remaining_safe
        new_mult = self.multiplier * (odds * 0.98) # 2% House Edge
        return new_mult

    def create_grid(self):
        for i in range(self.total_tiles):
            row = i // 5
            btn = discord.ui.Button(style=discord.ButtonStyle.secondary, label="\u200b", custom_id=f"mine_{i}", row=row)
            btn.callback = self.tile_callback
            self.add_item(btn)

    def update_cashout_button(self):
        cashout_btn = discord.utils.get(self.children, custom_id="cashout")
        if not cashout_btn:
            # Placed cleanly on row 4
            cashout_btn = discord.ui.Button(style=discord.ButtonStyle.success, label="Cash Out", custom_id="cashout", row=4, disabled=True)
            cashout_btn.callback = self.cashout_callback
            self.add_item(cashout_btn)
        
        if self.revealed_safe > 0 and not self.ended:
            win_amt = self.bet * self.multiplier
            cashout_btn.label = f"Cash Out: ${win_amt:,.2f}"
            cashout_btn.disabled = False
        else:
             cashout_btn.label = "Cash Out"
             cashout_btn.disabled = True

    def reveal_all(self, interaction):
        for item in self.children:
            if item.custom_id and item.custom_id.startswith("mine_"):
                idx = int(item.custom_id.split("_")[1])
                item.disabled = True
                if self.grid[idx]: 
                    item.style = discord.ButtonStyle.danger
                    item.emoji = "💣"
                else: 
                    item.style = discord.ButtonStyle.success
                    if item.label == "\u200b": item.emoji = "💎"
        self.update_cashout_button()
    
    async def tile_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        if self.ended: return await interaction.response.defer()

        btn_id = interaction.data["custom_id"]
        idx = int(btn_id.split("_")[1])
        clicked_button = [x for x in self.children if x.custom_id == btn_id][0]

        if self.grid[idx]:
            # MINE HIT
            self.ended = True
            self.reveal_all(interaction)
            clicked_button.emoji = "💥"
            embed = interaction.message.embeds[0]
            embed.color = 0xe74c3c
            embed.title = "💥 BOOM! You hit a mine."
            await interaction.response.edit_message(embed=embed, view=self)
            self.stop()
        else:
            # SAFE TILE
            # FIX: Calculate multiplier BEFORE incrementing revealed_safe so the math doesn't see 0 remaining safe tiles!
            self.multiplier = self.calculate_next_multiplier()
            self.revealed_safe += 1
            
            clicked_button.style = discord.ButtonStyle.success
            clicked_button.emoji = "💎"
            clicked_button.disabled = True
            
            embed = interaction.message.embeds[0]

            if self.revealed_safe == self.safe_count:
                # AUTO WIN - FOUND ALL GEMS
                self.ended = True
                self.reveal_all(interaction)
                winnings = self.bet * self.multiplier
                db.update_balance(self.user.id, winnings)
                embed.color = 0x2ecc71
                embed.title = f"💎 PERFECT GAME! Won ${winnings:,.2f}!"
                self.stop()
            else:
                embed.description = f"**Current Multiplier: {self.multiplier:.2f}x**\nMines: {self.mine_count} | Gems left: {self.safe_count - self.revealed_safe}"
                self.update_cashout_button()

            await interaction.response.edit_message(embed=embed, view=self)

    async def cashout_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user.id: return await interaction.response.defer()
        if self.ended or self.revealed_safe == 0: return await interaction.response.defer()
        
        self.ended = True
        winnings = self.bet * self.multiplier
        db.update_balance(self.user.id, winnings)
        
        self.reveal_all(interaction)
        embed = interaction.message.embeds[0]
        embed.color = 0x2ecc71
        embed.title = f"💰 Cashed Out: ${winnings:,.2f}"
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()


# ==========================================
#             THE CASINO COG
# ==========================================
class Casino(commands.GroupCog, group_name="casino", group_description="Gamble your money away!"):
    def __init__(self, bot):
        self.bot = bot
        
        # ⚠️ IMPORTANT: REPLACE THIS NUMBER WITH YOUR DISCORD CHANNEL ID!
        self.announcement_channel_id = 123456789012345678 
        
        if not self.lottery_draw_task.is_running():
            self.lottery_draw_task.start()

    def cog_unload(self):
        self.lottery_draw_task.cancel()

    # --- THE 24 HOUR BACKGROUND TASK ---
    @tasks.loop(hours=24)
    async def lottery_draw_task(self):
        await self.bot.wait_until_ready()
        
        try:
            res = db.draw_lottery_winner()
            if isinstance(res, tuple):
                winner_id, pot = res
            else:
                winner_id = res
                pot = "the pot"
        except TypeError:
            try:
                winner_id, pot = db.draw_lottery()
            except:
                winner_id = db.draw_lottery()
                pot = "the pot"
            
        if winner_id:
            channel = self.bot.get_channel(self.announcement_channel_id)
            if channel:
                embed = discord.Embed(
                    title="🎟️ Daily Lottery Draw!", 
                    description=f"🎉 Congratulations to <@{winner_id}>!\n\nThey just won **${pot if isinstance(pot, float) else pot}**!\nBuy your tickets for tomorrow's draw with `/casino buy_ticket`.", 
                    color=0xf1c40f
                )
                await channel.send(embed=embed)

    @app_commands.command(name="buy_ticket", description="Buy a ticket for the Daily Lottery ($50)!")
    async def buy_ticket(self, interaction: discord.Interaction):
        success = db.buy_lottery_ticket(interaction.user.id)
            
        if success:
            await interaction.response.send_message(f"🎟️ **Ticket Secured!** You are entered into the daily draw for $50.", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ Insufficient funds. Tickets cost $50.", ephemeral=True)

    @app_commands.command(name="lottery_pool", description="Check how big the daily lottery pot has gotten!")
    async def lottery_pool(self, interaction: discord.Interaction):
        try:
            tickets, pot = db.get_lottery_stats()
        except:
            tickets, pot = 0, 0

        embed = discord.Embed(
            title="🎟️ Daily Lottery", 
            description=f"**Current Pot:** ${pot:,.2f}\n**Total Tickets Sold:** {tickets}\n\n*Draws every 24 hours!*",
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="lottery", description="Buy a quick scratch-off lottery ticket for $50!")
    async def lottery(self, interaction: discord.Interaction):
        cost = 50.0
        if not db.update_balance(interaction.user.id, -cost):
            return await interaction.response.send_message("❌ You need $50 to buy a scratch-off ticket!", ephemeral=True)
            
        roll = random.randint(1, 100)
        if roll == 1:
            winnings = cost * 50
            db.update_balance(interaction.user.id, winnings)
            embed = discord.Embed(title="🎫 Lottery Results", description=f"🎉 **JACKPOT!** You scratched off a winning ticket and got **${winnings:,.2f}**!", color=0xf1c40f)
        else:
            embed = discord.Embed(title="🎫 Lottery Results", description="🗑️ *Scratch, scratch...* Nothing. Better luck next time.", color=0x95a5a6)
            
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="poker", description="Host a Multiplayer Texas Hold'em Table!")
    @app_commands.describe(ante="The required buy-in and raise amount.")
    async def poker(self, interaction: discord.Interaction, ante: float):
        if ante < 10.0:
            return await interaction.response.send_message("❌ Minimum Buy-in is $10.00.", ephemeral=True)
            
        if not db.update_balance(interaction.user.id, -ante):
            return await interaction.response.send_message(f"❌ You don't have enough money for a ${ante:,.2f} Buy-in.", ephemeral=True)
            
        embed = discord.Embed(
            title=f"🃏 Texas Hold'em Lobby (Pot: ${ante:,.2f})", 
            description=f"Host: {interaction.user.mention}\n**Buy-in / Ante:** ${ante:,.2f}\n\nClick **Join Table** to pay the buy-in. Host clicks **Start** when ready!",
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=PokerLobby(interaction.user, ante))

    @app_commands.command(name="blackjack", description="Play a hand of Blackjack against the dealer.")
    async def blackjack(self, interaction: discord.Interaction, bet: float):
        if bet < 5.0:
            return await interaction.response.send_message("❌ Minimum bet is $5.00.", ephemeral=True)
            
        if not db.update_balance(interaction.user.id, -bet):
            return await interaction.response.send_message("❌ Insufficient funds!", ephemeral=True)
            
        game = AdvancedBlackjackGame(interaction.user, bet)
        if game.game_over: 
            game.clear_items()
        await interaction.response.send_message(embed=game.get_embed(), view=game)

    @app_commands.command(name="slots", description="Spin the slot machine!")
    async def slots(self, interaction: discord.Interaction, bet: float):
        if bet < 1.0:
            return await interaction.response.send_message("❌ Minimum bet is $1.00.", ephemeral=True)
        if not db.update_balance(interaction.user.id, -bet):
            return await interaction.response.send_message("❌ Insufficient funds!", ephemeral=True)

        emojis = ["🍒", "🍋", "🍇", "🔔", "💎", "7️⃣"]
        result = [random.choice(emojis) for _ in range(3)]
        
        embed = discord.Embed(title="🎰 Slot Machine", description=f"## [ {result[0]} | {result[1]} | {result[2]} ]\n\n", color=0xe67e22)
        
        if result[0] == result[1] == result[2]:
            winnings = bet * 10
            embed.description += f"**JACKPOT!** You won ${winnings:,.2f}!"
            embed.color = 0xf1c40f
            db.update_balance(interaction.user.id, winnings)
        elif result[0] == result[1] or result[1] == result[2] or result[0] == result[2]:
            winnings = bet * 2
            embed.description += f"**Small Win!** You got two matches and won ${winnings:,.2f}!"
            embed.color = 0x2ecc71
            db.update_balance(interaction.user.id, winnings)
        else:
            embed.description += "You lost. Better luck next time!"
            embed.color = 0xe74c3c
            
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="roulette", description="Bet on a color (Red/Black) or Green!")
    @app_commands.choices(choice=[
        app_commands.Choice(name="Red (2x Payout)", value="red"),
        app_commands.Choice(name="Black (2x Payout)", value="black"),
        app_commands.Choice(name="Green (14x Payout)", value="green")
    ])
    async def roulette(self, interaction: discord.Interaction, bet: float, choice: app_commands.Choice[str]):
        if bet < 1.0:
            return await interaction.response.send_message("❌ Minimum bet is $1.00.", ephemeral=True)
        if not db.update_balance(interaction.user.id, -bet):
            return await interaction.response.send_message("❌ Insufficient funds!", ephemeral=True)

        roll = random.randint(0, 36)
        if roll == 0: color = "green"
        elif roll % 2 == 0: color = "black"
        else: color = "red"
        
        color_emoji = "🟩" if color == "green" else ("🟥" if color == "red" else "⬛")
        
        embed = discord.Embed(title="🎡 Roulette", description=f"The ball landed on... **{roll} {color_emoji}**\n\n", color=0x3498db)
        
        if choice.value == color:
            multiplier = 14 if color == "green" else 2
            winnings = bet * multiplier
            db.update_balance(interaction.user.id, winnings)
            embed.description += f"🎉 **You won!** Payout: ${winnings:,.2f}"
            embed.color = 0x2ecc71
        else:
            embed.description += f"❌ **You lost.** Your bet was on {choice.name}."
            embed.color = 0xe74c3c

        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="russian_roulette", description="Host a multiplayer game of Russian Roulette.")
    @app_commands.describe(bet="The buy-in amount to join the game.")
    async def russian_roulette(self, interaction: discord.Interaction, bet: float):
        if bet < 5.0:
            return await interaction.response.send_message("❌ Minimum buy-in is $5.00.", ephemeral=True)
            
        if not db.update_balance(interaction.user.id, -bet):
            return await interaction.response.send_message(f"❌ You don't have enough money for a ${bet:,.2f} buy-in.", ephemeral=True)
            
        embed = discord.Embed(
            title=f"🔫 Russian Roulette Lobby (Pot: ${bet:,.2f})", 
            description=f"Host: {interaction.user.mention}\n**Buy-in:** ${bet:,.2f}\n\nClick **Join** to pay the buy-in and enter the circle.",
            color=0xe74c3c
        )
        await interaction.response.send_message(embed=embed, view=RouletteLobbyUI(interaction.user, bet))

    # --- NEW: SLAP JADD'S BELLY ---
    @app_commands.command(name="slap", description="Push your luck by slapping Jad's belly. Don't make him fart!")
    @app_commands.describe(bet="Amount to bet")
    async def slap_game(self, interaction: discord.Interaction, bet: float):
        if bet <= 0:
            return await interaction.response.send_message("❌ Bet must be positive.", ephemeral=True)
        if not db.update_balance(interaction.user.id, -bet):
            return await interaction.response.send_message("❌ Insufficient funds!", ephemeral=True)
        
        game_view = BellySlapGame(interaction.user, bet)
        embed = game_view.get_embed(status="playing")
        
        await interaction.response.send_message(embed=embed, view=game_view)
        game_view.message = await interaction.original_response()

    # --- NEW: MINES ---
    @app_commands.command(name="mines", description="Classic 20-tile minesweeper betting game.")
    @app_commands.describe(bet="Amount to bet", mines="Number of mines (1-19)")
    async def mines_game(self, interaction: discord.Interaction, bet: float, mines: int):
        if bet <= 0:
            return await interaction.response.send_message("❌ Bet must be positive.", ephemeral=True)
        if mines < 1 or mines > 19:
            return await interaction.response.send_message("❌ Mines must be between 1 and 19 for a 20-tile grid.", ephemeral=True)

        if not db.update_balance(interaction.user.id, -bet):
            return await interaction.response.send_message("❌ Insufficient funds!", ephemeral=True)

        game_view = MinesGame(interaction.user, bet, mines)
        
        embed = discord.Embed(
            title=f"💣 Mines ({mines})", 
            description=f"**Current Multiplier: 1.00x**\nMines: {mines} | Gems left: {20 - mines}", 
            color=0x3498db
        )
        embed.set_footer(text=f"Player: {interaction.user.display_name} | Bet: ${bet:,.2f}")

        await interaction.response.send_message(embed=embed, view=game_view)

async def setup(bot):
    await bot.add_cog(Casino(bot))