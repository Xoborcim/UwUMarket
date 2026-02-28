"""
Discord cog: Guild and Guild War commands.
Mirrors the website's guild create/join/leave/disband and guild war challenge/accept/fight.
"""
import discord
from discord import app_commands
from discord.ext import commands
import database as db


async def _ensure_user(interaction: discord.Interaction):
    """Ensure the command user exists in the DB (sync from Discord)."""
    await db.sync_user_data(
        interaction.user.id,
        interaction.user.display_name or interaction.user.name,
        None,
    )


class Guilds(commands.Cog, name="Guilds"):
    def __init__(self, bot):
        self.bot = bot

    async def _sync_and_get_guild_info(self, interaction: discord.Interaction):
        await _ensure_user(interaction)
        return await db.get_user_guild_info(interaction.user.id)

    # --- GUILD COMMANDS ---

    @app_commands.command(name="guild_create", description="Create a new guild (you become the leader).")
    @app_commands.describe(name="Guild name (2–32 characters)")
    async def guild_create(self, interaction: discord.Interaction, name: str):
        await _ensure_user(interaction)
        name = (name or "").strip()
        guild_id, err = await db.create_guild(interaction.user.id, name)
        if err:
            return await interaction.response.send_message(f"❌ {err}", ephemeral=True)
        await interaction.response.send_message(
            f"✅ Guild **{name}** created! You are the leader. Use `/guild_info` to see your guild, or visit the website Guild Hall.",
            ephemeral=True,
        )

    @app_commands.command(name="guild_join", description="Join an existing guild by name.")
    @app_commands.describe(guild_name="Exact guild name to join")
    async def guild_join(self, interaction: discord.Interaction, guild_name: str):
        await _ensure_user(interaction)
        guild = await db.get_guild_by_name(guild_name)
        if not guild:
            return await interaction.response.send_message(
                f"❌ No guild named **{guild_name}** found. Use `/guild_list` to see guild names.",
                ephemeral=True,
            )
        ok, msg = await db.join_guild(interaction.user.id, guild["id"])
        if not ok:
            return await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
        await interaction.response.send_message(f"✅ {msg}", ephemeral=True)

    @app_commands.command(name="guild_leave", description="Leave your current guild.")
    async def guild_leave(self, interaction: discord.Interaction):
        await _ensure_user(interaction)
        ok, msg = await db.leave_guild(interaction.user.id)
        if not ok:
            return await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
        await interaction.response.send_message(f"✅ {msg}", ephemeral=True)

    @app_commands.command(name="guild_disband", description="Disband your guild (leader only). All members are removed.")
    async def guild_disband(self, interaction: discord.Interaction):
        await _ensure_user(interaction)
        ok, msg = await db.disband_guild(interaction.user.id)
        if not ok:
            return await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
        await interaction.response.send_message(f"✅ {msg}", ephemeral=True)

    @app_commands.command(name="guild_info", description="Show your current guild and its stats.")
    async def guild_info(self, interaction: discord.Interaction):
        await _ensure_user(interaction)
        info = await db.get_user_guild_info(interaction.user.id)
        if not info:
            return await interaction.response.send_message(
                "❌ You are not in a guild. Use `/guild_create` to create one or `/guild_join` to join by name. You can also use the website Guild Hall.",
                ephemeral=True,
            )
        state = await db.get_guild_state(info["guild_id"])
        if not state:
            return await interaction.response.send_message("❌ Guild data could not be loaded.", ephemeral=True)
        role = "Leader" if info["guild_role"] == "leader" else "Member"
        embed = discord.Embed(
            title=f"🏛️ {state['name']}",
            description=f"You are **{role}**.\nTreasury: **${state['treasury']:,.2f}** • Level: **{state['level']}** • Members: **{state['user_count']}**",
            color=0x3498db,
        )
        embed.set_footer(text="Use /guild_leave to leave, or visit the website for donations and world boss.")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="guild_list", description="List top guilds by rank (treasury + level).")
    @app_commands.describe(limit="Number of guilds to show (1–20)")
    async def guild_list(self, interaction: discord.Interaction, limit: int = 10):
        await _ensure_user(interaction)
        limit = max(1, min(20, limit))
        guilds = await db.get_top_guilds(limit)
        if not guilds:
            return await interaction.response.send_message("No guilds exist yet. Create one with `/guild_create`!", ephemeral=True)
        lines = []
        for i, g in enumerate(guilds, 1):
            g = dict(g)
            lines.append(f"**{i}.** {g['name']} — Lv.{g.get('level') or 1} · ${(g.get('treasury') or 0):,.0f}")
        embed = discord.Embed(
            title="🏰 Top Guilds",
            description="\n".join(lines),
            color=0xf1c40f,
        )
        embed.set_footer(text="Join a guild with /guild_join <name> (use exact name from this list).")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # --- GUILD WAR COMMANDS ---

    @app_commands.command(name="guild_war_challenge", description="Declare war on another guild (leader only).")
    @app_commands.describe(defender_guild_name="Exact name of the guild you want to declare war on")
    async def guild_war_challenge(self, interaction: discord.Interaction, defender_guild_name: str):
        await _ensure_user(interaction)
        info = await db.get_user_guild_info(interaction.user.id)
        if not info:
            return await interaction.response.send_message("❌ You must be in a guild to declare war.", ephemeral=True)
        if info["guild_role"] != "leader":
            return await interaction.response.send_message("❌ Only your guild leader can declare war.", ephemeral=True)
        defender = await db.get_guild_by_name(defender_guild_name.strip())
        if not defender:
            return await interaction.response.send_message(
                f"❌ No guild named **{defender_guild_name}** found. Use `/guild_list` to see guild names.",
                ephemeral=True,
            )
        war_id, err = await db.create_guild_war(info["guild_id"], defender["id"], interaction.user.id)
        if err:
            return await interaction.response.send_message(f"❌ {err}", ephemeral=True)
        await interaction.response.send_message(
            f"⚔️ War declared on **{defender['name']}**! Their leader must accept with `/guild_war_accept`. War ID: `{war_id}`",
            ephemeral=True,
        )

    @app_commands.command(name="guild_war_accept", description="Accept a pending war (defender guild leader only).")
    async def guild_war_accept(self, interaction: discord.Interaction):
        await _ensure_user(interaction)
        info = await db.get_user_guild_info(interaction.user.id)
        if not info:
            return await interaction.response.send_message("❌ You must be in a guild.", ephemeral=True)
        war = await db.get_active_war_for_guild(info["guild_id"])
        if not war:
            return await interaction.response.send_message("❌ Your guild has no pending war.", ephemeral=True)
        if war["status"] != "pending":
            return await interaction.response.send_message("❌ Your guild's current war is not pending (already active or ended).", ephemeral=True)
        if war["defender_guild_id"] != info["guild_id"]:
            return await interaction.response.send_message("❌ Only the **defender** guild can accept. Your guild is the challenger.", ephemeral=True)
        ok, msg = await db.accept_guild_war(war["id"], interaction.user.id)
        if not ok:
            return await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
        await interaction.response.send_message(f"✅ {msg}", ephemeral=True)

    @app_commands.command(name="guild_war_status", description="Show your guild's current war (if any).")
    async def guild_war_status(self, interaction: discord.Interaction):
        await _ensure_user(interaction)
        info = await db.get_user_guild_info(interaction.user.id)
        if not info:
            return await interaction.response.send_message("❌ You are not in a guild.", ephemeral=True)
        war = await db.get_active_war_for_guild(info["guild_id"])
        if not war:
            return await interaction.response.send_message(
                "Your guild is not in a war. Leaders can declare with `/guild_war_challenge <guild_name>`.",
                ephemeral=True,
            )
        war = dict(war)
        status = war.get("status", "active")
        embed = discord.Embed(
            title=f"⚔️ War: {war.get('challenger_name')} vs {war.get('defender_name')}",
            description=f"**Status:** {status}\n**Score:** {war.get('challenger_wins') or 0} – {war.get('defender_wins') or 0}\nFirst to **{db.GUILD_WAR_WINS_TO_VICTORY}** wins. Fight with `/guild_war_fight @enemy_member`.",
            color=0xe74c3c,
        )
        if war.get("expires_at"):
            embed.set_footer(text=f"War expires: {str(war['expires_at'])[:10]}")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="guild_war_fight", description="Fight a member of the enemy guild in the current war.")
    @app_commands.describe(opponent="Discord user who is a member of the enemy guild")
    async def guild_war_fight(self, interaction: discord.Interaction, opponent: discord.Member):
        await _ensure_user(interaction)
        await db.sync_user_data(opponent.id, opponent.display_name or opponent.name, None)
        if opponent.bot:
            return await interaction.response.send_message("❌ You cannot fight a bot.", ephemeral=True)
        if opponent.id == interaction.user.id:
            return await interaction.response.send_message("❌ You cannot fight yourself.", ephemeral=True)
        info = await db.get_user_guild_info(interaction.user.id)
        if not info:
            return await interaction.response.send_message("❌ You must be in a guild to fight in a guild war.", ephemeral=True)
        war = await db.get_active_war_for_guild(info["guild_id"])
        if not war or war.get("status") != "active":
            return await interaction.response.send_message("❌ Your guild has no active war. Use `/guild_war_status` to check.", ephemeral=True)
        defender_guild = await db.get_user_guild_info(opponent.id)
        if not defender_guild:
            return await interaction.response.send_message(f"❌ {opponent.display_name} is not in any guild.", ephemeral=True)
        if defender_guild["guild_id"] not in (war["challenger_guild_id"], war["defender_guild_id"]):
            return await interaction.response.send_message(f"❌ {opponent.display_name} is not in the enemy guild.", ephemeral=True)
        if defender_guild["guild_id"] == info["guild_id"]:
            return await interaction.response.send_message("❌ You cannot fight a guildmate. Choose an enemy guild member.", ephemeral=True)
        # Run PvP using same logic as app.py
        from cogs.rpg import CLASSES, SHOP_GEAR

        async def _get_stats(uid):
            gear_data, class_name = await db.get_rpg_profile(uid)
            gear_names = [g.strip() for g in (gear_data.split(",") if gear_data else ["Rusty Dagger"]) if g.strip()]
            equipped = await db.get_equipped_gear(uid)
            equipped_list = [dict(r) for r in equipped] if equipped else []
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
            return {"hp": cls["hp"], "max_hp": cls["hp"], "atk": max(1, total_atk + cls["atk_mod"]), "def": max(0, 5 + total_def + cls["def_mod"])}

        def _run_pvp(stats_a, stats_b):
            import random
            a_hp, b_hp = float(stats_a["hp"]), float(stats_b["hp"])
            a_atk, a_def = stats_a["atk"], stats_a["def"]
            b_atk, b_def = stats_b["atk"], stats_b["def"]
            while a_hp > 0 and b_hp > 0:
                dmg = max(1, a_atk - b_def + random.randint(-2, 2))
                b_hp -= dmg
                if b_hp <= 0:
                    return 1
                dmg = max(1, b_atk - a_def + random.randint(-2, 2))
                a_hp -= dmg
                if a_hp <= 0:
                    return 2
            return 1 if a_hp > 0 else 2

        stats_attacker = await _get_stats(interaction.user.id)
        stats_defender = await _get_stats(opponent.id)
        winner = _run_pvp(stats_attacker, stats_defender)
        winner_id = interaction.user.id if winner == 1 else opponent.id
        result, err = await db.record_guild_war_battle(war["id"], interaction.user.id, opponent.id, winner_id)
        if err:
            return await interaction.response.send_message(f"❌ {err}", ephemeral=True)
        cw, dw = result
        you_won = winner_id == interaction.user.id
        msg = f"{'✅ You won' if you_won else '❌ You lost'} the battle! **{interaction.user.display_name}** vs **{opponent.display_name}**.\n**War score:** {cw} – {dw}"
        if cw >= db.GUILD_WAR_WINS_TO_VICTORY or dw >= db.GUILD_WAR_WINS_TO_VICTORY:
            msg += "\n🏆 **The war has ended!** Victorious guild earned the war bonus."
        await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Guilds(bot))
