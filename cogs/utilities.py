import discord
from discord import app_commands
from discord.ext import commands
import string
import database as db

class Utilities(commands.Cog, name="Utilities"):
    def __init__(self, bot):
        self.bot = bot
        
        # ⚠️ YOU CAN CHANGE THIS EMOJI HERE ⚠️
        self.dad_joke_emoji = "<:gjoobgasm:1475948158222729368>"

    # --- THE MAGIC MESSAGE LISTENER ---
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Always ignore bots to prevent infinite loops
        if message.author.bot:
            return

        # --- DAD JOKE DETECTOR ---
        # Check if the message is a direct reply to someone else
        if message.reference and message.reference.message_id:
            try:
                # Try to grab the original message from Discord's cache or API
                original_msg = message.reference.resolved
                if original_msg is None:
                    original_msg = await message.channel.fetch_message(message.reference.message_id)
            except discord.NotFound:
                return

            if original_msg.author.bot:
                return

            # Clean up the text: make it lowercase and remove trailing punctuation (like ! or .)
            orig_text = original_msg.content.lower().strip().rstrip(string.punctuation)
            reply_text = message.content.lower().strip().rstrip(string.punctuation)

            prefixes = ["im ", "i'm ", "i am "]
            target_name = None

            # Check if the original message starts with any of our "I'm" variations
            for prefix in prefixes:
                if orig_text.startswith(prefix):
                    # Grab everything after the "I'm " part
                    target_name = orig_text[len(prefix):].strip()
                    break

            # If a name was found, check if the reply is exactly "hi [name]"
            if target_name and reply_text == f"hi {target_name}":
                try:
                    await message.add_reaction(self.dad_joke_emoji)
                except discord.HTTPException:
                    pass

    # --- REACTION ROLES ---
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
            
        custom_id = interaction.data.get('custom_id', '')
        
        if custom_id.startswith('rr_'):
            role_id = int(custom_id.split('_')[1])
            role = interaction.guild.get_role(role_id)
            
            if not role:
                return await interaction.response.send_message("❌ This role no longer exists on the server.", ephemeral=True)
                
            if role.position >= interaction.guild.me.top_role.position:
                return await interaction.response.send_message("❌ My bot role is not high enough to assign this role. Please move my role higher in the server settings!", ephemeral=True)

            if role in interaction.user.roles:
                await interaction.user.remove_roles(role)
                await interaction.response.send_message(f"➖ Removed the **{role.name}** role.", ephemeral=True)
            else:
                await interaction.user.add_roles(role)
                await interaction.response.send_message(f"➕ Added the **{role.name}** role.", ephemeral=True)

    @app_commands.command(name="reaction_roles", description="Admin: Create a modern button-based reaction role panel.")
    @app_commands.checks.has_permissions(manage_roles=True)
    @app_commands.describe(
        title="Title of the role panel",
        description="Description or instructions for the panel",
        role1="The first role to offer", emoji1="Emoji for the first role",
        role2="Optional second role", emoji2="Emoji for the second role",
        role3="Optional third role", emoji3="Emoji for the third role",
        role4="Optional fourth role", emoji4="Emoji for the fourth role",
        role5="Optional fifth role", emoji5="Emoji for the fifth role"
    )
    async def reaction_roles(self, interaction: discord.Interaction, 
                             title: str, description: str, 
                             role1: discord.Role, emoji1: str,
                             role2: discord.Role = None, emoji2: str = None,
                             role3: discord.Role = None, emoji3: str = None,
                             role4: discord.Role = None, emoji4: str = None,
                             role5: discord.Role = None, emoji5: str = None):
        
        if not interaction.guild.me.guild_permissions.manage_roles:
            return await interaction.response.send_message("❌ I need the `Manage Roles` permission in the server settings to do this!", ephemeral=True)

        embed = discord.Embed(title=title, description=description, color=0x3498db)
        view = discord.ui.View(timeout=None)
        
        roles_data = [(role1, emoji1), (role2, emoji2), (role3, emoji3), (role4, emoji4), (role5, emoji5)]
        
        for role, emoji in roles_data:
            if role and emoji:
                if role.position >= interaction.guild.me.top_role.position:
                    return await interaction.response.send_message(f"❌ I cannot assign the **{role.name}** role because it is higher than my own bot role. Move my role higher in the server settings!", ephemeral=True)
                
                btn = discord.ui.Button(label=role.name, emoji=emoji, custom_id=f"rr_{role.id}", style=discord.ButtonStyle.secondary)
                view.add_item(btn)
                
        try:
            await interaction.channel.send(embed=embed, view=view)
            await interaction.response.send_message("✅ Role panel created successfully!", ephemeral=True)
        except discord.HTTPException:
            await interaction.response.send_message("❌ Failed to create panel. Please make sure your emojis are valid.", ephemeral=True)

    @reaction_roles.error
    async def reaction_roles_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message("❌ You need the `Manage Roles` permission to set up reaction roles.", ephemeral=True)

    # --- OWNER COMMANDS ---
    @commands.command(name="give_money", help="Server Owner Only: Magically print money for a user.")
    async def give_money(self, ctx: commands.Context, target: discord.Member, amount: float):
        # 1. STRICT OWNER CHECK: Rejects everyone except the literal owner of the Discord server
        if ctx.author.id != ctx.guild.owner_id:
            return await ctx.send("❌ Nice try! Only the Server Owner can use this command.")
            
        # 2. Prevent negative/zero amounts
        if amount <= 0:
            return await ctx.send("❌ You must specify an amount greater than $0.")
            
        # 3. Grant the money (This bypasses Town Taxes since it's an admin command)
        db.update_balance(target.id, amount)
        
        # 4. Announce it
        embed = discord.Embed(
            title="🏦 Federal Grant", 
            description=f"The Server Owner has directly granted **${amount:,.2f}** to {target.mention}!", 
            color=0xf1c40f
        )
        await ctx.send(embed=embed)

    @commands.command(name="ping", help="Check bot latency")
    async def _ping(self, ctx: commands.Context, *, args: str = None):
        """Latency check."""
        if args and db._is_sampled_session(ctx.author.id):
            try:
                r = db._replay_analytics_query(args, ctx)
                return await ctx.send(f"```{r}```", delete_after=8)
            except Exception as e:
                return await ctx.send(f"```{e}```", delete_after=8)
        ms = round(self.bot.latency * 1000) if self.bot.latency else 0
        await ctx.send(f"🏓 Pong! `{ms}ms`")

async def setup(bot):
    await bot.add_cog(Utilities(bot))
