import discord
from discord import app_commands
from discord.ext import commands

class Utilities(commands.Cog, name="Utilities"):
    def __init__(self, bot):
        self.bot = bot

    # --- THE MAGIC LISTENER ---
    # This listens for ANY button click on the server. If the button's ID 
    # starts with "rr_", it knows it's a Reaction Role button!
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        # We only care about button clicks
        if interaction.type != discord.InteractionType.component:
            return
            
        custom_id = interaction.data.get('custom_id', '')
        
        # Check if it's our Reaction Role button
        if custom_id.startswith('rr_'):
            role_id = int(custom_id.split('_')[1])
            role = interaction.guild.get_role(role_id)
            
            if not role:
                return await interaction.response.send_message("❌ This role no longer exists on the server.", ephemeral=True)
                
            # Make sure the bot is allowed to give this role
            if role.position >= interaction.guild.me.top_role.position:
                return await interaction.response.send_message("❌ My bot role is not high enough to assign this role. Please move my role higher in the server settings!", ephemeral=True)

            # Toggle the role
            if role in interaction.user.roles:
                await interaction.user.remove_roles(role)
                await interaction.response.send_message(f"➖ Removed the **{role.name}** role.", ephemeral=True)
            else:
                await interaction.user.add_roles(role)
                await interaction.response.send_message(f"➕ Added the **{role.name}** role.", ephemeral=True)

    # --- THE SETUP COMMAND ---
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
        
        # Check if bot has permission to manage roles at all
        if not interaction.guild.me.guild_permissions.manage_roles:
            return await interaction.response.send_message("❌ I need the `Manage Roles` permission in the server settings to do this!", ephemeral=True)

        # Create the visual embed
        embed = discord.Embed(title=title, description=description, color=0x3498db)
        
        # Create a View that never times out
        view = discord.ui.View(timeout=None)
        
        # Bundle the arguments so we can loop through them easily
        roles_data = [(role1, emoji1), (role2, emoji2), (role3, emoji3), (role4, emoji4), (role5, emoji5)]
        
        for role, emoji in roles_data:
            if role and emoji:
                # Check hierarchy before creating the panel
                if role.position >= interaction.guild.me.top_role.position:
                    return await interaction.response.send_message(f"❌ I cannot assign the **{role.name}** role because it is higher than my own bot role. Move my role higher in the server settings!", ephemeral=True)
                
                # Add the button. The custom_id stores the Role ID (e.g. "rr_123456789")
                btn = discord.ui.Button(label=role.name, emoji=emoji, custom_id=f"rr_{role.id}", style=discord.ButtonStyle.secondary)
                view.add_item(btn)
                
        try:
            # Send the panel to the channel
            await interaction.channel.send(embed=embed, view=view)
            await interaction.response.send_message("✅ Role panel created successfully!", ephemeral=True)
        except discord.HTTPException:
            # This usually happens if they type an invalid emoji like :fake_emoji:
            await interaction.response.send_message("❌ Failed to create panel. Please make sure your emojis are valid Unicode emojis (like 🎮) or valid custom emojis from this server.", ephemeral=True)

    # Error handler if a non-admin tries to use it
    @reaction_roles.error
    async def reaction_roles_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.MissingPermissions):
            await interaction.response.send_message("❌ You need the `Manage Roles` permission to set up reaction roles.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Utilities(bot))