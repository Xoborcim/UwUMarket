import discord
from discord import app_commands
from discord.ext import commands
import database as db
import random
import os
import aiosqlite

TIER_WEIGHTS = {
    "Common": 50, "Uncommon": 25, "Rare": 15, 
    "Epic": 9, "Legendary": 5, "Mythic": 1
}

TIER_COLORS = {
    "Common": 0xb2bec3, "Uncommon": 0x2ecc71, "Rare": 0x3498db,
    "Epic": 0x9b59b6, "Legendary": 0xf1c40f, "Mythic": 0xe74c3c
}

SCRAP_VALUES = {
    "Common": 10.0, "Uncommon": 25.0, "Rare": 50.0, 
    "Epic": 150.0, "Legendary": 500.0, "Mythic": 2000.0
}

# --- HELPER FUNCTION: FIND IMAGE FILE ---
def get_item_filepath(item_data):
    """Safely finds the image file for an item, ignoring capitalization."""
    item_dict = dict(item_data)
    set_name = item_dict.get('set_name')
    if not set_name: set_name = "Base_Set"
        
    folder_path = f"lootboxes/{set_name}/{item_dict['tier']}"
    if not os.path.exists(folder_path): return None

    target_name_clean = item_dict['item_name'].replace(' ', '').lower()
    
    try:
        for file in os.listdir(folder_path):
            file_clean = file.replace('_', '').replace('.png', '').lower()
            if file_clean == target_name_clean:
                return os.path.join(folder_path, file)
    except FileNotFoundError:
        pass
    return None



# --- UI COMPONENTS FOR THE MARKET ---

class SellItemModal(discord.ui.Modal, title="Sell an Item"):
    item_id = discord.ui.TextInput(
        label="Item ID", style=discord.TextStyle.short, placeholder="e.g. 5", required=True
    )
    price = discord.ui.TextInput(
        label="Listing Price ($)", style=discord.TextStyle.short, placeholder="e.g. 150.00", required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            i_id = int(self.item_id.value.strip())
            p = float(self.price.value.strip())
        except ValueError:
            return await interaction.response.send_message("❌ Please enter valid numbers.", ephemeral=True)
            
        # Added Await!
        success, msg = await db.list_item_on_market(interaction.user.id, i_id, p)
        if success:
            await interaction.response.send_message(f"✅ **Market Listing Created!** Item #{i_id} is now for sale for **${p:,.2f}**.", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ {msg}", ephemeral=True)


class MarketGalleryUI(discord.ui.View):
    def __init__(self, listings):
        super().__init__(timeout=300)
        self.listings = listings
        self.index = 0
        self.update_navigation_buttons()

    def update_navigation_buttons(self):
        # Enable/Disable Previous and Next depending on page
        if not self.listings:
            return
        self.prev_btn.disabled = self.index == 0
        self.next_btn.disabled = self.index >= len(self.listings) - 1

    def generate_message_data(self):
        """Generates the embed and file for the current page."""
        item = self.listings[self.index]
        set_name = dict(item).get('set_name', 'Base_Set')
        
        embed = discord.Embed(
            title=f"🛒 Player Market ({self.index + 1}/{len(self.listings)})", 
            description=f"## {item['item_name']}\n**Tier:** {item['tier']} | **Set:** {set_name.replace('_', ' ')}\n**Seller:** <@{item['user_id']}>\n\n### 💰 Price: ${item['list_price']:,.2f}", 
            color=TIER_COLORS.get(item['tier'], 0xf39c12)
        )
        embed.set_footer(text=f"Item ID: {item['item_id']} | Use the arrows to browse")
        
        filepath = get_item_filepath(item)
        if filepath:
            file = discord.File(filepath, filename="item.png")
            embed.set_image(url="attachment://item.png")
            return embed, file
        else:
            embed.add_field(name="Error", value="⚠️ Image file missing from server.")
            return embed, None

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary, row=0)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.index -= 1
        self.update_navigation_buttons()
        embed, file = self.generate_message_data()
        
        # When editing, we must use 'attachments'
        if file:
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
        else:
            await interaction.response.edit_message(embed=embed, attachments=[], view=self)

    @discord.ui.button(label="Buy Item", style=discord.ButtonStyle.primary, emoji="🛒", row=0)
    async def buy_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        item = self.listings[self.index]
        # Added Await!
        success, msg = await db.buy_market_item(interaction.user.id, item['item_id'])
        
        if success:
            self.listings.pop(self.index)
            await interaction.channel.send(f"🎉 <@{interaction.user.id}> {msg}")
            
            if not self.listings:
                embed = discord.Embed(title="🛒 Market Empty", description="All items have been bought!", color=0x95a5a6)
                for c in self.children: c.disabled = True
                return await interaction.response.edit_message(embed=embed, attachments=[], view=self)
            
            if self.index >= len(self.listings):
                self.index = len(self.listings) - 1
                
            self.update_navigation_buttons()
            embed, file = self.generate_message_data()
            if file:
                await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
            else:
                await interaction.response.edit_message(embed=embed, attachments=[], view=self)
        else:
            await interaction.response.send_message(f"❌ {msg}", ephemeral=True)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=0)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.index += 1
        self.update_navigation_buttons()
        embed, file = self.generate_message_data()
        
        if file:
            await interaction.response.edit_message(embed=embed, attachments=[file], view=self)
        else:
            await interaction.response.edit_message(embed=embed, attachments=[], view=self)

    @discord.ui.button(label="Sell an Item", style=discord.ButtonStyle.success, emoji="🏷️", row=1)
    async def sell_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SellItemModal())

    @discord.ui.button(label="Check Inventory", style=discord.ButtonStyle.secondary, emoji="🎒", row=1)
    async def inv_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Added Await!
        items = await db.get_inventory(interaction.user.id)
        if not items: return await interaction.response.send_message("🎒 Your inventory is empty.", ephemeral=True)
        embed = discord.Embed(title="🎒 Your Inventory", color=0x2c3e50)
        desc = "Find the ID of the item you want to sell.\n\n"
        for item in items: desc += f"`ID: {item['item_id']}` | **{item['item_name']}** ({item['tier']})\n"
        embed.description = desc
        await interaction.response.send_message(embed=embed, ephemeral=True)


# --- MAIN COG ---

class Items(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def get_available_sets(self):
        if not os.path.exists("lootboxes"): return []
        return [d for d in os.listdir("lootboxes") if os.path.isdir(os.path.join("lootboxes", d))]

    async def set_autocomplete(self, interaction: discord.Interaction, current: str):
        sets = self.get_available_sets()
        return [app_commands.Choice(name=s.replace("_", " "), value=s) for s in sets if current.lower() in s.lower()][:25]

    def get_random_item(self, set_name, tier):
        folder_path = f"lootboxes/{set_name}/{tier}"
        if not os.path.exists(folder_path): return None
        files = [f for f in os.listdir(folder_path) if f.endswith('.png')]
        if not files: return None
        chosen = random.choice(files)
        return chosen.replace(".png", "").replace("_", " ").title(), os.path.join(folder_path, chosen)

    @app_commands.command(name="buy_lootbox", description="Open a lootbox from a specific set for $500!")
    @app_commands.autocomplete(set_name=set_autocomplete)
    async def buy_lootbox(self, interaction: discord.Interaction, set_name: str):
        if set_name not in self.get_available_sets():
            return await interaction.response.send_message("❌ Invalid set name.", ephemeral=True)
            
        valid_tiers = []
        for tier in TIER_WEIGHTS.keys():
            path = f"lootboxes/{set_name}/{tier}"
            if os.path.exists(path) and any(f.endswith('.png') for f in os.listdir(path)):
                valid_tiers.append(tier)
                
        if not valid_tiers:
            return await interaction.response.send_message(f"❌ The set **{set_name}** has no .png images inside it! Add some first.", ephemeral=True)

        cost = 500.0
        # Added Await!
        if not await db.update_balance(interaction.user.id, -cost):
            return await interaction.response.send_message("❌ Insufficient funds ($500 needed).", ephemeral=True)
        
        valid_weights = [TIER_WEIGHTS[t] for t in valid_tiers]
        rolled_tier = random.choices(valid_tiers, weights=valid_weights, k=1)[0]
        
        item_name, filepath = self.get_random_item(set_name, rolled_tier)
        
        # Added Await!
        await db.add_item(interaction.user.id, item_name, rolled_tier, set_name)
        
        embed = discord.Embed(title=f"🎁 {set_name.replace('_', ' ')} Lootbox!", description=f"You unboxed a **{rolled_tier}** item:\n### {item_name}", color=TIER_COLORS[rolled_tier])
        file = discord.File(filepath, filename="item.png")
        embed.set_image(url="attachment://item.png")
        await interaction.response.send_message(embed=embed, file=file)

    @app_commands.command(name="scrap", description="Dismantle an unwanted item for cash.")
    async def scrap(self, interaction: discord.Interaction, item_id: int):
        # Added Await!
        item = await db.get_item_by_id(item_id)
        if not item or item['user_id'] != interaction.user.id:
            return await interaction.response.send_message("❌ You don't own this item.", ephemeral=True)
            
        scrap_value = SCRAP_VALUES.get(item['tier'], 5.0)
        # Added Await!
        success, msg = await db.scrap_item(interaction.user.id, item_id, scrap_value)
        
        if success: await interaction.response.send_message(f"♻️ {msg}")
        else: await interaction.response.send_message(f"❌ {msg}", ephemeral=True)

    @app_commands.command(name="collection", description="Check your collection progress for a set.")
    @app_commands.autocomplete(set_name=set_autocomplete)
    async def collection(self, interaction: discord.Interaction, set_name: str):
        if set_name not in self.get_available_sets():
            return await interaction.response.send_message("❌ Invalid set.", ephemeral=True)
            
        total_items = 0
        all_items = []
        for tier in TIER_WEIGHTS.keys():
            path = f"lootboxes/{set_name}/{tier}"
            if os.path.exists(path):
                items = [f.replace(".png", "").replace("_", " ").title() for f in os.listdir(path) if f.endswith('.png')]
                total_items += len(items)
                all_items.extend(items)
                
        if total_items == 0: return await interaction.response.send_message("❌ This set has no items yet.", ephemeral=True)
        
        # FIX: Replaced raw synchronous SQLite call with proper aiosqlite block
        async with aiosqlite.connect(db.DB_NAME) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute("SELECT DISTINCT item_name FROM inventory WHERE user_id = ? AND set_name = ?", (interaction.user.id, set_name)) as cursor:
                owned = await cursor.fetchall()
                owned_names = [row['item_name'] for row in owned]
        
        owned_count = len([i for i in owned_names if i in all_items])
        percentage = (owned_count / total_items) * 100
        
        embed = discord.Embed(title=f"📚 {set_name.replace('_', ' ')} Collection Book", description=f"You have discovered **{owned_count}/{total_items}** unique items.\n", color=0x3498db)
        
        bar_length = 20
        filled = int((owned_count / total_items) * bar_length)
        bar = "▓" * filled + "░" * (bar_length - filled)
        embed.add_field(name="Progress", value=f"`{bar}` {percentage:.1f}%")
        
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="delist", description="Remove one of your items from the P2P market.")
    @app_commands.describe(item_id="The ID of the item you want to remove from the market")
    async def delist_item(self, interaction: discord.Interaction, item_id: int):
        # Added Await!
        success, msg = await db.delist_market_item(interaction.user.id, item_id)
        
        if success:
            await interaction.response.send_message(f"✅ {msg}", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ {msg}", ephemeral=True)

    @app_commands.command(name="inventory", description="View your items.")
    async def inventory(self, interaction: discord.Interaction):
        # Added Await!
        items = await db.get_inventory(interaction.user.id)
        if not items: return await interaction.response.send_message("🎒 Your inventory is empty.", ephemeral=True)
        embed = discord.Embed(title="🎒 Your Inventory", color=0x2c3e50)
        desc = "Use `/view_item [ID]` to see the image, or `/market` to sell it.\n\n"
        for item in items: desc += f"`ID: {item['item_id']}` | **{item['item_name']}** ({item['tier']})\n"
        embed.description = desc
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="view_item", description="Look at an item you own.")
    async def view_item(self, interaction: discord.Interaction, item_id: int):
        # Added Await!
        item = await db.get_item_by_id(item_id)
        
        # 1. Check Ownership
        if not item or item['user_id'] != interaction.user.id: 
            return await interaction.response.send_message("❌ You don't own this item.", ephemeral=True)
            
        filepath = get_item_filepath(item)

        if not filepath: 
            return await interaction.response.send_message(f"❌ Image file missing for **{item['item_name']}**. Make sure it's in the correct folder.", ephemeral=True)

        set_name = dict(item).get('set_name', 'Base_Set')

        # 4. Send Image
        embed = discord.Embed(title=item['item_name'], description=f"Tier: **{item['tier']}** | Set: **{set_name.replace('_', ' ')}**", color=TIER_COLORS.get(item['tier'], 0x95a5a6))
        file = discord.File(filepath, filename="item.png")
        embed.set_image(url="attachment://item.png")
        
        await interaction.response.send_message(embed=embed, file=file)

    # --- THE NEW INTERACTIVE GALLERY MARKET ---
    @app_commands.command(name="market", description="The Player-to-Player Trading Hub.")
    async def market(self, interaction: discord.Interaction):
        # Added Await!
        listings = await db.get_market_listings()
        
        if not listings:
            embed = discord.Embed(title="🛒 Player Market", description="**The market is currently empty.**\n\nClick below to list your own items.", color=0xf39c12)
            view = MarketGalleryUI([])
            view.children[0].disabled = True # Disable Prev
            view.children[1].disabled = True # Disable Buy
            view.children[2].disabled = True # Disable Next
            return await interaction.response.send_message(embed=embed, view=view)
            
        view = MarketGalleryUI(listings)
        embed, file = view.generate_message_data()
        
        # When sending a NEW message, we must use 'file='
        if file:
            await interaction.response.send_message(embed=embed, file=file, view=view)
        else:
            await interaction.response.send_message(embed=embed, view=view)

async def setup(bot):
    await bot.add_cog(Items(bot))
