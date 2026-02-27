import asyncio

import aiosqlite

from database import DB_NAME
from cogs.rpg import SHOP_GEAR


async def migrate_starter_weapons_to_inventory() -> None:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row

        # Find all users who have any starter_weapon data saved.
        async with db.execute(
            "SELECT user_id, starter_weapon FROM users "
            "WHERE starter_weapon IS NOT NULL AND starter_weapon != ''"
        ) as cur:
            rows = await cur.fetchall()

        print(f"Found {len(rows)} users with starter_weapon data")

        inserted = 0
        for row in rows:
            uid = row["user_id"]
            sw = row["starter_weapon"] or ""
            gear_list = [g.strip() for g in sw.split(",") if g.strip()]

            # Ignore the default Rusty Dagger; migrate only purchased gear.
            for g in gear_list:
                if g == "Rusty Dagger":
                    continue

                data = SHOP_GEAR.get(g)
                if not data:
                    print(f"Skipping unknown gear '{g}' for user {uid}")
                    continue

                atk = int(data.get("atk", 0) or 0)
                df = int(data.get("def", 0) or 0)
                it = int(data.get("int", 0) or 0)

                # Infer slot from stats.
                if atk > 0 and df == 0 and it == 0:
                    slot = "weapon"
                elif df > 0 and atk == 0 and it == 0:
                    slot = "armor"
                elif it > 0 and atk == 0 and df == 0:
                    slot = "mage"
                else:
                    slot = "weapon"

                # Put everything under a virtual RPG shop set.
                tier = "Legendary"
                set_name = "RPG_Shop"

                await db.execute(
                    """
                    INSERT INTO inventory (
                        user_id,
                        item_name,
                        tier,
                        set_name,
                        item_type,
                        slot,
                        atk_bonus,
                        def_bonus,
                        int_bonus,
                        is_equipped,
                        is_listed,
                        list_price
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0.0)
                    """,
                    (uid, g, tier, set_name, "Gear", slot, atk, df, it),
                )
                inserted += 1

        # Clear starter_weapon so old stackable bonuses are removed;
        # get_rpg_profile will fall back to Rusty Dagger.
        await db.execute("UPDATE users SET starter_weapon = NULL")
        await db.commit()

        print(f"Inserted {inserted} inventory items from starter_weapon data")


if __name__ == "__main__":
    asyncio.run(migrate_starter_weapons_to_inventory())

