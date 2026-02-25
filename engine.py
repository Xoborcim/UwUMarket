async def buy_shares(user_id, market_id, outcome_label, investment):
    if investment < 0.01: return False, "Minimum bet is $0.01"
    investment = round(investment, 2)

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try:
            # 1. Check User Balance
            await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
                bal = (await cursor.fetchone())['balance']
                
            if bal < investment: return False, "Insufficient funds."

            # 2. Get Both Market Pools
            async with db.execute("SELECT * FROM outcomes WHERE market_id = ?", (market_id,)) as cursor:
                outcomes = await cursor.fetchall()
                
            if len(outcomes) != 2:
                return False, "CPMM math currently requires exactly 2 outcomes (Binary Market)."
                
            target = next((o for o in outcomes if o['label'] == outcome_label), None)
            other = next((o for o in outcomes if o['label'] != outcome_label), None)
            
            if not target or not other: return False, "Invalid outcome."

            # 3. Apply the CPMM Engine Math!
            target_pool = target['pool_balance']
            other_pool = other['pool_balance']
            
            k = target_pool * other_pool
            new_other_pool = other_pool + investment
            new_target_pool = k / new_other_pool
            shares = target_pool - new_target_pool
            
            probability = other_pool / (target_pool + other_pool) # Price BEFORE the buy

            # 4. Save the new CPMM Pool States to the Database
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (investment, user_id))
            await db.execute("UPDATE outcomes SET pool_balance = ? WHERE outcome_id = ?", (new_target_pool, target['outcome_id']))
            await db.execute("UPDATE outcomes SET pool_balance = ? WHERE outcome_id = ?", (new_other_pool, other['outcome_id']))
            await db.execute("UPDATE markets SET volume = volume + ? WHERE market_id = ?", (investment, market_id))

            # 5. Update Player's Portfolio
            async with db.execute("SELECT position_id FROM positions WHERE user_id = ? AND outcome_id = ?", (user_id, target['outcome_id'])) as cursor:
                existing = await cursor.fetchone()
                
            if existing:
                await db.execute("UPDATE positions SET shares_held = shares_held + ? WHERE position_id = ?", (shares, existing['position_id']))
            else:
                await db.execute("INSERT INTO positions (user_id, market_id, outcome_id, shares_held) VALUES (?, ?, ?, ?)", (user_id, market_id, target['outcome_id'], shares))
            
            await db.commit()
            return True, f"Bought {shares:.2f} shares @ ~{int(probability*100)}¢"
            
        except Exception as e: 
            return False, str(e)
