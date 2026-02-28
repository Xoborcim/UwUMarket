# Making Polyville More MMO-RPG Like

Ideas to deepen the fantasy/RPG feel across the bot and website. Ordered by impact vs effort.

---

## ✅ Done: Interactable NPCs

- **Bazaar:** The Merchant (links to Inventory), Bazaar Guard (flavor).
- **Town Hall:** Council Elder, Innkeeper (links to Tavern Games).
- **Lootboxes:** Crate Keeper, The Collector.
- **Casino:** House Dealer, Tipsy Adventurer.

Click any NPC to open a dialogue modal; some have an action button (e.g. "Open Inventory", "Tavern Games"). Data lives in `app.py` (`NPCS_BY_PAGE`); add more NPCs or dialogue lines there. To add NPCs on Discord, consider a `/talk <npc>` command that shows the same dialogue and links.

---

## Quick wins (theming & copy)

- **Rename UI labels** to fantasy terms:
  - "Market" → **"Bazaar"** or "Trading Post"
  - "Citizen Profile" → **"Adventurer Profile"** or "Character Sheet"
  - "Net Worth" → **"Gold Purse"** or "Treasury"
  - "Town Hall" → **"Guild Hall"** or "Council"
  - Lootbox: "Open Box" → **"Unlock Crate"** / "Crack Open"
  - Casino → **"Tavern Games"** or "Gambling Den" (with one-line flavor text)
- **Tagline** in nav or footer: e.g. *"Trade, dungeon, thrive."* or *"City of Adventurers"*
- **Profile**: Show class emoji next to class name (e.g. **Fighter**), and a short line like *"Ready for the dungeon"* when they have a class set.

---

## Website: character sheet feel

- **Profile as character sheet**: Group stats into blocks — "Combat" (HP, ATK, DEF, INT), "World" (Gold, Job, Max Floor), "Equipment" (current armory). Optional: tabs for Stats / Gear / Achievements.
- **RPG Stats page**: Add a line like *"The realm's finest push deeper every day."* and maybe a "Hall of Legends" subsection for top 3 by max floor (with class emoji).
- **Leaderboard**: Add a second view or filter: "By Dungeon Floor" so it feels like an adventurer ranking, not just wealth.

---

## Progression & identity

- **Adventurer rank / title**: A simple title derived from progress, e.g. "Novice" (no class), "Apprentice" (class set), "Veteran" (Floor 10+), "Champion" (Floor 25+), "Legend" (top 5 max floor). Show on profile and optionally in nav next to username.
- **Class on the website**: You already show RPG class on profile; consider showing it in the **nav** for the logged-in user (e.g. next to gold: "Fighter | $1,234") so the site feels tied to the Discord RPG identity.
- **Daily login / streak**: Small gold or "reputation" reward for visiting the site or using the bot daily (classic MMO retention).

---

## Quests & goals

- **Daily quests (website or bot)**:
  - "Sell 1 item on the Bazaar" → small gold reward
  - "Open 1 lootbox" or "Donate to the Guild Hall"
  - "Complete a dungeon run" (track via existing RPG analytics)
  - Show on a "Quests" or "Contracts" page; complete = one-time reward and checkmark.
- **Achievements / badges**: Unlockable titles or icons — "First Lootbox", "Reach Floor 10", "Earn $1M", "Donate 10k to Town". Display on profile and optionally in leaderboard.

---

## World & community

- **Guild / Town as faction**: You already have town level, treasury, food. Surface it as "Your Guild" with a progress bar, level, and a line like *"Your contributions strengthen the realm."* Link from Town Hall and profile.
- **Global / server leaderboards**: You have leaderboard; add or highlight "Top by Dungeon Floor" and "Top Donors to the Guild" so it feels like a living world.
- **Lore blurb**: One short paragraph on the homepage or an "About" section: e.g. *"Polyville is a city of adventurers. Trade at the Bazaar, brave the dungeons in Discord, and grow your legend."*

---

## Economy & items

- **Set bonuses**: When a player equips 2/4/6 items from the same set (e.g. RPG_Set_1), show a small bonus on profile (e.g. "+5 ATK for 2-piece"). Requires defining set bonuses in meta or DB.
- **"List for gold"** instead of "Sell" on market to keep the fantasy tone.
- **Lootbox sets**: More sets that are explicitly "adventurer gear" with clear tier names (e.g. "Scout", "Knight", "Archmage") reinforce the RPG feel.

---

## Optional larger features

- **Stamina / energy**: Limit dungeon runs or lootbox opens per day (e.g. 5 runs, 20 crates) to create scarcity and "come back tomorrow" — can feel bad if too strict.
- **World boss**: A server-wide health bar that everyone damages (e.g. via dungeon runs or a dedicated command); rewards at milestones. High effort but very MMO-like.
- **Discord ↔ Web link**: Already same account; show "Last dungeon: Floor 12" or "Last run: 2h ago" on profile by reading from `rpg_analytics` or a last_run timestamp.

---

## Summary

| Area           | Idea                          | Effort |
|----------------|-------------------------------|--------|
| Theming        | Rename Market, Profile, etc.  | Low    |
| Profile        | Character-sheet layout, rank  | Low–Med|
| Nav            | Show class next to user       | Low    |
| Quests         | 3–5 daily quests + page      | Medium |
| Achievements   | Badges/titles on profile      | Medium |
| Set bonuses    | 2/4/6 piece bonuses          | Medium |
| World boss     | Server-wide event             | High   |

Starting with **copy renames** and **profile/nav tweaks** gives a strong MMO-RPG feel for relatively little code.
