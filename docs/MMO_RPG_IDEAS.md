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

## ✅ Done: Guild (Town as Your Guild)

- **Town Hall** is framed as **Your Guild — Polyville**. Guild level and population are shown; a **progress bar** shows treasury progress toward the next guild level (cosmetic goal). Tagline: *"Your contributions strengthen the realm."*

## ✅ Done: Adventurer rank & character sheet

- **Profile** shows an **Adventurer rank** under the name: Novice → Apprentice → Veteran → Champion → Legend (based on class and dungeon max floor).
- Stats are grouped into **Combat** (HP, ATK, DEF, INT), **World** (Gold Purse, RPG Class, Max Floor, Guild Job), and **Equipment** (Active Armory). "Town Job" was renamed to "Guild Job".

## ✅ Done: Leaderboard RPG labels

- **Hall of Fame** subtitle and section titles use adventurer/dungeon wording: "Richest Adventurers", "By Dungeon Floor", and the subtitle mentions dungeon floor and collectors.

---

## ✅ Done: Theming & copy

- Nav: **Bazaar**, **Guild Hall**, **Tavern Games**; class + gold in nav (class hidden on small mobile).
- Profile: **Adventurer Profile**, **Gold Purse**, class emoji, *"Ready for the dungeon"* when class set.
- Footer lore: *"Polyville — a city of adventurers. Trade at the Bazaar, brave the dungeons in Discord, and grow your legend."*
- Inventory: **List for gold** button and **List for Gold** modal.

---

## Website: character sheet feel

- ~~**Profile as character sheet**~~ **Done.** Combat / World / Equipment blocks; Adventurer rank; set bonuses; last dungeon run; achievements.
- ~~**RPG Stats page**~~ **Done.** Tagline *"The realm's finest push deeper every day"* and **Hall of Legends** (top 3 by floor with class emoji).
- ~~**Leaderboard**~~ **Done.** Richest Adventurers, By Dungeon Floor, Master Collectors, **Top Donors to the Guild**.

---

## Progression & identity

- ~~**Adventurer rank / title**~~ **Done.** Novice → Apprentice → Veteran → Champion → Legend on profile.
- ~~**Class in nav**~~ **Done.** Nav shows class emoji + name next to gold for logged-in user.
- ~~**Daily login / streak**~~ **Done.** Visiting the Bazaar (market) records streak; first login of the day gets gold (scales with streak, cap 200). Banner on market when reward given.

---

## Quests & goals

- ~~**Daily quests**~~ **Done.** **Quests** page: List an item ($100), Crack a crate ($50), Support the guild ($75), Brave the dungeon ($150). Completing an action marks the quest; user claims reward. Dungeon can be marked complete via **/api/quests/complete_dungeon** (honor system or call from Discord bot).
- ~~**Achievements**~~ **Done.** First Crate, Dungeon Diver (floor 10), Millionaire ($1M), Guild Patron (donate $10k), Merchant (sell one). Unlock on actions and profile load; shown on profile.

---

## World & community

- ~~**Guild / Town as faction**~~ **Done.** Town is "Your Guild" with progress bar and tagline.
- ~~**Leaderboard**~~ **Done.** Richest, By Dungeon Floor, Collectors, **Top Donors to the Guild** (total_donated_gold tracked).
- ~~**Lore blurb**~~ **Done.** Footer: *"Polyville — a city of adventurers. Trade at the Bazaar, brave the dungeons in Discord, and grow your legend."*

---

## Economy & items

- ~~**Set bonuses**~~ **Done.** Profile shows set bonuses from equipped gear (e.g. RPG_Set_1: 2-piece +2 ATK, 4-piece +3 DEF, 6-piece +5 ATK/DEF). Defined in app `SET_BONUSES`.
- ~~**"List for gold"**~~ **Done.** Inventory button and modal.
- **Lootbox sets**: More sets that are explicitly "adventurer gear" with clear tier names (e.g. "Scout", "Knight", "Archmage") reinforce the RPG feel.

---

## Optional larger features

- **Stamina / energy**: Not implemented (can limit runs/crates per day if desired).
- ~~**World boss**~~ **Done (minimal).** Guild Hall shows **World Boss** HP bar. Spend gold to deal damage ($100 = 10 damage). When HP hits 0, boss respawns at full HP. Discord bot can call same DB to deal damage on dungeon run completion.
- ~~**Last dungeon run**~~ **Done.** Profile shows "Last Dungeon Run: Floor X — timestamp" when `last_rpg_run_floor` / `last_rpg_run_at` are set (updated by Discord bot in `log_rpg_run`).

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
