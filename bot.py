#!/usr/bin/env python3
"""
GWE Fleet Bot — Complete Edition
Features: PTI, PM, Parking, DOT, Units, Reports, Broadcast, Groups, Admins
"""

import os
import html
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set

import pytz
import telebot
from telebot import types
from apscheduler.schedulers.background import BackgroundScheduler

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env variable is not set!")

_raw_admins = os.environ.get("ADMIN_IDS", "6939239782")
ADMIN_IDS: Set[int] = {int(x) for x in _raw_admins.split(",") if x.strip().isdigit()}

DB_PATH  = os.environ.get("DB_PATH", "/app/data/fleet.db")
ET_TZ    = pytz.timezone("US/Eastern")
CONTACT  = "@Chester_FLEET"

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ─────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────
_state_lock = threading.Lock()
_states: Dict[int, Dict[str, Any]] = {}


def set_state(uid: int, state: str, data: Optional[Dict] = None) -> None:
    with _state_lock:
        _states[uid] = {"state": state, "data": data or {}}


def get_state(uid: int) -> Dict[str, Any]:
    with _state_lock:
        return dict(_states.get(uid, {"state": None, "data": {}}))


def clear_state(uid: int) -> None:
    with _state_lock:
        _states.pop(uid, None)


# ─────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────
def _ensure_db_dir():
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    _ensure_db_dir()
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS groups (
            id          INTEGER PRIMARY KEY,
            title       TEXT NOT NULL DEFAULT '',
            unit_code   TEXT,
            driver_name TEXT
        );

        CREATE TABLE IF NOT EXISTS pti_reports (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id    INTEGER NOT NULL,
            driver_name TEXT NOT NULL,
            ts_et       TEXT NOT NULL,
            date_et     TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_pti ON pti_reports(group_id, date_et);

        CREATE TABLE IF NOT EXISTS pm_plans (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id    INTEGER NOT NULL,
            created_at  TEXT NOT NULL,
            is_done     INTEGER NOT NULL DEFAULT 0,
            done_at     TEXT,
            amount      REAL,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS parking (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            unit_number         TEXT NOT NULL,
            unit_type           TEXT NOT NULL DEFAULT 'TRUCK',
            location            TEXT NOT NULL,
            start_date          TEXT NOT NULL,
            end_date            TEXT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at          TEXT NOT NULL,
            created_by          INTEGER,
            closed_at           TEXT,
            home_group_id       INTEGER,
            alert_24h_sent      INTEGER NOT NULL DEFAULT 0,
            alert_12h_sent      INTEGER NOT NULL DEFAULT 0,
            alert_2h_sent       INTEGER NOT NULL DEFAULT 0,
            alert_exp_sent      INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS dot_docs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            unit_number   TEXT NOT NULL UNIQUE,
            unit_type     TEXT NOT NULL DEFAULT 'TRUCK',
            expiry_date   TEXT NOT NULL,
            photo_file_id TEXT,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS units (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            unit_number     TEXT NOT NULL UNIQUE,
            unit_type       TEXT NOT NULL DEFAULT 'TRUCK',
            year_model      TEXT,
            vin             TEXT,
            plate           TEXT,
            state           TEXT,
            trailer_type    TEXT,
            trailer_length  TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS admins (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            added_at    TEXT NOT NULL
        );
        """)
        for aid in ADMIN_IDS:
            c.execute(
                "INSERT OR IGNORE INTO admins(user_id, added_at) VALUES(?,?)",
                (aid, now_s())
            )


# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────
def now_et() -> datetime:
    return datetime.now(ET_TZ)


def now_s() -> str:
    return now_et().strftime("%Y-%m-%d %H:%M:%S")


def h(v) -> str:
    return html.escape(str(v or ""))


def is_admin(uid: int) -> bool:
    try:
        with db() as c:
            return c.execute("SELECT 1 FROM admins WHERE user_id=?", (uid,)).fetchone() is not None
    except Exception:
        return uid in ADMIN_IDS


def admin_ids() -> List[int]:
    try:
        with db() as c:
            return [r["user_id"] for r in c.execute("SELECT user_id FROM admins").fetchall()]
    except Exception:
        return list(ADMIN_IDS)


def notify_admins(text: str, kb=None) -> None:
    for aid in admin_ids():
        try:
            bot.send_message(aid, text, reply_markup=kb)
        except Exception:
            pass


def utype(unit_number: str) -> str:
    return "TRAILER" if unit_number.upper().startswith("T") else "TRUCK"


def ensure_group(chat) -> None:
    if chat.type not in ("group", "supergroup"):
        return
    title = chat.title or ""
    m = re.search(r"\bGL\d+\b", title)
    code = m.group(0) if m else None
    with db() as c:
        if c.execute("SELECT 1 FROM groups WHERE id=?", (chat.id,)).fetchone():
            c.execute(
                "UPDATE groups SET title=?, unit_code=COALESCE(NULLIF(unit_code,''),?) WHERE id=?",
                (title, code, chat.id)
            )
        else:
            c.execute(
                "INSERT INTO groups(id, title, unit_code) VALUES(?,?,?)",
                (chat.id, title, code)
            )


# ─────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────
def kb_main():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("📋 PTI", "🔧 PM")
    k.row("🅿️ Parking", "🔍 DOT")
    k.row("🚛 Units", "📊 Reports")
    k.row("📢 Broadcast", "⚙️ Settings")
    return k


def kb_cancel():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("❌ Cancel")
    return k


def kb_pti():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("❌ Missing today", "📅 Missing this week")
    k.row("🔔 Send reminder", "👤 Driver report")
    k.row("⬅️ Back")
    return k


def kb_pm():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("📋 View PM plan", "🔔 Send PM reminder")
    k.row("⬅️ Back")
    return k


def kb_parking():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("📋 Active parkings", "➕ Add parking")
    k.row("✅ Close parking", "🔄 Extend parking")
    k.row("⬅️ Back")
    return k


def kb_dot():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("⚠️ Expiring soon", "➕ Add DOT document")
    k.row("⬅️ Back")
    return k


def kb_units():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("🔍 Search unit", "➕ Add unit")
    k.row("📥 Bulk import")
    k.row("⬅️ Back")
    return k


def kb_reports():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("📊 This month", "📊 Choose month")
    k.row("⬅️ Back")
    return k


def kb_settings():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("👥 Groups", "👤 Admins")
    k.row("⬅️ Back")
    return k


def kb_groups():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("📋 Show all groups", "📥 Import groups")
    k.row("❌ Delete group")
    k.row("⬅️ Back")
    return k


def kb_admins():
    k = types.ReplyKeyboardMarkup(resize_keyboard=True)
    k.row("📋 List admins", "➕ Add admin")
    k.row("❌ Remove admin")
    k.row("⬅️ Back")
    return k


# ─────────────────────────────────────────
#  IMPORT HELPERS
# ─────────────────────────────────────────
def import_groups(text: str) -> str:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    added = updated = errors = 0
    for line in lines:
        try:
            if "|" in line:
                raw_id, title = line.split("|", 1)
            elif "," in line:
                raw_id, title = line.split(",", 1)
            else:
                raw_id, title = line, f"Group {line.strip()}"
            gid   = int(raw_id.strip())
            title = title.strip()
            m     = re.search(r"\bGL\d+\b", title)
            code  = m.group(0) if m else None
            with db() as c:
                if c.execute("SELECT 1 FROM groups WHERE id=?", (gid,)).fetchone():
                    c.execute("UPDATE groups SET title=?, unit_code=COALESCE(NULLIF(unit_code,''),?) WHERE id=?",
                              (title, code, gid))
                    updated += 1
                else:
                    c.execute("INSERT INTO groups(id,title,unit_code) VALUES(?,?,?)", (gid, title, code))
                    added += 1
        except Exception:
            errors += 1
    return (f"✅ <b>Groups imported</b>\n"
            f"➕ Added: <b>{added}</b>\n"
            f"🔄 Updated: <b>{updated}</b>\n"
            f"❌ Errors: <b>{errors}</b>")


def import_units(text: str) -> str:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    added = updated = errors = 0
    ts = now_s()
    for line in lines:
        try:
            p = [x.strip() for x in line.split(",")]
            if len(p) < 4:
                errors += 1
                continue
            num  = p[0].upper()
            ut   = utype(num)
            if ut == "TRUCK":
                if len(p) < 5:
                    errors += 1
                    continue
                year_model, vin, plate, state = p[1], p[2].upper(), p[3].upper(), p[4].upper()
                tt = tl = None
            else:
                if len(p) >= 6:
                    tt, tl = p[1], p[2]
                    vin, plate, state = p[3].upper(), p[4].upper(), p[5].upper()
                    year_model = ""
                else:
                    year_model, vin, plate = p[1], p[2].upper(), p[3].upper()
                    state = p[4].upper() if len(p) > 4 else ""
                    tt = tl = None
            with db() as c:
                if c.execute("SELECT 1 FROM units WHERE unit_number=?", (num,)).fetchone():
                    c.execute("""UPDATE units SET unit_type=?,year_model=?,vin=?,plate=?,state=?,
                                 trailer_type=?,trailer_length=?,updated_at=? WHERE unit_number=?""",
                              (ut, year_model, vin, plate, state, tt, tl, ts, num))
                    updated += 1
                else:
                    c.execute("""INSERT INTO units(unit_number,unit_type,year_model,vin,plate,state,
                                 trailer_type,trailer_length,created_at,updated_at)
                                 VALUES(?,?,?,?,?,?,?,?,?,?)""",
                              (num, ut, year_model, vin, plate, state, tt, tl, ts, ts))
                    added += 1
        except Exception:
            errors += 1
    return (f"✅ <b>Units imported</b>\n"
            f"➕ Added: <b>{added}</b>\n"
            f"🔄 Updated: <b>{updated}</b>\n"
            f"❌ Errors: <b>{errors}</b>")


# ─────────────────────────────────────────
#  PTI LOGIC
# ─────────────────────────────────────────
def pti_missing_today() -> str:
    today = now_et().strftime("%Y-%m-%d")
    with db() as c:
        groups = c.execute("SELECT id, title FROM groups ORDER BY title").fetchall()
        missing = [g["title"] for g in groups
                   if not c.execute("SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=?",
                                    (g["id"], today)).fetchone()]
    if not missing:
        return "✅ All groups sent PTI today!"
    return (f"🔴 <b>Missing PTI today — {len(missing)} groups:</b>\n\n" +
            "\n".join(f"• {h(t)}" for t in missing))


def pti_missing_week() -> str:
    today    = now_et().date()
    week_ago = (today - timedelta(days=6)).strftime("%Y-%m-%d")
    today_s  = today.strftime("%Y-%m-%d")
    with db() as c:
        groups = c.execute("SELECT id, title FROM groups ORDER BY title").fetchall()
        missing = [g["title"] for g in groups
                   if not c.execute(
                       "SELECT 1 FROM pti_reports WHERE group_id=? AND date_et BETWEEN ? AND ?",
                       (g["id"], week_ago, today_s)).fetchone()]
    if not missing:
        return "✅ All groups sent PTI this week!"
    return (f"🔴 <b>Missing PTI this week — {len(missing)} groups:</b>\n\n" +
            "\n".join(f"• {h(t)}" for t in missing))


def pti_remind() -> str:
    today = now_et().strftime("%Y-%m-%d")
    with db() as c:
        groups = c.execute("SELECT id FROM groups").fetchall()
        missing = [g["id"] for g in groups
                   if not c.execute("SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=?",
                                    (g["id"], today)).fetchone()]
    sent = 0
    for gid in missing:
        try:
            bot.send_message(gid, "🔔 <b>Reminder:</b> Please send today's PTI!\n\nUse: <code>/pti</code>")
            sent += 1
        except Exception:
            pass
    return f"✅ Reminder sent to <b>{sent}</b> groups."


def pti_driver(name: str) -> str:
    with db() as c:
        rows = c.execute(
            "SELECT ts_et FROM pti_reports WHERE driver_name LIKE ? ORDER BY ts_et DESC",
            (f"%{name}%",)).fetchall()
    if not rows:
        return f"❌ No PTI found for <b>{h(name)}</b>."
    months: Dict[str, int] = {}
    for r in rows:
        try:
            key = datetime.strptime(r["ts_et"], "%Y-%m-%d %H:%M:%S").strftime("%B %Y")
            months[key] = months.get(key, 0) + 1
        except Exception:
            pass
    lines = [f"📋 <b>PTI: {h(name)}</b> — {len(rows)} total\n"]
    for mon, cnt in months.items():
        lines.append(f"• {h(mon)}: <b>{cnt}</b>")
    return "\n".join(lines)


# ─────────────────────────────────────────
#  PM LOGIC
# ─────────────────────────────────────────
def pm_view(chat_id: int) -> None:
    with db() as c:
        rows = c.execute("""SELECT p.id, g.title FROM pm_plans p
                            JOIN groups g ON p.group_id=g.id
                            WHERE p.is_done=0 ORDER BY p.created_at""").fetchall()
    if not rows:
        bot.send_message(chat_id, "✅ PM plan is empty.", reply_markup=kb_pm())
        return
    bot.send_message(chat_id, f"🔧 <b>PM Plan — {len(rows)} units</b>", reply_markup=kb_pm())
    for r in rows:
        k = types.InlineKeyboardMarkup()
        k.row(
            types.InlineKeyboardButton("✅ Done", callback_data=f"PM_DONE:{r['id']}"),
            types.InlineKeyboardButton("❌ Remove", callback_data=f"PM_DEL:{r['id']}")
        )
        bot.send_message(chat_id, f"🔧 <b>{h(r['title'])}</b>", reply_markup=k)


def pm_remind() -> str:
    with db() as c:
        rows = c.execute("""SELECT g.id FROM pm_plans p
                            JOIN groups g ON p.group_id=g.id
                            WHERE p.is_done=0""").fetchall()
    if not rows:
        return "✅ No units in PM plan."
    sent = 0
    for r in rows:
        try:
            bot.send_message(r["id"],
                "🔔 <b>PM Reminder:</b> Your unit is scheduled for maintenance. Please contact fleet!")
            sent += 1
        except Exception:
            pass
    return f"✅ Reminder sent to <b>{sent}</b> units."


# ─────────────────────────────────────────
#  PARKING LOGIC
# ─────────────────────────────────────────
def parking_list() -> str:
    today = now_et().date()
    with db() as c:
        rows = c.execute("SELECT * FROM parking WHERE status='ACTIVE' ORDER BY end_date").fetchall()
    if not rows:
        return "✅ No active parkings."
    lines = [f"🅿️ <b>Active Parkings — {len(rows)}</b>\n"]
    for r in rows:
        end = datetime.strptime(r["end_date"], "%Y-%m-%d").date()
        d   = (end - today).days
        st  = ("⛔ EXPIRED" if d < 0 else "⚠️ TODAY" if d == 0
               else f"⚠️ {d}d left" if d <= 2 else f"✅ {d}d left")
        lines.append(f"• <b>{h(r['unit_number'])}</b> — {h(r['location'])}\n"
                     f"  📅 {r['end_date']}  {st}")
    return "\n".join(lines)


def parking_add(unit: str, location: str, start: str, end: str,
                uid: int, home_gid: Optional[int] = None) -> str:
    try:
        s = datetime.strptime(start, "%m/%d/%Y").strftime("%Y-%m-%d")
        e = datetime.strptime(end,   "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return "❌ Invalid date format. Use MM/DD/YYYY"
    with db() as c:
        c.execute("""INSERT INTO parking(unit_number,unit_type,location,start_date,end_date,
                     status,created_at,created_by,home_group_id)
                     VALUES(?,?,?,?,?,'ACTIVE',?,?,?)""",
                  (unit.upper(), utype(unit), location, s, e, now_s(), uid, home_gid))
    if home_gid:
        try:
            bot.send_message(home_gid,
                f"🅿️ <b>Parking arranged!</b>\n\n"
                f"🚛 Unit: <b>{h(unit.upper())}</b>\n"
                f"📍 Location: <b>{h(location)}</b>\n"
                f"📅 {start} → {end}\n\nSafe travels! 🚛")
        except Exception:
            pass
    return (f"✅ <b>Parking added!</b>\n\n"
            f"🚛 Unit: <b>{h(unit.upper())}</b>\n"
            f"📍 Location: <b>{h(location)}</b>\n"
            f"📅 {start} → {end}")


def parking_close(unit: str) -> str:
    with db() as c:
        row = c.execute(
            "SELECT id FROM parking WHERE unit_number=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
            (unit.upper(),)).fetchone()
        if not row:
            return f"❌ No active parking for <b>{h(unit.upper())}</b>."
        c.execute("UPDATE parking SET status='CLOSED', closed_at=? WHERE id=?",
                  (now_s(), row["id"]))
    return f"✅ Parking for <b>{h(unit.upper())}</b> closed — unit picked up!"


def parking_extend(unit: str, days: int) -> str:
    with db() as c:
        row = c.execute(
            "SELECT id, end_date FROM parking WHERE unit_number=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
            (unit.upper(),)).fetchone()
        if not row:
            return f"❌ No active parking for <b>{h(unit.upper())}</b>."
        new_end = (datetime.strptime(row["end_date"], "%Y-%m-%d") + timedelta(days=days))
        c.execute("""UPDATE parking SET end_date=?,
                     alert_24h_sent=0, alert_12h_sent=0,
                     alert_2h_sent=0,  alert_exp_sent=0
                     WHERE id=?""", (new_end.strftime("%Y-%m-%d"), row["id"]))
    return (f"✅ Extended <b>{h(unit.upper())}</b>\n"
            f"📅 New end: <b>{new_end.strftime('%m/%d/%Y')}</b> (+{days}d)")


# ─────────────────────────────────────────
#  DOT LOGIC
# ─────────────────────────────────────────
def dot_expiring() -> str:
    today    = now_et().date()
    deadline = (today + timedelta(days=30)).strftime("%Y-%m-%d")
    with db() as c:
        rows = c.execute(
            "SELECT * FROM dot_docs WHERE expiry_date <= ? ORDER BY expiry_date",
            (deadline,)).fetchall()
    if not rows:
        return "✅ No DOT docs expiring in 30 days."
    lines = [f"⚠️ <b>DOT Expiring — {len(rows)} units</b>\n"]
    for r in rows:
        exp  = datetime.strptime(r["expiry_date"], "%Y-%m-%d").date()
        d    = (exp - today).days
        st   = f"⛔ EXPIRED {abs(d)}d" if d < 0 else ("⚠️ TODAY" if d == 0 else f"{d}d left")
        lines.append(f"• <b>{h(r['unit_number'])}</b> — "
                     f"{exp.strftime('%m/%d/%Y')}  ({st})")
    return "\n".join(lines)


# ─────────────────────────────────────────
#  UNITS LOGIC
# ─────────────────────────────────────────
def unit_info(num: str) -> str:
    with db() as c:
        u = c.execute("SELECT * FROM units WHERE unit_number=?", (num.upper(),)).fetchone()
        d = c.execute(
            "SELECT expiry_date FROM dot_docs WHERE unit_number=? ORDER BY expiry_date DESC LIMIT 1",
            (num.upper(),)).fetchone()
    if not u:
        return f"❌ Unit <b>{h(num.upper())}</b> not found."
    dot = ""
    if d:
        try:
            exp   = datetime.strptime(d["expiry_date"], "%Y-%m-%d").date()
            days  = (exp - now_et().date()).days
            icon  = "⛔" if days < 0 else "⚠️" if days <= 30 else "✅"
            dot   = f"\n{icon} DOT: <b>{exp.strftime('%m/%d/%Y')}</b> ({days}d)"
        except Exception:
            pass
    if u["unit_type"] == "TRUCK":
        return (f"🚛 <b>{h(u['unit_number'])}</b>\n"
                f"📅 {h(u['year_model'])}\n"
                f"🔑 VIN: <code>{h(u['vin'])}</code>\n"
                f"🔢 Plate: <code>{h(u['plate'])}</code>  {h(u['state'])}{dot}")
    return (f"🚜 <b>{h(u['unit_number'])}</b>\n"
            f"📅 {h(u['year_model'])}\n"
            f"📏 {h(u['trailer_type'] or '')} {h(u['trailer_length'] or '')}\n"
            f"🔑 VIN: <code>{h(u['vin'])}</code>\n"
            f"🔢 Plate: <code>{h(u['plate'])}</code>  {h(u['state'])}{dot}")


# ─────────────────────────────────────────
#  REPORTS LOGIC
# ─────────────────────────────────────────
def report(month: int, year: int) -> str:
    s = f"{year}-{month:02d}-01"
    e = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"
    with db() as c:
        pti   = c.execute("SELECT COUNT(*) FROM pti_reports WHERE date_et>=? AND date_et<?",
                          (s[:10], e[:10])).fetchone()[0]
        pm_r  = c.execute("SELECT COUNT(*), SUM(COALESCE(amount,0)) FROM pm_plans WHERE is_done=1 AND done_at>=? AND done_at<?",
                          (s, e)).fetchone()
        pk    = c.execute("SELECT COUNT(*) FROM parking WHERE created_at>=? AND created_at<?",
                          (s, e)).fetchone()[0]
        pk_a  = c.execute("SELECT COUNT(*) FROM parking WHERE status='ACTIVE'").fetchone()[0]
    pm_cnt = pm_r[0] or 0
    pm_amt = pm_r[1] or 0.0
    label  = datetime(year, month, 1).strftime("%B %Y")
    return (f"📊 <b>Report — {h(label)}</b>\n\n"
            f"📋 PTI reports: <b>{pti}</b>\n"
            f"🔧 PM done: <b>{pm_cnt}</b>  💰 <b>${pm_amt:,.2f}</b>\n"
            f"🅿️ Parkings added: <b>{pk}</b>  active: <b>{pk_a}</b>")


# ─────────────────────────────────────────
#  GROUP COMMANDS  (inside truck groups)
# ─────────────────────────────────────────
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    if msg.chat.type in ("group", "supergroup"):
        ensure_group(msg.chat)
        return
    uid = msg.from_user.id
    if is_admin(uid):
        clear_state(uid)
        bot.send_message(msg.chat.id,
                         "👋 <b>GWE Fleet Bot</b> 🚛\nChoose an option:",
                         reply_markup=kb_main())
    else:
        bot.send_message(msg.chat.id, f"⛔ Admins only.\nContact: {CONTACT}")


@bot.message_handler(commands=["pti"])
def cmd_pti(msg):
    if msg.chat.type not in ("group", "supergroup"):
        return
    ensure_group(msg.chat)
    gid  = msg.chat.id
    text = (msg.text or "").strip()
    parts = text.split(maxsplit=1)
    try:
        with db() as c:
            if len(parts) > 1:
                name = parts[1].strip()
                c.execute("UPDATE groups SET driver_name=? WHERE id=?", (name, gid))
            else:
                row = c.execute("SELECT driver_name FROM groups WHERE id=?", (gid,)).fetchone()
                if not row or not row["driver_name"]:
                    bot.reply_to(msg, "👋 First time? Register with:\n<code>/pti Your Full Name</code>")
                    return
                name = row["driver_name"]
            # ensure driver exists
            if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='drivers'").fetchone():
                pass  # drivers table optional
            ts    = now_et()
            today = ts.strftime("%Y-%m-%d")
            c.execute(
                "INSERT OR IGNORE INTO pti_reports(group_id,driver_name,ts_et,date_et) VALUES(?,?,?,?)",
                (gid, name, ts.strftime("%Y-%m-%d %H:%M:%S"), today))
            new = c.execute("SELECT changes()").fetchone()[0] > 0
        bot.reply_to(msg, "✅ PTI sent!" if new else "✅ PTI already recorded today.")
        notify_admins(
            f"🚨 <b>New PTI Report</b>\n\n"
            f"👤 Driver: <b>{h(name)}</b>\n"
            f"🕒 Time: {ts.strftime('%Y-%m-%d %H:%M')} (ET)\n"
            f"🚛 Group: <b>{h(msg.chat.title)}</b>"
        )
    except Exception as e:
        print(f"[PTI ERROR] {e}")
        bot.reply_to(msg, "❌ Error. Please try again.")


@bot.message_handler(commands=["pm"])
def cmd_pm(msg):
    if msg.chat.type not in ("group", "supergroup"):
        return
    ensure_group(msg.chat)
    gid = msg.chat.id
    try:
        with db() as c:
            if c.execute("SELECT 1 FROM pm_plans WHERE group_id=? AND is_done=0", (gid,)).fetchone():
                bot.reply_to(msg, "✅ Already in PM plan.")
                return
            c.execute("INSERT INTO pm_plans(group_id,created_at,is_done) VALUES(?,?,0)", (gid, now_s()))
        bot.reply_to(msg, "✅ PM request sent!")
        notify_admins(
            f"🔧 <b>New PM Request</b>\n\n"
            f"🚛 <b>{h(msg.chat.title)}</b>\n"
            f"🕒 {now_et().strftime('%Y-%m-%d %H:%M')} (ET)"
        )
    except Exception as e:
        print(f"[PM ERROR] {e}")
        bot.reply_to(msg, "❌ Error.")


@bot.message_handler(commands=["home"])
def cmd_home(msg):
    if msg.chat.type not in ("group", "supergroup"):
        return
    ensure_group(msg.chat)
    gid    = msg.chat.id
    driver = msg.from_user
    name   = (driver.first_name or "") + (f" {driver.last_name}" if driver.last_name else "")
    ts     = now_et()
    with db() as c:
        row = c.execute("SELECT unit_code FROM groups WHERE id=?", (gid,)).fetchone()
    code = row["unit_code"] if row and row["unit_code"] else "?"
    k = types.InlineKeyboardMarkup()
    k.row(
        types.InlineKeyboardButton("🅿️ Arrange Parking", callback_data=f"HOME_PARK:{gid}:{code}"),
        types.InlineKeyboardButton("❌ Ignore",          callback_data=f"HOME_IGN:{gid}")
    )
    notify_admins(
        f"🏠 <b>Home Time Request</b>\n\n"
        f"👤 Driver: <b>{h(name.strip())}</b>\n"
        f"🚛 Unit: <b>{h(code)}</b>\n"
        f"📍 Group: <b>{h(msg.chat.title)}</b>\n"
        f"🕒 {ts.strftime('%Y-%m-%d %H:%M')} (ET)",
        kb=k
    )
    bot.reply_to(msg, "🏠 Home time request sent to fleet!")


@bot.message_handler(commands=["unit"])
def cmd_unit(msg):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(msg, "Usage: <code>/unit GL1234</code>")
        return
    bot.reply_to(msg, unit_info(parts[1].strip()))


# ─────────────────────────────────────────
#  PHOTO HANDLER  (DOT documents)
# ─────────────────────────────────────────
@bot.message_handler(content_types=["photo"],
                     func=lambda m: m.chat.type == "private" and is_admin(m.from_user.id))
def on_photo(msg):
    uid = msg.from_user.id
    st  = get_state(uid)
    if st["state"] != "DOT_PHOTO":
        bot.send_message(msg.chat.id,
                         "📸 Photo received.\nTo save a DOT doc: 🔍 DOT → ➕ Add DOT document")
        return
    d         = st["data"]
    file_id   = msg.photo[-1].file_id
    unit_num  = d["unit_number"]
    expiry    = d["expiry_date"]
    ut        = utype(unit_num)
    ts        = now_s()
    with db() as c:
        if c.execute("SELECT 1 FROM dot_docs WHERE unit_number=?", (unit_num,)).fetchone():
            c.execute("UPDATE dot_docs SET expiry_date=?,photo_file_id=?,updated_at=? WHERE unit_number=?",
                      (expiry, file_id, ts, unit_num))
        else:
            c.execute("INSERT INTO dot_docs(unit_number,unit_type,expiry_date,photo_file_id,created_at,updated_at)"
                      " VALUES(?,?,?,?,?,?)", (unit_num, ut, expiry, file_id, ts, ts))
    clear_state(uid)
    exp_disp = datetime.strptime(expiry, "%Y-%m-%d").strftime("%m/%d/%Y")
    bot.send_message(msg.chat.id,
                     f"✅ <b>DOT Document Saved!</b>\n\n"
                     f"🚛 Unit: <b>{h(unit_num)}</b>\n"
                     f"📅 Expires: <b>{exp_disp}</b>",
                     reply_markup=kb_dot())
    set_state(uid, "DOT")


# ─────────────────────────────────────────
#  ADMIN TEXT HANDLER
# ─────────────────────────────────────────
@bot.message_handler(content_types=["text"],
                     func=lambda m: m.chat.type == "private" and is_admin(m.from_user.id))
def on_admin_text(msg):
    uid  = msg.from_user.id
    text = msg.text.strip()
    st   = get_state(uid)
    s    = st["state"]
    d    = st["data"]

    # ── universal back / cancel ──────────────────────────
    if text in ("⬅️ Back", "❌ Cancel"):
        clear_state(uid)
        bot.send_message(msg.chat.id, "🏠 Main menu:", reply_markup=kb_main())
        return

    # ── main menu ────────────────────────────────────────
    if s is None:
        menu_map = {
            "📋 PTI":       ("PTI",      kb_pti(),      "📋 <b>PTI</b>"),
            "🔧 PM":        ("PM",       kb_pm(),       "🔧 <b>PM Maintenance</b>"),
            "🅿️ Parking":  ("PARKING",  kb_parking(),  "🅿️ <b>Parking</b>"),
            "🔍 DOT":       ("DOT",      kb_dot(),      "🔍 <b>DOT Inspection</b>"),
            "🚛 Units":     ("UNITS",    kb_units(),    "🚛 <b>Units</b>"),
            "📊 Reports":   ("REPORTS",  kb_reports(),  "📊 <b>Reports</b>"),
            "⚙️ Settings":  ("SETTINGS", kb_settings(), "⚙️ <b>Settings</b>"),
        }
        if text in menu_map:
            new_s, kb_fn, label = menu_map[text]
            set_state(uid, new_s)
            bot.send_message(msg.chat.id, label, reply_markup=kb_fn)
        elif text == "📢 Broadcast":
            set_state(uid, "BROADCAST")
            bot.send_message(msg.chat.id,
                             "📢 <b>Broadcast</b>\n\nType message to send to ALL groups:",
                             reply_markup=kb_cancel())
        return

    # ════════════════════════════════════════════════════
    #  PTI
    # ════════════════════════════════════════════════════
    if s == "PTI":
        if text == "❌ Missing today":
            bot.send_message(msg.chat.id, pti_missing_today(), reply_markup=kb_pti())
        elif text == "📅 Missing this week":
            bot.send_message(msg.chat.id, pti_missing_week(), reply_markup=kb_pti())
        elif text == "🔔 Send reminder":
            bot.send_message(msg.chat.id, pti_remind(), reply_markup=kb_pti())
        elif text == "👤 Driver report":
            set_state(uid, "PTI_DRIVER")
            bot.send_message(msg.chat.id, "👤 Enter driver name:", reply_markup=kb_cancel())
        return

    if s == "PTI_DRIVER":
        bot.send_message(msg.chat.id, pti_driver(text), reply_markup=kb_pti())
        set_state(uid, "PTI")
        return

    # ════════════════════════════════════════════════════
    #  PM
    # ════════════════════════════════════════════════════
    if s == "PM":
        if text == "📋 View PM plan":
            pm_view(msg.chat.id)
        elif text == "🔔 Send PM reminder":
            bot.send_message(msg.chat.id, pm_remind(), reply_markup=kb_pm())
        return

    if s == "PM_AMOUNT":
        try:
            amount   = float(text.replace("$", "").replace(",", "").strip())
            plan_id  = d["plan_id"]
            with db() as c2:
                row = c2.execute("SELECT g.title FROM pm_plans p JOIN groups g ON p.group_id=g.id WHERE p.id=?",
                                 (plan_id,)).fetchone()
                c2.execute("UPDATE pm_plans SET is_done=1, done_at=?, amount=? WHERE id=?",
                           (now_s(), amount, plan_id))
            title = row["title"] if row else "Unknown"
            clear_state(uid)
            set_state(uid, "PM")
            bot.send_message(msg.chat.id,
                             f"✅ <b>PM Done!</b>\n\n"
                             f"🚛 {h(title)}\n"
                             f"💰 Amount: <b>${amount:,.2f}</b>",
                             reply_markup=kb_pm())
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Enter a valid amount (e.g. <code>546.13</code>):")
        return

    # ════════════════════════════════════════════════════
    #  PARKING
    # ════════════════════════════════════════════════════
    if s == "PARKING":
        if text == "📋 Active parkings":
            bot.send_message(msg.chat.id, parking_list(), reply_markup=kb_parking())
        elif text == "➕ Add parking":
            set_state(uid, "PARK_UNIT")
            bot.send_message(msg.chat.id,
                             "🚛 Enter unit number:\n<i>e.g. GL1234 or T5678</i>",
                             reply_markup=kb_cancel())
        elif text == "✅ Close parking":
            set_state(uid, "PARK_CLOSE")
            bot.send_message(msg.chat.id, "🚛 Enter unit number to close:", reply_markup=kb_cancel())
        elif text == "🔄 Extend parking":
            set_state(uid, "PARK_EXT_UNIT")
            bot.send_message(msg.chat.id, "🚛 Enter unit number to extend:", reply_markup=kb_cancel())
        return

    if s == "PARK_UNIT":
        set_state(uid, "PARK_LOC", {**d, "unit": text.upper()})
        bot.send_message(msg.chat.id, f"Unit: <b>{h(text.upper())}</b>\n\n📍 Enter parking location:")
        return

    if s == "PARK_LOC":
        set_state(uid, "PARK_START", {**d, "location": text})
        bot.send_message(msg.chat.id,
                         f"Location: <b>{h(text)}</b>\n\n📅 Start date <i>(MM/DD/YYYY)</i>:")
        return

    if s == "PARK_START":
        try:
            datetime.strptime(text, "%m/%d/%Y")
            set_state(uid, "PARK_END", {**d, "start": text})
            bot.send_message(msg.chat.id, f"Start: <b>{text}</b>\n\n📅 End date <i>(MM/DD/YYYY)</i>:")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Wrong format. Use MM/DD/YYYY:")
        return

    if s == "PARK_END":
        try:
            datetime.strptime(text, "%m/%d/%Y")
            result = parking_add(d["unit"], d["location"], d["start"], text,
                                 uid, d.get("home_gid"))
            clear_state(uid)
            set_state(uid, "PARKING")
            bot.send_message(msg.chat.id, result, reply_markup=kb_parking())
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Wrong format. Use MM/DD/YYYY:")
        return

    if s == "PARK_CLOSE":
        result = parking_close(text)
        clear_state(uid)
        set_state(uid, "PARKING")
        bot.send_message(msg.chat.id, result, reply_markup=kb_parking())
        return

    if s == "PARK_EXT_UNIT":
        set_state(uid, "PARK_EXT_DAYS", {"unit": text.upper()})
        bot.send_message(msg.chat.id,
                         f"<b>{h(text.upper())}</b>\n🔄 How many days to extend?")
        return

    if s == "PARK_EXT_DAYS":
        try:
            days = int(text.strip())
            if days <= 0:
                raise ValueError
            result = parking_extend(d["unit"], days)
            clear_state(uid)
            set_state(uid, "PARKING")
            bot.send_message(msg.chat.id, result, reply_markup=kb_parking())
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Enter a positive number:")
        return

    # ════════════════════════════════════════════════════
    #  DOT
    # ════════════════════════════════════════════════════
    if s == "DOT":
        if text == "⚠️ Expiring soon":
            bot.send_message(msg.chat.id, dot_expiring(), reply_markup=kb_dot())
        elif text == "➕ Add DOT document":
            set_state(uid, "DOT_UNIT")
            bot.send_message(msg.chat.id,
                             "🚛 Enter unit number:\n<i>e.g. GL1234</i>",
                             reply_markup=kb_cancel())
        return

    if s == "DOT_UNIT":
        set_state(uid, "DOT_DATE", {"unit_number": text.upper()})
        bot.send_message(msg.chat.id,
                         f"Unit: <b>{h(text.upper())}</b>\n\n📅 Expiry date <i>(MM/DD/YYYY)</i>:")
        return

    if s == "DOT_DATE":
        try:
            exp = datetime.strptime(text, "%m/%d/%Y").strftime("%Y-%m-%d")
            set_state(uid, "DOT_PHOTO", {**d, "expiry_date": exp})
            bot.send_message(msg.chat.id,
                             f"Expiry: <b>{text}</b>\n\n📸 Now send the photo of the DOT document:")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Wrong format. Use MM/DD/YYYY:")
        return

    # ════════════════════════════════════════════════════
    #  UNITS
    # ════════════════════════════════════════════════════
    if s == "UNITS":
        if text == "🔍 Search unit":
            set_state(uid, "UNIT_SEARCH")
            bot.send_message(msg.chat.id, "🔍 Enter unit number:", reply_markup=kb_cancel())
        elif text == "➕ Add unit":
            set_state(uid, "UNIT_ADD")
            bot.send_message(msg.chat.id,
                             "➕ Send unit info:\n\n"
                             "<b>Truck:</b>\n<code>GL1234, 2020 Freightliner, VIN..., PLATE, STATE</code>\n\n"
                             "<b>Trailer:</b>\n<code>T1234, Dry Van, 53ft, VIN..., PLATE, STATE</code>",
                             reply_markup=kb_cancel())
        elif text == "📥 Bulk import":
            set_state(uid, "UNIT_BULK")
            bot.send_message(msg.chat.id,
                             "📥 <b>Bulk Import</b>\n\nOne unit per line (max 50 at once):\n\n"
                             "<code>GL1234, Year Make, VIN, PLATE, STATE</code>\n"
                             "<code>T1234, Dry Van, 53ft, VIN, PLATE, STATE</code>",
                             reply_markup=kb_cancel())
        return

    if s == "UNIT_SEARCH":
        bot.send_message(msg.chat.id, unit_info(text), reply_markup=kb_units())
        set_state(uid, "UNITS")
        return

    if s in ("UNIT_ADD", "UNIT_BULK"):
        bot.send_chat_action(msg.chat.id, "typing")
        bot.send_message(msg.chat.id, import_units(text), reply_markup=kb_units())
        set_state(uid, "UNITS")
        return

    # ════════════════════════════════════════════════════
    #  REPORTS
    # ════════════════════════════════════════════════════
    if s == "REPORTS":
        now = now_et()
        if text == "📊 This month":
            bot.send_message(msg.chat.id, report(now.month, now.year), reply_markup=kb_reports())
        elif text == "📊 Choose month":
            set_state(uid, "REPORT_MONTH")
            bot.send_message(msg.chat.id,
                             "📅 Enter month and year:\n<i>MM/YYYY — e.g. 03/2025</i>",
                             reply_markup=kb_cancel())
        return

    if s == "REPORT_MONTH":
        try:
            dt = datetime.strptime(text, "%m/%Y")
            bot.send_message(msg.chat.id, report(dt.month, dt.year), reply_markup=kb_reports())
            set_state(uid, "REPORTS")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Wrong format. Use MM/YYYY:")
        return

    # ════════════════════════════════════════════════════
    #  BROADCAST
    # ════════════════════════════════════════════════════
    if s == "BROADCAST":
        with db() as c2:
            gids = [r["id"] for r in c2.execute("SELECT id FROM groups").fetchall()]
        sent = 0
        for gid in gids:
            try:
                bot.send_message(gid, text)
                sent += 1
            except Exception:
                pass
        clear_state(uid)
        bot.send_message(msg.chat.id, f"✅ Sent to <b>{sent}</b> groups.", reply_markup=kb_main())
        return

    # ════════════════════════════════════════════════════
    #  SETTINGS → GROUPS
    # ════════════════════════════════════════════════════
    if s == "SETTINGS":
        if text == "👥 Groups":
            set_state(uid, "GROUPS")
            bot.send_message(msg.chat.id, "👥 <b>Groups</b>", reply_markup=kb_groups())
        elif text == "👤 Admins":
            set_state(uid, "ADMINS")
            bot.send_message(msg.chat.id, "👤 <b>Admins</b>", reply_markup=kb_admins())
        return

    if s == "GROUPS":
        if text == "📋 Show all groups":
            with db() as c2:
                rows = c2.execute("SELECT id, title FROM groups ORDER BY title").fetchall()
            if not rows:
                bot.send_message(msg.chat.id, "No groups yet.", reply_markup=kb_groups())
                return
            chunk, out = [], []
            for r in rows:
                line = f"• {h(r['title'])}\n  <code>{r['id']}</code>"
                if sum(len(x) for x in chunk) + len(line) > 3500:
                    out.append("\n".join(chunk))
                    chunk = [line]
                else:
                    chunk.append(line)
            if chunk:
                out.append("\n".join(chunk))
            header = f"👥 <b>Groups — {len(rows)}</b>\n\n"
            for i, part in enumerate(out):
                kb_ = kb_groups() if i == len(out) - 1 else None
                bot.send_message(msg.chat.id,
                                 (header if i == 0 else "") + part,
                                 reply_markup=kb_)
        elif text == "📥 Import groups":
            set_state(uid, "GROUPS_IMPORT")
            bot.send_message(msg.chat.id,
                             "📥 <b>Import Groups</b>\n\nOne per line:\n"
                             "<code>group_id|Title</code>\n\n"
                             "Example:\n"
                             "<code>-1001234567890|GL1234 John Smith\n"
                             "-1009876543210|GL5678 Alex Johnson</code>",
                             reply_markup=kb_cancel())
        elif text == "❌ Delete group":
            set_state(uid, "GROUPS_DEL_SEARCH")
            bot.send_message(msg.chat.id, "🔍 Enter group name to search:", reply_markup=kb_cancel())
        return

    if s == "GROUPS_IMPORT":
        bot.send_chat_action(msg.chat.id, "typing")
        bot.send_message(msg.chat.id, import_groups(text), reply_markup=kb_groups())
        set_state(uid, "GROUPS")
        return

    if s == "GROUPS_DEL_SEARCH":
        with db() as c2:
            rows = c2.execute("SELECT id, title FROM groups WHERE title LIKE ? ORDER BY title LIMIT 10",
                              (f"%{text}%",)).fetchall()
        if not rows:
            bot.send_message(msg.chat.id, "❌ No groups found.", reply_markup=kb_groups())
        else:
            k = types.InlineKeyboardMarkup()
            for r in rows:
                k.add(types.InlineKeyboardButton(h(r["title"]),
                                                 callback_data=f"GRP_DEL:{r['id']}"))
            bot.send_message(msg.chat.id, "Select group to delete:", reply_markup=k)
        set_state(uid, "GROUPS")
        return

    # ════════════════════════════════════════════════════
    #  SETTINGS → ADMINS
    # ════════════════════════════════════════════════════
    if s == "ADMINS":
        if text == "📋 List admins":
            with db() as c2:
                rows = c2.execute("SELECT user_id, username FROM admins").fetchall()
            lines = [f"👤 <b>Admins — {len(rows)}</b>\n"]
            for r in rows:
                u = f"@{r['username']}" if r["username"] else "no username"
                lines.append(f"• <code>{r['user_id']}</code> — {h(u)}")
            bot.send_message(msg.chat.id, "\n".join(lines), reply_markup=kb_admins())
        elif text == "➕ Add admin":
            set_state(uid, "ADMIN_ADD")
            bot.send_message(msg.chat.id, "Enter user ID to add:", reply_markup=kb_cancel())
        elif text == "❌ Remove admin":
            set_state(uid, "ADMIN_DEL")
            bot.send_message(msg.chat.id, "Enter user ID to remove:", reply_markup=kb_cancel())
        return

    if s == "ADMIN_ADD":
        try:
            new_id = int(text.strip())
            with db() as c2:
                c2.execute("INSERT OR IGNORE INTO admins(user_id, added_at) VALUES(?,?)",
                           (new_id, now_s()))
            bot.send_message(msg.chat.id, f"✅ <code>{new_id}</code> added as admin.",
                             reply_markup=kb_admins())
            set_state(uid, "ADMINS")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Invalid ID. Enter a number:")
        return

    if s == "ADMIN_DEL":
        try:
            rem_id = int(text.strip())
            if rem_id == uid:
                bot.send_message(msg.chat.id, "❌ You cannot remove yourself.")
                return
            with db() as c2:
                c2.execute("DELETE FROM admins WHERE user_id=?", (rem_id,))
            bot.send_message(msg.chat.id, f"✅ <code>{rem_id}</code> removed.",
                             reply_markup=kb_admins())
            set_state(uid, "ADMINS")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Invalid ID. Enter a number:")
        return

    # ── fallback ──────────────────────────────────────────
    clear_state(uid)
    bot.send_message(msg.chat.id, "🏠 Main menu:", reply_markup=kb_main())


@bot.message_handler(func=lambda m: m.chat.type == "private" and not is_admin(m.from_user.id))
def on_non_admin(msg):
    bot.send_message(msg.chat.id, f"⛔ Admins only.\nContact: {CONTACT}")


# ─────────────────────────────────────────
#  CALLBACK QUERIES
# ─────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("PM_DONE:"))
def cb_pm_done(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Admins only.")
        return
    plan_id = int(call.data.split(":")[1])
    set_state(call.from_user.id, "PM_AMOUNT", {"plan_id": plan_id})
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
                     "💰 Enter PM service amount:\n<i>e.g. 546.13</i>",
                     reply_markup=kb_cancel())


@bot.callback_query_handler(func=lambda c: c.data.startswith("PM_DEL:"))
def cb_pm_del(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Admins only.")
        return
    plan_id = int(call.data.split(":")[1])
    with db() as c2:
        row = c2.execute("SELECT g.title FROM pm_plans p JOIN groups g ON p.group_id=g.id WHERE p.id=?",
                         (plan_id,)).fetchone()
        c2.execute("DELETE FROM pm_plans WHERE id=?", (plan_id,))
    title = row["title"] if row else "Unknown"
    bot.answer_callback_query(call.id, "Removed ✅")
    try:
        bot.edit_message_text(f"❌ <b>{h(title)}</b> removed from PM plan.",
                              call.message.chat.id, call.message.message_id)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("GRP_DEL:"))
def cb_grp_del(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Admins only.")
        return
    gid = int(call.data.split(":")[1])
    with db() as c2:
        row = c2.execute("SELECT title FROM groups WHERE id=?", (gid,)).fetchone()
        c2.execute("DELETE FROM pti_reports WHERE group_id=?", (gid,))
        c2.execute("DELETE FROM pm_plans    WHERE group_id=?", (gid,))
        c2.execute("DELETE FROM groups      WHERE id=?",       (gid,))
    title = row["title"] if row else str(gid)
    bot.answer_callback_query(call.id, "Deleted ✅")
    bot.send_message(call.message.chat.id, f"✅ <b>{h(title)}</b> deleted.",
                     reply_markup=kb_groups())


@bot.callback_query_handler(func=lambda c: c.data.startswith("HOME_PARK:"))
def cb_home_park(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Admins only.")
        return
    _, gid_s, code = call.data.split(":", 2)
    set_state(call.from_user.id, "PARK_UNIT", {"home_gid": int(gid_s)})
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    bot.send_message(call.message.chat.id,
                     f"🅿️ <b>Arranging parking for home time</b>\n\n"
                     f"🚛 Enter unit number (suggested: <code>{h(code)}</code>):",
                     reply_markup=kb_cancel())


@bot.callback_query_handler(func=lambda c: c.data.startswith("HOME_IGN:"))
def cb_home_ign(call):
    bot.answer_callback_query(call.id, "Ignored.")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("PKE:"))
def cb_park_ext(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Admins only.")
        return
    pid = int(call.data.split(":")[1])
    with db() as c2:
        row = c2.execute("SELECT unit_number FROM parking WHERE id=?", (pid,)).fetchone()
    if not row:
        bot.answer_callback_query(call.id, "Not found.")
        return
    set_state(call.from_user.id, "PARK_EXT_DAYS", {"unit": row["unit_number"]})
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
                     f"🔄 How many days to extend <b>{h(row['unit_number'])}</b>?",
                     reply_markup=kb_cancel())


@bot.callback_query_handler(func=lambda c: c.data.startswith("PKC:"))
def cb_park_close(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Admins only.")
        return
    pid = int(call.data.split(":")[1])
    with db() as c2:
        row = c2.execute("SELECT unit_number FROM parking WHERE id=?", (pid,)).fetchone()
        c2.execute("UPDATE parking SET status='CLOSED', closed_at=? WHERE id=?", (now_s(), pid))
    unit = row["unit_number"] if row else str(pid)
    bot.answer_callback_query(call.id, "Closed ✅")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    bot.send_message(call.message.chat.id, f"✅ Parking for <b>{h(unit)}</b> closed!")


# ─────────────────────────────────────────
#  SCHEDULED JOBS
# ─────────────────────────────────────────
def job_parking_alerts():
    try:
        now_dt = now_et()
        with db() as c2:
            rows = c2.execute("SELECT * FROM parking WHERE status='ACTIVE'").fetchall()
        for r in rows:
            try:
                end_dt   = ET_TZ.localize(
                    datetime.strptime(r["end_date"], "%Y-%m-%d").replace(hour=23, minute=59))
                diff_h   = (end_dt - now_dt).total_seconds() / 3600
                pid      = r["id"]
                unit     = h(r["unit_number"])
                loc      = h(r["location"])
                end_disp = datetime.strptime(r["end_date"], "%Y-%m-%d").strftime("%m/%d/%Y")

                def _alert(label, field, _pid=pid, _unit=unit, _loc=loc, _end=end_disp):
                    txt = (f"⚠️ <b>PARKING — {label}</b>\n\n"
                           f"🚛 Unit: <b>{_unit}</b>\n"
                           f"📍 Location: <b>{_loc}</b>\n"
                           f"📅 Expires: <b>{_end}</b>")
                    k = types.InlineKeyboardMarkup()
                    k.row(
                        types.InlineKeyboardButton("🔄 Extend",    callback_data=f"PKE:{_pid}"),
                        types.InlineKeyboardButton("✅ Picked Up", callback_data=f"PKC:{_pid}")
                    )
                    for aid in admin_ids():
                        try:
                            bot.send_message(aid, txt, reply_markup=k)
                        except Exception:
                            pass
                    with db() as cx:
                        cx.execute(f"UPDATE parking SET {field}=1 WHERE id=?", (_pid,))

                if 0 < diff_h <= 24 and not r["alert_24h_sent"]:
                    _alert("24 HOURS LEFT ⏰",   "alert_24h_sent")
                if 0 < diff_h <= 12 and not r["alert_12h_sent"]:
                    _alert("12 HOURS LEFT ⚠️",  "alert_12h_sent")
                if 0 < diff_h <= 2  and not r["alert_2h_sent"]:
                    _alert("2 HOURS LEFT 🚨",    "alert_2h_sent")
                if diff_h <= 0      and not r["alert_exp_sent"]:
                    _alert("EXPIRED ⛔",          "alert_exp_sent")
            except Exception as e:
                print(f"[PARK ALERT] {e}")
    except Exception as e:
        print(f"[JOB PARK] {e}")


def job_dot_alerts():
    try:
        today    = now_et().date()
        deadline = (today + timedelta(days=30)).strftime("%Y-%m-%d")
        with db() as c2:
            rows = c2.execute(
                "SELECT * FROM dot_docs WHERE expiry_date <= ? ORDER BY expiry_date",
                (deadline,)).fetchall()
        if not rows:
            return
        lines = [f"⚠️ <b>DOT EXPIRY ALERT — {len(rows)} units</b>\n"]
        for r in rows:
            exp   = datetime.strptime(r["expiry_date"], "%Y-%m-%d").date()
            days  = (exp - today).days
            st    = (f"⛔ EXPIRED {abs(days)}d" if days < 0
                     else "⚠️ TODAY" if days == 0 else f"{days}d left")
            lines.append(f"• <b>{h(r['unit_number'])}</b> — {exp.strftime('%m/%d/%Y')} ({st})")
        notify_admins("\n".join(lines))
    except Exception as e:
        print(f"[JOB DOT] {e}")


def job_parking_summary():
    try:
        today = now_et().date()
        with db() as c2:
            rows = c2.execute(
                "SELECT * FROM parking WHERE status='ACTIVE' ORDER BY end_date").fetchall()
        if not rows:
            return
        lines = [f"🅿️ <b>DAILY PARKING — {today.strftime('%m/%d/%Y')}</b>\n"
                 f"Active: <b>{len(rows)}</b>\n"]
        for r in rows:
            end   = datetime.strptime(r["end_date"], "%Y-%m-%d").date()
            d     = (end - today).days
            icon  = "⛔" if d < 0 else "⚠️" if d <= 1 else "✅"
            lines.append(f"{icon} <b>{h(r['unit_number'])}</b> — {h(r['location'])} — {end.strftime('%m/%d/%Y')}")
        notify_admins("\n".join(lines))
    except Exception as e:
        print(f"[JOB SUMMARY] {e}")


def job_pti_reminder():
    try:
        today = now_et().strftime("%Y-%m-%d")
        with db() as c2:
            groups = c2.execute("SELECT id FROM groups").fetchall()
            missing = [g["id"] for g in groups
                       if not c2.execute(
                           "SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=?",
                           (g["id"], today)).fetchone()]
        for gid in missing:
            try:
                bot.send_message(
                    gid,
                    "🔔 <b>Daily PTI Reminder</b>\n\nPlease send today's PTI!\nUse: <code>/pti</code>")
            except Exception:
                pass
    except Exception as e:
        print(f"[JOB PTI] {e}")


# ─────────────────────────────────────────
#  HEALTH SERVER
# ─────────────────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"GWE Fleet Bot OK")

    def log_message(self, *_):
        pass


def start_health():
    port = int(os.environ.get("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever()


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("🚛 GWE Fleet Bot starting…")
    init_db()
    print(f"✅ Database ready: {DB_PATH}")

    threading.Thread(target=start_health, daemon=True).start()
    print("✅ Health server started")

    scheduler = BackgroundScheduler(timezone=ET_TZ)
    scheduler.add_job(job_parking_alerts,  "interval", minutes=30)
    scheduler.add_job(job_dot_alerts,      "cron",  hour=9,  minute=0)
    scheduler.add_job(job_dot_alerts,      "cron",  hour=16, minute=0)
    scheduler.add_job(job_parking_summary, "cron",  hour=8,  minute=0)
    scheduler.add_job(job_pti_reminder,    "cron",  hour=15, minute=0)
    scheduler.start()
    print("✅ Scheduler started")

    while True:
        try:
            print("🔄 Starting polling…")
            bot.delete_webhook(drop_pending_updates=True)
            bot.infinity_polling(timeout=60, long_polling_timeout=60,
                                 allowed_updates=["message", "callback_query"])
        except Exception as e:
            print(f"[POLLING ERROR] {e}")
            time.sleep(15)
