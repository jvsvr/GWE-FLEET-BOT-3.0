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

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set.")

_admin_ids_env = os.getenv("ADMIN_IDS", "6939239782")
ADMIN_IDS: Set[int] = set(int(x.strip()) for x in _admin_ids_env.split(",") if x.strip().isdigit())

FLEET_CONTACT = "@Chester_FLEET"
DB_PATH = os.getenv("DB_PATH", "gwe_fleet_bot.db")
ET_TZ = pytz.timezone("US/Eastern")
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

_lock = threading.Lock()
user_states: Dict[int, Dict[str, Any]] = {}

def set_state(uid, state, data=None):
    with _lock:
        user_states[uid] = {"state": state, "data": data or {}}

def get_state(uid):
    with _lock:
        return dict(user_states.get(uid, {"state": None, "data": {}}))

def clear_state(uid):
    with _lock:
        user_states.pop(uid, None)

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[DB_ERROR] {repr(e)}")
        raise
    finally:
        conn.close()

def now_str():
    return get_et_now().strftime("%Y-%m-%d %H:%M:%S")

def get_et_now():
    return datetime.now(ET_TZ)

def h(s):
    return html.escape(str(s or ""))

def is_admin(uid):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM admins WHERE user_id=?", (uid,))
            return cur.fetchone() is not None
    except:
        return uid in ADMIN_IDS

def get_all_admin_ids():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM admins")
            return [r["user_id"] for r in cur.fetchall()]
    except:
        return list(ADMIN_IDS)

def ensure_group(chat):
    if chat.type not in ("group", "supergroup"):
        return None
    title = chat.title or ""
    m = re.search(r"\bGL\d+\b", title)
    unit_code = m.group(0) if m else None
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM groups WHERE id=?", (chat.id,))
            if cur.fetchone():
                cur.execute("UPDATE groups SET title=?, unit_code=COALESCE(NULLIF(unit_code,''),?) WHERE id=?",
                            (title, unit_code, chat.id))
            else:
                cur.execute("INSERT INTO groups (id,title,unit_code,driver_name) VALUES (?,?,?,?)",
                            (chat.id, title, unit_code, None))
        return chat.id
    except Exception as e:
        print(f"[ERROR] ensure_group: {repr(e)}")
        return None

def get_or_create_driver(name):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM drivers WHERE name=?", (name,))
            row = cur.fetchone()
            if row: return int(row["id"])
            cur.execute("INSERT INTO drivers (name) VALUES (?)", (name,))
            return int(cur.lastrowid)
    except:
        return -1

def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY, title TEXT, unit_code TEXT, driver_name TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS pti_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER,
            driver_name TEXT, timestamp_et TEXT, date_et TEXT,
            FOREIGN KEY(group_id) REFERENCES groups(id))""")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_pti ON pti_reports(group_id, date_et)")
        cur.execute("""CREATE TABLE IF NOT EXISTS pm_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER,
            created_at TEXT, is_done INTEGER DEFAULT 0, done_at TEXT, amount REAL,
            FOREIGN KEY(group_id) REFERENCES groups(id))""")
        cur.execute("CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
        cur.execute("""CREATE TABLE IF NOT EXISTS parking (
            id INTEGER PRIMARY KEY AUTOINCREMENT, unit_number TEXT, unit_type TEXT,
            location TEXT, start_date TEXT, end_date TEXT, status TEXT DEFAULT 'ACTIVE',
            created_at TEXT, created_by INTEGER, closed_at TEXT, home_group_id INTEGER DEFAULT NULL,
            alert_24h_sent INTEGER DEFAULT 0, alert_12h_sent INTEGER DEFAULT 0,
            alert_2h_sent INTEGER DEFAULT 0, alert_expired_sent INTEGER DEFAULT 0)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS dot_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT, unit_number TEXT UNIQUE, unit_type TEXT,
            expiry_date TEXT, photo_file_id TEXT, created_at TEXT, updated_at TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS units (
            id INTEGER PRIMARY KEY AUTOINCREMENT, unit_number TEXT UNIQUE, unit_type TEXT,
            year_model TEXT, vin TEXT, plate TEXT, state TEXT, trailer_type TEXT,
            trailer_length TEXT, created_at TEXT, updated_at TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY, username TEXT, added_at TEXT)""")
        for aid in ADMIN_IDS:
            cur.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?,?)",
                        (aid, now_str()))

# KEYBOARDS
def kb_main():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 PTI", "🔧 PM")
    kb.row("🅿️ Parking", "🔍 DOT")
    kb.row("🚛 Units", "📊 Reports")
    kb.row("📢 Broadcast", "⚙️ Settings")
    return kb

def kb_back():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("⬅️ Main Menu")
    return kb

def kb_cancel():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("❌ Cancel")
    return kb

def kb_pti():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("❌ Missing today", "📅 Missing this week")
    kb.row("🔔 Send reminder", "👤 Driver report")
    kb.row("⬅️ Main Menu")
    return kb

def kb_pm():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 View PM plan", "🔔 Send PM reminder")
    kb.row("⬅️ Main Menu")
    return kb

def kb_parking():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 Active parkings", "➕ Add parking")
    kb.row("✅ Close parking", "🔄 Extend parking")
    kb.row("⬅️ Main Menu")
    return kb

def kb_dot():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("⚠️ Expiring soon", "➕ Add DOT document")
    kb.row("⬅️ Main Menu")
    return kb

def kb_units():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("🔍 Search unit", "➕ Add unit")
    kb.row("📥 Bulk import units")
    kb.row("⬅️ Main Menu")
    return kb

def kb_reports():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📊 This month", "📊 Choose month")
    kb.row("⬅️ Main Menu")
    return kb

def kb_settings():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("👥 Groups", "👤 Admins")
    kb.row("⬅️ Main Menu")
    return kb

def kb_groups():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 Show all groups", "📥 Bulk import groups")
    kb.row("❌ Delete group")
    kb.row("⬅️ Main Menu")
    return kb

def kb_admins():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📋 List admins", "➕ Add admin")
    kb.row("❌ Remove admin")
    kb.row("⬅️ Main Menu")
    return kb

# BULK IMPORT
def bulk_import_groups(text):
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    added = updated = 0
    errors = []
    for line in lines:
        try:
            if "|" in line: parts = line.split("|", 1)
            elif "," in line: parts = line.split(",", 1)
            else: parts = [line.strip()]
            gid = int(parts[0].strip())
            title = parts[1].strip() if len(parts) > 1 else f"Group {gid}"
            m = re.search(r"\bGL\d+\b", title)
            unit_code = m.group(0) if m else None
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM groups WHERE id=?", (gid,))
                if cur.fetchone():
                    cur.execute("UPDATE groups SET title=?, unit_code=COALESCE(NULLIF(unit_code,''),?) WHERE id=?",
                                (title, unit_code, gid))
                    updated += 1
                else:
                    cur.execute("INSERT INTO groups (id,title,unit_code,driver_name) VALUES (?,?,?,?)",
                                (gid, title, unit_code, None))
                    added += 1
        except ValueError:
            errors.append(f"Bad ID: {line[:30]}")
        except Exception as e:
            errors.append(f"Error: {line[:30]}")
    result = f"✅ Groups imported!\n➕ Added: <b>{added}</b>\n🔄 Updated: <b>{updated}</b>"
    if errors: result += f"\n❌ Errors: {len(errors)}"
    return result

def bulk_import_units(text):
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    added = updated = 0
    errors = []
    ts = now_str()
    for line in lines:
        try:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 4:
                errors.append(f"Too few fields: {line[:40]}")
                continue
            unit_number = parts[0].upper()
            unit_type = "TRAILER" if unit_number.startswith("T") else "TRUCK"
            if unit_type == "TRUCK":
                if len(parts) < 5:
                    errors.append(f"Needs 5 fields: {line[:40]}")
                    continue
                year_model, vin, plate, state = parts[1], parts[2].upper(), parts[3].upper(), parts[4].upper()
                trailer_type = trailer_length = None
            else:
                if len(parts) >= 6:
                    trailer_type, trailer_length = parts[1], parts[2]
                    vin, plate, state = parts[3].upper(), parts[4].upper(), parts[5].upper()
                    year_model = ""
                else:
                    year_model, vin, plate = parts[1], parts[2].upper(), parts[3].upper()
                    state = parts[4].upper() if len(parts) > 4 else ""
                    trailer_type = trailer_length = None
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM units WHERE unit_number=?", (unit_number,))
                if cur.fetchone():
                    cur.execute("""UPDATE units SET unit_type=?,year_model=?,vin=?,plate=?,
                        state=?,trailer_type=?,trailer_length=?,updated_at=? WHERE unit_number=?""",
                                (unit_type, year_model, vin, plate, state, trailer_type, trailer_length, ts, unit_number))
                    updated += 1
                else:
                    cur.execute("""INSERT INTO units (unit_number,unit_type,year_model,vin,plate,state,
                        trailer_type,trailer_length,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                                (unit_number, unit_type, year_model, vin, plate, state, trailer_type, trailer_length, ts, ts))
                    added += 1
        except Exception as e:
            errors.append(f"Error: {line[:40]}")
    result = f"✅ Units imported!\n➕ Added: <b>{added}</b>\n🔄 Updated: <b>{updated}</b>"
    if errors: result += f"\n❌ Errors: {len(errors)}"
    return result

# PTI
def pti_missing_today():
    today = get_et_now().strftime("%Y-%m-%d")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM groups ORDER BY title")
        groups = cur.fetchall()
        missing = [g["title"] for g in groups if not conn.execute(
            "SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=? LIMIT 1",
            (g["id"], today)).fetchone()]
    if not missing: return "✅ All groups sent PTI today!"
    return f"🔴 <b>Missing PTI today — {len(missing)} groups:</b>\n\n" + "\n".join(f"• {h(t)}" for t in missing)

def pti_missing_week():
    today = get_et_now().date()
    week_ago = today - timedelta(days=6)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM groups ORDER BY title")
        groups = cur.fetchall()
        missing = [g["title"] for g in groups if not conn.execute(
            "SELECT 1 FROM pti_reports WHERE group_id=? AND date_et BETWEEN ? AND ? LIMIT 1",
            (g["id"], week_ago.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))).fetchone()]
    if not missing: return "✅ All groups sent PTI this week!"
    return f"🔴 <b>Missing PTI this week — {len(missing)} groups:</b>\n\n" + "\n".join(f"• {h(t)}" for t in missing)

def pti_send_reminder():
    today = get_et_now().strftime("%Y-%m-%d")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM groups")
        groups = cur.fetchall()
        missing = [g["id"] for g in groups if not conn.execute(
            "SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=? LIMIT 1",
            (g["id"], today)).fetchone()]
    if not missing: return "✅ All groups sent PTI today!"
    sent = sum(1 for gid in missing if not bot.send_message(gid, "🔔 <b>Reminder:</b> Please send today's PTI!\n\nUse: <code>/pti</code>") is None)
    sent = 0
    for gid in missing:
        try:
            bot.send_message(gid, "🔔 <b>Reminder:</b> Please send today's PTI!\n\nUse: <code>/pti</code>")
            sent += 1
        except: pass
    return f"✅ PTI reminder sent to <b>{sent}</b> groups."

def pti_driver_report(name):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT timestamp_et FROM pti_reports WHERE driver_name LIKE ? ORDER BY timestamp_et DESC",
                    (f"%{name}%",))
        rows = cur.fetchall()
    if not rows: return f"❌ No PTI reports found for '<b>{h(name)}</b>'."
    by_month = {}
    for r in rows:
        try:
            dt = datetime.strptime(r["timestamp_et"], "%Y-%m-%d %H:%M:%S")
            key = dt.strftime("%B %Y")
            by_month[key] = by_month.get(key, 0) + 1
        except: pass
    lines = [f"📋 <b>PTI Report: {h(name)}</b>\nTotal: <b>{len(rows)}</b>\n"]
    for month, cnt in by_month.items():
        lines.append(f"• {h(month)}: <b>{cnt}</b>")
    return "\n".join(lines)

# PM
def pm_send_planned(chat_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT p.id, g.title FROM pm_plans p
            JOIN groups g ON p.group_id=g.id WHERE p.is_done=0 ORDER BY p.created_at""")
        rows = cur.fetchall()
    if not rows:
        bot.send_message(chat_id, "✅ No units in PM plan.", reply_markup=kb_pm())
        return
    bot.send_message(chat_id, f"🔧 <b>PM Plan — {len(rows)} units</b>", reply_markup=kb_pm())
    for r in rows:
        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton("✅ Done", callback_data=f"pm_done:{r['id']}"),
               types.InlineKeyboardButton("❌ Remove", callback_data=f"pm_remove:{r['id']}"))
        bot.send_message(chat_id, f"🔧 <b>{h(r['title'])}</b>", reply_markup=kb)

def pm_send_reminder():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT g.id FROM pm_plans p JOIN groups g ON p.group_id=g.id WHERE p.is_done=0")
        rows = cur.fetchall()
    if not rows: return "✅ No planned PM units."
    sent = 0
    for r in rows:
        try:
            bot.send_message(r["id"], "🔔 <b>PM Reminder:</b> Your unit is scheduled for maintenance. Contact fleet!")
            sent += 1
        except: pass
    return f"✅ PM reminder sent to <b>{sent}</b> units."

# PARKING
def parking_active():
    today = get_et_now().date()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM parking WHERE status='ACTIVE' ORDER BY end_date")
        rows = cur.fetchall()
    if not rows: return "✅ No active parkings."
    lines = [f"🅿️ <b>Active Parkings — {len(rows)}</b>\n"]
    for r in rows:
        try:
            end_dt = datetime.strptime(r["end_date"], "%Y-%m-%d")
            days_left = (end_dt.date() - today).days
            status = (f"⛔ EXPIRED {abs(days_left)}d ago" if days_left < 0
                      else "⚠️ TODAY" if days_left == 0
                      else f"⚠️ {days_left}d left" if days_left <= 2
                      else f"✅ {days_left}d left")
            lines.append(f"• <b>{h(r['unit_number'])}</b> — {h(r['location'])}\n  📅 {r['end_date']} ({status})")
        except: continue
    return "\n".join(lines)

def parking_add(unit, location, start, end, uid, home_group_id=None):
    try:
        unit_type = "TRAILER" if unit.upper().startswith("T") else "TRUCK"
        start_dt = datetime.strptime(start, "%m/%d/%Y")
        end_dt = datetime.strptime(end, "%m/%d/%Y")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""INSERT INTO parking (unit_number,unit_type,location,start_date,end_date,
                status,created_at,created_by,home_group_id) VALUES (?,?,?,?,?,'ACTIVE',?,?,?)""",
                        (unit.upper(), unit_type, location, start_dt.strftime("%Y-%m-%d"),
                         end_dt.strftime("%Y-%m-%d"), now_str(), uid, home_group_id))
        msg = (f"✅ <b>Parking Added!</b>\n\n🚛 Unit: <b>{h(unit.upper())}</b>\n"
               f"📍 Location: <b>{h(location)}</b>\n📅 {start} → {end}")
        if home_group_id:
            try:
                bot.send_message(home_group_id,
                                 f"🅿️ <b>Parking arranged for your home time!</b>\n\n"
                                 f"📍 Location: <b>{h(location)}</b>\n📅 {start} → {end}\n\nSafe travels! 🚛")
            except Exception as e:
                print(f"[ERROR] notify home group: {repr(e)}")
        return msg
    except ValueError:
        return "❌ Invalid date format. Use MM/DD/YYYY"
    except Exception as e:
        return f"❌ Error: {repr(e)}"

def parking_close(unit):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM parking WHERE unit_number=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
                    (unit.upper(),))
        row = cur.fetchone()
        if not row: return f"❌ No active parking for <b>{h(unit.upper())}</b>."
        cur.execute("UPDATE parking SET status='CLOSED', closed_at=? WHERE id=?", (now_str(), row["id"]))
    return f"✅ Parking for <b>{h(unit.upper())}</b> closed — unit picked up!"

def parking_extend(unit, days):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, end_date FROM parking WHERE unit_number=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
                    (unit.upper(),))
        row = cur.fetchone()
        if not row: return f"❌ No active parking for <b>{h(unit.upper())}</b>."
        new_end = datetime.strptime(row["end_date"], "%Y-%m-%d") + timedelta(days=days)
        cur.execute("UPDATE parking SET end_date=?,alert_24h_sent=0,alert_12h_sent=0,alert_2h_sent=0,alert_expired_sent=0 WHERE id=?",
                    (new_end.strftime("%Y-%m-%d"), row["id"]))
    return f"✅ Extended <b>{h(unit.upper())}</b> → <b>{new_end.strftime('%m/%d/%Y')}</b> (+{days}d)"

# DOT
def dot_expiring():
    today = get_et_now().date()
    deadline = today + timedelta(days=30)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM dot_documents WHERE expiry_date <= ? ORDER BY expiry_date",
                    (deadline.strftime("%Y-%m-%d"),))
        rows = cur.fetchall()
    if not rows: return "✅ No DOT documents expiring in 30 days."
    lines = [f"⚠️ <b>DOT Expiring Soon — {len(rows)} units</b>\n"]
    for r in rows:
        try:
            exp_dt = datetime.strptime(r["expiry_date"], "%Y-%m-%d")
            days_left = (exp_dt.date() - today).days
            status = (f"⛔ EXPIRED {abs(days_left)}d ago" if days_left < 0
                      else "⚠️ TODAY" if days_left == 0 else f"{days_left}d left")
            lines.append(f"• <b>{h(r['unit_number'])}</b> — {exp_dt.strftime('%m/%d/%Y')} ({status})")
        except: continue
    return "\n".join(lines)

# UNITS
def unit_search(unit_number):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM units WHERE unit_number=?", (unit_number.upper(),))
        unit = cur.fetchone()
        cur.execute("SELECT expiry_date FROM dot_documents WHERE unit_number=? ORDER BY expiry_date DESC LIMIT 1",
                    (unit_number.upper(),))
        dot = cur.fetchone()
    if not unit: return f"❌ Unit <b>{h(unit_number.upper())}</b> not found."
    if unit["unit_type"] == "TRUCK":
        dot_info = ""
        if dot:
            try:
                exp_dt = datetime.strptime(dot["expiry_date"], "%Y-%m-%d")
                days_left = (exp_dt.date() - get_et_now().date()).days
                icon = "⛔" if days_left < 0 else "⚠️" if days_left <= 30 else "✅"
                dot_info = f"\n{icon} DOT: <b>{exp_dt.strftime('%m/%d/%Y')}</b> ({days_left}d)"
            except: pass
        return (f"🚛 <b>{h(unit['unit_number'])}</b>\n"
                f"📅 {h(unit['year_model'])}\n"
                f"🔑 VIN: <code>{h(unit['vin'])}</code>\n"
                f"🔢 Plate: <code>{h(unit['plate'])}</code> | {h(unit['state'])}{dot_info}")
    return (f"🚜 <b>{h(unit['unit_number'])}</b>\n"
            f"📅 {h(unit['year_model'])}\n"
            f"📏 {h(unit['trailer_type'] or '')} {h(unit['trailer_length'] or '')}\n"
            f"🔑 VIN: <code>{h(unit['vin'])}</code>\n"
            f"🔢 Plate: <code>{h(unit['plate'])}</code> | {h(unit['state'])}")

# REPORTS
def monthly_report(month, year):
    start = f"{year}-{month:02d}-01"
    end = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as c FROM pti_reports WHERE date_et >= ? AND date_et < ?", (start[:10], end[:10]))
        pti_total = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c, SUM(COALESCE(amount,0)) as s FROM pm_plans WHERE is_done=1 AND done_at >= ? AND done_at < ?", (start, end))
        pm_row = cur.fetchone()
        pm_count, pm_total = pm_row["c"] or 0, pm_row["s"] or 0
        cur.execute("SELECT COUNT(*) as c FROM parking WHERE created_at >= ? AND created_at < ?", (start, end))
        park_count = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM parking WHERE status='ACTIVE'")
        park_active = cur.fetchone()["c"]
    month_name = datetime(year, month, 1).strftime("%B %Y")
    return (f"📊 <b>Report — {h(month_name)}</b>\n\n"
            f"📋 PTI reports: <b>{pti_total}</b>\n"
            f"🔧 PM done: <b>{pm_count}</b> | Cost: <b>${pm_total:,.2f}</b>\n"
            f"🅿️ Parkings added: <b>{park_count}</b> | Active: <b>{park_active}</b>")

# GROUP COMMANDS
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    if msg.chat.type in ("group", "supergroup"):
        ensure_group(msg.chat)
        return
    uid = msg.from_user.id
    if is_admin(uid):
        clear_state(uid)
        bot.send_message(msg.chat.id, "👋 Welcome to <b>GWE Fleet Bot</b> 🚛", reply_markup=kb_main())
    else:
        bot.send_message(msg.chat.id, f"⛔ Admins only. Contact: {h(FLEET_CONTACT)}")

@bot.message_handler(commands=["pti"])
def cmd_pti(msg):
    chat = msg.chat
    if chat.type not in ("group", "supergroup"): return
    gid = ensure_group(chat)
    if not gid: return
    text = (msg.text or "").strip()
    parts = text.split(maxsplit=1)
    try:
        with get_db() as conn:
            cur = conn.cursor()
            if len(parts) > 1:
                driver_name = parts[1].strip()
                cur.execute("UPDATE groups SET driver_name=? WHERE id=?", (driver_name, gid))
            else:
                cur.execute("SELECT driver_name FROM groups WHERE id=?", (gid,))
                row = cur.fetchone()
                if not row or not row["driver_name"]:
                    bot.reply_to(msg, "👋 First time? Use:\n<code>/pti Your Full Name</code>")
                    return
                driver_name = row["driver_name"]
            get_or_create_driver(driver_name)
            now_et = get_et_now()
            today = now_et.strftime("%Y-%m-%d")
            cur.execute("INSERT OR IGNORE INTO pti_reports (group_id,driver_name,timestamp_et,date_et) VALUES (?,?,?,?)",
                        (gid, driver_name, now_et.strftime("%Y-%m-%d %H:%M:%S"), today))
            inserted = cur.rowcount > 0
        bot.reply_to(msg, "✅ PTI sent!" if inserted else "✅ PTI already recorded today.")
        notif = (f"🚨 <b>New PTI Report</b>\n\n"
                 f"👤 Driver: <b>{h(driver_name)}</b>\n"
                 f"🕒 Time: {now_et.strftime('%Y-%m-%d %H:%M')} (ET)\n"
                 f"🚛 Group: <b>{h(chat.title)}</b>")
        for aid in get_all_admin_ids():
            try: bot.send_message(aid, notif)
            except: pass
    except Exception as e:
        print(f"[ERROR] pti: {repr(e)}")

@bot.message_handler(commands=["pm"])
def cmd_pm(msg):
    chat = msg.chat
    if chat.type not in ("group", "supergroup"): return
    gid = ensure_group(chat)
    if not gid: return
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM pm_plans WHERE group_id=? AND is_done=0", (gid,))
            if cur.fetchone():
                bot.reply_to(msg, "✅ Already in PM plan.")
                return
            cur.execute("INSERT INTO pm_plans (group_id,created_at,is_done) VALUES (?,?,0)", (gid, now_str()))
        bot.reply_to(msg, "✅ PM request sent!")
        notif = f"🔧 <b>New PM Request</b>\n\n🚛 <b>{h(chat.title)}</b>\n🕒 {get_et_now().strftime('%Y-%m-%d %H:%M')} (ET)"
        for aid in get_all_admin_ids():
            try: bot.send_message(aid, notif)
            except: pass
    except Exception as e:
        bot.reply_to(msg, "❌ Error.")

@bot.message_handler(commands=["home"])
def cmd_home(msg):
    chat = msg.chat
    if chat.type not in ("group", "supergroup"): return
    ensure_group(chat)
    driver = msg.from_user
    driver_name = (driver.first_name or "") + (f" {driver.last_name}" if driver.last_name else "")
    now_et = get_et_now()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT unit_code FROM groups WHERE id=?", (chat.id,))
        g = cur.fetchone()
    unit_code = g["unit_code"] if g and g["unit_code"] else "?"
    notif = (f"🏠 <b>Home Time Request</b>\n\n"
             f"👤 Driver: <b>{h(driver_name.strip())}</b>\n"
             f"🚛 Unit: <b>{h(unit_code)}</b>\n"
             f"📍 Group: <b>{h(chat.title)}</b>\n"
             f"🕒 {now_et.strftime('%Y-%m-%d %H:%M')} (ET)")
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("🅿️ Arrange Parking", callback_data=f"home_park:{chat.id}:{unit_code}"),
           types.InlineKeyboardButton("❌ Ignore", callback_data=f"home_ignore:{chat.id}"))
    for aid in get_all_admin_ids():
        try: bot.send_message(aid, notif, reply_markup=kb)
        except: pass
    bot.reply_to(msg, "🏠 Home time request sent to fleet!")

@bot.message_handler(commands=["unit", "info"])
def cmd_unit_info(msg):
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(msg, "Usage: <code>/unit GL1234</code>")
        return
    bot.reply_to(msg, unit_search(parts[1].strip()))

# PHOTO HANDLER
@bot.message_handler(content_types=["photo"],
                     func=lambda m: m.chat.type == "private" and is_admin(m.from_user.id))
def handle_photo(msg):
    uid = msg.from_user.id
    st = get_state(uid)
    if st["state"] != "DOT_WAIT_PHOTO":
        bot.send_message(msg.chat.id, "📸 Photo received. Use 🔍 DOT → ➕ Add DOT document to save it.")
        return
    file_id = msg.photo[-1].file_id
    d = st["data"]
    unit_number = d.get("unit_number", "")
    unit_type = d.get("unit_type", "TRUCK")
    expiry_date = d.get("expiry_date", "")
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM dot_documents WHERE unit_number=?", (unit_number,))
            if cur.fetchone():
                cur.execute("UPDATE dot_documents SET expiry_date=?,photo_file_id=?,unit_type=?,updated_at=? WHERE unit_number=?",
                            (expiry_date, file_id, unit_type, now_str(), unit_number))
            else:
                cur.execute("INSERT INTO dot_documents (unit_number,unit_type,expiry_date,photo_file_id,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                            (unit_number, unit_type, expiry_date, file_id, now_str(), now_str()))
        clear_state(uid)
        exp = datetime.strptime(expiry_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        bot.send_message(msg.chat.id,
                         f"✅ <b>DOT Saved!</b>\n🚛 <b>{h(unit_number)}</b>\n📅 Expires: <b>{exp}</b>",
                         reply_markup=kb_dot())
        set_state(uid, "DOT_MENU")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {repr(e)}")
        clear_state(uid)

# MAIN TEXT HANDLER
@bot.message_handler(content_types=["text"],
                     func=lambda m: m.chat.type == "private" and is_admin(m.from_user.id))
def handle_admin(msg):
    uid = msg.from_user.id
    text = msg.text.strip()
    st = get_state(uid)
    state = st["state"]
    data = st["data"]

    if text in ("⬅️ Main Menu", "❌ Cancel"):
        clear_state(uid)
        bot.send_message(msg.chat.id, "🏠 Main menu:", reply_markup=kb_main())
        return

    if state is None:
        if text == "📋 PTI": set_state(uid, "PTI_MENU"); bot.send_message(msg.chat.id, "📋 <b>PTI</b>", reply_markup=kb_pti())
        elif text == "🔧 PM": set_state(uid, "PM_MENU"); bot.send_message(msg.chat.id, "🔧 <b>PM Maintenance</b>", reply_markup=kb_pm())
        elif text == "🅿️ Parking": set_state(uid, "PARKING_MENU"); bot.send_message(msg.chat.id, "🅿️ <b>Parking</b>", reply_markup=kb_parking())
        elif text == "🔍 DOT": set_state(uid, "DOT_MENU"); bot.send_message(msg.chat.id, "🔍 <b>DOT Inspection</b>", reply_markup=kb_dot())
        elif text == "🚛 Units": set_state(uid, "UNITS_MENU"); bot.send_message(msg.chat.id, "🚛 <b>Units</b>", reply_markup=kb_units())
        elif text == "📊 Reports": set_state(uid, "REPORTS_MENU"); bot.send_message(msg.chat.id, "📊 <b>Reports</b>", reply_markup=kb_reports())
        elif text == "📢 Broadcast": set_state(uid, "BROADCAST_WAIT"); bot.send_message(msg.chat.id, "📢 Type your message to send to ALL groups:", reply_markup=kb_cancel())
        elif text == "⚙️ Settings": set_state(uid, "SETTINGS_MENU"); bot.send_message(msg.chat.id, "⚙️ <b>Settings</b>", reply_markup=kb_settings())
        return

    # PTI
    if state == "PTI_MENU":
        if text == "❌ Missing today": bot.send_message(msg.chat.id, pti_missing_today(), reply_markup=kb_pti())
        elif text == "📅 Missing this week": bot.send_message(msg.chat.id, pti_missing_week(), reply_markup=kb_pti())
        elif text == "🔔 Send reminder": bot.send_message(msg.chat.id, pti_send_reminder(), reply_markup=kb_pti())
        elif text == "👤 Driver report": set_state(uid, "PTI_DRIVER"); bot.send_message(msg.chat.id, "Enter driver name:", reply_markup=kb_cancel())
        return
    if state == "PTI_DRIVER":
        bot.send_message(msg.chat.id, pti_driver_report(text), reply_markup=kb_pti())
        set_state(uid, "PTI_MENU"); return

    # PM
    if state == "PM_MENU":
        if text == "📋 View PM plan": pm_send_planned(msg.chat.id)
        elif text == "🔔 Send PM reminder": bot.send_message(msg.chat.id, pm_send_reminder(), reply_markup=kb_pm())
        return
    if state == "PM_DONE_AMOUNT":
        try:
            amount = float(text.replace("$","").replace(",","").strip())
            plan_id = data.get("plan_id")
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("SELECT g.title FROM pm_plans p JOIN groups g ON p.group_id=g.id WHERE p.id=?", (plan_id,))
                row = cur.fetchone()
                cur.execute("UPDATE pm_plans SET is_done=1, done_at=?, amount=? WHERE id=?", (now_str(), amount, plan_id))
            title = row["title"] if row else "Unknown"
            clear_state(uid)
            bot.send_message(msg.chat.id,
                             f"✅ <b>PM Done!</b>\n🚛 {h(title)}\n💰 Amount: <b>${amount:,.2f}</b>",
                             reply_markup=kb_pm())
            set_state(uid, "PM_MENU")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Invalid. Enter amount like <code>546.13</code>:")
        return

    # PARKING
    if state == "PARKING_MENU":
        if text == "📋 Active parkings": bot.send_message(msg.chat.id, parking_active(), reply_markup=kb_parking())
        elif text == "➕ Add parking": set_state(uid, "PARK_UNIT"); bot.send_message(msg.chat.id, "🚛 Enter unit number:", reply_markup=kb_cancel())
        elif text == "✅ Close parking": set_state(uid, "PARK_CLOSE"); bot.send_message(msg.chat.id, "🚛 Enter unit number to close:", reply_markup=kb_cancel())
        elif text == "🔄 Extend parking": set_state(uid, "PARK_EXTEND_UNIT"); bot.send_message(msg.chat.id, "🚛 Enter unit number to extend:", reply_markup=kb_cancel())
        return
    if state == "PARK_UNIT":
        set_state(uid, "PARK_LOCATION", {**data, "unit_number": text.upper()})
        bot.send_message(msg.chat.id, f"Unit: <b>{h(text.upper())}</b>\n📍 Enter location:"); return
    if state == "PARK_LOCATION":
        set_state(uid, "PARK_START", {**data, "location": text})
        bot.send_message(msg.chat.id, f"Location: <b>{h(text)}</b>\n📅 Start date (MM/DD/YYYY):"); return
    if state == "PARK_START":
        try:
            datetime.strptime(text, "%m/%d/%Y")
            set_state(uid, "PARK_END", {**data, "start_date": text})
            bot.send_message(msg.chat.id, f"Start: <b>{text}</b>\n📅 End date (MM/DD/YYYY):")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Use MM/DD/YYYY:")
        return
    if state == "PARK_END":
        try:
            datetime.strptime(text, "%m/%d/%Y")
            result = parking_add(data["unit_number"], data["location"], data["start_date"], text, uid, data.get("home_group_id"))
            clear_state(uid); set_state(uid, "PARKING_MENU")
            bot.send_message(msg.chat.id, result, reply_markup=kb_parking())
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Use MM/DD/YYYY:")
        return
    if state == "PARK_CLOSE":
        bot.send_message(msg.chat.id, parking_close(text), reply_markup=kb_parking())
        clear_state(uid); set_state(uid, "PARKING_MENU"); return
    if state == "PARK_EXTEND_UNIT":
        set_state(uid, "PARK_EXTEND_DAYS", {"unit_number": text.upper()})
        bot.send_message(msg.chat.id, f"<b>{h(text.upper())}</b> — How many days to extend?"); return
    if state == "PARK_EXTEND_DAYS":
        try:
            days = int(text.strip())
            bot.send_message(msg.chat.id, parking_extend(data["unit_number"], days), reply_markup=kb_parking())
            clear_state(uid); set_state(uid, "PARKING_MENU")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Enter a number:")
        return

    # DOT
    if state == "DOT_MENU":
        if text == "⚠️ Expiring soon": bot.send_message(msg.chat.id, dot_expiring(), reply_markup=kb_dot())
        elif text == "➕ Add DOT document": set_state(uid, "DOT_UNIT"); bot.send_message(msg.chat.id, "🚛 Enter unit number:", reply_markup=kb_cancel())
        return
    if state == "DOT_UNIT":
        unit_n = text.upper()
        set_state(uid, "DOT_EXPIRY", {"unit_number": unit_n, "unit_type": "TRAILER" if unit_n.startswith("T") else "TRUCK"})
        bot.send_message(msg.chat.id, f"Unit: <b>{h(unit_n)}</b>\n📅 Expiry date (MM/DD/YYYY):"); return
    if state == "DOT_EXPIRY":
        try:
            exp_dt = datetime.strptime(text, "%m/%d/%Y")
            set_state(uid, "DOT_WAIT_PHOTO", {**data, "expiry_date": exp_dt.strftime("%Y-%m-%d")})
            bot.send_message(msg.chat.id, f"Expiry: <b>{text}</b>\n📸 Now send the photo:")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Use MM/DD/YYYY:")
        return

    # UNITS
    if state == "UNITS_MENU":
        if text == "🔍 Search unit": set_state(uid, "UNIT_SEARCH"); bot.send_message(msg.chat.id, "🔍 Enter unit number:", reply_markup=kb_cancel())
        elif text == "➕ Add unit": set_state(uid, "UNIT_ADD"); bot.send_message(msg.chat.id, "Send unit info:\n<code>GL1234, 2020 Freightliner, VIN..., PLATE, STATE</code>\nor\n<code>T1234, Dry Van, 53ft, VIN..., PLATE, STATE</code>", reply_markup=kb_cancel())
        elif text == "📥 Bulk import units": set_state(uid, "UNIT_BULK"); bot.send_message(msg.chat.id, "📥 Send units (one per line, max 50 at a time):\n<code>GL1234, Year Make, VIN, PLATE, STATE</code>", reply_markup=kb_cancel())
        return
    if state == "UNIT_SEARCH":
        bot.send_message(msg.chat.id, unit_search(text), reply_markup=kb_units())
        set_state(uid, "UNITS_MENU"); return
    if state in ("UNIT_ADD", "UNIT_BULK"):
        bot.send_chat_action(msg.chat.id, "typing")
        bot.send_message(msg.chat.id, bulk_import_units(text), reply_markup=kb_units())
        set_state(uid, "UNITS_MENU"); return

    # REPORTS
    if state == "REPORTS_MENU":
        if text == "📊 This month":
            now = get_et_now()
            bot.send_message(msg.chat.id, monthly_report(now.month, now.year), reply_markup=kb_reports())
        elif text == "📊 Choose month": set_state(uid, "REPORT_MONTH"); bot.send_message(msg.chat.id, "Enter month (MM/YYYY):", reply_markup=kb_cancel())
        return
    if state == "REPORT_MONTH":
        try:
            dt = datetime.strptime(text, "%m/%Y")
            bot.send_message(msg.chat.id, monthly_report(dt.month, dt.year), reply_markup=kb_reports())
            set_state(uid, "REPORTS_MENU")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Use MM/YYYY:")
        return

    # BROADCAST
    if state == "BROADCAST_WAIT":
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM groups")
            groups = cur.fetchall()
        sent = 0
        for g in groups:
            try: bot.send_message(g["id"], text); sent += 1
            except: pass
        clear_state(uid)
        bot.send_message(msg.chat.id, f"✅ Sent to <b>{sent}</b> groups.", reply_markup=kb_main())
        return

    # SETTINGS
    if state == "SETTINGS_MENU":
        if text == "👥 Groups": set_state(uid, "GROUPS_MENU"); bot.send_message(msg.chat.id, "👥 <b>Groups</b>", reply_markup=kb_groups())
        elif text == "👤 Admins": set_state(uid, "ADMINS_MENU"); bot.send_message(msg.chat.id, "👤 <b>Admins</b>", reply_markup=kb_admins())
        return

    if state == "GROUPS_MENU":
        if text == "📋 Show all groups":
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, title FROM groups ORDER BY title")
                rows = cur.fetchall()
            if not rows:
                bot.send_message(msg.chat.id, "No groups yet.", reply_markup=kb_groups())
            else:
                chunks = []
                chunk = [f"👥 <b>Groups — {len(rows)}</b>\n"]
                for r in rows:
                    line = f"• {h(r['title'])}\n  <code>{r['id']}</code>"
                    if len("\n".join(chunk + [line])) > 3800:
                        chunks.append("\n".join(chunk))
                        chunk = [line]
                    else:
                        chunk.append(line)
                if chunk: chunks.append("\n".join(chunk))
                for i, c in enumerate(chunks):
                    bot.send_message(msg.chat.id, c, reply_markup=kb_groups() if i == len(chunks)-1 else None)
        elif text == "📥 Bulk import groups":
            set_state(uid, "GROUPS_BULK")
            bot.send_message(msg.chat.id,
                             "📥 Send one group per line:\n<code>group_id|Title</code>\n\nExample:\n<code>-1001234567890|GL1234 John Smith</code>",
                             reply_markup=kb_cancel())
        elif text == "❌ Delete group":
            set_state(uid, "GROUPS_DELETE")
            bot.send_message(msg.chat.id, "🔍 Search group by name:", reply_markup=kb_cancel())
        return
    if state == "GROUPS_BULK":
        bot.send_chat_action(msg.chat.id, "typing")
        bot.send_message(msg.chat.id, bulk_import_groups(text), reply_markup=kb_groups())
        set_state(uid, "GROUPS_MENU"); return
    if state == "GROUPS_DELETE":
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM groups WHERE title LIKE ? ORDER BY title LIMIT 10", (f"%{text}%",))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(msg.chat.id, "❌ No groups found.", reply_markup=kb_groups())
        else:
            kb = types.InlineKeyboardMarkup()
            for r in rows:
                kb.add(types.InlineKeyboardButton(h(r["title"]), callback_data=f"delgroup:{r['id']}"))
            bot.send_message(msg.chat.id, "Choose group to delete:", reply_markup=kb)
        set_state(uid, "GROUPS_MENU"); return

    if state == "ADMINS_MENU":
        if text == "📋 List admins":
            with get_db() as conn:
                cur = conn.cursor()
                cur.execute("SELECT user_id, username, added_at FROM admins")
                rows = cur.fetchall()
            lines = [f"👤 <b>Admins — {len(rows)}</b>\n"]
            for r in rows:
                u = f"@{r['username']}" if r["username"] else "no username"
                lines.append(f"• <code>{r['user_id']}</code> — {h(u)}")
            bot.send_message(msg.chat.id, "\n".join(lines), reply_markup=kb_admins())
        elif text == "➕ Add admin": set_state(uid, "ADMIN_ADD"); bot.send_message(msg.chat.id, "Enter user ID to add:", reply_markup=kb_cancel())
        elif text == "❌ Remove admin": set_state(uid, "ADMIN_REMOVE"); bot.send_message(msg.chat.id, "Enter user ID to remove:", reply_markup=kb_cancel())
        return
    if state == "ADMIN_ADD":
        try:
            new_id = int(text.strip())
            with get_db() as conn:
                conn.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?,?)", (new_id, now_str()))
            bot.send_message(msg.chat.id, f"✅ <code>{new_id}</code> added as admin.", reply_markup=kb_admins())
            set_state(uid, "ADMINS_MENU")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Invalid ID:")
        return
    if state == "ADMIN_REMOVE":
        try:
            rem_id = int(text.strip())
            if rem_id == uid:
                bot.send_message(msg.chat.id, "❌ Cannot remove yourself.")
                return
            with get_db() as conn:
                conn.execute("DELETE FROM admins WHERE user_id=?", (rem_id,))
            bot.send_message(msg.chat.id, f"✅ <code>{rem_id}</code> removed.", reply_markup=kb_admins())
            set_state(uid, "ADMINS_MENU")
        except ValueError:
            bot.send_message(msg.chat.id, "❌ Invalid ID:")
        return

    clear_state(uid)
    bot.send_message(msg.chat.id, "🏠 Main menu:", reply_markup=kb_main())

@bot.message_handler(func=lambda m: m.chat.type == "private" and not is_admin(m.from_user.id))
def handle_non_admin(msg):
    bot.send_message(msg.chat.id, f"⛔ Admins only. Contact: {h(FLEET_CONTACT)}")

# CALLBACKS
@bot.callback_query_handler(func=lambda c: c.data.startswith("pm_done:"))
def cb_pm_done(call):
    try:
        if not is_admin(call.from_user.id): bot.answer_callback_query(call.id, "Admins only."); return
        plan_id = int(call.data.split(":")[1])
        set_state(call.from_user.id, "PM_DONE_AMOUNT", {"plan_id": plan_id})
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "💰 Enter PM amount (e.g. <code>546.13</code>):", reply_markup=kb_cancel())
    except Exception as e:
        bot.answer_callback_query(call.id, "Error.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("pm_remove:"))
def cb_pm_remove(call):
    try:
        if not is_admin(call.from_user.id): bot.answer_callback_query(call.id, "Admins only."); return
        plan_id = int(call.data.split(":")[1])
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT g.title FROM pm_plans p JOIN groups g ON p.group_id=g.id WHERE p.id=?", (plan_id,))
            row = cur.fetchone()
            cur.execute("DELETE FROM pm_plans WHERE id=?", (plan_id,))
        title = row["title"] if row else "Unknown"
        bot.answer_callback_query(call.id, "Removed ✅")
        try: bot.edit_message_text(f"❌ <b>{h(title)}</b> removed from PM plan.", call.message.chat.id, call.message.message_id)
        except: pass
    except Exception as e:
        bot.answer_callback_query(call.id, "Error.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("delgroup:"))
def cb_del_group(call):
    try:
        if not is_admin(call.from_user.id): bot.answer_callback_query(call.id, "Admins only."); return
        gid = int(call.data.split(":")[1])
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT title FROM groups WHERE id=?", (gid,))
            row = cur.fetchone()
            cur.execute("DELETE FROM pti_reports WHERE group_id=?", (gid,))
            cur.execute("DELETE FROM pm_plans WHERE group_id=?", (gid,))
            cur.execute("DELETE FROM groups WHERE id=?", (gid,))
        title = row["title"] if row else str(gid)
        bot.answer_callback_query(call.id, "Deleted ✅")
        bot.send_message(call.message.chat.id, f"✅ <b>{h(title)}</b> deleted.", reply_markup=kb_groups())
    except Exception as e:
        bot.answer_callback_query(call.id, "Error.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("home_park:"))
def cb_home_park(call):
    try:
        if not is_admin(call.from_user.id): bot.answer_callback_query(call.id, "Admins only."); return
        parts = call.data.split(":")
        group_id = int(parts[1])
        unit_code = parts[2] if len(parts) > 2 else "?"
        set_state(call.from_user.id, "PARK_UNIT", {"home_group_id": group_id})
        bot.answer_callback_query(call.id)
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
        except: pass
        bot.send_message(call.message.chat.id,
                         f"🅿️ <b>Arranging parking for home time</b>\n\n"
                         f"🚛 Enter unit number (suggested: <code>{h(unit_code)}</code>):",
                         reply_markup=kb_cancel())
    except Exception as e:
        bot.answer_callback_query(call.id, "Error.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("home_ignore:"))
def cb_home_ignore(call):
    try:
        bot.answer_callback_query(call.id, "Ignored.")
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
        except: pass
    except: pass

@bot.callback_query_handler(func=lambda c: c.data.startswith("park_extend:"))
def cb_park_extend(call):
    try:
        if not is_admin(call.from_user.id): bot.answer_callback_query(call.id, "Admins only."); return
        pid = int(call.data.split(":")[1])
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT unit_number FROM parking WHERE id=?", (pid,))
            row = cur.fetchone()
        if not row: bot.answer_callback_query(call.id, "Not found."); return
        set_state(call.from_user.id, "PARK_EXTEND_DAYS", {"unit_number": row["unit_number"]})
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, f"🔄 How many days to extend <b>{h(row['unit_number'])}</b>?", reply_markup=kb_cancel())
    except Exception as e:
        bot.answer_callback_query(call.id, "Error.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("park_close:"))
def cb_park_close(call):
    try:
        if not is_admin(call.from_user.id): bot.answer_callback_query(call.id, "Admins only."); return
        pid = int(call.data.split(":")[1])
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT unit_number FROM parking WHERE id=?", (pid,))
            row = cur.fetchone()
            cur.execute("UPDATE parking SET status='CLOSED', closed_at=? WHERE id=?", (now_str(), pid))
        unit = row["unit_number"] if row else str(pid)
        bot.answer_callback_query(call.id, "Closed ✅")
        try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
        except: pass
        bot.send_message(call.message.chat.id, f"✅ Parking for <b>{h(unit)}</b> closed!")
    except Exception as e:
        bot.answer_callback_query(call.id, "Error.")

# SCHEDULED JOBS
def job_parking_alerts():
    try:
        now = get_et_now()
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM parking WHERE status='ACTIVE'")
            rows = cur.fetchall()
        for r in rows:
            try:
                end_dt = ET_TZ.localize(datetime.strptime(r["end_date"], "%Y-%m-%d").replace(hour=23, minute=59))
                diff_h = (end_dt - now).total_seconds() / 3600
                pid, unit, loc = r["id"], h(r["unit_number"]), h(r["location"])
                end_d = datetime.strptime(r["end_date"], "%Y-%m-%d").strftime("%m/%d/%Y")
                def send_alert(label, field, _pid=pid, _unit=unit, _loc=loc, _end=end_d):
                    msg_text = f"⚠️ <b>PARKING — {label}</b>\n🚛 {_unit}\n📍 {_loc}\n📅 {_end}"
                    kb = types.InlineKeyboardMarkup()
                    kb.row(types.InlineKeyboardButton("🔄 Extend", callback_data=f"park_extend:{_pid}"),
                           types.InlineKeyboardButton("✅ Picked Up", callback_data=f"park_close:{_pid}"))
                    for aid in get_all_admin_ids():
                        try: bot.send_message(aid, msg_text, reply_markup=kb)
                        except: pass
                    with get_db() as c2: c2.execute(f"UPDATE parking SET {field}=1 WHERE id=?", (_pid,))
                if 0 < diff_h <= 24 and not r["alert_24h_sent"]: send_alert("24 HOURS LEFT ⏰", "alert_24h_sent")
                if 0 < diff_h <= 12 and not r["alert_12h_sent"]: send_alert("12 HOURS LEFT ⚠️", "alert_12h_sent")
                if 0 < diff_h <= 2 and not r["alert_2h_sent"]: send_alert("2 HOURS LEFT 🚨", "alert_2h_sent")
                if diff_h <= 0 and not r["alert_expired_sent"]: send_alert("EXPIRED ⛔", "alert_expired_sent")
            except Exception as e: print(f"[ERROR] parking alert {r['id']}: {repr(e)}")
    except Exception as e: print(f"[ERROR] job_parking_alerts: {repr(e)}")

def job_dot_alerts():
    try:
        today = get_et_now().date()
        deadline = today + timedelta(days=30)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM dot_documents WHERE expiry_date <= ? ORDER BY expiry_date", (deadline.strftime("%Y-%m-%d"),))
            rows = cur.fetchall()
        if not rows: return
        lines = [f"⚠️ <b>DOT EXPIRY ALERT — {len(rows)} units</b>\n"]
        for r in rows:
            try:
                exp_dt = datetime.strptime(r["expiry_date"], "%Y-%m-%d")
                days_left = (exp_dt.date() - today).days
                status = f"⛔ EXPIRED {abs(days_left)}d ago" if days_left < 0 else ("⚠️ TODAY" if days_left == 0 else f"{days_left}d left")
                lines.append(f"• <b>{h(r['unit_number'])}</b> — {exp_dt.strftime('%m/%d/%Y')} ({status})")
            except: continue
        for aid in get_all_admin_ids():
            try: bot.send_message(aid, "\n".join(lines))
            except: pass
    except Exception as e: print(f"[ERROR] job_dot_alerts: {repr(e)}")

def job_parking_summary():
    try:
        today = get_et_now().date()
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM parking WHERE status='ACTIVE' ORDER BY end_date")
            rows = cur.fetchall()
        if not rows: return
        lines = [f"🅿️ <b>DAILY PARKING — {today.strftime('%m/%d/%Y')}</b>\nActive: <b>{len(rows)}</b>\n"]
        for r in rows:
            try:
                end_dt = datetime.strptime(r["end_date"], "%Y-%m-%d")
                days_left = (end_dt.date() - today).days
                icon = "⛔" if days_left < 0 else ("⚠️" if days_left <= 1 else "✅")
                lines.append(f"{icon} <b>{h(r['unit_number'])}</b> — {h(r['location'])} — {end_dt.strftime('%m/%d/%Y')}")
            except: continue
        for aid in get_all_admin_ids():
            try: bot.send_message(aid, "\n".join(lines))
            except: pass
    except Exception as e: print(f"[ERROR] job_parking_summary: {repr(e)}")

def job_pti_reminder():
    try:
        today = get_et_now().strftime("%Y-%m-%d")
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM groups")
            groups = cur.fetchall()
            missing = [g["id"] for g in groups if not conn.execute(
                "SELECT 1 FROM pti_reports WHERE group_id=? AND date_et=? LIMIT 1",
                (g["id"], today)).fetchone()]
        for gid in missing:
            try: bot.send_message(gid, "🔔 <b>Daily PTI Reminder</b>\nPlease send today's PTI!\nUse: <code>/pti</code>")
            except: pass
    except Exception as e: print(f"[ERROR] job_pti_reminder: {repr(e)}")

# HEALTH SERVER
def start_health_server():
    port = int(os.getenv("PORT", "8080"))
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers()
            self.wfile.write(b"GWE Fleet Bot OK")
        def log_message(self, *args): return
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

# MAIN
if __name__ == "__main__":
    init_db()
    threading.Thread(target=start_health_server, daemon=True).start()
    scheduler = BackgroundScheduler(timezone=ET_TZ)
    scheduler.add_job(job_parking_alerts, "interval", minutes=30)
    scheduler.add_job(job_dot_alerts, "cron", hour=9, minute=0)
    scheduler.add_job(job_dot_alerts, "cron", hour=16, minute=0)
    scheduler.add_job(job_parking_summary, "cron", hour=8, minute=0)
    scheduler.add_job(job_pti_reminder, "cron", hour=15, minute=0)
    scheduler.start()
    print("[SCHEDULER] Started.")
    print("🚛 GWE Fleet Bot is running!")
    while True:
        try:
            bot.delete_webhook(drop_pending_updates=True)
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"[POLLING_ERROR] {repr(e)}")
            time.sleep(10)
