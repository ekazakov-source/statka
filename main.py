# main.py
import os
import sqlite3
import json
import shutil
from datetime import date, datetime
from zoneinfo import ZoneInfo
from flask import (
    Flask, request, redirect, url_for, render_template_string,
    session, Response, send_file
)
import bcrypt
import logging
logging.basicConfig(level=logging.INFO)

# ==================== App & Config ====================
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "super_secret_key_change_me")
PORT = int(os.environ.get("PORT", 81))

DB_PATH = os.getenv("DATA_PATH", os.path.abspath("data.db"))
BACKUP_DIR = os.path.abspath("backups")
os.makedirs(BACKUP_DIR, exist_ok=True)

# ==================== CPA tables ====================
CPA_SLOTS = {
    "Australia": 180, "Austria": 300, "Belgium": 280, "Canada": 180, "Czech Republic": 170,
    "Denmark": 300, "France": 170, "Germany": 250, "Ireland": 180, "Italy": 170,
    "Netherlands": 230, "Norway": 300, "Poland": 170, "Romania": 110, "Slovakia": 160,
    "Slovenia": 160, "Switzerland": 300, "Spain": 250, "Hungary": 110,
    "Greece": None, "Portugal": None
}
CPA_CRASH = {
    "Australia": 120, "Austria": 120, "Belgium": 120, "Canada": 110, "Czech Republic": 115,
    "Denmark": 140, "France": 100, "Germany": 135, "Ireland": 110, "Italy": 130,
    "Netherlands": 120, "Norway": 130, "Poland": 100, "Romania": 85, "Slovakia": 100,
    "Slovenia": 100, "Switzerland": 140, "Spain": 110, "Hungary": 90,
    "Greece": None, "Portugal": None
}
FLAGS = {
    "Australia": "🇦🇺", "Austria": "🇦🇹", "Belgium":"🇧🇪", "Canada":"🇨🇦", "Czech Republic":"🇨🇿",
    "Denmark":"🇩🇰", "France":"🇫🇷", "Germany":"🇩🇪", "Ireland":"🇮🇪", "Italy":"🇮🇹",
    "Netherlands":"🇳🇱", "Norway":"🇳🇴", "Poland":"🇵🇱", "Romania":"🇷🇴", "Slovakia":"🇸🇰",
    "Slovenia":"🇸🇮", "Switzerland":"🇨🇭", "Spain":"🇪🇸", "Hungary":"🇭🇺",
    "Greece":"🇬🇷", "Portugal":"🇵🇹"
}

# ==================== DB bootstrap / migrations ====================
def ensure_daily_backup():
    today = date.today().isoformat()
    backup_path = os.path.join(BACKUP_DIR, f"data-{today}.db")
    if os.path.exists(DB_PATH) and not os.path.exists(backup_path):
        try:
            shutil.copyfile(DB_PATH, backup_path)
        except Exception:
            pass

def migrate():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    with conn:
        conn.execute("PRAGMA journal_mode=WAL;")

        # users
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE NOT NULL,
          password_hash TEXT NOT NULL,
          role TEXT NOT NULL CHECK(role IN ('BUYER','TEAM_LEAD','ADMIN')) DEFAULT 'BUYER',
          is_active INTEGER NOT NULL DEFAULT 1,
          is_deleted INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """)
        cols_u = {r[1]: True for r in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "is_deleted" not in cols_u:
            conn.execute("ALTER TABLE users ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")

        # socs
        conn.execute("""
        CREATE TABLE IF NOT EXISTS socs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          is_closed INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """)

        # cabinets
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cabinets (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          soc_id INTEGER NOT NULL,
          name TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('ACTIVE','BANNED')) DEFAULT 'ACTIVE',
          currency TEXT NOT NULL CHECK(currency IN ('USD','EUR')),
          cab_type TEXT NOT NULL CHECK(cab_type IN ('AGENCY','FARM')),
          commission_pct REAL NOT NULL DEFAULT 6.0,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """)

        # fx_rates
        conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          date TEXT NOT NULL,
          from_currency TEXT NOT NULL CHECK(from_currency IN ('USD','EUR')),
          to_currency TEXT NOT NULL CHECK(to_currency IN ('USD')),
          rate REAL NOT NULL,
          UNIQUE(date, from_currency, to_currency)
        )
        """)

        # day_locks
        conn.execute("""
        CREATE TABLE IF NOT EXISTS day_locks (
          user_id INTEGER NOT NULL,
          date TEXT NOT NULL,
          locked_at TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY (user_id, date)
        )
        """)

        # audit_log
        conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT (datetime('now')),
          actor_user TEXT NOT NULL,
          action TEXT NOT NULL,
          payload TEXT
        )
        """)

        # records (legacy + new columns)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT,
            date TEXT,
            geo TEXT,
            vertical TEXT,
            spend INTEGER,
            deps INTEGER,
            revenue INTEGER,
            profit INTEGER
        )
        """)
        cols = {r[1]: True for r in conn.execute("PRAGMA table_info(records)").fetchall()}
        if "created_at" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'))")
        if "updated_at" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN updated_at TEXT")
        if "user_id" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN user_id INTEGER")
        if "cabinet_id" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN cabinet_id INTEGER")
        if "spend_raw" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN spend_raw REAL")
        if "spend_currency" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN spend_currency TEXT")
        if "spend_usd" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN spend_usd REAL")

        conn.execute("DROP INDEX IF EXISTS ux_records_user_date_geo_vert")
        conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_records_user_date_geo_vert_cab
        ON records(user, date, geo, vertical, cabinet_id)
        """)
    conn.close()

    # первичный ADMIN
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    cnt = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    if cnt == 0:
        ph = bcrypt.hashpw(b"chinCHIN", bcrypt.gensalt()).decode()
        conn.execute("INSERT INTO users (username, password_hash, role) VALUES (?,?,?)",
                     ("ADMIN_HEAD", ph, "ADMIN"))
        today = date.today().isoformat()
        conn.execute("INSERT OR IGNORE INTO fx_rates (date, from_currency, to_currency, rate) VALUES (?,?,?,?)",
                     (today, "EUR", "USD", 1.10))
        conn.commit()
    conn.close()

migrate()

# ==================== Helpers ====================
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def audit(actor_user: str, action: str, payload: dict):
    try:
        conn = db()
        with conn:
            conn.execute(
                "INSERT INTO audit_log (actor_user, action, payload) VALUES (?,?,?)",
                (actor_user, action, json.dumps(payload, ensure_ascii=False))
            )
        conn.close()
    except Exception:
        pass

def hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()

def check_password(pw: str, ph: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode(), ph.encode())
    except Exception:
        return False

def utc_to_msk(ts: str | None) -> str:
    if not ts: return "—"
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
        msk = dt.astimezone(ZoneInfo("Europe/Moscow"))
        return msk.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts

def safe_int(x, default=0):
    try:
        if x is None: return default
        s = str(x).strip()
        if not s: return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default

def safe_float(x, default=0.0):
    try:
        if x is None: return default
        s = str(x).strip().replace(",", ".")
        if not s: return default
        return float(s)
    except Exception:
        return default

def get_fx_rate(d: str, from_currency: str) -> float:
    if from_currency == "USD":
        return 1.0
    conn = db()
    row = conn.execute("""
        SELECT rate FROM fx_rates
        WHERE date<=? AND from_currency=? AND to_currency='USD'
        ORDER BY date DESC LIMIT 1
    """, (d, from_currency)).fetchone()
    conn.close()
    return float(row["rate"]) if row else 1.10  # fallback

def is_day_locked(user_id: int, d: str) -> bool:
    conn = db()
    row = conn.execute("SELECT 1 FROM day_locks WHERE user_id=? AND date=?", (user_id, d)).fetchone()
    conn.close()
    return bool(row)

# ==================== Auth ====================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        conn = db()
        user = conn.execute(
            "SELECT * FROM users WHERE username=? AND is_active=1 AND is_deleted=0",
            (username,)
        ).fetchone()
        conn.close()
        if user and check_password(password, user["password_hash"]):
            session["uid"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            return redirect(url_for("data_input"))
        return render_template_string(LOGIN_TPL, error="Неверные логин/пароль или пользователь неактивен")
    return render_template_string(LOGIN_TPL)

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))

def require_login():
    return "uid" in session

def require_tl():
    return require_login() and session.get("role") in ("TEAM_LEAD","ADMIN")

def require_admin():
    return require_login() and session.get("role") == "ADMIN"

# ==================== ACCOUNTS (SOC / CABS / USERS / FX) ====================
@app.route("/accounts", methods=["GET"])
def accounts():
    if not require_login(): return redirect(url_for("login"))
    uid = session["uid"]
    role = session.get("role","BUYER")

    conn = db()
    users = []
    if role in ("TEAM_LEAD","ADMIN"):
        users = conn.execute(
            "SELECT id,username,role,is_active,is_deleted FROM users ORDER BY username"
        ).fetchall()

    socs = conn.execute("SELECT * FROM socs WHERE user_id=? ORDER BY name", (uid,)).fetchall()
    soc_ids = [s["id"] for s in socs] or [-1]
    cabs = conn.execute(f"""
        SELECT * FROM cabinets WHERE soc_id IN ({','.join('?'*len(soc_ids))})
        ORDER BY name
    """, soc_ids).fetchall()

    fx_rows = conn.execute("SELECT * FROM fx_rates ORDER BY date DESC LIMIT 30").fetchall()
    conn.close()

    by_soc = {}
    for c in cabs:
        by_soc.setdefault(c["soc_id"], []).append(c)

    today_iso = date.today().isoformat()

    return render_template_string(
        ACCOUNTS_TPL,
        role=role, users=users, socs=socs, by_soc=by_soc, fx_rows=fx_rows,
        today_iso=today_iso
    )

@app.route("/accounts/user_add", methods=["POST"])
def user_add():
    if not require_tl(): return "Forbidden", 403
    username = request.form.get("username","").strip()
    password = request.form.get("password","")
    role = request.form.get("role","BUYER")
    if not username or not password or role not in ("BUYER","TEAM_LEAD","ADMIN"):
        return redirect(url_for("accounts"))
    ph = hash_password(password)
    conn = db()
    try:
        with conn:
            conn.execute("INSERT INTO users (username,password_hash,role) VALUES (?,?,?)",
                         (username, ph, role))
        audit(session["username"], "ADD_USER", {"username":username, "role":role})
    except Exception:
        pass
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/user_toggle", methods=["POST"])
def user_toggle():
    if not require_tl(): return "Forbidden", 403
    uid = safe_int(request.form.get("id"))
    # поле может называться is_active (старый вариант) либо status_action (новый селект)
    if "status_action" in request.form:
        val = request.form.get("status_action")
        if val in ("0","1"):
            active = int(val)
        else:
            return "Bad request", 400
    else:
        active = safe_int(request.form.get("is_active"),1)
    conn = db()
    with conn:
        conn.execute("UPDATE users SET is_active=? WHERE id=?", (active, uid))
    audit(session["username"], "TOGGLE_USER", {"id":uid,"is_active":active})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/user_delete", methods=["POST"])
def user_delete():
    # Полное удаление записи пользователя (освобождаем username). Доступ только ADMIN.
    if not require_admin(): return "Forbidden", 403
    uid = safe_int(request.form.get("id"))
    # не позволяем удалить самого себя
    if uid == session.get("uid"):
        return redirect(url_for("accounts"))
    conn = db()
    row = conn.execute("SELECT username FROM users WHERE id=?", (uid,)).fetchone()
    if row:
        uname = row["username"]
        with conn:
            conn.execute("DELETE FROM users WHERE id=?", (uid,))
        audit(session["username"], "DELETE_USER", {"id": uid, "username": uname})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/user_pass", methods=["POST"])
def user_pass():
    if not require_tl(): return "Forbidden", 403
    uid = safe_int(request.form.get("id"))
    pw = request.form.get("password","")
    if not pw: return redirect(url_for("accounts"))
    ph = hash_password(pw)
    conn = db()
    with conn:
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (ph, uid))
    audit(session["username"], "RESET_PASS", {"id":uid})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/soc_add", methods=["POST"])
def soc_add():
    if not require_login(): return redirect(url_for("login"))
    uid = session["uid"]
    name = request.form.get("name","").strip()
    if not name: return redirect(url_for("accounts"))
    conn = db()
    with conn:
        conn.execute("INSERT INTO socs (user_id,name) VALUES (?,?)",(uid,name))
    audit(session["username"], "ADD_SOC", {"name":name})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/soc_update", methods=["POST"])
def soc_update():
    if not require_login(): return redirect(url_for("login"))
    soc_id = safe_int(request.form.get("soc_id"))
    name = request.form.get("name","").strip()
    is_closed = 1 if request.form.get("is_closed")=="1" else 0
    conn = db()
    with conn:
        if name:
            conn.execute("UPDATE socs SET name=? WHERE id=?", (name, soc_id))
        conn.execute("UPDATE socs SET is_closed=? WHERE id=?", (is_closed, soc_id))
    audit(session["username"], "UPDATE_SOC", {"soc_id":soc_id,"name":name,"is_closed":is_closed})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/cab_add", methods=["POST"])
def cab_add():
    if not require_login(): return redirect(url_for("login"))
    soc_id = safe_int(request.form.get("soc_id"))
    name = request.form.get("name","").strip()
    currency = request.form.get("currency","")
    cab_type = request.form.get("cab_type","")
    commission_pct = safe_float(request.form.get("commission_pct"), 6.0)
    if not (soc_id and name and currency in ("USD","EUR") and cab_type in ("AGENCY","FARM")):
        return redirect(url_for("accounts"))
    conn = db()
    with conn:
        conn.execute("""
            INSERT INTO cabinets (soc_id,name,currency,cab_type,commission_pct)
            VALUES (?,?,?,?,?)
        """, (soc_id,name,currency,cab_type,commission_pct))
    audit(session["username"], "ADD_CAB", {"soc_id":soc_id,"name":name})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/cab_update", methods=["POST"])
def cab_update():
    if not require_login(): return redirect(url_for("login"))
    cab_id = safe_int(request.form.get("cab_id"))
    status = request.form.get("status","")
    currency = request.form.get("currency","")
    cab_type = request.form.get("cab_type","")
    commission_pct = safe_float(request.form.get("commission_pct"), 6.0)
    conn = db()
    with conn:
        if status in ("ACTIVE","BANNED"):
            conn.execute("UPDATE cabinets SET status=? WHERE id=?", (status, cab_id))
        if currency in ("USD","EUR"):
            conn.execute("UPDATE cabinets SET currency=? WHERE id=?", (currency, cab_id))
        if cab_type in ("AGENCY","FARM"):
            conn.execute("UPDATE cabinets SET cab_type=?, commission_pct=? WHERE id=?",
                         (cab_type, commission_pct, cab_id))
    audit(session["username"], "UPDATE_CAB", {"cab_id":cab_id})
    conn.close()
    return redirect(url_for("accounts"))

@app.route("/accounts/fx_set", methods=["POST"])
def fx_set():
    if not require_tl(): return "Forbidden", 403
    d = request.form.get("date") or date.today().isoformat()
    rate = safe_float(request.form.get("eurusd"), 1.10)
    conn = db()
    with conn:
        conn.execute("""
        INSERT INTO fx_rates (date,from_currency,to_currency,rate)
        VALUES (?,?,?,?)
        ON CONFLICT(date,from_currency,to_currency) DO UPDATE SET rate=excluded.rate
        """, (d, "EUR", "USD", rate))
    audit(session["username"], "FX_SET", {"date":d,"EURUSD":rate})
    conn.close()
    return redirect(url_for("accounts"))

# ==================== ВНЕСЕНИЕ ДАННЫХ ====================
@app.route("/input", methods=["GET"])
def data_input():
    if not require_login(): return redirect(url_for("login"))
    uid = session["uid"]
    chosen_date = request.args.get("date") or date.today().isoformat()
    chosen_soc = request.args.get("soc_id")
    chosen_cab = request.args.get("cab_id")

    conn = db()
    socs = conn.execute("SELECT * FROM socs WHERE user_id=? ORDER BY name", (uid,)).fetchall()
    cabs_by_soc = {}
    for s in socs:
        cabs = conn.execute("SELECT * FROM cabinets WHERE soc_id=? ORDER BY name", (s["id"],)).fetchall()
        cabs_by_soc[s["id"]] = cabs

    chosen_soc = int(chosen_soc) if (chosen_soc and str(chosen_soc).isdigit()) else None
    chosen_cab = int(chosen_cab) if (chosen_cab and str(chosen_cab).isdigit()) else None
    if not chosen_soc and socs:
        chosen_soc = socs[0]["id"]
    if chosen_soc and not chosen_cab:
        cands = cabs_by_soc.get(chosen_soc, [])
        active = [c for c in cands if c["status"] == "ACTIVE"]
        chosen_cab = (active[0]["id"] if active else (cands[0]["id"] if cands else None))

    cab = None
    if chosen_cab:
        cab = conn.execute("SELECT * FROM cabinets WHERE id=?", (chosen_cab,)).fetchone()

    existing = {}
    if cab:
        rows = conn.execute("""
            SELECT geo, vertical, spend_raw, spend_currency, deps
            FROM records
            WHERE user_id=? AND cabinet_id=? AND date=?
        """, (uid, cab["id"], chosen_date)).fetchall()
        for r in rows:
            existing.setdefault(r["geo"], {})[r["vertical"]] = {
                "spend_raw": r["spend_raw"] or 0.0,
                "currency": r["spend_currency"] or cab["currency"],
                "deps": int(r["deps"] or 0)
            }
    conn.close()

    geos = sorted(set(CPA_SLOTS.keys()) | set(CPA_CRASH.keys()))
    return render_template_string(INPUT_TPL,
        chosen_date=chosen_date, socs=socs, cabs_by_soc=cabs_by_soc,
        chosen_soc=chosen_soc, chosen_cab=chosen_cab, cab=cab,
        geos=geos, flags=FLAGS, cpa_slots=CPA_SLOTS, cpa_crash=CPA_CRASH,
        existing=existing
    )

@app.route("/input/save", methods=["POST"])
def input_save():
    if not require_login(): return redirect(url_for("login"))
    uid = session["uid"]; uname = session["username"]
    chosen_date = request.form.get("date") or date.today().isoformat()
    soc_id = safe_int(request.form.get("soc_id"))
    cab_id = safe_int(request.form.get("cab_id"))

    if is_day_locked(uid, chosen_date):
        return redirect(url_for("data_input", date=chosen_date, soc_id=soc_id, cab_id=cab_id))

    conn = db()
    cab = conn.execute("SELECT * FROM cabinets WHERE id=?", (cab_id,)).fetchone()
    if not cab:
        conn.close()
        return redirect(url_for("data_input", date=chosen_date, soc_id=soc_id, cab_id=cab_id))

    fx = get_fx_rate(chosen_date, cab["currency"])
    rows = []
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    geos = sorted(set(CPA_SLOTS.keys()) | set(CPA_CRASH.keys()))
    for geo in geos:
        dep_s = safe_int(request.form.get(f"deps_slots_{geo}"), 0)
        sp_s_raw = safe_float(request.form.get(f"spend_slots_{geo}"), 0.0)

        dep_c = safe_int(request.form.get(f"deps_crash_{geo}"), 0)
        sp_c_raw = safe_float(request.form.get(f"spend_crash_{geo}"), 0.0)

        for vertical, deps, sp_raw, cpa in [
            ("Slots", dep_s, sp_s_raw, CPA_SLOTS.get(geo)),
            ("Crash", dep_c, sp_c_raw, CPA_CRASH.get(geo)),
        ]:
            if cpa is None:
                continue
            factor = 1.0 + (float(cab["commission_pct"])/100.0) if cab["cab_type"]=="AGENCY" else 1.0
            spend_usd = round(sp_raw * factor * fx, 4)
            revenue = int(deps) * int(cpa)
            profit = int(revenue) - int(round(spend_usd))

            rows.append((
                uname, uid, chosen_date, geo, vertical, cab_id,
                int(round(sp_raw)),            # spend_raw как целое — по требованию
                cab["currency"],
                int(round(spend_usd)),         # legacy int
                int(deps), int(revenue), int(profit),
                float(spend_usd),              # точное
                now_ts, now_ts
            ))

    success = False
    try:
        if rows:
            ensure_daily_backup()
            with conn:
                conn.executemany("""
                INSERT INTO records (user, user_id, date, geo, vertical, cabinet_id,
                                     spend_raw, spend_currency, spend, deps, revenue, profit, spend_usd,
                                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user, date, geo, vertical, cabinet_id) DO UPDATE SET
                  spend_raw=excluded.spend_raw,
                  spend_currency=excluded.spend_currency,
                  spend=excluded.spend,
                  spend_usd=excluded.spend_usd,
                  deps=excluded.deps,
                  revenue=excluded.revenue,
                  profit=excluded.profit,
                  updated_at=excluded.updated_at
                """, rows)
            success = True
            audit(uname, "UPSERT_RECORDS", {"date":chosen_date,"cabinet_id":cab_id,"rows":len(rows)})
    except Exception as e:
        logging.exception("save failed: %s", e)
    finally:
        conn.close()

    if success:
        return redirect(url_for("data_input", date=chosen_date, soc_id=soc_id, cab_id=cab_id, saved=1))
    else:
        return redirect(url_for("data_input", date=chosen_date, soc_id=soc_id, cab_id=cab_id, error=1))

@app.route("/day/lock", methods=["POST"])
def day_lock():
    if not require_tl(): return "Forbidden", 403
    uid = safe_int(request.form.get("user_id"))
    d = request.form.get("date") or date.today().isoformat()
    conn = db()
    with conn:
        conn.execute("INSERT OR IGNORE INTO day_locks (user_id,date) VALUES (?,?)", (uid,d))
    audit(session["username"], "CLOSE_DAY", {"user_id":uid,"date":d})
    conn.close()
    return redirect(url_for("data_input", date=d))

# ==================== ОТЧЁТЫ ====================
@app.route("/dashboard", methods=["GET", "POST"])
def dashboard():
    if not require_login(): return redirect(url_for("login"))
    role = session.get("role","BUYER")
    session_user = session["username"]
    session_uid  = session["uid"]

    if request.method == "POST":
        start_date = request.form.get("start_date") or date.today().isoformat()
        end_date   = request.form.get("end_date") or date.today().isoformat()
        sel_user   = request.form.get("selected_user")
        sel_soc    = request.form.get("soc_id")
        sel_cab    = request.form.get("cab_id")
    else:
        start_date = request.args.get("start_date") or date.today().isoformat()
        end_date   = request.args.get("end_date") or date.today().isoformat()
        sel_user   = request.args.get("selected_user")
        sel_soc    = request.args.get("soc_id")
        sel_cab    = request.args.get("cab_id")

    conn = db()
    if role in ("TEAM_LEAD","ADMIN"):
        users = conn.execute("SELECT id,username FROM users WHERE is_deleted=0 ORDER BY username").fetchall()
        view_user = sel_user or "ALL"
        view_uid = None
        if view_user != "ALL":
            row = conn.execute("SELECT id FROM users WHERE username=?", (view_user,)).fetchone()
            view_uid = row["id"] if row else None
    else:
        users = None
        view_user = session_user
        view_uid = session_uid

    socs = []
    cabs = []
    if view_user != "ALL" and view_uid:
        socs = conn.execute("SELECT * FROM socs WHERE user_id=? ORDER BY name", (view_uid,)).fetchall()
        if sel_soc:
            cabs = conn.execute("SELECT * FROM cabinets WHERE soc_id=? ORDER BY name", (sel_soc,)).fetchall()

    where = "date>=? AND date<=?"
    params = [start_date, end_date]
    if view_user != "ALL":
        where = "user=? AND " + where
        params = [view_user] + params
    if sel_soc and sel_soc not in ("", "ALL"):
        where += " AND cabinet_id IN (SELECT id FROM cabinets WHERE soc_id=?)"
        params.append(sel_soc)
    if sel_cab and sel_cab not in ("", "ALL"):
        where += " AND cabinet_id=?"
        params.append(sel_cab)

    by_vert = {}
    for r in conn.execute(f"""
        SELECT vertical,
               SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records WHERE {where}
        GROUP BY vertical
    """, params).fetchall():
        by_vert[r["vertical"]] = dict(r)

    by_vert_geo = {}
    for r in conn.execute(f"""
        SELECT vertical, geo,
               SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records WHERE {where}
        GROUP BY vertical, geo
        ORDER BY vertical, geo
    """, params).fetchall():
        by_vert_geo.setdefault(r["vertical"], []).append(dict(r))

    total = dict(conn.execute(f"""
        SELECT SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records WHERE {where}
    """, params).fetchone())

    total_by_geo = [dict(r) for r in conn.execute(f"""
        SELECT geo,
               SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records WHERE {where}
        GROUP BY geo
        ORDER BY geo
    """, params).fetchall()]

    by_day = [dict(r) for r in conn.execute(f"""
        SELECT date,
               SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit,
               MAX(updated_at) AS last
        FROM records WHERE {where}
        GROUP BY date
        ORDER BY date
    """, params).fetchall()]
    for d in by_day:
        d["last_msk"] = utc_to_msk(d.get("last"))

    labels = [d["date"] for d in by_day]
    def pack_ts(rows):
        spend, profit, deps, cac, roi = [], [], [], [], []
        for d in rows:
            s = int(d.get("spend") or 0)
            p = int(d.get("profit") or 0)
            r = int(d.get("revenue") or 0)
            ft= int(d.get("deps") or 0)
            spend.append(s); profit.append(p); deps.append(ft)
            cac.append( (s/ft) if ft>0 else None )
            roi.append( ((r - s)*100.0/s) if s>0 else None )
        return dict(spend=spend, profit=profit, deps=deps, cac=cac, roi=roi)

    ts_total = pack_ts(by_day)

    per_v_day = {}
    for r in conn.execute(f"""
        SELECT date, vertical,
               SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records WHERE {where}
        GROUP BY date, vertical
        ORDER BY date, vertical
    """, params).fetchall():
        per_v_day.setdefault(r["vertical"], []).append(dict(r))

    def align_series(vname):
        rows = per_v_day.get(vname, [])
        idx = {row["date"]:row for row in rows}
        arr = []
        for lab in labels:
            arr.append(idx.get(lab, {"date":lab,"spend":0,"deps":0,"revenue":0,"profit":0}))
        return pack_ts(arr)

    ts_slots = align_series("Slots")
    ts_crash = align_series("Crash")

    per_geo_cab = {}
    for r in conn.execute(f"""
        SELECT geo, cabinet_id,
               (SELECT soc_id FROM cabinets c WHERE c.id=records.cabinet_id) AS soc_id,
               SUM(spend_usd) AS spend, SUM(deps) AS deps,
               SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records WHERE {where}
        GROUP BY geo, cabinet_id
        HAVING spend>0
        ORDER BY geo
    """, params).fetchall():
        per_geo_cab.setdefault(r["geo"], []).append(dict(r))

    cab_names = {}
    soc_names = {}
    for r in conn.execute("SELECT id,name,soc_id FROM cabinets").fetchall():
        cab_names[r["id"]] = r["name"]
        soc_names.setdefault(r["soc_id"], None)
    for s in conn.execute("SELECT id,name FROM socs").fetchall():
        soc_names[s["id"]] = s["name"]

    conn.close()

    return render_template_string(DASH_TPL,
        role=role, session_user=session_user, users=users,
        view_user=view_user, start_date=start_date, end_date=end_date,
        socs=socs, cabs=cabs, sel_soc=sel_soc, sel_cab=sel_cab,
        by_vert=by_vert, by_vert_geo=by_vert_geo, total=total, total_by_geo=total_by_geo,
        by_day=by_day, labels=json.dumps(labels),
        ts_total=json.dumps(ts_total), ts_slots=json.dumps(ts_slots), ts_crash=json.dumps(ts_crash),
        per_geo_cab=per_geo_cab, cab_names=cab_names, soc_names=soc_names, flags=FLAGS
    )

# ==================== Export CSV / Backup / Health ====================
@app.route("/export_csv")
def export_csv():
    if not require_login(): return redirect(url_for("login"))

    start = request.args.get("start") or date.today().isoformat()
    end   = request.args.get("end") or date.today().isoformat()
    user  = request.args.get("user") or session["username"]
    soc_id= request.args.get("soc_id")
    cab_id= request.args.get("cab_id")

    where = "date>=? AND date<=?"
    params = [start, end]
    if user != "ALL":
        where = "user=? AND " + where
        params = [user] + params
    if soc_id and soc_id not in ("","ALL"):
        where += " AND cabinet_id IN (SELECT id FROM cabinets WHERE soc_id=?)"
        params.append(soc_id)
    if cab_id and cab_id not in ("","ALL"):
        where += " AND cabinet_id=?"
        params.append(cab_id)

    conn = db()
    rows = conn.execute(f"""
        SELECT user,date,vertical,geo,cabinet_id,spend_raw,spend_currency,spend_usd,deps,revenue,profit,updated_at
        FROM records WHERE {where}
        ORDER BY date, user, vertical, geo
    """, params).fetchall()
    conn.close()

    out = ["user,date,vertical,geo,cabinet,spend_raw,spend_currency,spend_usd,deps,revenue,profit,updated_at"]
    for r in rows:
        out.append("{user},{date},{vertical},{geo},{cab},{sraw},{sc},{susd},{deps},{rev},{prof},{upd}".format(
            user=r["user"], date=r["date"], vertical=r["vertical"], geo=r["geo"],
            cab=r["cabinet_id"] or "", sraw=str(r["spend_raw"] or 0),
            sc=r["spend_currency"] or "", susd=str(r["spend_usd"] or 0),
            deps=int(r["deps"] or 0), rev=int(r["revenue"] or 0),
            prof=int(r["profit"] or 0), upd=r["updated_at"] or ""
        ))

    filename = f"report_{user}_{start}_{end}.csv"
    return Response("\n".join(out), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"})

@app.route("/backup")
def backup_download():
    if not os.path.exists(DB_PATH):
        return "No DB yet", 404
    return send_file(DB_PATH, as_attachment=True)

@app.route("/health")
def health():
    return "ok", 200

# ==================== Templates ====================
LOGIN_TPL = """
<!doctype html><html><head>
<meta charset="utf-8"><title>Login</title>
<style>
body{font-family:Inter,Arial,sans-serif;background:#f6f7fb;display:flex;height:100vh;align-items:center;justify-content:center}
.card{background:#fff;padding:28px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.08);width:min(380px,92vw)}
input,button{width:100%;padding:12px 14px;border-radius:10px;border:1px solid #ddd}
button{background:#2563eb;color:#fff;border:none;margin-top:10px;cursor:pointer}
.err{color:#b91c1c;margin-top:8px}
</style></head><body>
<div class="card">
  <h3>Вход</h3>
  {% if error %}<div class="err">{{error}}</div>{% endif %}
  <form method="post">
    <input name="username" placeholder="Логин" required>
    <input name="password" type="password" placeholder="Пароль" required>
    <button type="submit">Войти</button>
  </form>
</div>
</body></html>
"""

# ---- ACCOUNTS PAGE ----
ACCOUNTS_TPL = """
<!doctype html><html><head>
<meta charset="utf-8"><title>Аккаунты</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{--bg:#f6f7fb;--card:#fff;--primary:#2563eb;--muted:#6b7280}
body{font-family:Inter,Arial,sans-serif;background:var(--bg);margin:14px;color:#0f172a}
.header,.card{background:#fff;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.06);padding:12px 14px}
.row{display:flex;gap:12px;flex-wrap:wrap}
.btn{padding:8px 12px;border-radius:10px;border:1px solid #d1d5db;background:#fff;cursor:pointer}
.btn.primary{background:var(--primary);color:#fff;border:none}
.small{font-size:12px;color:var(--muted)}
.badge{padding:2px 8px;border-radius:999px;font-size:12px}
.badge.green{background:#dcfce7;color:#166534}
.badge.red{background:#fee2e2;color:#991b1b}
.input{padding:8px 10px;border-radius:10px;border:1px solid #e5e7eb}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #eef2f7;padding:6px 8px;text-align:left;vertical-align:top}
.flex{display:flex;gap:8px;flex-wrap:wrap}
.section{margin-top:12px}
</style></head><body>

<div class="header" style="display:flex;gap:10px;align-items:center">
  <div class="flex">
    <a class="btn" href="/accounts">АККАУНТЫ</a>
    <a class="btn" href="/input">ВНЕСЕНИЕ ДАННЫХ</a>
    <a class="btn" href="/dashboard">ОТЧЁТЫ</a>
  </div>
  <div style="margin-left:auto">
    <form method="post" action="/logout"><button class="btn primary">Выйти</button></form>
  </div>
</div>

<div class="row">

  <div class="card" style="flex:1;min-width:320px">
    <h3>Мои СОЦы</h3>
    <form method="post" action="/accounts/soc_add" class="flex">
      <input class="input" name="name" placeholder="Название SOC" required>
      <button class="btn primary">Добавить SOC</button>
    </form>

    {% for s in socs %}
      <div class="section">
        <div style="display:flex;align-items:center;gap:8px">
          <strong>{{s.name}}</strong>
          {% if s.is_closed %}<span class="badge red">закрыт</span>{% endif %}
          <form method="post" action="/accounts/soc_update" class="flex" style="margin-left:auto">
            <input type="hidden" name="soc_id" value="{{s.id}}">
            <input class="input" name="name" placeholder="Переименовать">
            <label class="small"><input type="checkbox" name="is_closed" value="1" {% if s.is_closed %}checked{% endif %}> Закрыть</label>
            <button class="btn">Сохранить</button>
          </form>
        </div>

        <div style="margin:6px 0 6px 12px">
          <form method="post" action="/accounts/cab_add" class="flex">
            <input type="hidden" name="soc_id" value="{{s.id}}">
            <input class="input" name="name" placeholder="Название кабинета" required>
            <select class="input" name="currency" required>
              <option value="">Валюта*</option>
              <option>USD</option><option>EUR</option>
            </select>
            <select class="input" name="cab_type" required>
              <option value="">Тип*</option>
              <option value="AGENCY">Агентский</option>
              <option value="FARM">Фарм</option>
            </select>
            <input class="input" name="commission_pct" placeholder="Комиссия %, по умолч. 6">
            <button class="btn">Добавить кабинет</button>
          </form>

          <table style="margin-top:8px">
            <tr><th>Кабинет</th><th>Статус</th><th>Валюта</th><th>Тип</th><th>Комиссия%</th><th>Действия</th></tr>
            {% for c in by_soc.get(s.id, []) %}
              <tr>
                <td>{{c.name}}</td>
                <td>{% if c.status=='ACTIVE' %}<span class="badge green">ACTIVE</span>{% else %}<span class="badge red">BANNED</span>{% endif %}</td>
                <td>{{c.currency}}</td>
                <td>{{'Агентский' if c.cab_type=='AGENCY' else 'Фарм'}}</td>
                <td>{{"%.2f"|format(c.commission_pct)}}</td>
                <td>
                  <form method="post" action="/accounts/cab_update" class="flex">
                    <input type="hidden" name="cab_id" value="{{c.id}}">
                    <select class="input" name="status">
                      <option value="">Статус</option>
                      <option value="ACTIVE">ACTIVE</option>
                      <option value="BANNED">BANNED</option>
                    </select>
                    <select class="input" name="currency">
                      <option value="">Валюта</option>
                      <option>USD</option><option>EUR</option>
                    </select>
                    <select class="input" name="cab_type">
                      <option value="">Тип</option>
                      <option value="AGENCY">Агентский</option>
                      <option value="FARM">Фарм</option>
                    </select>
                    <input class="input" name="commission_pct" placeholder="Комиссия %">
                    <button class="btn">Сохранить</button>
                  </form>
                </td>
              </tr>
            {% endfor %}
          </table>
        </div>
      </div>
    {% endfor %}
  </div>

  <div class="card" style="flex:1;min-width:360px">
    <h3>Курсы валют (EUR→USD)</h3>
    {% if role in ('TEAM_LEAD','ADMIN') %}
      <form method="post" action="/accounts/fx_set" class="flex">
        <input class="input" type="date" name="date" value="{{today_iso}}">
        <input class="input" name="eurusd" placeholder="EUR→USD, напр. 1.10">
        <button class="btn primary">Сохранить курс</button>
      </form>
    {% else %}
      <div class="small">Курсы может задавать только Тим-лид/Админ</div>
    {% endif %}

    <table style="margin-top:8px">
      <tr><th>Дата</th><th>Пара</th><th>Курс</th></tr>
      {% for fx in fx_rows %}
        <tr><td>{{fx.date}}</td><td>{{fx.from_currency}}→{{fx.to_currency}}</td><td>{{"%.4f"|format(fx.rate)}}</td></tr>
      {% endfor %}
    </table>

    {% if role in ('TEAM_LEAD','ADMIN') %}
      <div class="section">
        <h3>Пользователи</h3>
        <form method="post" action="/accounts/user_add" class="flex">
          <input class="input" name="username" placeholder="username" required>
          <input class="input" name="password" placeholder="пароль" required>
          <select class="input" name="role">
            <option value="BUYER">BUYER</option>
            <option value="TEAM_LEAD">TEAM_LEAD</option>
            <option value="ADMIN">ADMIN</option>
          </select>
          <button class="btn">Добавить</button>
        </form>

        <table style="margin-top:8px;width:100%">
          <tr><th>Логин</th><th>Роль</th><th>Статус/Удаление</th><th>Пароль</th></tr>
          {% for u in users %}
            <tr>
              <td>{{u.username}}</td>
              <td>{{u.role}}</td>
              <td>
                <form method="post" class="flex" onsubmit="return false;">
                  <input type="hidden" name="id" value="{{u.id}}">
                  <select class="input" name="status_action"
                          onchange="userAction(this.form, this.value)">
                    <option value="1" {% if u.is_active %}selected{% endif %}>active</option>
                    <option value="0" {% if not u.is_active %}selected{% endif %}>disabled</option>
                    <option value="DEL">delete</option>
                  </select>
                  <button class="btn" onclick="submitStatus(this)">OK</button>
                </form>
              </td>
              <td>
                <form method="post" action="/accounts/user_pass" class="flex">
                  <input type="hidden" name="id" value="{{u.id}}">
                  <input class="input" name="password" placeholder="новый пароль">
                  <button class="btn">Сменить</button>
                </form>
              </td>
            </tr>
          {% endfor %}
        </table>
      </div>
    {% endif %}
  </div>

</div>

<script>
function userAction(form, val){
  // только меняем action, отправка по кнопке OK
  if(val === 'DEL'){
    form.setAttribute('data-target', '/accounts/user_delete');
  }else{
    form.setAttribute('data-target', '/accounts/user_toggle');
  }
}
function submitStatus(btn){
  const form = btn.closest('form');
  const sel  = form.querySelector('select[name="status_action"]');
  const target = form.getAttribute('data-target') || '/accounts/user_toggle';
  if(sel.value === 'DEL'){
    if(!confirm('Удалить пользователя?')) return;
    if(!confirm('Точно удалить?')) return;
  }
  // создаём скрытую «настоящую» форму для POST
  const f = document.createElement('form');
  f.method='post';
  f.action=target;
  const id = form.querySelector('input[name="id"]').value;
  const hid = document.createElement('input'); hid.type='hidden'; hid.name='id'; hid.value=id; f.appendChild(hid);
  if(sel.value !== 'DEL'){
    const st = document.createElement('input'); st.type='hidden'; st.name='status_action'; st.value=sel.value; f.appendChild(st);
  }
  document.body.appendChild(f); f.submit();
}
</script>

</body></html>
"""

# ---- INPUT (DATA ENTRY) ----
INPUT_TPL = """
<!doctype html><html><head>
<meta charset="utf-8"><title>Внесение данных</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{
  --bg:#f6f7fb; --card:#fff; --primary:#2563eb; --muted:#6b7280;
  --green:#16a34a; --red:#dc2626; --line:#eef2f7; --sub:#f3f4f6;
}
*{box-sizing:border-box}
html,body{height:100%}
body{font-family:Inter,Arial,sans-serif;background:var(--bg);margin:14px;color:#0f172a}
.header,.card{background:#fff;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.06);padding:12px 14px}
.flex{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.btn{padding:8px 12px;border-radius:10px;border:1px solid #d1d5db;background:#fff;cursor:pointer}
.btn.primary{background:var(--primary);color:#fff;border:none}
.btn.soc{background:#f3f4f6}
.btn.soc.selected{background:#dbeafe;border-color:#93c5fd}
.btn.cab.ACTIVE{background:#dcfce7;color:#14532d;border:none}
.btn.cab.BANNED{background:#fee2e2;color:#7f1d1d;border:none}
.btn.cab.selected{outline:2px solid #2563eb}
.input{padding:8px 10px;border-radius:10px;border:1px solid #e5e7eb;min-width:90px}
.small{color:#6b7280;font-size:12px}
.badge{padding:2px 8px;border-radius:999px;font-size:12px}
.badge.info{background:#e0f2fe;color:#075985}
.badge.tip{background:#fef3c7;color:#92400e}
.tbl-wrap{overflow:auto;border-radius:12px;border:1px solid var(--line);background:#fff;max-width:100%}
table{width:100%;border-collapse:separate;border-spacing:0;table-layout:auto}
th,td{border-bottom:1px solid var(--line);padding:12px 14px;white-space:nowrap;text-align:center}
th.left,td.left{text-align:left}
thead tr:nth-child(1) th{background:#f0f3fa;font-weight:700}
thead tr:nth-child(2) th{background:var(--sub);color:#334155}
thead th{position:sticky;top:0;z-index:2}
tbody tr:nth-child(even) td{background:#fafbff}
.vert{font-weight:600}
.geo{font-weight:600}
.sep td{border-top:1px solid var(--line)}
.footer-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:12px}
.toast{position:fixed;left:50%;bottom:16px;transform:translateX(-50%) translateY(10px);background:#16a34a;color:#fff;padding:10px 14px;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.15);opacity:0;transition:.2s;z-index:9999}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.toast.error{background:#dc2626}
@media (max-width:900px){
  .input{min-width:72px}
  th,td{padding:10px}
}
</style>

<script>
let formDirty=false;
let allowUnload=false;

function markDirty(){ formDirty=true; }

function go(href){
  if(!formDirty || confirm('Есть несохранённые изменения. Уйти без сохранения?')){
    window.location = href;
  }
}

window.onbeforeunload = function(e){
  if(!formDirty || allowUnload) return;
  e.preventDefault(); e.returnValue=''; return '';
};

function onDateChange(form){
  if(!formDirty || confirm('Есть несохранённые изменения. Переключить дату без сохранения?')){
    window.location = form.action;
  }
}

// «0 UX»: при фокусе очищаем 0, при blur возвращаем 0, если пусто
function ZeroUX(){
  document.querySelectorAll('input[type="number"]').forEach(el=>{
    el.addEventListener('focus', function(){
      if(this.value==='0' || this.value==='0.0' || this.value==='0.00') this.value='';
    });
    el.addEventListener('blur', function(){
      if(this.value.trim()==='') this.value='0';
    });
  });
}

function armSaveForm(){
  const f=document.getElementById('saveForm');
  if(!f) return;
  f.addEventListener('submit', ()=>{
    allowUnload=true;
    window.onbeforeunload = null;
  });
}

function toastOnSaved(){
  const url = new URL(window.location.href);
  if(url.searchParams.get('saved')==='1'){
    const t=document.getElementById('toast');
    t.textContent='Сохранено!';
    t.classList.add('show');
    setTimeout(()=>t.classList.remove('show'), 1800);
    url.searchParams.delete('saved');
    history.replaceState({},'',url.toString());
    formDirty=false; allowUnload=false;
  }
  if(url.searchParams.get('error')==='1'){
    const t=document.getElementById('toast');
    t.textContent='Ошибка сохранения';
    t.classList.add('show','error');
    setTimeout(()=>{t.classList.remove('show','error');}, 2200);
    url.searchParams.delete('error');
    history.replaceState({},'',url.toString());
  }
}

document.addEventListener('DOMContentLoaded', ()=>{
  ZeroUX(); armSaveForm(); toastOnSaved();
});
</script>
</head><body>

<div class="header" style="display:flex;gap:10px;align-items:center">
  <div class="flex">
    <a class="btn" href="/accounts">АККАУНТЫ</a>
    <a class="btn primary" href="/input">ВНЕСЕНИЕ ДАННЫХ</a>
    <a class="btn" href="/dashboard">ОТЧЁТЫ</a>
  </div>

  <form method="get" action="/input" class="flex" style="margin-left:auto" onsubmit="return false;">
    <label>Дата:</label>
    <input class="input" type="date" name="date" value="{{chosen_date}}"
           onchange="this.form.action='/input?date='+this.value{% if chosen_soc %}+'&soc_id={{chosen_soc}}'{% endif %}{% if chosen_cab %}+'&cab_id={{chosen_cab}}'{% endif %}; onDateChange(this.form)">
  </form>

  <form method="post" action="/logout"><button class="btn">Выйти</button></form>
</div>

<div class="card">
  <div style="font-weight:600;margin-bottom:6px">Выбор аккаунта</div>
  <div class="flex">
    {% for s in socs %}
      <button class="btn soc {% if chosen_soc and chosen_soc==s.id %}selected{% endif %}"
              onclick="go('/input?date={{chosen_date}}&soc_id={{s.id}}{% if chosen_cab %}&cab_id={{chosen_cab}}{% endif %}')">
        {{s.name}}
      </button>
    {% endfor %}
  </div>

  {% if chosen_soc %}
    <div class="flex" style="margin-top:8px">
      {% for c in cabs_by_soc.get(chosen_soc|int, []) %}
        <button class="btn cab {{c.status}} {% if chosen_cab and chosen_cab==c.id %}selected{% endif %}"
                onclick="go('/input?date={{chosen_date}}&soc_id={{chosen_soc}}&cab_id={{c.id}}')">
          {{c.name}} — {{c.currency}} / {{'AG' if c.cab_type=='AGENCY' else 'FARM'}}
        </button>
      {% endfor %}
    </div>
  {% else %}
    <div class="small" style="margin-top:8px">Выберите SOC, затем кабинет</div>
  {% endif %}
</div>

{% if cab %}
  <div class="card">
    <div class="flex small" style="margin-bottom:6px">
      {% set soc_match = (socs|selectattr('id','equalto',cab.soc_id)|list) %}
      <div><strong>SOC:</strong> {{ soc_match[0].name if soc_match and soc_match[0] else '' }}</div>
      <div><span class="badge info">Валюта: {{cab.currency}}</span></div>
      {% if cab.cab_type=='AGENCY' %}
        <div><span class="badge tip">Комиссия: {{'%.2f'|format(cab.commission_pct)}}% будет прибавлена к SPEND при сохранении</span></div>
      {% endif %}
    </div>

    <form id="saveForm" method="post" action="/input/save" oninput="markDirty()">
      <input type="hidden" name="date" value="{{chosen_date}}">
      <input type="hidden" name="soc_id" value="{{chosen_soc}}">
      <input type="hidden" name="cab_id" value="{{chosen_cab}}">

      <div class="tbl-wrap">
        <table>
          <thead>
            <tr>
              <th class="left">GEO</th>
              <th class="left">VERTICAL</th>
              <th>CPA, $</th>
              <th>SPEND ({{cab.currency}})</th>
              <th>FTD</th>
            </tr>
          </thead>
          <tbody>
            {% for geo in geos %}
              {% set cpa_s = cpa_slots.get(geo) %}
              {% set cpa_c = cpa_crash.get(geo) %}
              {% if cpa_s is not none or cpa_c is not none %}
                <tr>
                  <td class="left geo" rowspan="2"><span style="margin-right:6px">{{flags.get(geo,'')}}</span>{{geo}}</td>
                  <td class="left vert">Slots 🎰 7️⃣7️⃣7️⃣</td>
                  <td>{{ cpa_s if cpa_s is not none else '—' }}</td>
                  <td>
                    {% set ex = (existing.get(geo,{}).get('Slots') or {}) %}
                    <input class="input" type="number" step="1" min="0"
                           name="spend_slots_{{geo}}" value="{{ ex.get('spend_raw', 0)|int }}">
                  </td>
                  <td>
                    <input class="input" type="number" step="1" min="0"
                           name="deps_slots_{{geo}}" value="{{ ex.get('deps', 0)|int }}">
                  </td>
                </tr>
                <tr class="sep">
                  <td class="left vert">Crash 💥</td>
                  <td>{{ cpa_c if cpa_c is not none else '—' }}</td>
                  <td>
                    {% set ex2 = (existing.get(geo,{}).get('Crash') or {}) %}
                    <input class="input" type="number" step="1" min="0"
                           name="spend_crash_{{geo}}" value="{{ ex2.get('spend_raw', 0)|int }}">
                  </td>
                  <td>
                    <input class="input" type="number" step="1" min="0"
                           name="deps_crash_{{geo}}" value="{{ ex2.get('deps', 0)|int }}">
                  </td>
                </tr>
              {% endif %}
            {% endfor %}
          </tbody>
        </table>
      </div>

      <div class="footer-actions">
        <button class="btn primary" type="submit">Сохранить</button>
      </div>
    </form>
  </div>
{% endif %}

<div id="toast" class="toast">Сохранено!</div>
</body></html>
"""

# ---- DASHBOARD ----
DASH_TPL = """
<!doctype html><html><head>
<meta charset="utf-8"><title>Отчёты</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js" crossorigin="anonymous"></script>
<style>
:root{--bg:#f6f7fb;--card:#fff;--primary:#2563eb;--muted:#6b7280;--pos:#0a8a0a;--neg:#c1121f;--line:#eef2f7}
*{box-sizing:border-box}
html,body{height:100%}
body{font-family:Inter,Arial,sans-serif;background:var(--bg);margin:14px;color:#0f172a}
.header,.card{background:#fff;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.06);padding:12px 14px}
.flex{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.btn{padding:8px 12px;border-radius:10px;border:1px solid #d1d5db;background:#fff;cursor:pointer}
.btn.primary{background:var(--primary);color:#fff;border:none}
.btn.ghost{background:#f3f4f6;border-color:#e5e7eb;color:#111827}
table{border-collapse:separate;border-spacing:0;width:100%;border-radius:12px;overflow:hidden}
th,td{border:1px solid var(--line);padding:8px 10px;text-align:right;white-space:nowrap}
th{background:#f0f3fa}
th.left, td.left{text-align:left}
.roi-pos{color:var(--pos);font-weight:600}
.roi-neg{color:var(--neg);font-weight:600}
.small{color:var(--muted)}
.subwrap{overflow:auto;max-width:100%}
</style>
<script>
let currentGraphKey=null;

function savePngFull(){
  // снимок всей страницы, а не только блока
  html2canvas(document.body,{scale:2,backgroundColor:'#ffffff',useCORS:true}).then(canvas=>{
    const a=document.createElement('a'); a.href=canvas.toDataURL('image/png'); a.download='report.png'; a.click();
  });
}

function buildChart(datasets, labels, title){
  document.getElementById('chartTitle').textContent=title;
  const ctx=document.getElementById('tsChart').getContext('2d');
  if(window._chart) window._chart.destroy();
  window._chart = new Chart(ctx,{
    type:'line',
    data:{labels,datasets},
    options:{responsive:true, interaction:{mode:'index',intersect:false},
      scales:{
        y:{beginAtZero:true,title:{display:true,text:'USD / CAC'}},
        y1:{beginAtZero:true,title:{display:true,text:'ROI %'},position:'right',grid:{drawOnChartArea:false}},
        y2:{beginAtZero:true,title:{display:true,text:'FTD'},position:'right',grid:{drawOnChartArea:false}}
      }
    }
  });
}
function setRange(preset){
  const sd=document.querySelector('input[name="start_date"]');
  const ed=document.querySelector('input[name="end_date"]');
  const toLocalISO=(d)=>{const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,'0'),dd=String(d.getDate()).padStart(2,'0');return `${y}-${m}-${dd}`};
  const t=new Date(); let s,e;
  if(preset==='today'){ s=new Date(t.getFullYear(),t.getMonth(),t.getDate()); e=new Date(s); }
  else if(preset==='yesterday'){ e=new Date(t.getFullYear(),t.getMonth(),t.getDate()-1); s=new Date(e); }
  else if(preset==='thisweek'){ const d=new Date(t.getFullYear(),t.getMonth(),t.getDate()); const w=(d.getDay()+6)%7; s=new Date(d.getFullYear(),d.getMonth(),d.getDate()-w); e=d; }
  else if(preset==='thismonth'){ s=new Date(t.getFullYear(),t.getMonth(),1); e=new Date(t.getFullYear(),t.getMonth(),t.getDate()); }
  else if(preset==='lastmonth'){ const y=t.getFullYear(),m=t.getMonth(); s=new Date(y,m-1,1); e=new Date(y,m,0); }
  else return;
  sd.value=toLocalISO(s); ed.value=toLocalISO(e); sd.form.submit();
}
function onGraph(key, title){
  if(currentGraphKey===key){ currentGraphKey=null; document.getElementById('chartWrap').style.display='none'; return; }
  currentGraphKey=key; document.getElementById('chartWrap').style.display='block';
  const packs={ total: {{ts_total|safe}}, slots: {{ts_slots|safe}}, crash: {{ts_crash|safe}} };
  const ts=packs[key]||packs.total; const labels={{labels|safe}};
  buildChart([
    {label:'Spend',data:ts.spend,yAxisID:'y',tension:.3},
    {label:'Profit',data:ts.profit,yAxisID:'y',tension:.3},
    {label:'CAC',data:ts.cac,yAxisID:'y',spanGaps:true,tension:.3},
    {label:'FTD',data:ts.deps,yAxisID:'y2',tension:.3,stepped:true},
    {label:'ROI %',data:ts.roi,yAxisID:'y1',tension:.3}
  ], labels, title);
}
</script>
</head><body>

<div class="header" id="fullReport">
  <style>
    .toolbar{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
    .group{display:flex;gap:8px;align-items:center;background:#f8fafc;border:1px solid #e5e7eb;padding:8px 10px;border-radius:14px}
    .group .title{font-size:12px;color:#6b7280;margin-right:6px;white-space:nowrap}
    .linkbar{display:flex;gap:10px}
    .linkbar .btn{padding:8px 10px}
  </style>

  <!-- Навигация -->
  <div class="linkbar">
    <a class="btn" href="/accounts">АККАУНТЫ</a>
    <a class="btn" href="/input">ВНЕСЕНИЕ ДАННЫХ</a>
    <a class="btn primary" href="/dashboard">ОТЧЁТЫ</a>
  </div>

  <!-- Панель инструментов -->
  <form method="post" class="toolbar" style="margin-left:auto">

    <!-- ФИЛЬТРЫ -->
    <div class="group">
      <div class="title">Период</div>
      <input class="btn" type="date" name="start_date" value="{{start_date}}" onchange="this.form.submit()">
      <span>→</span>
      <input class="btn" type="date" name="end_date" value="{{end_date}}" onchange="this.form.submit()">
    </div>

    <div class="group">
      <div class="title">Фильтр</div>
      {% if users %}
        <select class="btn" name="selected_user" onchange="this.form.submit()">
          <option value="ALL" {% if view_user=='ALL' %}selected{% endif %}>ALL</option>
          {% for u in users %}
            <option value="{{u.username}}" {% if view_user==u.username %}selected{% endif %}>{{u.username}}</option>
          {% endfor %}
        </select>
      {% endif %}
      <select class="btn" name="soc_id" onchange="this.form.submit()">
        <option value="" {% if not sel_soc %}selected{% endif %}>SOC: ALL</option>
        {% for s in socs %}
          <option value="{{s.id}}" {% if sel_soc and sel_soc|int==s.id %}selected{% endif %}>{{s.name}}</option>
        {% endfor %}
      </select>
      <select class="btn" name="cab_id" onchange="this.form.submit()">
        <option value="" {% if not sel_cab %}selected{% endif %}>Cab: ALL</option>
        {% for c in cabs %}
          <option value="{{c.id}}" {% if sel_cab and sel_cab|int==c.id %}selected{% endif %}>{{c.name}}</option>
        {% endfor %}
      </select>
      <button class="btn primary">Показать</button>
    </div>

    <!-- ПРЕСЕТЫ ДАТ -->
    <div class="group">
      <div class="title">Быстрый период</div>
      <button class="btn" type="button" onclick="setRange('today')">Сегодня</button>
      <button class="btn" type="button" onclick="setRange('yesterday')">Вчера</button>
      <button class="btn" type="button" onclick="setRange('thisweek')">Тек. неделя</button>
      <button class="btn" type="button" onclick="setRange('thismonth')">Тек. месяц</button>
      <button class="btn" type="button" onclick="setRange('lastmonth')">Прошлый месяц</button>
    </div>

    <!-- ДЕЙСТВИЯ -->
    <div class="group">
      <div class="title">Экспорт</div>
      <button class="btn" type="button" onclick="savePngFull()">📸 PNG</button>
      <a class="btn"
         href="/export_csv?start={{start_date}}&end={{end_date}}&user={{view_user}}&soc_id={{sel_soc}}&cab_id={{sel_cab}}">📄 CSV</a>
    </div>

    <!-- ГРАФИКИ -->
    <div class="group">
      <div class="title">График</div>
      <button class="btn" type="button"
              onclick="onGraph('total','Динамика ({{start_date}} → {{end_date}}) — Итого')">Итого</button>
      <button class="btn" type="button"
              onclick="onGraph('slots','Динамика ({{start_date}} → {{end_date}}) — Slots 🎰 7️⃣7️⃣7️⃣')">Slots</button>
      <button class="btn" type="button"
              onclick="onGraph('crash','Динамика ({{start_date}} → {{end_date}}) — Crash 💥')">Crash</button>
    </div>
  </form>
</div>

<div class="card" id="chartWrap" style="display:none;margin-top:12px">
  <h3 id="chartTitle" style="margin:0 0 10px">Динамика</h3>
  <canvas id="tsChart" height="120"></canvas>
</div>

<!-- Результаты по вертикалям -->
<div class="card" style="margin-top:12px">
  <h3 style="margin:0 0 10px">Результаты по вертикалям</h3>
  <div class="subwrap">
    <table>
      <thead>
        <tr>
          <th class="left">Vertical</th><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>ГЕО / По кабам</th>
        </tr>
      </thead>
      <tbody>
        {% for v, d in by_vert.items() %}
          {% set vlabel = 'Slots 🎰 7️⃣7️⃣7️⃣' if v=='Slots' else 'Crash 💥' %}
          {% set s = d.spend or 0 %}{% set ft = d.deps or 0 %}{% set rev = d.revenue or 0 %}{% set pr = d.profit or 0 %}
          {% set cac = (s/ft) if ft>0 else None %}{% set roi = ((rev - s)/s*100) if s>0 else None %}
          <tr>
            <td class="left">{{vlabel}}</td>
            <td>{{"%d"|format(s)}}</td>
            <td>{{ft}}</td>
            <td>{% if cac is not none %}{{"%.2f"|format(cac)}}{% else %}—{% endif %}</td>
            <td>{{"%d"|format(rev)}}</td>
            <td>{{"%d"|format(pr)}}</td>
            <td>{% if roi is not none %}<span class="{{'roi-pos' if roi>=0 else 'roi-neg'}}">{{"%.1f%%"|format(roi)}}</span>{% else %}—{% endif %}</td>
            <td class="left">
              <button class="btn" type="button"
                onclick="const id='g_{{v}}'; const el=document.getElementById(id); el.style.display=(el.style.display==='none'||!el.style.display)?'block':'none';">
                GEO ▼
              </button>
            </td>
          </tr>
          <tr id="g_{{v}}" style="display:none"><td colspan="8">
            <div class="subwrap">
              <table style="width:100%">
                <tr>
                  <th class="left">GEO</th><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>По кабам</th>
                </tr>
                {% for g in by_vert_geo.get(v,[]) %}
                  {% set gs=g.spend or 0 %}{% set gf=g.deps or 0 %}{% set gr=g.revenue or 0 %}{% set gp=g.profit or 0 %}
                  {% set gcac=(gs/gf) if gf>0 else None %}{% set groi=((gr-gs)/gs*100) if gs>0 else None %}
                  {% set zero = (gs==0 and gf==0 and gr==0 and gp==0) %}
                  {% if not zero %}
                  <tr>
                    <td class="left"><span style="margin-right:6px">{{flags.get(g.geo,'')}}</span>{{g.geo}}</td>
                    <td>{{"%d"|format(gs)}}</td>
                    <td>{{gf}}</td>
                    <td>{% if gcac is not none %}{{"%.2f"|format(gcac)}}{% else %}—{% endif %}</td>
                    <td>{{"%d"|format(gr)}}</td>
                    <td>{{"%d"|format(gp)}}</td>
                    <td>{% if groi is not none %}<span class="{{'roi-pos' if groi>=0 else 'roi-neg'}}">{{"%.1f%%"|format(groi)}}</span>{% else %}—{% endif %}</td>
                    <td>
                      {% set key = g.geo %}
                      {% if per_geo_cab.get(key) %}
                        <button class="btn" type="button"
                          onclick="const id='cab_{{v}}_{{g.geo|replace(' ','_')}}'; const el=document.getElementById(id); el.style.display=(el.style.display==='none'||!el.style.display)?'block':'none';">
                          По кабам
                        </button>
                      {% endif %}
                    </td>
                  </tr>
                  {% if per_geo_cab.get(key) %}
                    <tr id="cab_{{v}}_{{g.geo|replace(' ','_')}}" style="display:none"><td colspan="8">
                      <div class="subwrap">
                        <table style="width:100%">
                          <tr><th class="left">SOC</th><th class="left">Cab</th><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th></tr>
                          {% for row in per_geo_cab.get(key) %}
                            {% set ss=row.spend or 0 %}{% set dd=row.deps or 0 %}{% set rr=row.revenue or 0 %}{% set pp=row.profit or 0 %}
                            {% set cc=(ss/dd) if dd>0 else None %}{% set rri=((rr-ss)/ss*100) if ss>0 else None %}
                            <tr>
                              <td class="left">{{ soc_names.get(row.soc_id,'') }}</td>
                              <td class="left">{{ cab_names.get(row.cabinet_id,'') }}</td>
                              <td>{{"%d"|format(ss)}}</td>
                              <td>{{dd}}</td>
                              <td>{% if cc is not none %}{{"%.2f"|format(cc)}}{% else %}—{% endif %}</td>
                              <td>{{"%d"|format(rr)}}</td>
                              <td>{{"%d"|format(pp)}}</td>
                              <td>{% if rri is not none %}<span class="{{'roi-pos' if rri>=0 else 'roi-neg'}}">{{"%.1f%%"|format(rri)}}</span>{% else %}—{% endif %}</td>
                            </tr>
                          {% endfor %}
                        </table>
                      </div>
                    </td></tr>
                  {% endif %}
                  {% endif %}
                {% endfor %}
              </table>
            </div>
          </td></tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<!-- Итого за период -->
<div class="card" style="margin-top:12px">
  <h3 style="margin:0 0 10px">Итого за период</h3>
  {% set ts=total.spend or 0 %}{% set td=total.deps or 0 %}{% set tr=total.revenue or 0 %}{% set tp=total.profit or 0 %}
  {% set tcac=(ts/td) if td>0 else None %}{% set troi=((tr-ts)/ts*100) if ts>0 else None %}
  <div class="subwrap">
    <table>
      <thead><tr><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>ГЕО</th></tr></thead>
      <tbody>
        <tr>
          <td>{{"%d"|format(ts)}}</td>
          <td>{{td}}</td>
          <td>{% if tcac is not none %}{{"%.2f"|format(tcac)}}{% else %}—{% endif %}</td>
          <td>{{"%d"|format(tr)}}</td>
          <td>{{"%d"|format(tp)}}</td>
          <td>{% if troi is not none %}<span class="{{'roi-pos' if troi>=0 else 'roi-neg'}}">{{"%.1f%%"|format(troi)}}</span>{% else %}—{% endif %}</td>
          <td>
            <button class="btn" type="button"
              onclick="const el=document.getElementById('total_geo'); el.style.display=(el.style.display==='none'||!el.style.display)?'block':'none';">GEO ▼</button>
          </td>
        </tr>
        <tr id="total_geo" style="display:none"><td colspan="7">
          <div class="subwrap">
            <table style="width:100%">
              <tr><th class="left">GEO</th><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th></tr>
              {% for g in total_by_geo %}
                {% set s=g.spend or 0 %}{% set d=g.deps or 0 %}{% set r=g.revenue or 0 %}{% set p=g.profit or 0 %}
                {% set c=(s/d) if d>0 else None %}{% set ro=((r-s)/s*100) if s>0 else None %}
                {% set zero=(s==0 and d==0 and r==0 and p==0) %}
                {% if not zero %}
                  <tr>
                    <td class="left"><span style="margin-right:6px">{{flags.get(g.geo,'')}}</span>{{g.geo}}</td>
                    <td>{{"%d"|format(s)}}</td>
                    <td>{{d}}</td>
                    <td>{% if c is not none %}{{"%.2f"|format(c)}}{% else %}—{% endif %}</td>
                    <td>{{"%d"|format(r)}}</td>
                    <td>{{"%d"|format(p)}}</td>
                    <td>{% if ro is not none %}<span class="{{'roi-pos' if ro>=0 else 'roi-neg'}}">{{"%.1f%%"|format(ro)}}</span>{% else %}—{% endif %}</td>
                  </tr>
                {% endif %}
              {% endfor %}
            </table>
          </div>
        </td></tr>
      </tbody>
    </table>
  </div>
</div>

<!-- Разбивка по дням -->
<div class="card" style="margin-top:12px">
  <h3 style="margin:0 0 10px">Разбивка по дням</h3>
  <div class="subwrap">
    <table>
      <thead><tr><th>Дата</th><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>Последнее сохранение (МСК)</th></tr></thead>
      <tbody>
        {% for d in by_day %}
          {% set s=d.spend or 0 %}{% set ft=d.deps or 0 %}{% set r=d.revenue or 0 %}{% set p=d.profit or 0 %}
          {% set c=(s/ft) if ft>0 else None %}{% set ro=((r-s)/s*100) if s>0 else None %}
          <tr>
            <td class="left">{{d.date}}</td>
            <td>{{"%d"|format(s)}}</td>
            <td>{{ft}}</td>
            <td>{% if c is not none %}{{"%.2f"|format(c)}}{% else %}—{% endif %}</td>
            <td>{{"%d"|format(r)}}</td>
            <td>{{"%d"|format(p)}}</td>
            <td>{% if ro is not none %}<span class="{{'roi-pos' if ro>=0 else 'roi-neg'}}">{{"%.1f%%"|format(ro)}}</span>{% else %}—{% endif %}</td>
            <td>{{d.last_msk or '—'}}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>

<p><a class="btn" href="/input">← К внесению данных</a></p>
</body></html>
"""

# ==================== Run ====================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
