import os
import sqlite3
import json
import shutil
from datetime import date, datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, redirect, url_for, render_template_string, session, send_file, Response

# ==================== App & Config ====================
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "super_secret_key_change_me")
PORT = int(os.environ.get("PORT", 81))

# Где лежит БД. Укажи DATA_PATH на проде на абсолютный путь к уже существующей БД.
DB_PATH = os.getenv("DATA_PATH", os.path.abspath("data.db"))
BACKUP_DIR = os.path.abspath("backups")
os.makedirs(BACKUP_DIR, exist_ok=True)

# ==================== Users & Roles ====================
ALLOWED_USERS = {"Denis_b10", "Evgenii_b03", "Vlad_b22"}
ROLE_MAP = {
    "Evgenii_b03": "TEAM_LEAD",
    "Denis_b10": "BUYER",
    "Vlad_b22": "BUYER",
}

# ==================== CPA tables (обновлено) ====================
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

# ==================== Flags ====================
FLAGS = {
    "Australia": "🇦🇺", "Austria": "🇦🇹", "Belgium":"🇧🇪", "Canada":"🇨🇦", "Czech Republic":"🇨🇿",
    "Denmark":"🇩🇰", "France":"🇫🇷", "Germany":"🇩🇪", "Ireland":"🇮🇪", "Italy":"🇮🇹",
    "Netherlands":"🇳🇱", "Norway":"🇳🇴", "Poland":"🇵🇱", "Romania":"🇷🇴", "Slovakia":"🇸🇰",
    "Slovenia":"🇸🇮", "Switzerland":"🇨🇭", "Spain":"🇪🇸", "Hungary":"🇭🇺",
    "Greece":"🇬🇷", "Portugal":"🇵🇹"
}

# ==================== DB init / migrations ====================
def ensure_daily_backup():
    """Ежедневный бэкап БД: backups/data-YYYY-MM-DD.db"""
    today = date.today().isoformat()
    backup_path = os.path.join(BACKUP_DIR, f"data-{today}.db")
    if os.path.exists(DB_PATH) and not os.path.exists(backup_path):
        try:
            shutil.copyfile(DB_PATH, backup_path)
        except Exception:
            pass

def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    with conn:
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
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        cols = {r["name"]: r for r in conn.execute("PRAGMA table_info(records)").fetchall()}
        if "created_at" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'))")
        if "updated_at" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN updated_at TEXT")
        conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_records_user_date_geo_vert
        ON records(user, date, geo, vertical)
        """)
    conn.close()

init_db()

# ==================== Utils ====================
def safe_int(x, default=0):
    try:
        if x is None: return default
        s = str(x).strip()
        if not s: return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default

def parse_int_list(s: str) -> int:
    if s is None or str(s).strip() == "": return 0
    raw = str(s).replace(",", " ").replace("+", " ")
    parts = [p for p in raw.split() if p.strip()]
    return sum(safe_int(p, 0) for p in parts)

def get_spend_sum(form, prefix: str, geo: str) -> int:
    values = form.getlist(f"{prefix}_spend_{geo}") or form.getlist(f"spend_{geo}")
    if not values: return 0
    total = 0
    for v in values:
        if any(ch in str(v) for ch in [",", "+", " "]):
            total += parse_int_list(v)
        else:
            total += safe_int(v, 0)
    return total

def get_deps(form, prefix: str, geo: str) -> int:
    val = form.get(f"{prefix}_deps_{geo}") or form.get(f"deps_{geo}")
    return safe_int(val, 0)

def fetch_existing_map(user: str, sel_date: str, vertical: str):
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT geo, spend, deps
        FROM records
        WHERE user=? AND date=? AND vertical=?
    """, (user, sel_date, vertical)).fetchall()
    conn.close()
    return {r["geo"]: {"spend": int(r["spend"] or 0), "deps": int(r["deps"] or 0)} for r in rows}

def distinct_users():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    db_users = [r["user"] for r in conn.execute("SELECT DISTINCT user FROM records").fetchall()]
    conn.close()
    return sorted(set(ALLOWED_USERS).union(db_users))

def utc_to_msk(ts: str | None) -> str:
    if not ts: return "—"
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
        msk = dt.astimezone(ZoneInfo("Europe/Moscow"))
        return msk.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts

# ==================== Auth ====================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        if username in ALLOWED_USERS:
            session["user"] = username
            session["role"] = ROLE_MAP.get(username, "BUYER")
            return redirect(url_for("panel"))
        return "Access denied"
    return render_template_string(LOGIN_TEMPLATE)

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))

# ==================== Panel ====================
@app.route("/panel", methods=["GET"])
def panel():
    if "user" not in session: return redirect(url_for("login"))
    user = session["user"]
    role = session.get("role", "BUYER")
    selected_date = request.args.get("date") or str(date.today())

    existing_slots = fetch_existing_map(user, selected_date, "Slots")
    existing_crash = fetch_existing_map(user, selected_date, "Crash")

    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT MAX(updated_at) AS last
        FROM records WHERE user=? AND date=?
    """, (user, selected_date)).fetchone()
    conn.close()
    last_saved_msk = utc_to_msk(row["last"] if row else None)

    sorted_slots = dict(sorted(CPA_SLOTS.items(), key=lambda kv: kv[0]))
    sorted_crash = dict(sorted(CPA_CRASH.items(), key=lambda kv: kv[0]))

    return render_template_string(
        PANEL_TEMPLATE,
        user=user, role=role, selected_date=selected_date,
        cpa_slots=sorted_slots, cpa_crash=sorted_crash, flags=FLAGS,
        existing_slots=existing_slots, existing_crash=existing_crash,
        last_saved_msk=last_saved_msk
    )

# Сохраняем СРАЗУ обе вертикали
@app.route("/save", methods=["POST"])
def save():
    if "user" not in session: return redirect(url_for("login"))
    user = session["user"]
    selected_date = request.form.get("selected_date") or str(date.today())
    next_dest = request.form.get("next", "panel")  # panel|dashboard
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    ensure_daily_backup()

    rows = []

    for geo, cpa in CPA_SLOTS.items():
        if cpa is None:  # не лить
            continue
        spend = get_spend_sum(request.form, "Slots", geo)
        deps  = get_deps(request.form, "Slots", geo)
        revenue = deps * int(cpa)
        profit  = revenue - spend
        rows.append((user, selected_date, geo, "Slots", spend, deps, revenue, profit, now_ts, now_ts))

    for geo, cpa in CPA_CRASH.items():
        if cpa is None:
            continue
        spend = get_spend_sum(request.form, "Crash", geo)
        deps  = get_deps(request.form, "Crash", geo)
        revenue = deps * int(cpa)
        profit  = revenue - spend
        rows.append((user, selected_date, geo, "Crash", spend, deps, revenue, profit, now_ts, now_ts))

    success = False
    if rows:
        try:
            conn = sqlite3.connect(DB_PATH)
            with conn:
                conn.executemany("""
                    INSERT INTO records (user, date, geo, vertical, spend, deps, revenue, profit, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user, date, geo, vertical) DO UPDATE SET
                        spend=excluded.spend,
                        deps=excluded.deps,
                        revenue=excluded.revenue,
                        profit=excluded.profit,
                        updated_at=excluded.updated_at
                """, rows)
            success = True
        except Exception:
            success = False

    if next_dest == "dashboard":
        return redirect(url_for("dashboard", start_date=selected_date, end_date=selected_date, saved=int(success)))
    return redirect(url_for("panel", date=selected_date, saved=int(success)))

# ==================== Dashboard ====================
@app.route("/dashboard", methods=["GET", "POST"])
def dashboard():
    if "user" not in session: return redirect(url_for("login"))
    session_user = session["user"]
    role = session.get("role", "BUYER")

    if request.method == "POST":
        start_date = request.form.get("start_date") or str(date.today())
        end_date   = request.form.get("end_date") or str(date.today())
        selected_user = request.form.get("selected_user")
    else:
        start_date = request.args.get("start_date") or str(date.today())
        end_date   = request.args.get("end_date") or str(date.today())
        selected_user = request.args.get("selected_user")

    if role == "TEAM_LEAD":
        view_user = selected_user or "ALL"
        user_options = distinct_users()
    else:
        view_user = session_user
        user_options = None

    params = [start_date, end_date]
    where = "date>=? AND date<=?"
    if view_user != "ALL":
        where = "user=? AND " + where
        params = [view_user] + params

    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row

    # by vertical (без deps — посчитаем из geo)
    cur1 = conn.execute(f"""
        SELECT vertical,
               SUM(spend)   AS spend,
               SUM(revenue) AS revenue,
               SUM(profit)  AS profit
        FROM records
        WHERE {where}
        GROUP BY vertical
    """, params)
    by_vert = {row["vertical"]: dict(row) for row in cur1.fetchall()}

    # vertical + geo (включая deps)
    cur2 = conn.execute(f"""
        SELECT vertical, geo,
               SUM(spend)   AS spend,
               SUM(revenue) AS revenue,
               SUM(profit)  AS profit,
               SUM(deps)    AS deps
        FROM records
        WHERE {where}
        GROUP BY vertical, geo
        ORDER BY vertical, geo
    """, params)
    by_vert_geo = {}
    for r in cur2.fetchall():
        v = r["vertical"]
        by_vert_geo.setdefault(v, []).append(dict(r))

    # totals
    cur3 = conn.execute(f"""
        SELECT SUM(spend) AS spend,
               SUM(revenue) AS revenue,
               SUM(profit) AS profit
        FROM records
        WHERE {where}
    """, params)
    total = dict(cur3.fetchone())

    # totals by geo + deps
    cur4 = conn.execute(f"""
        SELECT geo,
               SUM(spend)   AS spend,
               SUM(revenue) AS revenue,
               SUM(profit)  AS profit,
               SUM(deps)    AS deps
        FROM records
        WHERE {where}
        GROUP BY geo
        ORDER BY geo
    """, params)
    total_by_geo = [dict(r) for r in cur4.fetchall()]

    # breakdown by day + last save (MSK)
    cur5 = conn.execute(f"""
        SELECT date,
               SUM(spend)   AS spend,
               SUM(revenue) AS revenue,
               SUM(profit)  AS profit,
               MAX(updated_at) AS last
        FROM records
        WHERE {where}
        GROUP BY date
        ORDER BY date
    """, params)
    by_day = []
    for r in cur5.fetchall():
        item = dict(r)
        item["last_msk"] = utc_to_msk(item.get("last"))
        by_day.append(item)

    # deps by day
    cur_deps = conn.execute(f"""
        SELECT date, SUM(deps) AS deps
        FROM records
        WHERE {where}
        GROUP BY date
        ORDER BY date
    """, params)
    deps_map_total = {r["date"]: int(r["deps"] or 0) for r in cur_deps.fetchall()}

    # per-vertical by day (Slots/Crash)
    cur6 = conn.execute(f"""
        SELECT date, vertical,
               SUM(spend)   AS spend,
               SUM(revenue) AS revenue,
               SUM(profit)  AS profit,
               SUM(deps)    AS deps
        FROM records
        WHERE {where}
        GROUP BY date, vertical
        ORDER BY date, vertical
    """, params)
    vert_day = {}
    for r in cur6.fetchall():
        vert_day.setdefault(r["vertical"], {})[r["date"]] = dict(r)

    labels = [d["date"] for d in by_day]  # общая шкала дат

    def build_ts(day_map):
        spend_ts, profit_ts, deps_ts, cac_ts, roi_ts = [], [], [], [], []
        for lab in labels:
            row = day_map.get(lab)
            s = int(row.get("spend", 0)) if row else 0
            p = int(row.get("profit", 0)) if row else 0
            rv = int(row.get("revenue", 0)) if row else 0
            dp = int(row.get("deps", 0)) if row else 0
            spend_ts.append(s)
            profit_ts.append(p)
            deps_ts.append(dp)
            cac_ts.append((s / dp) if dp > 0 else None)
            roi_ts.append(((rv - s) * 100.0 / s) if s > 0 else None)
        return spend_ts, profit_ts, deps_ts, cac_ts, roi_ts

    # TOTAL series
    total_map = {d["date"]: {"spend": d["spend"], "profit": d["profit"], "revenue": d["revenue"], "deps": deps_map_total.get(d["date"], 0)} for d in by_day}
    t_spend, t_profit, t_deps, t_cac, t_roi = build_ts(total_map)

    # Slots / Crash series
    s_spend, s_profit, s_deps, s_cac, s_roi = build_ts(vert_day.get("Slots", {}))
    c_spend, c_profit, c_deps, c_cac, c_roi = build_ts(vert_day.get("Crash", {}))

    conn.close()

    return render_template_string(
        DASHBOARD_TEMPLATE,
        role=role, session_user=session_user, view_user=view_user, user_options=user_options,
        start_date=start_date, end_date=end_date, flags=FLAGS,
        by_vert=by_vert, by_vert_geo=by_vert_geo, total=total, total_by_geo=total_by_geo,
        by_day=by_day,
        labels=json.dumps(labels),
        ts_total=json.dumps({"spend": t_spend, "profit": t_profit, "deps": t_deps, "cac": t_cac, "roi": t_roi}),
        ts_slots=json.dumps({"spend": s_spend, "profit": s_profit, "deps": s_deps, "cac": s_cac, "roi": s_roi}),
        ts_crash=json.dumps({"spend": c_spend, "profit": c_profit, "deps": c_deps, "cac": c_cac, "roi": c_roi}),
    )

# ==================== Export CSV ====================
@app.route("/export_csv")
def export_csv():
    if "user" not in session:
        return redirect(url_for("login"))

    start = request.args.get("start") or str(date.today())
    end   = request.args.get("end")   or str(date.today())
    user  = request.args.get("user")  or session["user"]

    params = [start, end]
    where = "date>=? AND date<=?"
    if user != "ALL":
        where = "user=? AND " + where
        params = [user] + params

    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    rows = conn.execute(f"""
        SELECT user, date, vertical, geo, spend, deps, revenue, profit, updated_at
        FROM records
        WHERE {where}
        ORDER BY date, user, vertical, geo
    """, params).fetchall()
    conn.close()

    out = ["user,date,vertical,geo,spend,deps,revenue,profit,updated_at"]
    for r in rows:
        out.append("{user},{date},{vertical},{geo},{spend},{deps},{revenue},{profit},{updated_at}".format(
            user=r["user"], date=r["date"], vertical=r["vertical"], geo=r["geo"],
            spend=int(r["spend"] or 0), deps=int(r["deps"] or 0),
            revenue=int(r["revenue"] or 0), profit=int(r["profit"] or 0),
            updated_at=r["updated_at"] or ""
        ))
    csv_data = "\n".join(out)

    filename = f"report_{user}_{start}_{end}.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# ==================== Health/Backup ====================
@app.route("/health")
def health():
    return "ok", 200

@app.route("/backup")
def backup_download():
    if not os.path.exists(DB_PATH):
        return "No DB yet", 404
    return send_file(DB_PATH, as_attachment=True)

# ==================== Templates ====================
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"><title>Login</title>
<style>
body { font-family: Inter, Arial, sans-serif; background:#f6f7fb; display:flex; height:100vh; align-items:center; justify-content:center; }
.card { background:white; padding:28px; border-radius:16px; box-shadow: 0 10px 30px rgba(0,0,0,.08); width: min(360px, 92vw); }
h2 { margin:0 0 12px; }
input, button { width:100%; padding:12px 14px; border-radius:10px; border:1px solid #ddd; }
button { background:#2563eb; color:white; border:none; margin-top:10px; cursor:pointer; }
button:hover { filter:brightness(.97); }
</style>
</head>
<body>
  <div class="card">
    <h2>Вход</h2>
    <form method="post">
      <input name="username" placeholder="Логин">
      <button type="submit">Войти</button>
    </form>
  </div>
</body></html>
"""

PANEL_TEMPLATE = """
<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>Панель</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js" crossorigin="anonymous"></script>
<style>
:root { --bg:#f6f7fb; --card:#fff; --primary:#2563eb; --text:#0f172a; --muted:#6b7280; --pos:#0a8a0a; --neg:#c1121f; --active:#16a34a; }
body { font-family: Inter, Arial, sans-serif; background:var(--bg); margin: 14px; color:var(--text); }
.header, .card { background:var(--card); border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.06); padding:12px 14px; }
.header { display:flex; gap:12px; align-items:center; margin-bottom:12px; flex-wrap:wrap; }
.header .spacer { flex:1; }
.btn { padding:9px 14px; border-radius:12px; border:1px solid #d1d5db; background:#fff; cursor:pointer; color:#111827; }
.btn.primary { background:var(--primary); color:white; border:none; }
.btn.tab { background:#fff; color:#111; border:1px solid #d1d5db; }
.btn.tab.active { background:var(--active); color:#fff; border:none; }
.btn:hover { filter:brightness(.97); }
table { border-collapse: collapse; width: 100%; overflow:hidden; border-radius:12px; display:block; overflow-x:auto; }
th, td { border: 1px solid #eef2f7; padding: 8px 10px; text-align: center; white-space:nowrap; }
th { background: #f0f3fa; }
tbody tr:nth-child(odd) { background:#fafbfe; }
td:first-child, th:first-child { text-align: left; }
input[type=number] { width: 120px; max-width:42vw; padding:10px; border-radius:10px; border:1px solid #e5e7eb; }
.tabs { margin: 8px 0 12px; display:flex; gap:8px; flex-wrap:wrap; }
.hidden { display:none; }
.cpa-muted { color:#334155; font-weight:600; }
.spends { display:flex; gap:6px; flex-wrap:wrap; }
.flag { margin-right:6px; }
.addbtn { padding:4px 10px; border-radius:10px; border:1px solid #d1d5db; background:#fff; cursor:pointer; }
.meta { color:var(--muted); }
.capture { margin-left:8px; }
.selected-label { font-weight:600; color:#111; }
.toast {
  position: fixed; left: 50%; bottom: 16px; transform: translateX(-50%) translateY(10px);
  background: #16a34a; color: #fff; padding: 10px 14px;
  border-radius: 10px; box-shadow: 0 10px 30px rgba(0,0,0,.15);
  opacity: 0; transition: .2s; z-index: 9999;
}
.toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
.toast.error { background:#dc2626; }
@media (max-width: 640px) {
  .header { padding:10px; }
  .btn { padding:8px 12px; }
  input[type=number] { width: 100px; }
}
</style>
<script>
function savePng(domId, fileName){
  const node = document.getElementById(domId);
  html2canvas(node, {scale:2, backgroundColor:'#ffffff'}).then(canvas=>{
    const a=document.createElement('a');
    a.href=canvas.toDataURL('image/png'); a.download=fileName; a.click();
  });
}
function showTab(tabId, label){
  document.getElementById('sectionSlots').classList.add('hidden');
  document.getElementById('sectionCrash').classList.add('hidden');
  document.getElementById(tabId).classList.remove('hidden');
  document.getElementById('btnSlots').classList.remove('active');
  document.getElementById('btnCrash').classList.remove('active');
  if (tabId === 'sectionSlots') document.getElementById('btnSlots').classList.add('active');
  else document.getElementById('btnCrash').classList.add('active');
  document.getElementById('currentVert').textContent = label;
}
function addSpendInput(prefix, geo){
  const wrap = document.querySelector('[data-prefix="'+prefix+'"][data-geo="'+geo+'"] .spends');
  const inp = document.createElement('input');
  inp.type = 'number'; inp.name = prefix+'_spend_'+geo; inp.step='1'; inp.min='0'; inp.value='0';
  inp.inputMode = 'numeric';
  attachZeroHandlers(inp);
  wrap.appendChild(inp);
}
function attachZeroHandlers(el){
  el.addEventListener('focus', function(){ if (this.value === '0') this.value = ''; });
  el.addEventListener('blur', function(){ if (this.value.trim() === '') this.value = '0'; });
}
function initZeroInputs(){ document.querySelectorAll('input[type="number"]').forEach(attachZeroHandlers); }
function showToast(msg, isError=false){
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.toggle('error', !!isError);
  t.classList.add('show'); setTimeout(()=> t.classList.remove('show'), 2000);
}
function getQueryParam(name){ const u = new URL(window.location.href); return u.searchParams.get(name); }
window.addEventListener('DOMContentLoaded', () => {
  // автозагрузка по дате
  const df = document.getElementById('dateForm'); const di = document.getElementById('dateInput');
  di.addEventListener('change', ()=> df.submit());
  // стартовая вкладка
  showTab('sectionSlots', 'Выбрано: СЛОТС 🎰 7️⃣7️⃣7️⃣');
  initZeroInputs();
  // тост
  if (getQueryParam('saved') === '1') {
    showToast('Сохранено!');
    const url = new URL(window.location.href); url.searchParams.delete('saved');
    window.history.replaceState({}, '', url.toString());
  }
});
</script>
</head>
<body>

  <!-- Header -->
  <div class="header" id="panelCard">
    <div>
      <div style="font-weight:600;">Байер: {{user}} (роль: {{role}})</div>
      <div class="meta">Последнее сохранение (МСК): {{last_saved_msk}}</div>
      <div id="currentVert" class="selected-label">Выбрано:</div>
    </div>

    <form id="dateForm" method="get" action="/panel" style="display:flex; gap:6px; align-items:center;">
      <label>Дата:</label>
      <input class="btn" id="dateInput" type="date" name="date" value="{{selected_date}}">
    </form>

    <div class="spacer"></div>
    <a class="btn primary" href="/dashboard">Отчёт</a>
    <button class="btn" type="button" onclick="savePng('panelCard','panel_{{user}}_{{selected_date}}.png')">Скачать PNG</button>
    <form method="post" action="/logout">
      <button class="btn primary" type="submit">Выйти</button>
    </form>
  </div>

  <div class="tabs">
    <button class="btn tab" id="btnSlots" type="button" onclick="showTab('sectionSlots','Выбрано: СЛОТС 🎰 7️⃣7️⃣7️⃣')">СЛОТС 🎰</button>
    <button class="btn tab" id="btnCrash" type="button" onclick="showTab('sectionCrash','Выбрано: КРАШ 💥')">КРАШ 💥</button>
  </div>

  <!-- ЕДИНАЯ ФОРМА -->
  <form id="formAll" method="post" action="/save">
    <input type="hidden" name="selected_date" value="{{selected_date}}">

    <!-- СЛОТС -->
    <div class="card" id="sectionSlots">
      <table>
        <thead><tr><th>ГЕО</th><th>CPA, $</th><th>СПЕНД(ы)</th><th>ДЕПЫ</th></tr></thead>
        <tbody>
        {% for geo, cpa in cpa_slots.items() %}
          <tr data-prefix="Slots" data-geo="{{geo}}" {% if cpa == None %} style="background:#ffecec;" {% endif %}>
            <td style="text-align:left"><span class="flag">{{ flags.get(geo, '') }}</span>{{geo}}</td>
            {% if cpa == None %}
              <td class="cpa-muted">—</td><td colspan="2">Не лить</td>
            {% else %}
              <td class="cpa-muted">{{cpa}}</td>
              <td>
                <div class="spends">
                  <input name="Slots_spend_{{geo}}" type="number" step="1" min="0" value="{{ (existing_slots.get(geo, {}).get('spend', 0)) }}" inputmode="numeric">
                </div>
                <button type="button" class="addbtn" onclick="addSpendInput('Slots','{{geo}}')">+</button>
              </td>
              <td><input name="Slots_deps_{{geo}}" type="number" step="1" min="0" value="{{ (existing_slots.get(geo, {}).get('deps', 0)) }}" inputmode="numeric"></td>
            {% endif %}
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>

    <!-- КРАШ -->
    <div class="card hidden" id="sectionCrash">
      <table>
        <thead><tr><th>ГЕО</th><th>CPA, $</th><th>СПЕНД(ы)</th><th>ДЕПЫ</th></tr></thead>
        <tbody>
        {% for geo, cpa in cpa_crash.items() %}
          <tr data-prefix="Crash" data-geo="{{geo}}" {% if cpa == None %} style="background:#ffecec;" {% endif %}>
            <td style="text-align:left"><span class="flag">{{ flags.get(geo, '') }}</span>{{geo}}</td>
            {% if cpa == None %}
              <td class="cpa-muted">—</td><td colspan="2">Не лить</td>
            {% else %}
              <td class="cpa-muted">{{cpa}}</td>
              <td>
                <div class="spends">
                  <input name="Crash_spend_{{geo}}" type="number" step="1" min="0" value="{{ (existing_crash.get(geo, {}).get('spend', 0)) }}" inputmode="numeric">
                </div>
                <button type="button" class="addbtn" onclick="addSpendInput('Crash','{{geo}}')">+</button>
              </td>
              <td><input name="Crash_deps_{{geo}}" type="number" step="1" min="0" value="{{ (existing_crash.get(geo, {}).get('deps', 0)) }}" inputmode="numeric"></td>
            {% endif %}
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>

    <div style="display:flex;gap:10px;justify-content:flex-end;margin-top:12px;">
      <button class="btn primary" type="submit" name="next" value="panel">Сохранить</button>
      <button class="btn primary" type="submit" name="next" value="dashboard">Сохранить и рассчитать</button>
    </div>
  </form>

  <div id="toast" class="toast">Сохранено!</div>
</body></html>
"""

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>Дашборд</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js" crossorigin="anonymous"></script>
<style>
:root { --bg:#f6f7fb; --card:#fff; --primary:#2563eb; --text:#0f172a; --muted:#6b7280; --pos:#0a8a0a; --neg:#c1121f; --active:#1d4ed8; }
body { font-family: Inter, Arial, sans-serif; background:var(--bg); margin: 14px; color:var(--text); }
.header, .card { background:var(--card); border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.06); padding:12px 14px; }
.header { display:flex; gap:12px; align-items:center; margin-bottom:12px; flex-wrap:wrap; }
.header .spacer { flex:1; }
.btn { padding:9px 14px; border-radius:12px; border:1px solid #d1d5db; background:white; cursor:pointer; }
.btn.primary { background:var(--primary); color:white; border:none; }
.btn:hover { filter:brightness(.97); }
.btn.toggle.active { background:var(--active); color:#fff; }
.btn.ghost { background:#f3f4f6; border-color:#e5e7eb; color:#111827; }
.btn.ghost:hover { filter:brightness(.97); }
table { border-collapse: collapse; width: 100%; overflow:hidden; border-radius:12px; display:block; overflow-x:auto; }
th, td { border: 1px solid #eef2f7; padding: 8px 10px; text-align: right; white-space:nowrap; }
th { background: #f0f3fa; }
tbody tr:nth-child(odd) { background:#fafbfe; }
td:first-child, th:first-child { text-align: left; }
.roi-pos { color: var(--pos); font-weight: 600; }
.roi-neg { color: var(--neg); font-weight: 600; }
.geo-table { width: 100%; margin: 8px 0 8px 0; }
.flag { margin-right:6px; }
.meta { color:var(--muted); }
.controls { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
.quick { display:flex; gap:8px; flex-wrap:wrap; }
.toast {
  position: fixed; left: 50%; bottom: 16px; transform: translateX(-50%) translateY(10px);
  background: #16a34a; color: #fff; padding: 10px 14px;
  border-radius: 10px; box-shadow: 0 10px 30px rgba(0,0,0,.15);
  opacity: 0; transition: .2s; z-index: 9999;
}
.toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
.toast.error { background:#dc2626; }
@media (max-width: 640px) { th, td { padding: 7px 8px; } }
</style>
<script>
let currentGraphKey = null; // 'total' | 'slots' | 'crash'

function savePng(domId, fileName){
  const node = document.getElementById(domId);
  html2canvas(node, {scale:2, backgroundColor:'#ffffff'}).then(canvas=>{
    const a=document.createElement('a');
    a.href=canvas.toDataURL('image/png'); a.download=fileName; a.click();
  });
}
function showToast(msg, isError=false){
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.toggle('error', !!isError);
  t.classList.add('show'); setTimeout(()=> t.classList.remove('show'), 2000);
}
function getQueryParam(name){
  const url = new URL(window.location.href); return url.searchParams.get(name);
}
function buildChart(datasets, labels, title){
  const wrap = document.getElementById('chartWrap');
  wrap.style.display = 'block';
  document.getElementById('chartTitle').textContent = title;
  const ctx = document.getElementById('tsChart').getContext('2d');
  if (window._chart) window._chart.destroy();
  window._chart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive:true, interaction:{mode:'index', intersect:false},
      scales:{
        y:  { beginAtZero:true, title:{display:true, text:'USD / CAC'} },
        y1: { beginAtZero:true, title:{display:true, text:'ROI %'}, position:'right', grid:{drawOnChartArea:false} },
        y2: { beginAtZero:true, title:{display:true, text:'FTD (шт)'}, position:'right', grid:{drawOnChartArea:false} }
      }
    }
  });
}
function setActiveButton(key){
  ['btnGraphTotal','btnGraphSlots','btnGraphCrash'].forEach(id=>document.getElementById(id)?.classList.remove('active'));
  const map = {total:'btnGraphTotal', slots:'btnGraphSlots', crash:'btnGraphCrash'};
  if (map[key]) document.getElementById(map[key]).classList.add('active');
}
function onGraphButton(key, title){
  const labels = {{labels|safe}};
  const packs = {
    total: {{ts_total|safe}},
    slots: {{ts_slots|safe}},
    crash: {{ts_crash|safe}}
  };
  if (currentGraphKey === key){ // повторное нажатие — скрыть
    document.getElementById('chartWrap').style.display = 'none';
    currentGraphKey = null; setActiveButton(null); return;
  }
  currentGraphKey = key; setActiveButton(key);
  const ts = packs[key] || packs.total;
  buildChart([
    {label:'Spend',  data: ts.spend,  yAxisID:'y',  tension:0.3},
    {label:'Profit', data: ts.profit, yAxisID:'y',  tension:0.3},
    {label:'CAC',    data: ts.cac,    yAxisID:'y',  spanGaps:true, tension:0.3},
    {label:'FTD',    data: ts.deps,   yAxisID:'y2', tension:0.3, stepped:true},
    {label:'ROI %',  data: ts.roi,    yAxisID:'y1', tension:0.3}
  ], labels, title);
}

// Локальные ISO-даты, корректные для пресетов
function setRange(preset){
  const sd = document.querySelector('input[name="start_date"]');
  const ed = document.querySelector('input[name="end_date"]');
  const toLocalISO = (d) => {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  };
  const today = new Date();
  let startDate, endDate;

  if (preset === 'today') {
    startDate = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    endDate   = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  } else if (preset === 'yesterday') {
    const y = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1);
    startDate = y; endDate = y;
  } else if (preset === 'thisweek') {
    const d = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    const weekday = (d.getDay() + 6) % 7;   // Пн=0
    startDate = new Date(d.getFullYear(), d.getMonth(), d.getDate() - weekday);
    endDate   = d;
  } else if (preset === 'thismonth') {
    startDate = new Date(today.getFullYear(), today.getMonth(), 1);
    endDate   = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  } else if (preset === 'lastmonth') {
    const y = today.getFullYear(), m = today.getMonth();
    startDate = new Date(y, m - 1, 1);
    endDate   = new Date(y, m, 0);
  } else { return; }

  sd.value = toLocalISO(startDate);
  ed.value = toLocalISO(endDate);
  sd.form.submit();
}

window.addEventListener('DOMContentLoaded', () => {
  if (getQueryParam('saved') === '1') {
    showToast('Сохранено!');
    const url = new URL(window.location.href); url.searchParams.delete('saved');
    window.history.replaceState({}, '', url.toString());
  }
});
</script>
</head>
<body>

<div id="toast" class="toast">Сохранено!</div>

<!-- Весь отчёт (включая график) обёрнут для PNG -->
<div id="fullReport">

  <!-- Header -->
  <div class="header" id="dashCard">
    <div>
      <div style="font-weight:600;">Дашборд</div>
      {% if role == 'TEAM_LEAD' %}
        <div class="meta">Роль: Тим-лид. Отчёт по: {{ 'всем пользователям' if view_user=='ALL' else view_user }}</div>
      {% else %}
        <div class="meta">Роль: Байер ({{session_user}})</div>
      {% endif %}
    </div>

    <form method="post" class="controls">
      <span>С:</span> <input class="btn" type="date" name="start_date" value="{{start_date}}">
      <span>По:</span> <input class="btn" type="date" name="end_date" value="{{end_date}}">
      {% if role == 'TEAM_LEAD' %}
        <span>Пользователь:</span>
        <select class="btn" name="selected_user">
          <option value="ALL" {% if view_user=='ALL' %}selected{% endif %}>ALL (все)</option>
          {% for u in user_options %}
            <option value="{{u}}" {% if view_user==u %}selected{% endif %}>{{u}}</option>
          {% endfor %}
        </select>
      {% endif %}
      <button class="btn primary" type="submit">Показать</button>

      <!-- Кнопки скачивания -->
      <span style="width:14px"></span>
      <button class="btn ghost" type="button"
              onclick="savePng('fullReport','report_{{view_user}}_{{start_date}}_{{end_date}}.png')">
        📸 PNG
      </button>
      <a class="btn ghost" style="text-decoration:none; display:inline-flex; align-items:center"
         href="/export_csv?start={{start_date}}&end={{end_date}}&user={{view_user}}">
        📄 CSV
      </a>
    </form>

    <div class="quick">
      <button class="btn" type="button" onclick="setRange('today')">Сегодня</button>
      <button class="btn" type="button" onclick="setRange('yesterday')">Вчера</button>
      <button class="btn" type="button" onclick="setRange('thisweek')">Тек. неделя</button>
      <button class="btn" type="button" onclick="setRange('thismonth')">Тек. месяц</button>
      <button class="btn" type="button" onclick="setRange('lastmonth')">Прошлый месяц</button>
    </div>

    <div class="spacer"></div>

    <!-- Кнопки графиков (вынесены сверху) -->
    <button id="btnGraphTotal" class="btn toggle" type="button"
      onclick="onGraphButton('total', 'Динамика ({{start_date}} → {{end_date}}) — Итого за период')">График: Итого</button>
    <button id="btnGraphSlots" class="btn toggle" type="button"
      onclick="onGraphButton('slots', 'Динамика ({{start_date}} → {{end_date}}) — Результаты по Slots 🎰 7️⃣7️⃣7️⃣')">График: Slots</button>
    <button id="btnGraphCrash" class="btn toggle" type="button"
      onclick="onGraphButton('crash', 'Динамика ({{start_date}} → {{end_date}}) — Результаты по Crash 💥')">График: Crash</button>

    <form method="post" action="/logout">
      <button class="btn primary" type="submit">Выйти</button>
    </form>
  </div>

  <!-- Вертикали -->
  <div class="card">
    <h3 style="margin:0 0 10px;">Результаты по вертикалям</h3>
    <table>
      <thead>
        <tr>
          <th>Vertical</th>
          <th>Spend</th>
          <th>FTD</th>
          <th>CAC</th>
          <th>Revenue</th>
          <th>Profit</th>
          <th>ROI</th>
          <th>ГЕО</th>
        </tr>
      </thead>
      <tbody>
      {% for v, d in by_vert.items() %}
        {% set v_label = 'Slots 🎰 7️⃣7️⃣7️⃣' if v=='Slots' else 'Crash 💥' %}
        {% set spend = d.spend or 0 %}
        {% set revenue = d.revenue or 0 %}
        {% set profit = d.profit or 0 %}
        {% set v_deps = 0 %}
        {% for g in by_vert_geo.get(v, []) %}{% set v_deps = v_deps + (g.deps or 0) %}{% endfor %}
        {% set cac = (spend / v_deps) if v_deps>0 else None %}
        {% set roi = ((revenue - spend) / spend * 100) if spend>0 else None %}

        <tr>
          <td>{{v_label}}</td>
          <td>{{"%d"|format(spend)}}</td>
          <td>{{ v_deps }}</td>
          <td>{% if cac is not none %}{{ "%.2f"|format(cac) }}{% else %} — {% endif %}</td>
          <td>{{"%d"|format(revenue)}}</td>
          <td>{{"%d"|format(profit)}}</td>
          <td>{% if roi is not none %}<span class="{{ 'roi-pos' if roi>=0 else 'roi-neg' }}">{{ "%.1f%%"|format(roi) }}</span>{% else %} — {% endif %}</td>
          <td style="text-align:center"><button class="btn" type="button" onclick="const id='geo_tbl_{{v}}'; const el=document.getElementById(id); el.style.display=(el.style.display==='none'||el.style.display==='')?'table':'none';">Показать/Скрыть</button></td>
        </tr>

        <tr id="geo_tbl_{{v}}" style="display:none;">
          <td colspan="8" style="padding:0;">
            <table class="geo-table">
              <tr>
                <th style="text-align:left">GEO</th>
                <th>Spend</th>
                <th>FTD</th>
                <th>CAC</th>
                <th>Revenue</th>
                <th>Profit</th>
                <th>ROI</th>
              </tr>
              {% for g in by_vert_geo.get(v, []) %}
                {% set gsp = g.spend or 0 %}{% set grev = g.revenue or 0 %}{% set gpr = g.profit or 0 %}{% set gdep = g.deps or 0 %}
                {% set gcac = (gsp / gdep) if gdep>0 else None %}{% set groi = ((grev - gsp)/gsp*100) if gsp>0 else None %}
                {% set zero_row = (gsp==0 and grev==0 and gpr==0 and gdep==0) %}
                {% if not zero_row %}
                  <tr>
                    <td style="text-align:left"><span class="flag">{{ flags.get(g.geo, '') }}</span>{{g.geo}}</td>
                    <td>{{"%d"|format(gsp)}}</td>
                    <td>{{ gdep }}</td>
                    <td>{% if gcac is not none %}{{ "%.2f"|format(gcac) }}{% else %} — {% endif %}</td>
                    <td>{{"%d"|format(grev)}}</td>
                    <td>{{"%d"|format(gpr)}}</td>
                    <td>{% if groi is not none %}<span class="{{ 'roi-pos' if groi>=0 else 'roi-neg' }}">{{ "%.1f%%"|format(groi) }}</span>{% else %} — {% endif %}</td>
                  </tr>
                {% endif %}
              {% endfor %}
            </table>
          </td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <!-- ГРАФИК (теперь часть fullReport, чтобы попадал в PNG при открытии) -->
  <div class="card" id="chartWrap" style="display:none;">
    <h3 id="chartTitle" style="margin:0 0 10px;">Динамика</h3>
    <canvas id="tsChart" height="120"></canvas>
  </div>

  <!-- Итоги -->
  <div class="card">
    <h3 style="margin:0 0 10px;">Итого за период</h3>
    {% set t_spend = total.spend or 0 %}
    {% set t_rev   = total.revenue or 0 %}
    {% set t_profit= total.profit or 0 %}
    {% set t_deps = 0 %}
    {% for g in total_by_geo %}{% set t_deps = t_deps + (g.deps or 0) %}{% endfor %}
    {% set t_cac = (t_spend / t_deps) if t_deps>0 else None %}
    {% set t_roi = ((t_rev - t_spend)/t_spend*100) if t_spend>0 else None %}
    <table>
      <thead>
        <tr>
          <th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th>
          <th style="text-align:center">ГЕО (Slots 🎰 + Crash 💥)</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>{{"%d"|format(t_spend)}}</td>
          <td>{{ t_deps }}</td>
          <td>{% if t_cac is not none %}{{"%.2f"|format(t_cac)}}{% else %} — {% endif %}</td>
          <td>{{"%d"|format(t_rev)}}</td>
          <td>{{"%d"|format(t_profit)}}</td>
          <td>{% if t_roi is not none %}<span class="{{ 'roi-pos' if t_roi>=0 else 'roi-neg' }}">{{"%.1f%%"|format(t_roi)}}</span>{% else %} — {% endif %}</td>
          <td style="text-align:center"><button class="btn" type="button" onclick="const id='geo_total_tbl'; const el=document.getElementById(id); el.style.display=(el.style.display==='none'||el.style.display==='')?'table':'none';">Показать/Скрыть</button></td>
        </tr>
        <tr id="geo_total_tbl" style="display:none;">
          <td colspan="7" style="padding:0;">
            <table class="geo-table">
              <tr><th style="text-align:left">GEO</th><th>Spend</th><th>FTD</th><th>CAC</th><th>Revenue</th><th>Profit</th><th>ROI</th></tr>
              {% for g in total_by_geo %}
                {% set sp = g.spend or 0 %}{% set rv = g.revenue or 0 %}{% set pr = g.profit or 0 %}{% set dp = g.deps or 0 %}
                {% set cc = (sp/dp) if dp>0 else None %}{% set rr = ((rv - sp)/sp*100) if sp>0 else None %}
                {% set zero_row = (sp==0 and rv==0 and pr==0 and dp==0) %}
                {% if not zero_row %}
                  <tr>
                    <td style="text-align:left"><span class="flag">{{ flags.get(g.geo, '') }}</span>{{g.geo}}</td>
                    <td>{{"%d"|format(sp)}}</td>
                    <td>{{ dp }}</td>
                    <td>{% if cc is not none %}{{"%.2f"|format(cc)}}{% else %} — {% endif %}</td>
                    <td>{{"%d"|format(rv)}}</td>
                    <td>{{"%d"|format(pr)}}</td>
                    <td>{% if rr is not none %}<span class="{{ 'roi-pos' if rr>=0 else 'roi-neg' }}">{{"%.1f%%"|format(rr)}}</span>{% else %} — {% endif %}</td>
                  </tr>
                {% endif %}
              {% endfor %}
            </table>
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <!-- Breakdown -->
  <div class="card">
    <h3 style="margin:0 0 10px;">Разбивка по дням</h3>
    <table>
      <thead><tr><th>Дата</th><th>Spend</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>Последнее сохранение (МСК)</th></tr></thead>
      <tbody>
      {% for d in by_day %}
        {% set droi = ((d.revenue - d.spend) / d.spend * 100) if (d.spend and d.spend>0) else None %}
        <tr>
          <td>{{d.date}}</td>
          <td>{{"%d"|format(d.spend or 0)}}</td>
          <td>{{"%d"|format(d.revenue or 0)}}</td>
          <td>{{"%d"|format(d.profit or 0)}}</td>
          <td>{% if droi is not none %}<span class="{{ 'roi-pos' if droi>=0 else 'roi-neg' }}">{{ "%.1f%%"|format(droi) }}</span>{% else %} — {% endif %}</td>
          <td>{{ d.last_msk or "—" }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

</div> <!-- /#fullReport -->

<p><a class="btn primary" href="/panel">← Назад к панели</a></p>
</body></html>
"""

# ==================== Run ====================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
