import os
import sqlite3
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, redirect, url_for, render_template_string, session

app = Flask(__name__)
app.secret_key = "super_secret_key_change_me"
PORT = int(os.environ.get("PORT", 81))

# ===== Users & Roles =====
ALLOWED_USERS = {"Denis_b10", "Evgenii_b03", "Vlad_b22"}
ROLE_MAP = {
    "Evgenii_b03": "TEAM_LEAD",
    "Denis_b10": "BUYER",
    "Vlad_b22": "BUYER",
}

# ===== CPA tables =====
CPA_SLOTS = {
    "Australia": 180, "Austria": 300, "Canada": 180, "Belgium": 280, "Denmark": 300,
    "Ireland": 180, "Spain": 250, "Germany": 250, "Norway": 300, "Switzerland": 300,
    "Italy": 170, "Poland": 170, "France": 170, "Czech Republic": 170,
    "Greece": None, "Hungary": None, "Slovakia": 160, "Slovenia": 160, "Portugal": None
}
CPA_CRASH = {
    "Australia": 120, "Austria": 120, "Canada": 110, "Belgium": 120, "Denmark": 140,
    "Ireland": 110, "Spain": 110, "Germany": 135, "Norway": 130, "Switzerland": 140,
    "Italy": 130, "Poland": 100, "France": 100, "Czech Republic": 115,
    "Greece": None, "Hungary": None, "Slovakia": 100, "Slovenia": 100, "Portugal": None
}

# ===== Flags (emoji) =====
FLAGS = {
    "Australia": "🇦🇺", "Austria": "🇦🇹", "Canada":"🇨🇦", "Belgium":"🇧🇪", "Denmark":"🇩🇰",
    "Ireland":"🇮🇪", "Spain":"🇪🇸", "Germany":"🇩🇪", "Norway":"🇳🇴", "Switzerland":"🇨🇭",
    "Italy":"🇮🇹", "Poland":"🇵🇱", "France":"🇫🇷", "Czech Republic":"🇨🇿",
    "Greece":"🇬🇷", "Hungary":"🇭🇺", "Slovakia":"🇸🇰", "Slovenia":"🇸🇮", "Portugal":"🇵🇹"
}

# ===== DB init + migrations =====
DB_PATH = os.getenv("DATA_PATH", "data.db")
def init_db():
    conn = sqlite3.connect(DB_PATH)
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
init_db()

# ===== Utils =====
def safe_int(x, default=0):
    try:
        if x is None:
            return default
        s = str(x).strip()
        if not s:
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default

def parse_int_list(s: str) -> int:
    if s is None or str(s).strip() == "":
        return 0
    raw = str(s).replace(",", " ").replace("+", " ")
    parts = [p for p in raw.split() if p.strip()]
    total = 0
    for p in parts:
        total += safe_int(p, 0)
    return total

def get_spend_sum(form, geo: str) -> int:
    values = form.getlist(f"spend_{geo}")
    if not values:
        return 0
    total = 0
    for v in values:
        if any(ch in str(v) for ch in [",", "+", " "]):
            total += parse_int_list(v)
        else:
            total += safe_int(v, 0)
    return total

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

# ===== Auth =====
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

# ===== Panel =====
@app.route("/panel", methods=["GET"])
def panel():
    if "user" not in session: return redirect(url_for("login"))
    user = session["user"]
    role = session.get("role", "BUYER")
    selected_date = request.args.get("date") or str(date.today())
    existing_slots = fetch_existing_map(user, selected_date, "Slots")
    existing_crash = fetch_existing_map(user, selected_date, "Crash")

    # последнее сохранение (UTC->MSK) по этому юзеру и дате
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT MAX(updated_at) AS last
        FROM records WHERE user=? AND date=?
    """, (user, selected_date)).fetchone()
    conn.close()
    last_saved_msk = utc_to_msk(row["last"] if row else None)

    return render_template_string(
        PANEL_TEMPLATE,
        user=user, role=role, selected_date=selected_date,
        cpa_slots=CPA_SLOTS, cpa_crash=CPA_CRASH, flags=FLAGS,
        existing_slots=existing_slots, existing_crash=existing_crash,
        last_saved_msk=last_saved_msk
    )

# ===== Save (UPSERT) =====
@app.route("/save", methods=["POST"])
def save():
    if "user" not in session: return redirect(url_for("login"))
    user = session["user"]
    vertical = request.form.get("vertical", "Slots")
    selected_date = request.form.get("selected_date") or str(date.today())
    next_dest = request.form.get("next", "panel")  # panel|dashboard
    cpa_table = CPA_SLOTS if vertical == "Slots" else CPA_CRASH
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for geo, cpa in cpa_table.items():
        if cpa is None:  # не лить
            continue
        spend = get_spend_sum(request.form, geo)
        deps  = safe_int(request.form.get(f"deps_{geo}"), 0)
        revenue = deps * int(cpa)
        profit  = revenue - spend
        rows.append((user, selected_date, geo, vertical, spend, deps, revenue, profit, now_ts, now_ts))

    if rows:
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

    if next_dest == "dashboard":
        return redirect(url_for("dashboard", start_date=selected_date, end_date=selected_date))
    return redirect(url_for("panel", date=selected_date))

# ===== Dashboard =====
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

    # by vertical
    cur1 = conn.execute(f"""
        SELECT vertical, SUM(spend) AS spend, SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records
        WHERE {where}
        GROUP BY vertical
    """, params)
    by_vert = {row["vertical"]: dict(row) for row in cur1.fetchall()}

    # vertical + geo
    cur2 = conn.execute(f"""
        SELECT vertical, geo, SUM(spend) AS spend, SUM(revenue) AS revenue, SUM(profit) AS profit
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
        SELECT SUM(spend) AS spend, SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records
        WHERE {where}
    """, params)
    total = dict(cur3.fetchone())

    # totals by geo (slots+crash)
    cur4 = conn.execute(f"""
        SELECT geo, SUM(spend) AS spend, SUM(revenue) AS revenue, SUM(profit) AS profit
        FROM records
        WHERE {where}
        GROUP BY geo
        ORDER BY geo
    """, params)
    total_by_geo = [dict(r) for r in cur4.fetchall()]

    # breakdown by day + last save (MSK)
    cur5 = conn.execute(f"""
        SELECT date, SUM(spend) AS spend, SUM(revenue) AS revenue, SUM(profit) AS profit,
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

    # time series for chart
    labels = [d["date"] for d in by_day]
    spend_ts = [int(d["spend"] or 0) for d in by_day]
    profit_ts = [int(d["profit"] or 0) for d in by_day]
    roi_ts = []
    for d in by_day:
        s = int(d["spend"] or 0)
        if s > 0:
            roi_ts.append((int(d["revenue"] or 0) - s) * 100.0 / s)
        else:
            roi_ts.append(None)

    conn.close()

    return render_template_string(
        DASHBOARD_TEMPLATE,
        role=role, session_user=session_user, view_user=view_user, user_options=user_options,
        start_date=start_date, end_date=end_date, flags=FLAGS,
        by_vert=by_vert, by_vert_geo=by_vert_geo, total=total, total_by_geo=total_by_geo,
        by_day=by_day,
        labels=json.dumps(labels), spend_ts=json.dumps(spend_ts),
        profit_ts=json.dumps(profit_ts), roi_ts=json.dumps(roi_ts)
    )

# ===== Templates =====
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"><title>Login</title>
<style>
body { font-family: Inter, Arial, sans-serif; background:#f6f7fb; display:flex; height:100vh; align-items:center; justify-content:center; }
.card { background:white; padding:28px; border-radius:16px; box-shadow: 0 10px 30px rgba(0,0,0,.08); width: 360px; }
h2 { margin:0 0 12px; }
input, button { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #ddd; }
button { background:#1d4ed8; color:white; border:none; margin-top:10px; cursor:pointer; }
button:hover { background:#1643b7; }
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js" crossorigin="anonymous"></script>
<style>
:root { --bg:#f6f7fb; --card:#fff; --primary:#1d4ed8; --text:#0f172a; --muted:#6b7280; --pos:#0a8a0a; --neg:#c1121f; }
body { font-family: Inter, Arial, sans-serif; background:var(--bg); margin: 24px; color:var(--text); }
.header, .card { background:var(--card); border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.06); padding:16px 20px; }
.header { display:flex; gap:16px; align-items:center; margin-bottom:16px; }
.header .spacer { flex:1; }
.btn { padding:9px 14px; border-radius:12px; border:1px solid #d1d5db; background:white; cursor:pointer; }
.btn.primary { background:var(--primary); color:white; border:none; }
.btn:hover { filter:brightness(.97); }
table { border-collapse: collapse; width: 100%; overflow:hidden; border-radius:12px; }
th, td { border: 1px solid #eef2f7; padding: 8px 10px; text-align: center; }
th { background: #f0f3fa; }
tbody tr:nth-child(odd) { background:#fafbfe; }
td:first-child, th:first-child { text-align: left; }
input[type=number] { width: 120px; padding:8px; border-radius:10px; border:1px solid #e5e7eb; }
.tabs { margin: 12px 0 20px; display:flex; gap:8px; }
.hidden { display:none; }
.cpa-muted { color:#334155; font-weight:600; }
.spends { display:flex; gap:6px; flex-wrap:wrap; }
.flag { margin-right:6px; }
.addbtn { padding:4px 10px; border-radius:10px; border:1px solid #d1d5db; background:#fff; cursor:pointer; }
.meta { color:var(--muted); }
.capture { margin-left:8px; }
</style>
<script>
function showTab(tabId) {
  document.getElementById('formSlots').classList.add('hidden');
  document.getElementById('formCrash').classList.add('hidden');
  document.getElementById(tabId).classList.remove('hidden');
}
function addSpendInput(geo, formId){
  const form = document.getElementById(formId);
  const wrap = form.querySelector('[data-geo="'+geo+'"] .spends');
  const inp = document.createElement('input');
  inp.type = 'number'; inp.name = 'spend_'+geo; inp.step='1'; inp.min='0'; inp.value='0'; inp.style.width='120px';
  wrap.appendChild(inp);
}
function savePng(domId, fileName){
  const node = document.getElementById(domId);
  html2canvas(node, {scale:2, backgroundColor:'#ffffff'}).then(canvas=>{
    const a=document.createElement('a');
    a.href=canvas.toDataURL('image/png'); a.download=fileName; a.click();
  });
}
window.addEventListener('DOMContentLoaded', () => showTab('formSlots'));
</script>
</head>
<body>
  <div class="header" id="panelCard">
    <div>
      <div style="font-weight:600;">Байер: {{user}} (роль: {{role}})</div>
      <div class="meta">Последнее сохранение (МСК): {{last_saved_msk}}</div>
    </div>
    <form method="get" action="/panel">
      <label>Дата:</label>
      <input class="btn" type="date" name="date" value="{{selected_date}}">
      <button class="btn primary" type="submit">Загрузить</button>
    </form>
    <div class="spacer"></div>
    <a class="btn" href="/dashboard">Отчёт</a>
    <button class="btn capture" onclick="savePng('panelCard','panel_{{user}}_{{selected_date}}.png')">Скачать PNG</button>
    <form method="post" action="/logout">
      <button class="btn" type="submit">Выйти</button>
    </form>
  </div>

  <div class="tabs">
    <button class="btn" type="button" onclick="showTab('formSlots')">СЛОТС</button>
    <button class="btn" type="button" onclick="showTab('formCrash')">КРАШ</button>
  </div>

  <!-- СЛОТС -->
  <div class="card">
  <form id="formSlots" method="post" action="/save">
    <input type="hidden" name="vertical" value="Slots">
    <input type="hidden" name="selected_date" value="{{selected_date}}">
    <table>
      <thead><tr><th>ГЕО</th><th>CPA, $</th><th>СПЕНД(ы)</th><th>ДЕПЫ</th></tr></thead>
      <tbody>
      {% for geo, cpa in cpa_slots.items() %}
        <tr data-geo="{{geo}}" {% if cpa == None %} style="background:#ffecec;" {% endif %}>
          <td style="text-align:left"><span class="flag">{{ flags.get(geo, '') }}</span>{{geo}}</td>
          {% if cpa == None %}
            <td class="cpa-muted">—</td><td colspan="2">Не лить</td>
          {% else %}
            <td class="cpa-muted">{{cpa}}</td>
            <td>
              <div class="spends">
                <input name="spend_{{geo}}" type="number" step="1" min="0" value="{{ (existing_slots.get(geo, {}).get('spend', 0)) }}">
              </div>
              <button type="button" class="addbtn" onclick="addSpendInput('{{geo}}','formSlots')">+</button>
            </td>
            <td><input name="deps_{{geo}}" type="number" step="1" min="0" value="{{ (existing_slots.get(geo, {}).get('deps', 0)) }}"></td>
          {% endif %}
        </tr>
      {% endfor %}
      </tbody>
    </table>
    <div style="display:flex;gap:10px;justify-content:flex-end;margin-top:12px;">
      <button class="btn" type="submit" name="next" value="panel">Сохранить</button>
      <button class="btn primary" type="submit" name="next" value="dashboard">Сохранить и рассчитать</button>
    </div>
  </form>
  </div>

  <!-- КРАШ -->
  <div class="card">
  <form id="formCrash" class="hidden" method="post" action="/save">
    <input type="hidden" name="vertical" value="Crash">
    <input type="hidden" name="selected_date" value="{{selected_date}}">
    <table>
      <thead><tr><th>ГЕО</th><th>CPA, $</th><th>СПЕНД(ы)</th><th>ДЕПЫ</th></tr></thead>
      <tbody>
      {% for geo, cpa in cpa_crash.items() %}
        <tr data-geo="{{geo}}" {% if cpa == None %} style="background:#ffecec;" {% endif %}>
          <td style="text-align:left"><span class="flag">{{ flags.get(geo, '') }}</span>{{geo}}</td>
          {% if cpa == None %}
            <td class="cpa-muted">—</td><td colspan="2">Не лить</td>
          {% else %}
            <td class="cpa-muted">{{cpa}}</td>
            <td>
              <div class="spends">
                <input name="spend_{{geo}}" type="number" step="1" min="0" value="{{ (existing_crash.get(geo, {}).get('spend', 0)) }}">
              </div>
              <button type="button" class="addbtn" onclick="addSpendInput('{{geo}}','formCrash')">+</button>
            </td>
            <td><input name="deps_{{geo}}" type="number" step="1" min="0" value="{{ (existing_crash.get(geo, {}).get('deps', 0)) }}"></td>
          {% endif %}
        </tr>
      {% endfor %}
      </tbody>
    </table>
    <div style="display:flex;gap:10px;justify-content:flex-end;margin-top:12px;">
      <button class="btn" type="submit" name="next" value="panel">Сохранить</button>
      <button class="btn primary" type="submit" name="next" value="dashboard">Сохранить и рассчитать</button>
    </div>
  </form>
  </div>
</body></html>
"""

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>Дашборд</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js" crossorigin="anonymous"></script>
<style>
:root { --bg:#f6f7fb; --card:#fff; --primary:#1d4ed8; --text:#0f172a; --muted:#6b7280; --pos:#0a8a0a; --neg:#c1121f; }
body { font-family: Inter, Arial, sans-serif; background:var(--bg); margin: 24px; color:var(--text); }
.header, .card { background:var(--card); border-radius:16px; box-shadow:0 10px 30px rgba(0,0,0,.06); padding:16px 20px; }
.header { display:flex; gap:12px; align-items:center; margin-bottom:16px; }
.header .spacer { flex:1; }
.btn { padding:9px 14px; border-radius:12px; border:1px solid #d1d5db; background:white; cursor:pointer; }
.btn.primary { background:var(--primary); color:white; border:none; }
.btn:hover { filter:brightness(.97); }
table { border-collapse: collapse; width: 100%; overflow:hidden; border-radius:12px; }
th, td { border: 1px solid #eef2f7; padding: 8px 10px; text-align: right; }
th { background: #f0f3fa; }
tbody tr:nth-child(odd) { background:#fafbfe; }
td:first-child, th:first-child { text-align: left; }
.roi-pos { color: var(--pos); font-weight: 600; }
.roi-neg { color: var(--neg); font-weight: 600; }
.geo-table { width: 100%; margin: 8px 0 8px 0; }
.flag { margin-right:6px; }
.meta { color:var(--muted); }
.controls { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
.right-cell { text-align:center; white-space:nowrap; }
.card + .card { margin-top:16px; }
</style>
<script>
function toggle(id){
  const el = document.getElementById(id);
  el.style.display = (el.style.display === 'none' || el.style.display === '') ? 'table' : 'none';
}
function toggleBlock(id){
  const el = document.getElementById(id);
  el.style.display = (el.style.display === 'none' || el.style.display === '') ? 'block' : 'none';
}
function savePng(domId, fileName){
  const node = document.getElementById(domId);
  html2canvas(node, {scale:2, backgroundColor:'#ffffff'}).then(canvas=>{
    const a=document.createElement('a');
    a.href=canvas.toDataURL('image/png'); a.download=fileName; a.click();
  });
}
window.addEventListener('DOMContentLoaded', () => {
  const ctx = document.getElementById('tsChart');
  if (!ctx) return;
  const labels = {{labels|safe}};
  const spend = {{spend_ts|safe}};
  const profit = {{profit_ts|safe}};
  const roi = {{roi_ts|safe}};

  new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        {label:'Spend', data: spend, yAxisID:'y', tension:0.3},
        {label:'Profit', data: profit, yAxisID:'y', tension:0.3},
        {label:'ROI %', data: roi, yAxisID:'y1', tension:0.3}
      ]
    },
    options: {
      responsive:true,
      interaction:{mode:'index', intersect:false},
      scales:{
        y:{ beginAtZero:true, title:{display:true, text:'USD'} },
        y1:{ beginAtZero:true, title:{display:true, text:'ROI %'}, position:'right', grid:{drawOnChartArea:false} }
      }
    }
  });
});
</script>
</head>
<body>

<div id="fullReport">

  <!-- Шапка -->
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
    </form>

    <div class="spacer"></div>
    <button class="btn" type="button" onclick="toggleBlock('chartWrap')">Показать график</button>
    <!-- Сохраняем ВЕСЬ fullReport -->
    <button class="btn" type="button" onclick="savePng('fullReport','dashboard_{{start_date}}_{{end_date}}.png')">Скачать PNG</button>
    <form method="post" action="/logout">
      <button class="btn" type="submit">Выйти</button>
    </form>
  </div>

  <!-- Карточка: результаты по вертикалям -->
  <div class="card">
    <h3 style="margin:0 0 10px;">Результаты по вертикалям</h3>
    <table>
      <thead>
        <tr><th>Vertical</th><th>Spend</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>ГЕО</th><th class="right-cell">График</th></tr>
      </thead>
      <tbody>
      {% for v, d in by_vert.items() %}
        {% set roi = ((d.revenue - d.spend) / d.spend * 100) if (d.spend and d.spend>0) else None %}
        <tr>
          <td>{{v}}</td>
          <td>{{"%d"|format(d.spend or 0)}}</td>
          <td>{{"%d"|format(d.revenue or 0)}}</td>
          <td>{{"%d"|format(d.profit or 0)}}</td>
          <td>{% if roi is not none %}<span class="{{ 'roi-pos' if roi>=0 else 'roi-neg' }}">{{ "%.1f%%"|format(roi) }}</span>{% else %} — {% endif %}</td>
          <td class="right-cell"><button class="btn" type="button" onclick="toggle('geo_tbl_{{v}}')">Показать/Скрыть</button></td>
          <td class="right-cell"><button class="btn" type="button" onclick="toggleBlock('chartWrap')">График</button></td>
        </tr>
        <tr id="geo_tbl_{{v}}" style="display:none;">
          <td colspan="7" style="padding:0;">
            <table class="geo-table">
              <tr><th style="text-align:left">GEO</th><th>Spend</th><th>Revenue</th><th>Profit</th><th>ROI</th></tr>
              {% for g in by_vert_geo.get(v, []) %}
                {% set zero_row = ( (g.spend or 0)==0 and (g.revenue or 0)==0 and (g.profit or 0)==0 ) %}
                {% if not zero_row %}
                  {% set groi = ((g.revenue - g.spend)/g.spend*100) if (g.spend and g.spend>0) else None %}
                  <tr>
                    <td style="text-align:left"><span class="flag">{{ flags.get(g.geo, '') }}</span>{{g.geo}}</td>
                    <td>{{"%d"|format(g.spend or 0)}}</td>
                    <td>{{"%d"|format(g.revenue or 0)}}</td>
                    <td>{{"%d"|format(g.profit or 0)}}</td>
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

  <!-- Карточка: Итого за период -->
  <div class="card">
    <h3 style="margin:0 0 10px;">Итого за период</h3>
    {% set troi = ((total.revenue - total.spend) / total.spend * 100) if (total.spend and total.spend>0) else None %}
    <table>
      <thead><tr><th>Spend</th><th>Revenue</th><th>Profit</th><th>ROI</th><th>ГЕО (сумма Slots+Crash)</th></tr></thead>
      <tbody>
      <tr>
        <td>{{"%d"|format(total.spend or 0)}}</td>
        <td>{{"%d"|format(total.revenue or 0)}}</td>
        <td>{{"%d"|format(total.profit or 0)}}</td>
        <td>{% if troi is not none %}<span class="{{ 'roi-pos' if troi>=0 else 'roi-neg' }}">{{ "%.1f%%"|format(troi) }}</span>{% else %} — {% endif %}</td>
        <td class="right-cell"><button class="btn" type="button" onclick="toggle('geo_total_tbl')">Показать/Скрыть</button></td>
      </tr>
      <tr id="geo_total_tbl" style="display:none;">
        <td colspan="5" style="padding:0;">
          <table class="geo-table">
            <tr><th style="text-align:left">GEO</th><th>Spend</th><th>Revenue</th><th>Profit</th><th>ROI</th></tr>
            {% for g in total_by_geo %}
              {% set zero_row = ( (g.spend or 0)==0 and (g.revenue or 0)==0 and (g.profit or 0)==0 ) %}
              {% if not zero_row %}
                {% set groi = ((g.revenue - g.spend)/g.spend*100) if (g.spend and g.spend>0) else None %}
                <tr>
                  <td style="text-align:left"><span class="flag">{{ flags.get(g.geo, '') }}</span>{{g.geo}}</td>
                  <td>{{"%d"|format(g.spend or 0)}}</td>
                  <td>{{"%d"|format(g.revenue or 0)}}</td>
                  <td>{{"%d"|format(g.profit or 0)}}</td>
                  <td>{% if groi is not none %}<span class="{{ 'roi-pos' if groi>=0 else 'roi-neg' }}">{{ "%.1f%%"|format(groi) }}</span>{% else %} — {% endif %}</td>
                </tr>
              {% endif %}
            {% endfor %}
          </table>
        </td>
      </tr>
      </tbody>
    </table>
  </div>

</div> <!-- /#fullReport -->

  <!-- Отдельный блок с графиком (по кнопке) -->
  <div class="card" id="chartWrap" style="display:none;">
    <h3 style="margin:0 0 10px;">Динамика ({{start_date}} → {{end_date}})</h3>
    <canvas id="tsChart" height="120"></canvas>
    <div style="text-align:right;margin-top:8px;">
      <button class="btn" onclick="savePng('chartWrap','chart_{{start_date}}_{{end_date}}.png')">Скачать график (PNG)</button>
    </div>
  </div>

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

  <p><a class="btn" href="/panel">← Назад к панели</a></p>
</body></html>
"""

# ===== Run =====
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
