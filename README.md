# Flask CPA Dashboard

Простой дашборд для байеров/тимлидов:
- Логин по списку пользователей (роль BUYER/TEAM_LEAD)
- Ввод спенда/депов по GEO (Slots/Crash)
- Расчёт Revenue/Profit/ROI
- Сохранение в SQLite (`data.db`)
- Дашборд, аккордеоны по GEO, график (Chart.js)
- Экспорт PNG (html2canvas)

## Локальный запуск
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export PORT=8000            # Windows: set PORT=8000
python main.py              # или gunicorn main:app -b 0.0.0.0:8000