# bot.py — v3.4: удобства + wipe + howto + ZIP-отчёт + retention + whitelist
import os, io, csv, json, sqlite3, datetime, zipfile, re, textwrap
from html.parser import HTMLParser
from dotenv import load_dotenv
from telegram import InputFile, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackQueryHandler, Filters, CallbackContext

# ---------- ENV / Config ----------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    print("Error: TELEGRAM_TOKEN not set in .env")
    exit(1)

DB_PATH = "bot_data.db"
PAGE_SIZE = 50  # сколько имён показывать за раз

# retention (опционально): удалить снимки старше N дней (например 60)
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "0"))  # 0 = выкл.

# приватный доступ (опционально): перечисли chat_id через запятую
_allowed = os.getenv("ALLOWED_CHAT_IDS", "").strip()
ALLOWED_CHAT_IDS = set(x.strip() for x in _allowed.split(",") if x.strip())

SESSION_TTL_MIN = 60  # сколько минут живёт незавершённая загрузка (following->followers)

# ---------- DB ----------
def conn_cur():
    c = sqlite3.connect(DB_PATH)
    return c, c.cursor()

def init_db():
    conn, c = conn_cur()
    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ts TEXT NOT NULL,
            following_json TEXT NOT NULL,
            followers_json TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()

def load_last_snapshot(user_id):
    conn, c = conn_cur()
    c.execute("SELECT id, ts, following_json, followers_json FROM snapshots WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    sid, ts, fwing, fwers = row
    return {
        "id": sid,
        "ts": ts,
        "following": set(json.loads(fwing)),
        "followers": set(json.loads(fwers))
    }

def save_snapshot(user_id, following_set, followers_set):
    # retention: почистим старьё для этого пользователя
    if RETENTION_DAYS > 0:
        cleanup_old_snapshots(user_id, RETENTION_DAYS)

    conn, c = conn_cur()
    ts = datetime.datetime.utcnow().isoformat()
    c.execute(
        "INSERT INTO snapshots (user_id, ts, following_json, followers_json) VALUES (?, ?, ?, ?)",
        (user_id, ts, json.dumps(sorted(following_set)), json.dumps(sorted(followers_set)))
    )
    conn.commit()
    conn.close()

def cleanup_old_snapshots(user_id, days):
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat()
    conn, c = conn_cur()
    c.execute("DELETE FROM snapshots WHERE user_id=? AND ts<?", (user_id, cutoff))
    conn.commit()
    conn.close()

def wipe_user_history(user_id):
    conn, c = conn_cur()
    c.execute("DELETE FROM snapshots WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def get_user_stats(user_id):
    conn, c = conn_cur()
    c.execute("SELECT COUNT(*), MAX(ts) FROM snapshots WHERE user_id=?", (user_id,))
    count, last_ts = c.fetchone()
    conn.close()
    return (count or 0), last_ts

# ---------- Access control ----------
def is_allowed_user(update: Update):
    if not ALLOWED_CHAT_IDS:
        return True
    uid = str(update.effective_user.id)
    return uid in ALLOWED_CHAT_IDS

def ensure_allowed(update: Update):
    if not is_allowed_user(update):
        update.effective_message.reply_text("⛔️ Доступ ограничён.")
        return False
    return True

# ---------- ZIP parsing ----------
class SimpleHTMLUserParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.users = set()
    def handle_data(self, data):
        s = data.strip()
        if s and len(s) <= 50 and (" " not in s or s.count(" ") <= 1):
            if re.match(r"^[A-Za-z0-9._]+$", s):
                self.users.add(s.lower())

def parse_zip_for_users(b: bytes):
    following = set()
    followers = set()
    with zipfile.ZipFile(io.BytesIO(b), 'r') as z:
        for name in z.namelist():
            lower = name.lower()
            try:
                raw = z.read(name)
            except:
                continue

            if lower.endswith(".json"):
                data = None
                for enc in ("utf-8", "latin-1"):
                    try:
                        data = json.loads(raw.decode(enc))
                        break
                    except:
                        pass
                if data is None:
                    continue

                def collect_from(obj, key_candidates):
                    result = set()
                    def walk(x):
                        if isinstance(x, dict):
                            for k,v in x.items():
                                kl = k.lower()
                                if kl in key_candidates and isinstance(v, list):
                                    for item in v:
                                        if isinstance(item, dict):
                                            u = item.get("username")
                                            if not u:
                                                sld = item.get("string_list_data")
                                                if isinstance(sld, list) and sld:
                                                    u = sld[0].get("value")
                                            if u:
                                                result.add(u.strip().lower())
                                walk(v)
                        elif isinstance(x, list):
                            for it in x:
                                walk(it)
                    walk(obj)
                    return result

                followers |= collect_from(data, {"followers", "relationships_followers"})
                following |= collect_from(data, {"following", "relationships_following"})

            elif lower.endswith(".html") or lower.endswith(".htm"):
                try:
                    html = raw.decode("utf-8", errors="ignore")
                    p = SimpleHTMLUserParser()
                    p.feed(html)
                    if "follower" in lower:
                        followers |= p.users
                    elif "following" in lower:
                        following |= p.users
                    else:
                        followers |= p.users
                        following |= p.users
                except:
                    continue
    return sorted(following), sorted(followers)

# ---------- Plain text parsing ----------
USERNAME_RE = re.compile(r'@?([A-Za-z0-9](?:[A-Za-z0-9._]{0,28}[A-Za-z0-9])?)')
def to_user_set(text: str):
    candidates = USERNAME_RE.findall(text)
    norm = []
    for c in candidates:
        u = c.strip('.').strip('_').lower()
        if 2 <= len(u) <= 30 and not u.isdigit():
            norm.append(u)
    seen, out = set(), []
    for u in norm:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return set(out)

# ---------- Pagination state ----------
store_lists = {}  # uid -> dict of lists for pagination
def chunk(lst, start, size): return lst[start:start+size]

def build_keyboard(prefix, start, total, extra_row=True):
    row = []
    next_start = start + PAGE_SIZE
    if next_start < total:
        row.append(InlineKeyboardButton(f"Ещё ({next_start}/{total})", callback_data=f"page|{prefix}|{next_start}"))
    row.append(InlineKeyboardButton("⬅️ К спискам", callback_data="menu"))
    rows = [row]
    if extra_row:
        rows.append([
            InlineKeyboardButton("📥 Скачать ZIP отчёт", callback_data="download_zip"),
            InlineKeyboardButton("ℹ️ Туториал", callback_data="howto"),
        ])
        rows.append([
            InlineKeyboardButton("🗑 Очистить историю", callback_data="ask_wipe"),
        ])
    return InlineKeyboardMarkup(rows)

def send_page(update: Update, context: CallbackContext, uid: int, list_key: str, start: int):
    data = store_lists.get(uid, {})
    items = data.get(list_key, [])
    total = len(items)
    page = chunk(items, start, PAGE_SIZE)
    if not page:
        update.effective_message.reply_text("Пока тут пусто.")
        return
    title_map = {
        "mutual": "🤝 Взаимные",
        "only_in_following": "➡️ Только в following",
        "only_in_followers": "⬅️ Только в followers",
        "new_followers": "🟢 Новые подписчики",
        "unfollowers": "🔴 Отписались",
        "new_following": "➕ Вы зафолловили",
        "unfollowed_by_you": "➖ Вы отписались",
        "new_mutuals": "✨ Стали взаимными",
        "lost_mutuals": "💔 Потеряли взаимность",
    }
    title = title_map.get(list_key, list_key)
    text = f"{title} ({start+1}-{min(start+PAGE_SIZE, total)} из {total}):\n" + "\n".join(page)
    kb = build_keyboard(list_key, start, total)
    update.effective_message.reply_text(text, reply_markup=kb)

def show_menu(update: Update, context: CallbackContext, uid: int):
    data = store_lists.get(uid, {})
    def btn(label, key):
        count = len(data.get(key, []))
        return InlineKeyboardButton(f"{label} ({count})", callback_data=f"page|{key}|0")
    row1 = [
        btn("🤝 Взаимные", "mutual"),
        btn("➡️ Только в following", "only_in_following"),
        btn("⬅️ Только в followers", "only_in_followers"),
    ]
    row2 = [
        btn("🟢 Новые подписчики", "new_followers"),
        btn("🔴 Отписались", "unfollowers"),
    ]
    row3 = [
        btn("➕ Вы зафолловили", "new_following"),
        btn("➖ Вы отписались", "unfollowed_by_you"),
    ]
    row4 = [
        btn("✨ Стали взаимными", "new_mutuals"),
        btn("💔 Потеряли взаимность", "lost_mutuals"),
    ]
    row5 = [
        InlineKeyboardButton("📥 Скачать ZIP отчёт", callback_data="download_zip"),
        InlineKeyboardButton("ℹ️ Туториал", callback_data="howto"),
    ]
    row6 = [
        InlineKeyboardButton("🗑 Очистить историю", callback_data="ask_wipe"),
    ]
    kb = InlineKeyboardMarkup([row1, row2, row3, row4, row5, row6])
    update.effective_message.reply_text("Выберите список:", reply_markup=kb)

# ---------- Bot texts ----------
HELP = (
"Пришлите *архив Instagram (.zip)* из «Download your information» — я сам извлеку списки.\n"
"Либо пришлите подряд два сообщения/файла: сначала *following*, затем *followers* (можно просто вставить текст из браузера — я извлеку никнеймы).\n"
"Покажу сводку и изменения, а подробные списки — по кнопкам.\n"
"/delete — сбросить незавершённую загрузку, /wipe — удалить историю, /stats — статистика, /howto — как получить списки."
)

HOWTO = textwrap.dedent("""
📚 КАК ПОЛУЧИТЬ СПИСКИ БЫСТРО

① Способ А — официальный ZIP (рекомендовано)
• Instagram → Профиль → ☰ → Твоя активность → Скачать информацию → Запросить скачивание
• Формат: JSON; Типы данных: Followers/Following (можно отметить только их)
• Когда придёт письмо — скачайте ZIP и отправьте его сюда.

② Способ Б — «копировать из браузера»
• Откройте instagram.com → профиль → нажмите на Following (или Followers)
• Прокрутите до самого низа, чтобы загрузились все строки
• Внутри окна списка: открыть консоль (F12) и вставить код
  "(() => {
    const dlg = document.querySelector('div[role="dialog"]') || document.body;
    const links = Array.from(dlg.querySelectorAll('a[href^="/"]'));
    const usernames = Array.from(new Set(
      links
        .map(a => (a.getAttribute('href') || '').trim())
        .filter(href => /^\/[A-Za-z0-9._]+\/$/.test(href))   // только ссылки вида /username/
        .map(href => href.slice(1, -1).toLowerCase())        // убрать слэши
    ));
    console.log(`Found ${usernames.length} usernames`);
    console.log(usernames.join('\n'));
    try { copy(usernames.join('\n')); console.log('Copied to clipboard'); } catch (e) {}
  })();"
• Вставьте сюда как обычный текст или как файл txt. Потом повторите для второго списка.

Совет: повторяйте выгрузку раз в неделю/месяц — тогда я покажу, кто подписался/отписался и изменения взаимности.
""").strip()

# ---------- Session state ----------
# user_stage[uid] = {"following": set or None, "followers": set or None, "ts": datetime}
user_stage = {}
def session_is_stale(info):
    if not info or "ts" not in info: return True
    age = datetime.datetime.utcnow() - info["ts"]
    return age.total_seconds() > SESSION_TTL_MIN * 60

# ---------- Callbacks & Commands ----------
def start(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    update.message.reply_text(
        "Привет! Я покажу взаимные подписки и изменения со временем.\n\n" + HELP,
        parse_mode="Markdown"
    )

def help_cmd(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    update.message.reply_text(HELP, parse_mode="Markdown")

def howto_cmd(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    update.message.reply_text(HOWTO)

def stats_cmd(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    uid = update.effective_user.id
    cnt, last_ts = get_user_stats(uid)
    txt = f"📊 Снимков сохранено: {cnt}\nПоследний: {last_ts or '—'}"
    if RETENTION_DAYS > 0:
        txt += f"\nRetention: храню до {RETENTION_DAYS} дн."
    update.message.reply_text(txt)

def delete_cmd(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    uid = update.effective_user.id
    user_stage.pop(uid, None)
    update.message.reply_text("Ок, текущая незавершённая загрузка сброшена. Можно начать заново.")

def handle_callback(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    uid = update.effective_user.id
    cq = update.callback_query
    cq.answer()
    data = cq.data or ""

    if data == "menu":
        show_menu(update, context, uid)
        return
    if data == "howto":
        update.effective_message.reply_text(HOWTO)
        return
    if data == "ask_wipe":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, удалить историю", callback_data="wipe_confirm"),
            InlineKeyboardButton("❌ Отмена", callback_data="menu"),
        ]])
        update.effective_message.reply_text("Удалить все сохранённые снимки для этого чата?", reply_markup=kb)
        return
    if data == "wipe_confirm":
        wipe_user_history(uid)
        store_lists.pop(uid, None)
        update.effective_message.reply_text("Готово. История удалена. Начните заново: пришлите ZIP или списки.")
        return
    if data == "download_zip":
        send_current_zip(update, context, uid)
        return

    m = re.match(r"^page\|([^|]+)\|(\d+)$", data)
    if m:
        key = m.group(1)
        start = int(m.group(2))
        send_page(update, context, uid, key, start)
        return

def read_text_from_message(update: Update):
    msg = update.message
    if msg.document:
        return None
    if msg.text and not msg.text.startswith('/'):
        return msg.text
    return None

def handle_document(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    uid = update.effective_user.id
    doc = update.message.document
    if not doc: return
    fname = (doc.file_name or "").lower()

    # ZIP из Instagram
    if fname.endswith(".zip"):
        f = doc.get_file()
        bio = io.BytesIO()
        f.download(out=bio)
        b = bio.getvalue()
        following_list, followers_list = parse_zip_for_users(b)
        if not following_list and not followers_list:
            update.message.reply_text("Не нашёл списки в ZIP. Убедитесь, что это архив из Instagram Download (лучше JSON).")
            return
        A, B = set(following_list), set(followers_list)
        process_sets_and_reply(update, context, uid, A, B)
        return

    # Иначе пробуем как текст/CSV
    f = doc.get_file()
    bio = io.BytesIO()
    f.download(out=bio); bio.seek(0)
    text = None
    for enc in ("utf-8", "latin-1"):
        try:
            text = bio.read().decode(enc); break
        except:
            bio.seek(0)
    if text is None:
        update.message.reply_text("Не удалось прочитать документ. Пришлите .txt/.csv или ZIP из Instagram.")
        return
    handle_text_lists(update, context, uid, text)

def handle_text(update: Update, context: CallbackContext):
    if not ensure_allowed(update): return
    uid = update.effective_user.id
    text = read_text_from_message(update)
    if text is None: return
    handle_text_lists(update, context, uid, text)

def handle_text_lists(update: Update, context: CallbackContext, uid: int, text: str):
    # Режим «два текста»: сначала following, затем followers
    st = user_stage.get(uid)
    if not st or session_is_stale(st):
        st = {"following": None, "followers": None, "ts": datetime.datetime.utcnow()}
        user_stage[uid] = st

    s = to_user_set(text)
    if st["following"] is None:
        st["following"] = s
        st["ts"] = datetime.datetime.utcnow()
        update.message.reply_text(f"Принял *following* ({len(s)}). Теперь пришлите *followers*.", parse_mode="Markdown")
        return

    if st["followers"] is None:
        st["followers"] = s
        A, B = st["following"], st["followers"]
        process_sets_and_reply(update, context, uid, A, B)
        user_stage.pop(uid, None)

# ---------- Report / ZIP ----------
def build_zip_bytes_from_lists(lists_dict):
    def to_csv_bytes(rows, header="username"):
        sio = io.StringIO()
        w = csv.writer(sio)
        w.writerow([header])
        for r in rows: w.writerow([r])
        return sio.getvalue().encode("utf-8")

    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fname, rows in lists_dict.items():
            zf.writestr(fname, to_csv_bytes(rows))
    zbuf.seek(0); zbuf.name = "report.zip"
    return zbuf

def send_current_zip(update: Update, context: CallbackContext, uid: int):
    data = store_lists.get(uid)
    if not data:
        update.effective_message.reply_text("Сначала отправьте ZIP или списки, чтобы я сформировал отчёт.")
        return
    export = {
        "mutual.csv": data.get("mutual", []),
        "only_in_following.csv": data.get("only_in_following", []),
        "only_in_followers.csv": data.get("only_in_followers", []),
        "new_followers.csv": data.get("new_followers", []),
        "unfollowers.csv": data.get("unfollowers", []),
        "new_following.csv": data.get("new_following", []),
        "unfollowed_by_you.csv": data.get("unfollowed_by_you", []),
        "new_mutuals.csv": data.get("new_mutuals", []),
        "lost_mutuals.csv": data.get("lost_mutuals", []),
    }
    z = build_zip_bytes_from_lists(export)
    update.effective_message.reply_document(document=InputFile(z, filename="report.zip"), caption="Полный отчёт (CSV внутри).")

# ---------- Core processing ----------
def process_sets_and_reply(update: Update, context: CallbackContext, uid: int, A: set, B: set):
    mutual = sorted(A & B)
    only_in_following = sorted(A - B)
    only_in_followers = sorted(B - A)

    last = load_last_snapshot(uid)
    if last:
        prevA, prevB = last["following"], last["followers"]
        new_followers = sorted(B - prevB)
        unfollowers = sorted(prevB - B)
        new_following = sorted(A - prevA)
        unfollowed_by_you = sorted(prevA - A)
        prev_mutual = prevA & prevB
        new_mutuals = sorted((A & B) - prev_mutual)
        lost_mutuals = sorted(prev_mutual - (A & B))
        ts = last["ts"][:19] + " UTC"

        summary = (
            "Готово!\n"
            f"📸 Текущая сводка:\n"
            f"• following: {len(A)}  • followers: {len(B)}  • взаимные: {len(mutual)}\n\n"
            f"📈 Изменения с последнего раза ({ts}):\n"
            f"• 🟢 новые подписчики: {len(new_followers)}\n"
            f"• 🔴 отписались: {len(unfollowers)}\n"
            f"• ➕ вы зафолловили: {len(new_following)}\n"
            f"• ➖ вы отписались: {len(unfollowed_by_you)}\n"
            f"• ✨ стали взаимными: {len(new_mutuals)}\n"
            f"• 💔 потеряли взаимность: {len(lost_mutuals)}"
        )
        update.message.reply_text(summary)

        store_lists[uid] = {
            "mutual": mutual,
            "only_in_following": only_in_following,
            "only_in_followers": only_in_followers,
            "new_followers": new_followers,
            "unfollowers": unfollowers,
            "new_following": new_following,
            "unfollowed_by_you": unfollowed_by_you,
            "new_mutuals": new_mutuals,
            "lost_mutuals": lost_mutuals
        }
    else:
        summary = (
            "Снимок сохранён! Это первый раз, поэтому сравнить пока не с чем.\n\n"
            f"📸 Текущая сводка:\n"
            f"• following: {len(A)}  • followers: {len(B)}  • взаимные: {len(mutual)}\n"
            "Ниже можно открыть списки по кнопкам."
        )
        update.message.reply_text(summary)

        store_lists[uid] = {
            "mutual": mutual,
            "only_in_following": only_in_following,
            "only_in_followers": only_in_followers,
            "new_followers": [],
            "unfollowers": [],
            "new_following": [],
            "unfollowed_by_you": [],
            "new_mutuals": [],
            "lost_mutuals": []
        }

    show_menu(update, context, uid)
    save_snapshot(uid, A, B)

# ---------- Main ----------
def main():
    init_db()
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(CommandHandler("howto", howto_cmd))
    dp.add_handler(CommandHandler("stats", stats_cmd))
    dp.add_handler(CommandHandler("delete", delete_cmd))
    dp.add_handler(CallbackQueryHandler(handle_callback))
    dp.add_handler(MessageHandler(Filters.document, handle_document))
    dp.add_handler(MessageHandler(Filters.text & (~Filters.command), handle_text))
    print("Bot v3.4 starting...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
