"""
╔══════════════════════════════════════════════════════════════╗
║         BOT TU TIÊN DISCORD - V2 (PostgreSQL Edition)        ║
║  Cài đặt: pip install discord.py asyncpg                     ║
╚══════════════════════════════════════════════════════════════╝
"""

import discord
from discord.ext import commands, tasks
import asyncpg
import random
import asyncio
import os
import json
from datetime import datetime, timedelta

# ── CẤU HÌNH ──────────────────────────────────────────────────

# ===== LẤY TOKEN TỪ ENV =====
TOKEN = os.getenv("DISCORD_TOKEN")
DB_URL   = os.environ.get("DATABASE_URL")   # Railway tự cung cấp
if not TOKEN:
    print("❌ Không tìm thấy TOKEN trong biến môi trường!")
    exit()

# ===== INTENTS =====
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

# ===== TẠO BOT =====
PREFIX = "!"

# ── DỮ LIỆU GAME ──────────────────────────────────────────────
CANH_GIOI = [
    "Phàm Nhân", "Luyện Khí", "Trúc Cơ", "Kim Đan",
    "Nguyên Anh", "Hóa Thần", "Luyện Hư", "Hợp Thể",
    "Đại Thừa", "Độ Kiếp", "Tiên Nhân", "Chân Tiên",
    "Thiên Tiên", "Đại La Kim Tiên", "Thánh Nhân"
]

VAT_PHAM = {
    "Linh Thảo":    {"loai": "thuoc",    "cong_dung": "hồi 50 linh lực",  "gia": 100,  "rare": "thường"},
    "Hồi Linh Đan": {"loai": "thuoc",    "cong_dung": "hồi 200 linh lực", "gia": 500,  "rare": "hiếm"},
    "Tụ Linh Đan":  {"loai": "thuoc",    "cong_dung": "+500 kinh nghiệm", "gia": 800,  "rare": "hiếm"},
    "Phá Cảnh Đan": {"loai": "thuoc",    "cong_dung": "tăng 30% đột phá", "gia": 2000, "rare": "quý"},
    "Phi Kiếm":     {"loai": "vu_khi",   "cong_dung": "+20 tấn công",     "gia": 1500, "rare": "hiếm"},
    "Linh Giáp":    {"loai": "phong_gu", "cong_dung": "+30 phòng thủ",    "gia": 1500, "rare": "hiếm"},
    "Càn Khôn Túi": {"loai": "trang_bi", "cong_dung": "+10 túi đồ",       "gia": 3000, "rare": "quý"},
    "Tiên Kiếm":    {"loai": "vu_khi",   "cong_dung": "+80 tấn công",     "gia": 9999, "rare": "thần khí"},
}

KY_NANG = {
    "Kiếm Khí Thuật":      {"sat_thuong": 30,  "linh_luc": 20,  "cap_yeu": 1},
    "Lôi Pháp":            {"sat_thuong": 60,  "linh_luc": 40,  "cap_yeu": 3},
    "Hỏa Long Kiếm":       {"sat_thuong": 100, "linh_luc": 60,  "cap_yeu": 5},
    "Thiên Lôi Nhất Kích": {"sat_thuong": 200, "linh_luc": 120, "cap_yeu": 8},
    "Vạn Kiếm Quy Tông":   {"sat_thuong": 400, "linh_luc": 200, "cap_yeu": 11},
}

BOSS_LIST = [
    {"ten": "Yêu Hồ Hắc Phong",   "hp": 500,   "sat_thuong": 40,  "phan_thuong": 300,   "exp": 500,   "cap_yeu": 2},
    {"ten": "Ma Tướng Thiết Giáp", "hp": 1200,  "sat_thuong": 80,  "phan_thuong": 800,   "exp": 1200,  "cap_yeu": 4},
    {"ten": "Cổ Long Hắc Diệm",    "hp": 3000,  "sat_thuong": 150, "phan_thuong": 2000,  "exp": 3000,  "cap_yeu": 7},
    {"ten": "Ma Thần Hỗn Độn",     "hp": 8000,  "sat_thuong": 300, "phan_thuong": 5000,  "exp": 8000,  "cap_yeu": 10},
    {"ten": "Thái Cổ Yêu Hoàng",   "hp": 20000, "sat_thuong": 600, "phan_thuong": 15000, "exp": 20000, "cap_yeu": 13},
]

# ── THÀNH TÍCH ─────────────────────────────────────────────────
THANH_TICH = {
    "tan_dao":      {"ten": "⚔️ Tân Đạo",        "mo_ta": "Tạo nhân vật lần đầu"},
    "tulyen_10":    {"ten": "🧘 Siêng Năng",      "mo_ta": "Tu luyện 10 lần"},
    "tulyen_100":   {"ten": "🔥 Khổ Tu",          "mo_ta": "Tu luyện 100 lần"},
    "boss_1":       {"ten": "👹 Đồ Sát",          "mo_ta": "Giết boss đầu tiên"},
    "boss_50":      {"ten": "💀 Sát Thần",        "mo_ta": "Giết 50 boss"},
    "pvp_win_1":    {"ten": "🥊 Võ Đạo",          "mo_ta": "Thắng PvP lần đầu"},
    "pvp_win_10":   {"ten": "🏆 Chiến Thần",      "mo_ta": "Thắng 10 trận PvP"},
    "canh_gioi_5":  {"ten": "🌟 Kỳ Tài",          "mo_ta": "Đạt Hóa Thần (Lv.5)"},
    "canh_gioi_10": {"ten": "👑 Thiên Tài",        "mo_ta": "Đạt Độ Kiếp (Lv.10)"},
    "giau_co":      {"ten": "💰 Phú Gia Địch Quốc","mo_ta": "Tích lũy 100,000 Linh Thạch"},
}

# ══════════════════════════════════════════════════════════════
#  KHỞI TẠO BOT
# ══════════════════════════════════════════════════════════════
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)
pool: asyncpg.Pool = None

# ══════════════════════════════════════════════════════════════
#  DATABASE - KHỞI TẠO BẢNG
# ══════════════════════════════════════════════════════════════
async def init_db():
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:

        # Bảng nhân vật chính
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS nhanvat (
                user_id      BIGINT PRIMARY KEY,
                ten          TEXT NOT NULL,
                canh_gioi    INT  DEFAULT 0,
                exp          INT  DEFAULT 0,
                linh_luc     INT  DEFAULT 100,
                linh_luc_max INT  DEFAULT 100,
                tan_cong     INT  DEFAULT 10,
                phong_thu    INT  DEFAULT 5,
                linh_thach   INT  DEFAULT 50,
                tu_vi        INT  DEFAULT 0,
                last_tulyen  TIMESTAMPTZ,
                last_khampha TIMESTAMPTZ,
                ky_nang      TEXT DEFAULT '',
                tong_mon     TEXT DEFAULT '',
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Bảng túi đồ
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tui_do (
                user_id  BIGINT,
                vat_pham TEXT,
                so_luong INT DEFAULT 1,
                PRIMARY KEY (user_id, vat_pham)
            )
        """)

        # Bảng tông môn
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tong_mon (
                ten        TEXT PRIMARY KEY,
                chu_mon    BIGINT,
                mo_ta      TEXT DEFAULT '',
                linh_thach INT  DEFAULT 0,
                thanh_vien TEXT DEFAULT ''
            )
        """)

        # ── MỚI: Thống kê tổng hợp ──
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS thong_ke (
                user_id        BIGINT PRIMARY KEY REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                tong_tulyen    INT DEFAULT 0,
                tong_exp       BIGINT DEFAULT 0,
                tong_boss_giet INT DEFAULT 0,
                tong_pvp_thang INT DEFAULT 0,
                tong_pvp_thua  INT DEFAULT 0,
                tong_lt_kiem   BIGINT DEFAULT 0,
                tong_lt_tieu   BIGINT DEFAULT 0,
                dot_pha_count  INT DEFAULT 0,
                updated_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # ── MỚI: Nhật ký tu luyện ──
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS nhat_ky (
                id         BIGSERIAL PRIMARY KEY,
                user_id    BIGINT REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                loai       TEXT,   -- tulyen / khampha / boss / pvp / dotpha
                noi_dung   TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_nhatky_user ON nhat_ky(user_id, created_at DESC)")

        # ── MỚI: Lịch sử PvP ──
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lich_su_pvp (
                id          BIGSERIAL PRIMARY KEY,
                nguoi_thang BIGINT,
                nguoi_thua  BIGINT,
                ten_thang   TEXT,
                ten_thua    TEXT,
                lt_cuop     INT,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_pvp_thang ON lich_su_pvp(nguoi_thang)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_pvp_thua  ON lich_su_pvp(nguoi_thua)")

        # ── MỚI: Thành tích ──
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS thanh_tich (
                user_id    BIGINT REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                ma_tt      TEXT,
                dat_duoc_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, ma_tt)
            )
        """)

    print("✅ Database PostgreSQL đã sẵn sàng!")

# ══════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════
def exp_can(canh_gioi: int) -> int:
    return (canh_gioi + 1) * 500

def embed_mau(title, desc, color=0xAA55FF):
    e = discord.Embed(title=title, description=desc, color=color)
    e.set_footer(text="⚡ Tu Tiên Bot V2 | PostgreSQL Edition")
    return e

def cooldown_con(last_dt, giay: int) -> float:
    if not last_dt:
        return 0
    elapsed = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
    return max(0, giay - elapsed)

async def get_nv(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM nhanvat WHERE user_id=$1", user_id)

async def cap_nhat(user_id: int, **kwargs):
    if not kwargs:
        return
    cols = ", ".join(f"{k}=${i+2}" for i, k in enumerate(kwargs))
    vals = [user_id] + list(kwargs.values())
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE nhanvat SET {cols} WHERE user_id=$1", *vals)

async def them_nhat_ky(user_id: int, loai: str, noi_dung: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO nhat_ky (user_id, loai, noi_dung) VALUES ($1,$2,$3)",
            user_id, loai, noi_dung
        )
        # Giữ tối đa 50 nhật ký / người
        await conn.execute("""
            DELETE FROM nhat_ky WHERE id IN (
                SELECT id FROM nhat_ky WHERE user_id=$1
                ORDER BY created_at DESC OFFSET 50
            )
        """, user_id)

async def cap_nhat_thongke(user_id: int, **kwargs):
    if not kwargs:
        return
    cols = ", ".join(f"{k}={k}+${i+2}" for i, k in enumerate(kwargs))
    vals = [user_id] + list(kwargs.values())
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO thong_ke (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING",
            user_id
        )
        await conn.execute(
            f"UPDATE thong_ke SET {cols}, updated_at=NOW() WHERE user_id=$1", *vals
        )

async def kiem_tra_thanh_tich(ctx, user_id: int, nv: dict, thong_ke: dict):
    """Kiểm tra và trao thành tích mới"""
    async with pool.acquire() as conn:
        da_co = {r['ma_tt'] for r in await conn.fetch(
            "SELECT ma_tt FROM thanh_tich WHERE user_id=$1", user_id
        )}

    moi = []
    kiem = {
        "tan_dao":      True,
        "tulyen_10":    (thong_ke and thong_ke['tong_tulyen'] >= 10),
        "tulyen_100":   (thong_ke and thong_ke['tong_tulyen'] >= 100),
        "boss_1":       (thong_ke and thong_ke['tong_boss_giet'] >= 1),
        "boss_50":      (thong_ke and thong_ke['tong_boss_giet'] >= 50),
        "pvp_win_1":    (thong_ke and thong_ke['tong_pvp_thang'] >= 1),
        "pvp_win_10":   (thong_ke and thong_ke['tong_pvp_thang'] >= 10),
        "canh_gioi_5":  (nv and nv['canh_gioi'] >= 5),
        "canh_gioi_10": (nv and nv['canh_gioi'] >= 10),
        "giau_co":      (nv and nv['linh_thach'] >= 100000),
    }

    async with pool.acquire() as conn:
        for ma, dieu_kien in kiem.items():
            if dieu_kien and ma not in da_co:
                await conn.execute(
                    "INSERT INTO thanh_tich (user_id, ma_tt) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                    user_id, ma
                )
                moi.append(THANH_TICH[ma]["ten"])

    if moi:
        await ctx.send(embed=embed_mau(
            "🏅 Thành Tích Mới!",
            "\n".join(f"✨ **{t}** đã được mở khóa!" for t in moi),
            0xFFD700
        ))

# ══════════════════════════════════════════════════════════════
#  LỆNH: TẠO NHÂN VẬT
# ══════════════════════════════════════════════════════════════
@bot.command(name="taonv", aliases=["dangky"])
async def tao_nv(ctx, *, ten: str = None):
    if not ten:
        await ctx.send(embed=embed_mau("❌", "Dùng: `!taonv <tên nhân vật>`", 0xFF4444))
        return
    nv = await get_nv(ctx.author.id)
    if nv:
        await ctx.send(embed=embed_mau("❌", f"Bạn đã có nhân vật **{nv['ten']}**!", 0xFF4444))
        return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO nhanvat (user_id, ten) VALUES ($1,$2)",
            ctx.author.id, ten
        )
        await conn.execute(
            "INSERT INTO thong_ke (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            ctx.author.id
        )
    await them_nhat_ky(ctx.author.id, "system", f"Bắt đầu hành trình tu tiên với tên **{ten}**")
    nv_moi = await get_nv(ctx.author.id)
    tk = None
    await kiem_tra_thanh_tich(ctx, ctx.author.id, nv_moi, tk)
    await ctx.send(embed=embed_mau("🌟 Nhập Môn Tu Tiên!", f"""
**{ten}** đã bước vào con đường tu tiên!
🏔️ Cảnh Giới: **{CANH_GIOI[0]}**
💧 Linh Lực: **100/100**
⚔️ Tấn Công: **10** | 🛡️ Phòng Thủ: **5**
💎 Linh Thạch: **50**
Dùng `!tulyen` để bắt đầu tu luyện!
    """, 0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: THÔNG TIN
# ══════════════════════════════════════════════════════════════
@bot.command(name="tt", aliases=["thongtin", "info"])
async def thong_tin(ctx, member: discord.Member = None):
    target = member or ctx.author
    nv = await get_nv(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` để tạo!", 0xFF4444))
        return
    async with pool.acquire() as conn:
        tk = await conn.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", target.id)
        tt_count = await conn.fetchval("SELECT COUNT(*) FROM thanh_tich WHERE user_id=$1", target.id)
    exp_max = exp_can(nv['canh_gioi'])
    thanh = int((nv['exp'] / exp_max) * 10)
    bar = "█" * thanh + "░" * (10 - thanh)
    pvp_total = (tk['tong_pvp_thang'] + tk['tong_pvp_thua']) if tk else 0
    win_rate = f"{int(tk['tong_pvp_thang']/pvp_total*100)}%" if pvp_total > 0 else "N/A"
    e = embed_mau(f"📜 {nv['ten']}", f"""
🏔️ **Cảnh Giới:** {CANH_GIOI[nv['canh_gioi']]} (Lv.{nv['canh_gioi']})
✨ **Tu Vi:** {nv['tu_vi']:,}
📊 **EXP:** {nv['exp']}/{exp_max} [{bar}]
💧 **Linh Lực:** {nv['linh_luc']}/{nv['linh_luc_max']}
⚔️ **Tấn Công:** {nv['tan_cong']} | 🛡️ **Phòng Thủ:** {nv['phong_thu']}
💎 **Linh Thạch:** {nv['linh_thach']:,}
🏯 **Tông Môn:** {nv['tong_mon'] or 'Vô Môn'}
🏅 **Thành Tích:** {tt_count}/{len(THANH_TICH)}

📈 **Thống Kê**
🧘 Tu Luyện: {tk['tong_tulyen'] if tk else 0:,} lần | 👹 Boss: {tk['tong_boss_giet'] if tk else 0} con
⚔️ PvP: {tk['tong_pvp_thang'] if tk else 0}T/{tk['tong_pvp_thua'] if tk else 0}B ({win_rate}) | 🔥 Đột Phá: {tk['dot_pha_count'] if tk else 0} lần
    """)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════════
#  LỆNH: TU LUYỆN
# ══════════════════════════════════════════════════════════════
@bot.command(name="tulyen", aliases=["tl"])
async def tu_luyen(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    cd = cooldown_con(nv['last_tulyen'], 60)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳ Đang Tu Luyện", f"Còn **{cd:.0f}s** nữa!", 0xFFAA00))
        return

    exp_gain = random.randint(30, 80) + nv['canh_gioi'] * 10
    tu_vi_gain = random.randint(10, 30)
    ll_hoi = random.randint(5, 15)
    new_exp = nv['exp'] + exp_gain
    new_cg = nv['canh_gioi']
    dot_pha_msg = ""
    dot_pha_count = 0

    while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI) - 1:
        new_exp -= exp_can(new_cg)
        new_cg += 1
        dot_pha_count += 1
        dot_pha_msg = f"\n\n🎉 **ĐỘT PHÁ!** Đạt **{CANH_GIOI[new_cg]}**! 🎉"

    await cap_nhat(ctx.author.id,
        exp=new_exp, tu_vi=nv['tu_vi'] + tu_vi_gain, canh_gioi=new_cg,
        linh_luc=min(nv['linh_luc'] + ll_hoi, nv['linh_luc_max']),
        last_tulyen=datetime.utcnow()
    )
    await cap_nhat_thongke(ctx.author.id,
        tong_tulyen=1, tong_exp=exp_gain, dot_pha_count=dot_pha_count
    )
    log = f"+{exp_gain} EXP, +{tu_vi_gain} Tu Vi tại {CANH_GIOI[new_cg]}"
    if dot_pha_count:
        log += f" | ĐỘT PHÁ → {CANH_GIOI[new_cg]}"
    await them_nhat_ky(ctx.author.id, "tulyen", log)

    nv_moi = await get_nv(ctx.author.id)
    async with pool.acquire() as conn:
        tk = await conn.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", ctx.author.id)
    await kiem_tra_thanh_tich(ctx, ctx.author.id, nv_moi, tk)

    await ctx.send(embed=embed_mau("🧘 Tu Luyện Thành Công", f"""
✨ **+{exp_gain} EXP** | 🌀 **+{tu_vi_gain} Tu Vi** | 💧 **+{ll_hoi} Linh Lực**
📊 EXP: {new_exp}/{exp_can(new_cg)} | Cảnh Giới: {CANH_GIOI[new_cg]}
{dot_pha_msg}
    """, 0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: KHÁM PHÁ
# ══════════════════════════════════════════════════════════════
@bot.command(name="khampha", aliases=["kp"])
async def kham_pha(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    cd = cooldown_con(nv['last_khampha'], 120)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳ Mệt Mỏi", f"Còn **{cd:.0f}s** nữa!", 0xFFAA00))
        return

    lt = random.randint(10, 50) + nv['canh_gioi'] * 5
    results = [f"💎 +{lt} Linh Thạch"]
    vp_found = None
    if random.random() < 0.4:
        vp_found = random.choice(list(VAT_PHAM.keys()))
        results.append(f"🎁 Nhặt được **{vp_found}**!")
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO tui_do (user_id, vat_pham, so_luong) VALUES ($1,$2,1)
                ON CONFLICT (user_id, vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+1
            """, ctx.author.id, vp_found)

    await cap_nhat(ctx.author.id,
        linh_thach=nv['linh_thach'] + lt,
        last_khampha=datetime.utcnow()
    )
    await cap_nhat_thongke(ctx.author.id, tong_lt_kiem=lt)
    log = f"Tìm được {lt} Linh Thạch" + (f", nhặt {vp_found}" if vp_found else "")
    await them_nhat_ky(ctx.author.id, "khampha", log)
    await ctx.send(embed=embed_mau("🗺️ Khám Phá", "\n".join(results), 0x55AAFF))

# ══════════════════════════════════════════════════════════════
#  LỆNH: BOSS
# ══════════════════════════════════════════════════════════════
@bot.command(name="boss")
async def danh_boss(ctx, so_boss: int = None):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    if so_boss is None:
        lines = []
        for i, b in enumerate(BOSS_LIST, 1):
            lock = "🔒" if nv['canh_gioi'] < b['cap_yeu'] else "⚔️"
            lines.append(f"{lock} **{i}. {b['ten']}** — HP:{b['hp']} | Cần Lv.{b['cap_yeu']} | 💎{b['phan_thuong']}")
        await ctx.send(embed=embed_mau("👹 Danh Sách Boss", "\n".join(lines)))
        return

    if not (1 <= so_boss <= len(BOSS_LIST)):
        await ctx.send(embed=embed_mau("❌", f"Chọn boss 1-{len(BOSS_LIST)}", 0xFF4444))
        return
    boss = BOSS_LIST[so_boss - 1]
    if nv['canh_gioi'] < boss['cap_yeu']:
        await ctx.send(embed=embed_mau("❌", f"Cần **{CANH_GIOI[boss['cap_yeu']]}**!", 0xFF4444))
        return

    p_hp = nv['linh_luc']
    b_hp = boss['hp']
    rounds = []
    for turn in range(1, 21):
        p_atk = max(1, random.randint(nv['tan_cong'], nv['tan_cong'] * 2) - boss['sat_thuong'] // 4)
        b_atk = max(1, random.randint(boss['sat_thuong'] // 2, boss['sat_thuong']) - nv['phong_thu'])
        b_hp -= p_atk
        p_hp -= b_atk
        if turn <= 3:
            rounds.append(f"Lượt {turn}: Bạn -{p_atk}HP boss | Boss -{b_atk}HP bạn")
        if p_hp <= 0 or b_hp <= 0:
            break

    if p_hp > 0:
        await cap_nhat(ctx.author.id,
            linh_thach=nv['linh_thach'] + boss['phan_thuong'],
            exp=nv['exp'] + boss['exp'],
            linh_luc=max(1, p_hp)
        )
        await cap_nhat_thongke(ctx.author.id,
            tong_boss_giet=1, tong_lt_kiem=boss['phan_thuong'], tong_exp=boss['exp']
        )
        await them_nhat_ky(ctx.author.id, "boss",
            f"Đánh bại **{boss['ten']}** (+{boss['phan_thuong']} 💎, +{boss['exp']} EXP)"
        )
        result = "\n".join(rounds) + f"\n...\n\n🏆 **CHIẾN THẮNG!**\n💎 +{boss['phan_thuong']} | ✨ +{boss['exp']} EXP"
        color = 0x55FF55
    else:
        await cap_nhat(ctx.author.id, linh_luc=1)
        await them_nhat_ky(ctx.author.id, "boss", f"Bại trận trước **{boss['ten']}**")
        result = "\n".join(rounds) + "\n...\n\n💀 **THẤT BẠI!** Hồi phục rồi thử lại!"
        color = 0xFF4444

    nv_moi = await get_nv(ctx.author.id)
    async with pool.acquire() as conn:
        tk = await conn.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", ctx.author.id)
    await kiem_tra_thanh_tich(ctx, ctx.author.id, nv_moi, tk)
    await ctx.send(embed=embed_mau(f"⚔️ Boss: {boss['ten']}", result, color))

# ══════════════════════════════════════════════════════════════
#  LỆNH: PVP
# ══════════════════════════════════════════════════════════════
@bot.command(name="pvp")
async def pvp(ctx, doi_thu: discord.Member):
    if doi_thu.id == ctx.author.id:
        await ctx.send(embed=embed_mau("❌", "Không thể tự đánh mình!", 0xFF4444))
        return
    nv1 = await get_nv(ctx.author.id)
    nv2 = await get_nv(doi_thu.id)
    if not nv1 or not nv2:
        await ctx.send(embed=embed_mau("❌", "Một trong hai chưa có nhân vật!", 0xFF4444))
        return

    await ctx.send(embed=embed_mau("⚔️ Thách Đấu!",
        f"**{nv1['ten']}** thách **{nv2['ten']}**!\n{doi_thu.mention} gõ `chấp nhận` trong 30 giây!"))

    def check(m):
        return m.author.id == doi_thu.id and m.content.lower() in ["chấp nhận","chap nhan","ok","accept"]
    try:
        await bot.wait_for("message", check=check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.send(embed=embed_mau("⏰ Hết Giờ", f"{doi_thu.display_name} bỏ chạy!", 0xFF4444))
        return

    hp1, hp2 = nv1['linh_luc'], nv2['linh_luc']
    rounds = []
    for i in range(1, 11):
        a1 = max(1, random.randint(nv1['tan_cong'], nv1['tan_cong']*2) - nv2['phong_thu'])
        a2 = max(1, random.randint(nv2['tan_cong'], nv2['tan_cong']*2) - nv1['phong_thu'])
        hp2 -= a1; hp1 -= a2
        if i <= 3:
            rounds.append(f"Lượt {i}: {nv1['ten']} -{a1}HP | {nv2['ten']} -{a2}HP")
        if hp1 <= 0 or hp2 <= 0:
            break

    win_id  = ctx.author.id if hp2 <= 0 else doi_thu.id
    lose_id = doi_thu.id   if hp2 <= 0 else ctx.author.id
    nv_w = nv1 if win_id == ctx.author.id else nv2
    nv_l = nv2 if win_id == ctx.author.id else nv1

    lt_cuop = min(random.randint(20, 80), nv_l['linh_thach'])
    async with pool.acquire() as conn:
        await conn.execute("UPDATE nhanvat SET linh_thach=linh_thach+$2 WHERE user_id=$1", win_id, lt_cuop)
        await conn.execute("UPDATE nhanvat SET linh_thach=GREATEST(0,linh_thach-$2), linh_luc=1 WHERE user_id=$1", lose_id, lt_cuop)
        # Lưu lịch sử PvP
        await conn.execute("""
            INSERT INTO lich_su_pvp (nguoi_thang, nguoi_thua, ten_thang, ten_thua, lt_cuop)
            VALUES ($1,$2,$3,$4,$5)
        """, win_id, lose_id, nv_w['ten'], nv_l['ten'], lt_cuop)

    await cap_nhat_thongke(win_id,  tong_pvp_thang=1, tong_lt_kiem=lt_cuop)
    await cap_nhat_thongke(lose_id, tong_pvp_thua=1,  tong_lt_tieu=lt_cuop)
    await them_nhat_ky(win_id,  "pvp", f"Thắng **{nv_l['ten']}**, cướp {lt_cuop} 💎")
    await them_nhat_ky(lose_id, "pvp", f"Thua **{nv_w['ten']}**, mất {lt_cuop} 💎")

    for uid in [win_id, lose_id]:
        nv_check = await get_nv(uid)
        async with pool.acquire() as conn:
            tk = await conn.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", uid)
        await kiem_tra_thanh_tich(ctx, uid, nv_check, tk)

    result = "\n".join(rounds) + f"\n...\n\n🏆 **{nv_w['ten']} THẮNG!**\nCướp **{lt_cuop}** 💎 từ {nv_l['ten']}!"
    await ctx.send(embed=embed_mau("⚔️ Kết Quả PvP", result, 0xFF8800))

# ══════════════════════════════════════════════════════════════
#  LỆNH MỚI: NHẬT KÝ
# ══════════════════════════════════════════════════════════════
@bot.command(name="nhatky", aliases=["nk", "log"])
async def nhat_ky_cmd(ctx):
    """!nhatky — Xem nhật ký tu luyện gần nhất"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT loai, noi_dung, created_at FROM nhat_ky
            WHERE user_id=$1 ORDER BY created_at DESC LIMIT 10
        """, ctx.author.id)

    if not rows:
        await ctx.send(embed=embed_mau("📖 Nhật Ký Trống", "Hãy bắt đầu tu luyện!", 0x888888))
        return

    icon = {"tulyen":"🧘","khampha":"🗺️","boss":"👹","pvp":"⚔️","dotpha":"🔥","system":"📌"}
    lines = []
    for r in rows:
        ico = icon.get(r['loai'], "📝")
        tg = r['created_at'].strftime("%d/%m %H:%M")
        lines.append(f"{ico} `{tg}` {r['noi_dung']}")
    await ctx.send(embed=embed_mau(f"📖 Nhật Ký — {nv['ten']}", "\n".join(lines)))

# ══════════════════════════════════════════════════════════════
#  LỆNH MỚI: LỊCH SỬ PVP
# ══════════════════════════════════════════════════════════════
@bot.command(name="lichsupvp", aliases=["lspvp"])
async def lich_su_pvp_cmd(ctx, member: discord.Member = None):
    """!lichsupvp — Xem lịch sử PvP"""
    target = member or ctx.author
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM lich_su_pvp
            WHERE nguoi_thang=$1 OR nguoi_thua=$1
            ORDER BY created_at DESC LIMIT 10
        """, target.id)

    if not rows:
        await ctx.send(embed=embed_mau("⚔️ Chưa Có Trận Đấu", "Dùng `!pvp @người` để thách đấu!", 0x888888))
        return

    lines = []
    for r in rows:
        thang = r['nguoi_thang'] == target.id
        ket_qua = "🏆 THẮNG" if thang else "💀 THUA"
        doi = r['ten_thua'] if thang else r['ten_thang']
        lt = f"+{r['lt_cuop']}" if thang else f"-{r['lt_cuop']}"
        tg = r['created_at'].strftime("%d/%m %H:%M")
        lines.append(f"{ket_qua} vs **{doi}** | {lt}💎 | `{tg}`")

    nv = await get_nv(target.id)
    ten = nv['ten'] if nv else target.display_name
    await ctx.send(embed=embed_mau(f"⚔️ Lịch Sử PvP — {ten}", "\n".join(lines)))

# ══════════════════════════════════════════════════════════════
#  LỆNH MỚI: THÀNH TÍCH
# ══════════════════════════════════════════════════════════════
@bot.command(name="thanhtich", aliases=["tt2", "achievement"])
async def thanh_tich_cmd(ctx, member: discord.Member = None):
    """!thanhtich — Xem thành tích"""
    target = member or ctx.author
    async with pool.acquire() as conn:
        da_co = {r['ma_tt']: r['dat_duoc_at'] for r in await conn.fetch(
            "SELECT ma_tt, dat_duoc_at FROM thanh_tich WHERE user_id=$1", target.id
        )}

    lines = []
    for ma, info in THANH_TICH.items():
        if ma in da_co:
            tg = da_co[ma].strftime("%d/%m/%Y")
            lines.append(f"✅ {info['ten']} — _{info['mo_ta']}_ `({tg})`")
        else:
            lines.append(f"🔒 ~~{info['ten']}~~ — _{info['mo_ta']}_")

    nv = await get_nv(target.id)
    ten = nv['ten'] if nv else target.display_name
    done = len(da_co)
    total = len(THANH_TICH)
    await ctx.send(embed=embed_mau(
        f"🏅 Thành Tích — {ten} ({done}/{total})",
        "\n".join(lines)
    ))

# ══════════════════════════════════════════════════════════════
#  LỆNH MỚI: THỐNG KÊ
# ══════════════════════════════════════════════════════════════
@bot.command(name="thongke", aliases=["stats"])
async def thong_ke_cmd(ctx, member: discord.Member = None):
    """!thongke — Xem thống kê tổng hợp"""
    target = member or ctx.author
    nv = await get_nv(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Người này chưa có nhân vật!", 0xFF4444))
        return
    async with pool.acquire() as conn:
        tk = await conn.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", target.id)

    if not tk:
        await ctx.send(embed=embed_mau("📊 Chưa có dữ liệu", "Hãy tu luyện trước!", 0x888888))
        return

    pvp_total = tk['tong_pvp_thang'] + tk['tong_pvp_thua']
    win_rate = f"{int(tk['tong_pvp_thang']/pvp_total*100)}%" if pvp_total > 0 else "N/A"
    e = embed_mau(f"📊 Thống Kê — {nv['ten']}", f"""
🧘 **Tu Luyện:** {tk['tong_tulyen']:,} lần
✨ **Tổng EXP kiếm được:** {tk['tong_exp']:,}
🔥 **Số lần Đột Phá:** {tk['dot_pha_count']}

👹 **Boss đã giết:** {tk['tong_boss_giet']}
⚔️ **PvP:** {tk['tong_pvp_thang']}T / {tk['tong_pvp_thua']}B (tỉ lệ thắng: {win_rate})

💎 **Linh Thạch tích lũy:** {tk['tong_lt_kiem']:,}
💸 **Linh Thạch đã tiêu:** {tk['tong_lt_tieu']:,}
    """)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════════
#  CÁC LỆNH GIỮ NGUYÊN TỪ V1
# ══════════════════════════════════════════════════════════════
@bot.command(name="tuido", aliases=["bag","td"])
async def tui_do(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    async with pool.acquire() as conn:
        items = await conn.fetch("SELECT * FROM tui_do WHERE user_id=$1", ctx.author.id)
    if not items:
        await ctx.send(embed=embed_mau("🎒 Túi Đồ Trống","Dùng `!khampha` để tìm vật phẩm!",0x888888)); return
    lines = []
    for item in items:
        vp = VAT_PHAM.get(item['vat_pham'], {})
        icon = {"thường":"⚪","hiếm":"🔵","quý":"🟣","thần khí":"🟡"}.get(vp.get("rare",""),"⚪")
        lines.append(f"{icon} **{item['vat_pham']}** x{item['so_luong']} — {vp.get('cong_dung','')}")
    await ctx.send(embed=embed_mau(f"🎒 Túi Đồ — {nv['ten']}", "\n".join(lines)))

@bot.command(name="muahang", aliases=["shop"])
async def mua_hang(ctx, *, ten_vp: str = None):
    if not ten_vp:
        lines = []
        for k,v in VAT_PHAM.items():
            icon = {"thường":"⚪","hiếm":"🔵","quý":"🟣","thần khí":"🟡"}.get(v["rare"],"⚪")
            lines.append(f"{icon} **{k}** — {v['gia']}💎 | {v['cong_dung']}")
        await ctx.send(embed=embed_mau("🛒 Cửa Hàng", "\n".join(lines))); return
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    vp = VAT_PHAM.get(ten_vp)
    if not vp:
        await ctx.send(embed=embed_mau("❌","Vật phẩm không tồn tại!",0xFF4444)); return
    if nv['linh_thach'] < vp['gia']:
        await ctx.send(embed=embed_mau("❌",f"Cần **{vp['gia']}** 💎",0xFF4444)); return
    await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach'] - vp['gia'])
    await cap_nhat_thongke(ctx.author.id, tong_lt_tieu=vp['gia'])
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tui_do (user_id, vat_pham, so_luong) VALUES ($1,$2,1)
            ON CONFLICT (user_id, vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+1
        """, ctx.author.id, ten_vp)
    await ctx.send(embed=embed_mau("✅ Mua Thành Công!", f"Đã mua **{ten_vp}** (-{vp['gia']} 💎)", 0x55FFAA))

@bot.command(name="bxh", aliases=["rank"])
async def bang_xep_hang(ctx):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ten, canh_gioi, tu_vi FROM nhanvat ORDER BY canh_gioi DESC, tu_vi DESC LIMIT 10"
        )
    medals = ["🥇","🥈","🥉"] + ["🏅"]*7
    lines = [f"{medals[i]} **{r['ten']}** — {CANH_GIOI[r['canh_gioi']]} | Tu Vi: {r['tu_vi']:,}"
             for i, r in enumerate(rows)]
    await ctx.send(embed=embed_mau("🏆 Bảng Xếp Hạng", "\n".join(lines) or "Chưa có ai!"))

@bot.command(name="help", aliases=["huongdan","hd"])
async def help_cmd(ctx):
    e = embed_mau("📖 Hướng Dẫn Tu Tiên Bot V2", """
**👤 Nhân Vật**
`!taonv <tên>` · `!tt` · `!thongke`

**⚡ Tu Luyện**
`!tulyen` (60s) · `!khampha` (120s)

**⚔️ Chiến Đấu**
`!boss [số]` · `!pvp @người`

**🎒 Vật Phẩm**
`!tuido` · `!muahang` · `!dung <tên>`

**📊 Thống Kê & Lịch Sử**
`!thongke` — Thống kê tổng hợp
`!nhatky` — Nhật ký tu luyện
`!lichsupvp` — Lịch sử chiến đấu
`!thanhtich` — Huy hiệu thành tích

**🏯 Tông Môn & BXH**
`!lapmon <tên>` · `!thamgia <tên>` · `!bxh`
    """)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════════
#  KHỞI ĐỘNG
# ══════════════════════════════════════════════════════════════
@bot.event
async def on_ready():
    await init_db()
    print(f"✅ Bot Tu Tiên V2 online: {bot.user}")
    await bot.change_presence(activity=discord.Game(name="Tu Tiên V2 | !help"))

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=embed_mau("❌ Thiếu Tham Số", "Dùng `!help` để xem hướng dẫn!", 0xFF4444))
    elif isinstance(error, commands.CommandNotFound):
        pass
    else:
        print(f"Lỗi: {error}")

bot.run(TOKEN)
