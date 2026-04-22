"""
╔══════════════════════════════════════════════════════════╗
║           BOT TU TIÊN DISCORD - FULL VERSION             ║
║  Cài đặt: pip install discord.py aiosqlite               ║
╚══════════════════════════════════════════════════════════╝
"""

import discord
from discord.ext import commands, tasks
import aiosqlite
import random
import asyncio
import os
from datetime import datetime, timedelta

# ── CẤU HÌNH ──────────────────────────────────────────────
TOKEN = "MTQ5NjM2NzIyNDQyMjkyODQzNg.Gfjf7k.flG9WcVJYmxHVjhB_9CnUXhD95xfPNM2r7XstI"   # Dán token vào đây
PREFIX = "!"
DB_FILE = "tutien.db"

# ── CẢNH GIỚI TU TIÊN ─────────────────────────────────────
CANH_GIOI = [
    "Phàm Nhân", "Luyện Khí", "Trúc Cơ", "Kim Đan",
    "Nguyên Anh", "Hóa Thần", "Luyện Hư", "Hợp Thể",
    "Đại Thừa", "Độ Kiếp", "Tiên Nhân", "Chân Tiên",
    "Thiên Tiên", "Đại La Kim Tiên", "Thánh Nhân"
]

# ── VẬT PHẨM ──────────────────────────────────────────────
VAT_PHAM = {
    "Linh Thảo":      {"loai": "thuoc", "cong_dung": "hồi 50 linh lực",  "gia": 100,  "rare": "thường"},
    "Hồi Linh Đan":   {"loai": "thuoc", "cong_dung": "hồi 200 linh lực", "gia": 500,  "rare": "hiếm"},
    "Tụ Linh Đan":    {"loai": "thuoc", "cong_dung": "+500 kinh nghiệm", "gia": 800,  "rare": "hiếm"},
    "Phá Cảnh Đan":   {"loai": "thuoc", "cong_dung": "tăng 30% đột phá", "gia": 2000, "rare": "quý"},
    "Phi Kiếm":       {"loai": "vu_khi","cong_dung": "+20 tấn công",     "gia": 1500, "rare": "hiếm"},
    "Linh Giáp":      {"loai": "phong_gu","cong_dung": "+30 phòng thủ",  "gia": 1500, "rare": "hiếm"},
    "Càn Khôn Túi":   {"loai": "trang_bi","cong_dung": "+10 túi đồ",     "gia": 3000, "rare": "quý"},
    "Tiên Kiếm":      {"loai": "vu_khi","cong_dung": "+80 tấn công",     "gia": 9999, "rare": "thần khí"},
}

# ── KỸ NĂNG ───────────────────────────────────────────────
KY_NANG = {
    "Kiếm Khí Thuật":  {"sat_thuong": 30,  "linh_luc": 20, "cap_yeu": 1},
    "Lôi Pháp":        {"sat_thuong": 60,  "linh_luc": 40, "cap_yeu": 3},
    "Hỏa Long Kiếm":   {"sat_thuong": 100, "linh_luc": 60, "cap_yeu": 5},
    "Thiên Lôi Nhất Kích": {"sat_thuong": 200, "linh_luc": 120, "cap_yeu": 8},
    "Vạn Kiếm Quy Tông":   {"sat_thuong": 400, "linh_luc": 200, "cap_yeu": 11},
}

# ── BOSS ──────────────────────────────────────────────────
BOSS_LIST = [
    {"ten": "Yêu Hồ Hắc Phong",  "hp": 500,  "sat_thuong": 40,  "phan_thuong": 300,  "exp": 500,  "cap_yeu": 2},
    {"ten": "Ma Tướng Thiết Giáp","hp": 1200, "sat_thuong": 80,  "phan_thuong": 800,  "exp": 1200, "cap_yeu": 4},
    {"ten": "Cổ Long Hắc Diệm",   "hp": 3000, "sat_thuong": 150, "phan_thuong": 2000, "exp": 3000, "cap_yeu": 7},
    {"ten": "Ma Thần Hỗn Độn",    "hp": 8000, "sat_thuong": 300, "phan_thuong": 5000, "exp": 8000, "cap_yeu": 10},
    {"ten": "Thái Cổ Yêu Hoàng",  "hp": 20000,"sat_thuong": 600, "phan_thuong":15000, "exp":20000, "cap_yeu": 13},
]

# ── KHỞI TẠO BOT ──────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# ══════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS nhanvat (
                user_id     INTEGER PRIMARY KEY,
                ten         TEXT,
                canh_gioi   INTEGER DEFAULT 0,
                exp         INTEGER DEFAULT 0,
                linh_luc    INTEGER DEFAULT 100,
                linh_luc_max INTEGER DEFAULT 100,
                tan_cong    INTEGER DEFAULT 10,
                phong_thu   INTEGER DEFAULT 5,
                linh_thach  INTEGER DEFAULT 50,
                tu_vi        INTEGER DEFAULT 0,
                last_tulyen TEXT,
                last_khampha TEXT,
                ky_nang     TEXT DEFAULT '',
                tong_mon    TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tui_do (
                user_id  INTEGER,
                vat_pham TEXT,
                so_luong INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, vat_pham)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tong_mon (
                ten       TEXT PRIMARY KEY,
                chu_mon   INTEGER,
                mo_ta     TEXT,
                linh_thach INTEGER DEFAULT 0,
                thanh_vien TEXT DEFAULT ''
            )
        """)
        await db.commit()

async def get_nhanvat(user_id):
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM nhanvat WHERE user_id=?", (user_id,)) as cur:
            return await cur.fetchone()

async def tao_nhanvat(user_id, ten):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO nhanvat (user_id, ten) VALUES (?,?)",
            (user_id, ten)
        )
        await db.commit()

async def cap_nhat(user_id, **kwargs):
    if not kwargs:
        return
    cols = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [user_id]
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(f"UPDATE nhanvat SET {cols} WHERE user_id=?", vals)
        await db.commit()

# ══════════════════════════════════════════════════════════
#  HELPER
# ══════════════════════════════════════════════════════════
def exp_can(canh_gioi: int) -> int:
    return (canh_gioi + 1) * 500

def embed_mau(title, desc, color=0xAA55FF):
    e = discord.Embed(title=title, description=desc, color=color)
    e.set_footer(text="⚡ Tu Tiên Bot | Vạn Cổ Trường Tồn")
    return e

def cooldown_con(last_str, giay: int):
    if not last_str:
        return 0
    last = datetime.fromisoformat(last_str)
    elapsed = (datetime.now() - last).total_seconds()
    return max(0, giay - elapsed)

# ══════════════════════════════════════════════════════════
#  LỆNH: TẠO NHÂN VẬT
# ══════════════════════════════════════════════════════════
@bot.command(name="taonv", aliases=["dangky"])
async def tao_nv(ctx, *, ten: str = None):
    """!taonv <tên> — Tạo nhân vật tu tiên"""
    if ten is None:
        await ctx.send(embed=embed_mau("❌ Thiếu tên", "Dùng: `!taonv <tên nhân vật>`", 0xFF4444))
        return
    nv = await get_nhanvat(ctx.author.id)
    if nv:
        await ctx.send(embed=embed_mau("❌ Đã có nhân vật", f"Bạn đã có nhân vật **{nv['ten']}**!", 0xFF4444))
        return
    await tao_nhanvat(ctx.author.id, ten)
    e = embed_mau("🌟 Nhập Môn Tu Tiên!", f"""
**{ten}** đã bước vào con đường tu tiên!

🏔️ Cảnh Giới: **{CANH_GIOI[0]}**
💧 Linh Lực: **100/100**
⚔️ Tấn Công: **10** | 🛡️ Phòng Thủ: **5**
💎 Linh Thạch: **50**

Dùng `!tulyen` để bắt đầu tu luyện!
    """, 0x55FFAA)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════
#  LỆNH: THÔNG TIN
# ══════════════════════════════════════════════════════════
@bot.command(name="tt", aliases=["thongtin", "info"])
async def thong_tin(ctx, member: discord.Member = None):
    """!tt — Xem thông tin nhân vật"""
    target = member or ctx.author
    nv = await get_nhanvat(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌ Chưa có nhân vật", "Dùng `!taonv <tên>` để tạo!", 0xFF4444))
        return
    canh_gioi = CANH_GIOI[nv["canh_gioi"]]
    exp_hien = nv["exp"]
    exp_max = exp_can(nv["canh_gioi"])
    thanh = int((exp_hien / exp_max) * 10)
    bar = "█" * thanh + "░" * (10 - thanh)
    e = embed_mau(f"📜 {nv['ten']}", f"""
🏔️ **Cảnh Giới:** {canh_gioi} (Lv.{nv['canh_gioi']})
✨ **Tu Vi:** {nv['tu_vi']:,}
📊 **EXP:** {exp_hien}/{exp_max} [{bar}]
💧 **Linh Lực:** {nv['linh_luc']}/{nv['linh_luc_max']}
⚔️ **Tấn Công:** {nv['tan_cong']} | 🛡️ **Phòng Thủ:** {nv['phong_thu']}
💎 **Linh Thạch:** {nv['linh_thach']:,}
🏯 **Tông Môn:** {nv['tong_mon'] or 'Vô Môn'}
    """)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════
#  LỆNH: TU LUYỆN (cooldown 60s)
# ══════════════════════════════════════════════════════════
@bot.command(name="tulyen", aliases=["tl"])
async def tu_luyen(ctx):
    """!tulyen — Tu luyện kiếm kinh nghiệm (60s/lần)"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    cd = cooldown_con(nv["last_tulyen"], 60)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳ Đang Tu Luyện", f"Còn **{cd:.0f}s** nữa mới có thể tu luyện tiếp!", 0xFFAA00))
        return

    exp_gain = random.randint(30, 80) + nv["canh_gioi"] * 10
    tu_vi_gain = random.randint(10, 30)
    linh_luc_hoi = random.randint(5, 15)
    new_exp = nv["exp"] + exp_gain
    new_tu_vi = nv["tu_vi"] + tu_vi_gain
    new_ll = min(nv["linh_luc"] + linh_luc_hoi, nv["linh_luc_max"])
    new_cg = nv["canh_gioi"]
    dot_pha_msg = ""

    # Tự động lên cảnh giới nếu đủ EXP
    while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI) - 1:
        new_exp -= exp_can(new_cg)
        new_cg += 1
        dot_pha_msg = f"\n\n🎉 **ĐỘT PHÁ!** Đã đạt cảnh giới **{CANH_GIOI[new_cg]}**! 🎉"

    await cap_nhat(ctx.author.id,
        exp=new_exp, tu_vi=new_tu_vi, canh_gioi=new_cg,
        linh_luc=new_ll,
        last_tulyen=datetime.now().isoformat()
    )
    e = embed_mau("🧘 Tu Luyện Thành Công", f"""
✨ **+{exp_gain} EXP** | 🌀 **+{tu_vi_gain} Tu Vi** | 💧 **+{linh_luc_hoi} Linh Lực**
📊 EXP: {new_exp}/{exp_can(new_cg)} | Cảnh Giới: {CANH_GIOI[new_cg]}
{dot_pha_msg}
    """, 0x55FFAA)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════
#  LỆNH: KHÁM PHÁ (kiếm linh thạch & vật phẩm)
# ══════════════════════════════════════════════════════════
@bot.command(name="khampha", aliases=["kp"])
async def kham_pha(ctx):
    """!khampha — Khám phá tìm vật phẩm (120s/lần)"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    cd = cooldown_con(nv["last_khampha"], 120)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳ Mệt Mỏi", f"Còn **{cd:.0f}s** nữa mới đủ sức khám phá!", 0xFFAA00))
        return

    results = []
    # Linh thạch
    lt = random.randint(10, 50) + nv["canh_gioi"] * 5
    results.append(f"💎 +{lt} Linh Thạch")
    new_lt = nv["linh_thach"] + lt

    # Vật phẩm ngẫu nhiên (~40%)
    vat_pham_found = None
    if random.random() < 0.4:
        vat_pham_found = random.choice(list(VAT_PHAM.keys()))
        results.append(f"🎁 Nhặt được **{vat_pham_found}**!")
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("""
                INSERT INTO tui_do (user_id, vat_pham, so_luong) VALUES (?,?,1)
                ON CONFLICT(user_id, vat_pham) DO UPDATE SET so_luong=so_luong+1
            """, (ctx.author.id, vat_pham_found))
            await db.commit()

    await cap_nhat(ctx.author.id, linh_thach=new_lt, last_khampha=datetime.now().isoformat())
    await ctx.send(embed=embed_mau("🗺️ Khám Phá", "\n".join(results), 0x55AAFF))

# ══════════════════════════════════════════════════════════
#  LỆNH: TÚI ĐỒ
# ══════════════════════════════════════════════════════════
@bot.command(name="tuido", aliases=["bag", "td"])
async def tui_do(ctx):
    """!tuido — Xem túi đồ"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tui_do WHERE user_id=?", (ctx.author.id,)) as cur:
            items = await cur.fetchall()
    if not items:
        await ctx.send(embed=embed_mau("🎒 Túi Đồ Trống", "Dùng `!khampha` để tìm vật phẩm!", 0x888888))
        return
    lines = []
    for item in items:
        vp = VAT_PHAM.get(item["vat_pham"], {})
        rare = vp.get("rare", "?")
        icon = {"thường":"⚪","hiếm":"🔵","quý":"🟣","thần khí":"🟡"}.get(rare, "⚪")
        lines.append(f"{icon} **{item['vat_pham']}** x{item['so_luong']} — {vp.get('cong_dung','')}")
    await ctx.send(embed=embed_mau(f"🎒 Túi Đồ — {nv['ten']}", "\n".join(lines)))

# ══════════════════════════════════════════════════════════
#  LỆNH: DÙNG VẬT PHẨM
# ══════════════════════════════════════════════════════════
@bot.command(name="dung", aliases=["use"])
async def dung_vat_pham(ctx, *, ten_vp: str):
    """!dung <tên vật phẩm> — Sử dụng vật phẩm"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tui_do WHERE user_id=? AND vat_pham=?", (ctx.author.id, ten_vp)) as cur:
            item = await cur.fetchone()
    if not item or item["so_luong"] < 1:
        await ctx.send(embed=embed_mau("❌ Không có vật phẩm này", f"Kiểm tra `!tuido`", 0xFF4444))
        return
    vp = VAT_PHAM.get(ten_vp)
    if not vp or vp["loai"] != "thuoc":
        await ctx.send(embed=embed_mau("❌", "Vật phẩm này không thể dùng trực tiếp!", 0xFF4444))
        return

    hieu_ung = vp["cong_dung"]
    updates = {}
    msg = f"✅ Đã dùng **{ten_vp}**!\n"

    if "linh lực" in hieu_ung:
        so = int(''.join(filter(str.isdigit, hieu_ung)))
        new_ll = min(nv["linh_luc"] + so, nv["linh_luc_max"])
        updates["linh_luc"] = new_ll
        msg += f"💧 Hồi +{so} Linh Lực → {new_ll}/{nv['linh_luc_max']}"
    elif "kinh nghiệm" in hieu_ung:
        so = int(''.join(filter(str.isdigit, hieu_ung)))
        updates["exp"] = nv["exp"] + so
        msg += f"✨ +{so} EXP"
    elif "đột phá" in hieu_ung:
        msg += f"🔮 Tăng 30% tỉ lệ đột phá lần tu luyện tiếp theo!"

    if updates:
        await cap_nhat(ctx.author.id, **updates)

    # Giảm số lượng
    async with aiosqlite.connect(DB_FILE) as db:
        if item["so_luong"] <= 1:
            await db.execute("DELETE FROM tui_do WHERE user_id=? AND vat_pham=?", (ctx.author.id, ten_vp))
        else:
            await db.execute("UPDATE tui_do SET so_luong=so_luong-1 WHERE user_id=? AND vat_pham=?", (ctx.author.id, ten_vp))
        await db.commit()

    await ctx.send(embed=embed_mau("💊 Dùng Vật Phẩm", msg, 0x55FFAA))

# ══════════════════════════════════════════════════════════
#  LỆNH: HỌC KỸ NĂNG
# ══════════════════════════════════════════════════════════
@bot.command(name="hockynang", aliases=["hkn"])
async def hoc_ky_nang(ctx, *, ten_kn: str):
    """!hockynang <tên kỹ năng> — Học kỹ năng"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    kn = KY_NANG.get(ten_kn)
    if not kn:
        ds = "\n".join(f"**{k}** — cần Lv.{v['cap_yeu']}" for k, v in KY_NANG.items())
        await ctx.send(embed=embed_mau("📚 Danh Sách Kỹ Năng", ds))
        return
    if nv["canh_gioi"] < kn["cap_yeu"]:
        await ctx.send(embed=embed_mau("❌ Chưa Đủ Cảnh Giới", f"Cần đạt **{CANH_GIOI[kn['cap_yeu']]}** mới học được!", 0xFF4444))
        return
    ky_nang_hien = nv["ky_nang"].split(",") if nv["ky_nang"] else []
    if ten_kn in ky_nang_hien:
        await ctx.send(embed=embed_mau("⚠️", "Bạn đã học kỹ năng này rồi!", 0xFFAA00))
        return
    phi = kn["sat_thuong"] * 20
    if nv["linh_thach"] < phi:
        await ctx.send(embed=embed_mau("❌ Không Đủ Linh Thạch", f"Cần **{phi}** Linh Thạch!", 0xFF4444))
        return
    ky_nang_hien.append(ten_kn)
    await cap_nhat(ctx.author.id,
        ky_nang=",".join(ky_nang_hien),
        linh_thach=nv["linh_thach"] - phi
    )
    await ctx.send(embed=embed_mau("⚡ Học Kỹ Năng Thành Công!", f"""
🔮 **{ten_kn}** đã được lĩnh ngộ!
💥 Sát Thương: **{kn['sat_thuong']}** | 💧 Tiêu Hao: **{kn['linh_luc']}** Linh Lực
💎 Đã trả: **{phi}** Linh Thạch
    """, 0xAA55FF))

# ══════════════════════════════════════════════════════════
#  LỆNH: BOSS CHIẾN
# ══════════════════════════════════════════════════════════
@bot.command(name="boss")
async def danh_boss(ctx, so_boss: int = None):
    """!boss [số] — Đánh boss (xem danh sách hoặc chiến đấu)"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    if so_boss is None:
        lines = []
        for i, b in enumerate(BOSS_LIST, 1):
            lock = "🔒" if nv["canh_gioi"] < b["cap_yeu"] else "⚔️"
            lines.append(f"{lock} **{i}. {b['ten']}** — HP: {b['hp']} | Cần: Lv.{b['cap_yeu']} | Thưởng: {b['phan_thuong']}💎")
        await ctx.send(embed=embed_mau("👹 Danh Sách Boss", "\n".join(lines)))
        return

    if so_boss < 1 or so_boss > len(BOSS_LIST):
        await ctx.send(embed=embed_mau("❌", f"Boss không hợp lệ! Chọn 1-{len(BOSS_LIST)}", 0xFF4444))
        return

    boss = BOSS_LIST[so_boss - 1]
    if nv["canh_gioi"] < boss["cap_yeu"]:
        await ctx.send(embed=embed_mau("❌ Chưa Đủ Sức", f"Cần cảnh giới **{CANH_GIOI[boss['cap_yeu']]}**!", 0xFF4444))
        return

    # Mô phỏng chiến đấu
    player_hp = nv["linh_luc"]
    boss_hp = boss["hp"]
    rounds = []
    turn = 0
    while player_hp > 0 and boss_hp > 0 and turn < 20:
        turn += 1
        p_atk = random.randint(nv["tan_cong"], nv["tan_cong"] * 2)
        b_dmg = max(1, p_atk - boss["sat_thuong"] // 4)
        boss_hp -= b_dmg
        b_atk = random.randint(boss["sat_thuong"] // 2, boss["sat_thuong"])
        p_dmg = max(1, b_atk - nv["phong_thu"])
        player_hp -= p_dmg
        if turn <= 3:
            rounds.append(f"Lượt {turn}: Bạn gây **{b_dmg}** sát thương | Boss gây **{p_dmg}** sát thương")

    if player_hp > 0:
        # Thắng
        await cap_nhat(ctx.author.id,
            linh_thach=nv["linh_thach"] + boss["phan_thuong"],
            exp=nv["exp"] + boss["exp"],
            linh_luc=max(1, player_hp)
        )
        result = f"🏆 **CHIẾN THẮNG!**\n" + "\n".join(rounds) + f"\n...\n\n💎 +{boss['phan_thuong']} Linh Thạch | ✨ +{boss['exp']} EXP"
        color = 0x55FF55
    else:
        # Thua
        await cap_nhat(ctx.author.id, linh_luc=1)
        result = f"💀 **THẤT BẠI!**\n" + "\n".join(rounds) + "\n...\n\nBạn đã bị đánh bại! Hồi phục linh lực rồi thử lại!"
        color = 0xFF4444

    await ctx.send(embed=embed_mau(f"⚔️ Boss: {boss['ten']}", result, color))

# ══════════════════════════════════════════════════════════
#  LỆNH: PVP
# ══════════════════════════════════════════════════════════
@bot.command(name="pvp")
async def pvp(ctx, doi_thu: discord.Member):
    """!pvp @người — Thách đấu người khác"""
    if doi_thu.id == ctx.author.id:
        await ctx.send(embed=embed_mau("❌", "Không thể tự đánh mình!", 0xFF4444))
        return
    nv1 = await get_nhanvat(ctx.author.id)
    nv2 = await get_nhanvat(doi_thu.id)
    if not nv1 or not nv2:
        await ctx.send(embed=embed_mau("❌", "Một trong hai người chưa có nhân vật!", 0xFF4444))
        return

    # Xác nhận
    e = embed_mau("⚔️ Thách Đấu!", f"**{nv1['ten']}** thách đấu **{nv2['ten']}**!\n{doi_thu.mention} phản hồi `chấp nhận` trong 30 giây!")
    await ctx.send(embed=e)

    def check(m):
        return m.author.id == doi_thu.id and m.content.lower() in ["chấp nhận", "chap nhan", "ok", "accept"]

    try:
        await bot.wait_for("message", check=check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.send(embed=embed_mau("⏰ Hết Giờ", f"{doi_thu.display_name} đã bỏ chạy!", 0xFF4444))
        return

    # Đấu
    hp1, hp2 = nv1["linh_luc"], nv2["linh_luc"]
    rounds = []
    for i in range(1, 11):
        atk1 = max(1, random.randint(nv1["tan_cong"], nv1["tan_cong"] * 2) - nv2["phong_thu"])
        atk2 = max(1, random.randint(nv2["tan_cong"], nv2["tan_cong"] * 2) - nv1["phong_thu"])
        hp2 -= atk1
        hp1 -= atk2
        if i <= 3:
            rounds.append(f"Lượt {i}: {nv1['ten']} -{atk1}HP | {nv2['ten']} -{atk2}HP")
        if hp1 <= 0 or hp2 <= 0:
            break

    thang = nv1 if hp2 <= 0 else nv2
    thua = nv2 if hp2 <= 0 else nv1
    phan_thuong = random.randint(20, 80)

    winner_id = ctx.author.id if hp2 <= 0 else doi_thu.id
    loser_id = doi_thu.id if hp2 <= 0 else ctx.author.id

    # Chuyển linh thạch
    w = await get_nhanvat(winner_id)
    l = await get_nhanvat(loser_id)
    lay = min(phan_thuong, l["linh_thach"])
    await cap_nhat(winner_id, linh_thach=w["linh_thach"] + lay)
    await cap_nhat(loser_id, linh_thach=max(0, l["linh_thach"] - lay), linh_luc=1)

    result = "\n".join(rounds) + f"\n...\n\n🏆 **{thang['ten']} THẮNG!**\nCướp được **{lay}** Linh Thạch từ {thua['ten']}!"
    await ctx.send(embed=embed_mau("⚔️ Kết Quả PVP", result, 0xFF8800))

# ══════════════════════════════════════════════════════════
#  LỆNH: TÔNG MÔN
# ══════════════════════════════════════════════════════════
@bot.command(name="lapmonsec", aliases=["lapmon"])
async def lap_tong_mon(ctx, *, ten_mon: str):
    """!lapmon <tên> — Lập tông môn (cần Lv.3+, 1000 Linh Thạch)"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    if nv["canh_gioi"] < 3:
        await ctx.send(embed=embed_mau("❌", "Cần đạt **Kim Đan** mới lập tông môn!", 0xFF4444))
        return
    if nv["linh_thach"] < 1000:
        await ctx.send(embed=embed_mau("❌", "Cần **1000** Linh Thạch!", 0xFF4444))
        return
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tong_mon WHERE ten=?", (ten_mon,)) as cur:
            existing = await cur.fetchone()
        if existing:
            await ctx.send(embed=embed_mau("❌", "Tông môn này đã tồn tại!", 0xFF4444))
            return
        await db.execute("INSERT INTO tong_mon (ten, chu_mon, thanh_vien) VALUES (?,?,?)",
                         (ten_mon, ctx.author.id, str(ctx.author.id)))
        await db.commit()
    await cap_nhat(ctx.author.id, linh_thach=nv["linh_thach"] - 1000, tong_mon=ten_mon)
    await ctx.send(embed=embed_mau("🏯 Lập Tông Môn Thành Công!", f"""
🏯 **{ten_mon}** đã được thành lập!
👑 Chưởng Môn: **{nv['ten']}**
💎 Phí: **1000** Linh Thạch

Mời người khác vào: `!thamgia {ten_mon}`
    """, 0xFFD700))

@bot.command(name="thamgia")
async def tham_gia_tong_mon(ctx, *, ten_mon: str):
    """!thamgia <tên tông môn> — Tham gia tông môn"""
    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    if nv["tong_mon"]:
        await ctx.send(embed=embed_mau("❌", f"Bạn đã ở tông môn **{nv['tong_mon']}**!", 0xFF4444))
        return
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tong_mon WHERE ten=?", (ten_mon,)) as cur:
            mon = await cur.fetchone()
        if not mon:
            await ctx.send(embed=embed_mau("❌", "Tông môn không tồn tại!", 0xFF4444))
            return
        members = mon["thanh_vien"] + f",{ctx.author.id}"
        await db.execute("UPDATE tong_mon SET thanh_vien=? WHERE ten=?", (members, ten_mon))
        await db.commit()
    await cap_nhat(ctx.author.id, tong_mon=ten_mon)
    await ctx.send(embed=embed_mau("🏯 Gia Nhập Thành Công!", f"**{nv['ten']}** đã gia nhập **{ten_mon}**!", 0x55FFAA))

# ══════════════════════════════════════════════════════════
#  LỆNH: MUA VẬT PHẨM
# ══════════════════════════════════════════════════════════
@bot.command(name="muahang", aliases=["shop"])
async def mua_hang(ctx, *, ten_vp: str = None):
    """!muahang — Xem shop | !muahang <tên> — Mua vật phẩm"""
    if ten_vp is None:
        lines = []
        for k, v in VAT_PHAM.items():
            icon = {"thường":"⚪","hiếm":"🔵","quý":"🟣","thần khí":"🟡"}.get(v["rare"],"⚪")
            lines.append(f"{icon} **{k}** — {v['gia']}💎 | {v['cong_dung']}")
        await ctx.send(embed=embed_mau("🛒 Cửa Hàng Linh Đan", "\n".join(lines)))
        return

    nv = await get_nhanvat(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌", "Dùng `!taonv <tên>` trước!", 0xFF4444))
        return
    vp = VAT_PHAM.get(ten_vp)
    if not vp:
        await ctx.send(embed=embed_mau("❌", "Vật phẩm không tồn tại!", 0xFF4444))
        return
    if nv["linh_thach"] < vp["gia"]:
        await ctx.send(embed=embed_mau("❌ Không Đủ Linh Thạch", f"Cần **{vp['gia']}** 💎", 0xFF4444))
        return
    await cap_nhat(ctx.author.id, linh_thach=nv["linh_thach"] - vp["gia"])
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT INTO tui_do (user_id, vat_pham, so_luong) VALUES (?,?,1)
            ON CONFLICT(user_id, vat_pham) DO UPDATE SET so_luong=so_luong+1
        """, (ctx.author.id, ten_vp))
        await db.commit()
    await ctx.send(embed=embed_mau("✅ Mua Thành Công!", f"Đã mua **{ten_vp}** (-{vp['gia']} 💎)", 0x55FFAA))

# ══════════════════════════════════════════════════════════
#  LỆNH: BẢNG XẾP HẠNG
# ══════════════════════════════════════════════════════════
@bot.command(name="bxh", aliases=["rank"])
async def bang_xep_hang(ctx):
    """!bxh — Bảng xếp hạng tu vi"""
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT ten, canh_gioi, tu_vi FROM nhanvat ORDER BY canh_gioi DESC, tu_vi DESC LIMIT 10") as cur:
            rows = await cur.fetchall()
    medals = ["🥇","🥈","🥉"] + ["🏅"] * 7
    lines = []
    for i, r in enumerate(rows):
        lines.append(f"{medals[i]} **{r['ten']}** — {CANH_GIOI[r['canh_gioi']]} | Tu Vi: {r['tu_vi']:,}")
    await ctx.send(embed=embed_mau("🏆 Bảng Xếp Hạng Tu Vi", "\n".join(lines) or "Chưa có ai tu luyện!"))

# ══════════════════════════════════════════════════════════
#  LỆNH: HELP
# ══════════════════════════════════════════════════════════
@bot.command(name="help", aliases=["huongdan", "hd"])
async def help_cmd(ctx):
    """!help — Danh sách lệnh"""
    e = embed_mau("📖 Hướng Dẫn Tu Tiên Bot", """
**👤 Nhân Vật**
`!taonv <tên>` — Tạo nhân vật
`!tt [@người]` — Xem thông tin

**⚡ Tu Luyện**
`!tulyen` — Tu luyện (60s)
`!khampha` — Khám phá (120s)

**⚔️ Chiến Đấu**
`!boss` — Xem / đánh boss
`!pvp @người` — Thách đấu

**🎒 Vật Phẩm & Kỹ Năng**
`!tuido` — Xem túi đồ
`!dung <vật phẩm>` — Dùng vật phẩm
`!muahang` — Cửa hàng
`!hockynang <kỹ năng>` — Học kỹ năng

**🏯 Tông Môn**
`!lapmon <tên>` — Lập tông môn
`!thamgia <tên>` — Gia nhập

**📊 Khác**
`!bxh` — Bảng xếp hạng
    """)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════
#  KHỞI ĐỘNG
# ══════════════════════════════════════════════════════════
@bot.event
async def on_ready():
    await init_db()
    print(f"✅ Bot Tu Tiên đã online: {bot.user}")
    await bot.change_presence(activity=discord.Game(name="Tu Tiên | !help"))

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=embed_mau("❌ Thiếu Tham Số", f"Dùng `!help` để xem hướng dẫn!", 0xFF4444))
    elif isinstance(error, commands.CommandNotFound):
        pass

bot.run(TOKEN)
