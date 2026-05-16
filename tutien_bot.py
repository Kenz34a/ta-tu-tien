"""
╔══════════════════════════════════════════════════════════════════╗
║         BOT TU TIÊN DISCORD - V3 MEGA UPDATE                    ║
║  pip install discord.py asyncpg                                  ║
║  Tính năng: Tộc, Map, Boss thế giới, Công pháp, Đạo,           ║
║  Kết duyên, Kiếm Linh, Bế quan, Trồng cây, Trang bị, Đan dược ║
╚══════════════════════════════════════════════════════════════════╝
"""
import discord
from discord.ext import commands, tasks
import asyncpg, random, asyncio, os, json, math
from datetime import datetime, timedelta, timezone

# ══════════════════════════════════════════════════════════════
#  CẤU HÌNH
# ══════════════════════════════════════════════════════════════
TOKEN  = os.getenv("DISCORD_TOKEN")
DB_URL = os.getenv("DATABASE_URL")
PREFIX = "!"
if not TOKEN:
    print("❌ Thiếu DISCORD_TOKEN!"); exit()
if not DB_URL:
    print("❌ Thiếu DATABASE_URL! Bot sẽ không thể lưu dữ liệu."); exit()
print(f"✅ Token OK | DB: {DB_URL[:30]}...")

# ══════════════════════════════════════════════════════════════
#  CẢNH GIỚI & BẢN ĐỒ
# ══════════════════════════════════════════════════════════════
CANH_GIOI = [
    # Nhân Giới (0-5)
    "Phàm Nhân","Luyện Khí","Trúc Cơ","Kim Đan","Nguyên Anh","Hóa Thần",
    # Linh Giới (6-9)
    "Luyện Hư","Hợp Thể","Đại Thừa","Độ Kiếp",
    # Tiên Giới (10-14)
    "Tiên Nhân","Chân Tiên","Thiên Tiên","Đại La Kim Tiên","Thánh Nhân",
    # Thánh Giới (15-24)
    "Thiên Đạo Thánh Nhân","Đạo Tổ","Chúa Tể","Chí Tôn","Vô Thượng Chí Tôn",
    "Thiên Đế","Tiên Đế","Thần Đế","Đạo Chủ","Thiên Đạo",
    # Vũ Trụ Cấp (25-36)
    "Siêu Thoát","Bất Hủ","Bất Diệt","Vĩnh Hằng","Chưởng Khống Giả","Sáng Thế",
    "Sáng Thế Chủ","Toàn Năng","Toàn Tri","Siêu Việt","Vô Cực","Vô Thượng Đại Đạo",
    # Hỗn Độn Cảnh (37-49)
    "Hỗn Độn Sơ Khai","Hỗn Độn Trung Kỳ","Hỗn Độn Hậu Kỳ","Hỗn Độn Đỉnh Phong","Hỗn Độn Chi Tôn",
    "Khai Thiên Giả","Tịch Địa Giả","Định Càn Khôn","Chưởng Thiên Địa","Vạn Giới Chi Chủ",
    "Tam Giới Chí Tôn","Ngũ Giới Bá Chủ","Thất Giới Thần Tôn",
    # Thái Cổ Cảnh (50-64)
    "Thái Cổ Sơ Cảnh","Thái Cổ Trung Cảnh","Thái Cổ Hậu Cảnh","Thái Cổ Đỉnh Phong","Thái Cổ Chi Tôn",
    "Thượng Cổ Thần Linh","Hồng Hoang Chi Thể","Nguyên Thủy Thần Tôn","Vô Thủy Vô Chung","Vô Lượng Thiên Tôn",
    "Cửu Thiên Huyền Nữ","Thái Ất Kim Tiên","Linh Bảo Thiên Tôn","Nguyên Thủy Thiên Tôn","Vô Thượng Thái Cổ",
    # Thần Thoại Cảnh (65-79)
    "Thần Thoại Sơ Hiện","Thần Thoại Giác Tỉnh","Thần Thoại Phi Thăng","Thần Thoại Chứng Đạo","Thần Thoại Chi Cực",
    "Cực Đạo Thần Tôn","Vạn Cổ Thần Đế","Siêu Việt Thần Thoại","Bất Sinh Bất Diệt","Vô Thượng Thần Tôn",
    "Càn Khôn Chí Tôn","Vũ Trụ Bản Nguyên","Thời Không Chi Chủ","Nhân Quả Chứng Đạo","Đại Đạo Viên Mãn",
    # Vô Thượng Cảnh (80-99) + Cực Đỉnh (100)
    "Vô Thượng Sơ Đăng","Vô Thượng Trung Kỳ","Vô Thượng Hậu Kỳ","Vô Thượng Viên Mãn","Siêu Việt Vô Thượng",
    "Vô Cực Chí Tôn","Vô Biên Thần Uy","Vô Lượng Thần Lực","Vô Thủy Thần Tôn","Chứng Đạo Thành Thánh",
    "Thánh Đạo Sơ Chứng","Thánh Đạo Trung Chứng","Thánh Đạo Viên Mãn","Siêu Phàm Nhập Thánh","Bán Bộ Đại Đạo",
    "Đại Đạo Sơ Ngộ","Đại Đạo Trung Ngộ","Đại Đạo Hậu Ngộ","Đại Đạo Chi Cực","Vạn Đạo Quy Nhất",
    # 100 - Đỉnh tuyệt đối
    "☀️ Vô Thượng Chí Tôn Đại Đạo"
]

BAN_DO = {
    "nhan_gioi":  {"ten":"🟢 Nhân Giới",    "cap_min":0,  "cap_max":5,  "mo_ta":"Early game — Phàm Nhân → Hóa Thần",          "phi_thuong":6},
    "linh_gioi":  {"ten":"🔵 Linh Giới",    "cap_min":6,  "cap_max":9,  "mo_ta":"Mid game — Luyện Hư → Độ Kiếp",              "phi_thuong":10},
    "tien_gioi":  {"ten":"🟣 Tiên Giới",    "cap_min":10, "cap_max":14, "mo_ta":"Late game — Tiên Nhân → Thánh Nhân",          "phi_thuong":15},
    "thanh_gioi": {"ten":"🟡 Thánh Giới",   "cap_min":15, "cap_max":24, "mo_ta":"End game — Thánh Nhân → Thiên Đạo",          "phi_thuong":25},
    "vu_tru":     {"ten":"🔴 Vũ Trụ Cấp",   "cap_min":25, "cap_max":36, "mo_ta":"Ultra end — Siêu Thoát → Vô Thượng Đại Đạo","phi_thuong":37},
    "hon_don":    {"ten":"🌀 Hỗn Độn Cảnh", "cap_min":37, "cap_max":49, "mo_ta":"Extreme — Hỗn Độn → Thất Giới Thần Tôn",    "phi_thuong":50},
    "thai_co":    {"ten":"⚫ Thái Cổ Cảnh", "cap_min":50, "cap_max":64, "mo_ta":"Godlike — Thái Cổ → Vô Thượng Thái Cổ",     "phi_thuong":65},
    "than_thoai": {"ten":"🌟 Thần Thoại Cảnh","cap_min":65,"cap_max":79,"mo_ta":"Mythic — Thần Thoại → Đại Đạo Viên Mãn",    "phi_thuong":80},
    "vo_thuong":  {"ten":"☀️ Vô Thượng Cảnh","cap_min":80,"cap_max":100,"mo_ta":"Absolute — Vô Thượng → Vô Thượng Chí Tôn",  "phi_thuong":None},
}

def get_ban_do(canh_gioi_idx: int) -> str:
    for key, bd in BAN_DO.items():
        if bd["cap_min"] <= canh_gioi_idx <= bd["cap_max"]:
            return key
    return "vu_tru"

# ══════════════════════════════════════════════════════════════
#  TỘC
# ══════════════════════════════════════════════════════════════
TOC = {
    "Long Tộc":  {"icon":"🐉","mo_ta":"Bá chủ vạn thú, máu rồng cổ đại thần thánh",      "bonus_tan_cong":80, "bonus_phong_thu":30, "bonus_hp":5000, "bonus_exp":15,  "ki_nang_dac_biet":"Rồng Ngâm"},
    "Thần Tộc":  {"icon":"⚡","mo_ta":"Con cháu chư thần, thiên phú vượt vạn cổ",         "bonus_tan_cong":60, "bonus_phong_thu":60, "bonus_hp":3000, "bonus_exp":30,  "ki_nang_dac_biet":"Thần Ân"},
    "Nhân Tộc":  {"icon":"👤","mo_ta":"Tiềm năng vô hạn, vạn đạo dung hợp siêu việt",     "bonus_tan_cong":30, "bonus_phong_thu":30, "bonus_hp":1500, "bonus_exp":50,  "ki_nang_dac_biet":"Thiên Phú"},
    "Tiên Tộc":  {"icon":"🌸","mo_ta":"Thể chất thanh linh, linh lực thuần túy vô song",   "bonus_tan_cong":50, "bonus_phong_thu":50, "bonus_hp":4000, "bonus_exp":40,  "ki_nang_dac_biet":"Tiên Thể"},
    "Ma Tộc":    {"icon":"😈","mo_ta":"Sức mạnh hủy diệt thiên địa, đao ma vô thượng",     "bonus_tan_cong":120,"bonus_phong_thu":10, "bonus_hp":1000, "bonus_exp":20,  "ki_nang_dac_biet":"Ma Thể"},
    "Thú Tộc":   {"icon":"🐺","mo_ta":"Bản năng chiến đấu thuần túy, thể xác bất diệt",   "bonus_tan_cong":100,"bonus_phong_thu":80, "bonus_hp":8000, "bonus_exp":10,  "ki_nang_dac_biet":"Dã Tính"},
}

# ══════════════════════════════════════════════════════════════
#  LINH CĂN
# ══════════════════════════════════════════════════════════════
LINH_CAN = {
    "Thiên Linh Căn":   {"icon":"🌟","mo_ta":"Vạn năm hiếm có một, thiên phú tuyệt thế vô song",   "bonus_exp":200,"bonus_tuluyen":150,"ty_le":1},
    "Biến Linh Căn":    {"icon":"🌈","mo_ta":"5 hệ hỗn dung, tiến tốc kinh thiên động địa",          "bonus_exp":120,"bonus_tuluyen":80, "ty_le":4},
    "Tứ Linh Căn":      {"icon":"💫","mo_ta":"4 hệ linh căn cân bằng, thiên địa chứng đạo",          "bonus_exp":80, "bonus_tuluyen":50, "ty_le":10},
    "Tam Linh Căn":     {"icon":"✨","mo_ta":"3 hệ linh căn, khí vận phi thường",                    "bonus_exp":50, "bonus_tuluyen":35, "ty_le":20},
    "Song Linh Căn":    {"icon":"⭐","mo_ta":"2 hệ linh căn, tài chất xuất chúng",                   "bonus_exp":30, "bonus_tuluyen":20, "ty_le":30},
    "Đơn Linh Căn":     {"icon":"🔥","mo_ta":"1 hệ chuyên sâu, chuyên tinh hóa thần",               "bonus_exp":25, "bonus_tuluyen":25, "ty_le":25},
    "Phế Linh Căn":     {"icon":"💀","mo_ta":"Vô căn nhưng ý chí thép, nghịch thiên cải mệnh",      "bonus_exp":5,  "bonus_tuluyen":5,  "ty_le":10},
}

def random_linh_can() -> str:
    pool_lc = []
    for k,v in LINH_CAN.items():
        pool_lc.extend([k]*v["ty_le"])
    return random.choice(pool_lc)

# ══════════════════════════════════════════════════════════════
#  LỰC CHIẾN
# ══════════════════════════════════════════════════════════════
def tinh_luc_chien(nv) -> int:
    base = nv['tan_cong'] * 8 + nv['phong_thu'] * 6 + nv['linh_luc_max'] // 20
    cg = nv['canh_gioi']
    cg_bonus = int(500 * (2.2 ** cg))   # exponential nhưng không quá điên
    tv_bonus = nv['tu_vi'] // 500
    return base + cg_bonus + tv_bonus

def luc_chien_rank(lc: int) -> str:
    if lc < 5_000:         return "⚪ Phàm"
    if lc < 50_000:        return "🟢 Tinh Anh"
    if lc < 500_000:       return "🔵 Cường Giả"
    if lc < 5_000_000:     return "🟣 Tôn Giả"
    if lc < 50_000_000:    return "🟡 Hoàng Giả"
    if lc < 1_000_000_000: return "🔴 Đế Giả"
    return "⚫ Siêu Việt"

# ══════════════════════════════════════════════════════════════
#  BOSS (tất cả các giới)
# ══════════════════════════════════════════════════════════════
BOSS_LIST = [
    # Nhân Giới early
    {"ten":"Yêu Hồ Hắc Phong",   "hp":500,    "sat_thuong":40,   "phan_thuong":300,    "exp":500,    "cap_yeu":2,  "gioi":"nhan_gioi"},
    {"ten":"Ma Tướng Thiết Giáp", "hp":1200,   "sat_thuong":80,   "phan_thuong":800,    "exp":1200,   "cap_yeu":4,  "gioi":"nhan_gioi"},
    {"ten":"Cổ Long Hắc Diệm",    "hp":3000,   "sat_thuong":150,  "phan_thuong":2000,   "exp":3000,   "cap_yeu":5,  "gioi":"nhan_gioi"},
    # Nhân Giới late
    {"ten":"Huyết Ma Lão Tổ",     "hp":35000,  "sat_thuong":800,  "phan_thuong":25000,  "exp":35000,  "cap_yeu":4,  "gioi":"nhan_gioi"},
    {"ten":"Thiên Kiếm Tôn Giả",  "hp":50000,  "sat_thuong":1000, "phan_thuong":40000,  "exp":50000,  "cap_yeu":5,  "gioi":"nhan_gioi"},
    {"ten":"Băng Phượng Cổ Thần", "hp":70000,  "sat_thuong":1300, "phan_thuong":60000,  "exp":70000,  "cap_yeu":5,  "gioi":"nhan_gioi"},
    {"ten":"Lôi Đế Thượng Cổ",    "hp":100000, "sat_thuong":1600, "phan_thuong":90000,  "exp":100000, "cap_yeu":5,  "gioi":"nhan_gioi"},
    {"ten":"Ma Đế Vạn Cổ",        "hp":140000, "sat_thuong":2000, "phan_thuong":130000, "exp":140000, "cap_yeu":5,  "gioi":"nhan_gioi"},
    # Linh Giới
    {"ten":"Thiên Đạo Sứ Giả",    "hp":200000, "sat_thuong":2600, "phan_thuong":200000, "exp":200000, "cap_yeu":6,  "gioi":"linh_gioi"},
    {"ten":"Hư Không Cự Thú",     "hp":280000, "sat_thuong":3200, "phan_thuong":280000, "exp":280000, "cap_yeu":7,  "gioi":"linh_gioi"},
    {"ten":"Tinh Hà Cổ Thần",     "hp":380000, "sat_thuong":4000, "phan_thuong":380000, "exp":380000, "cap_yeu":8,  "gioi":"linh_gioi"},
    {"ten":"Hỗn Độn Ma Tổ",       "hp":520000, "sat_thuong":4800, "phan_thuong":520000, "exp":520000, "cap_yeu":9,  "gioi":"linh_gioi"},
    {"ten":"Thiên Đế Phân Thân",  "hp":700000, "sat_thuong":6000, "phan_thuong":700000, "exp":700000, "cap_yeu":9,  "gioi":"linh_gioi"},
    # Tiên Giới
    {"ten":"Tiên Cung Thủ Hộ",    "hp":1000000,"sat_thuong":8000, "phan_thuong":1000000,"exp":1000000,"cap_yeu":10, "gioi":"tien_gioi"},
    {"ten":"Hỗn Nguyên Cự Linh",  "hp":1500000,"sat_thuong":10000,"phan_thuong":1500000,"exp":1500000,"cap_yeu":11, "gioi":"tien_gioi"},
    {"ten":"Thái Cổ Tiên Thú",    "hp":2200000,"sat_thuong":13000,"phan_thuong":2200000,"exp":2200000,"cap_yeu":12, "gioi":"tien_gioi"},
    # Thánh Giới
    {"ten":"Đạo Tổ Hư Ảnh",       "hp":3200000,"sat_thuong":18000,"phan_thuong":3000000,"exp":3200000,"cap_yeu":15, "gioi":"thanh_gioi"},
    {"ten":"Thiên Mệnh Chi Tử",    "hp":5000000,"sat_thuong":23000,"phan_thuong":5000000,"exp":5000000,"cap_yeu":18, "gioi":"thanh_gioi"},
    {"ten":"Vũ Trụ Chi Linh",      "hp":8000000,"sat_thuong":30000,"phan_thuong":8000000,"exp":8000000,"cap_yeu":21, "gioi":"thanh_gioi"},
    {"ten":"Hồng Hoang Cổ Thần",   "hp":12000000,"sat_thuong":40000,"phan_thuong":12000000,"exp":12000000,"cap_yeu":23,"gioi":"thanh_gioi"},
    # Vũ Trụ Cấp
    {"ten":"Thiên Đạo Hóa Thân",   "hp":20000000,"sat_thuong":55000,"phan_thuong":20000000,"exp":20000000,"cap_yeu":25,"gioi":"vu_tru"},
    {"ten":"Hỗn Độn Sáng Thế Thần","hp":35000000,"sat_thuong":75000,"phan_thuong":35000000,"exp":35000000,"cap_yeu":30,"gioi":"vu_tru"},
    {"ten":"Vô Thượng Đại Đạo",    "hp":80000000,"sat_thuong":120000,"phan_thuong":80000000,"exp":80000000,"cap_yeu":34,"gioi":"vu_tru"},
]

# Boss thế giới — mỗi giới có danh sách boss, rotate mỗi lần hồi sinh
BOSS_THE_GIOI_LIST = {
    "nhan_gioi": [
        {"ten":"💀 Ma Đế Thiên Tuyệt",     "hp":5_000_000,     "sat_thuong":5_000,   "phan_thuong":500_000,    "exp":500_000,    "cap_yeu":3,  "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"🔥 Hỏa Linh Đại Yêu",      "hp":8_000_000,     "sat_thuong":7_000,   "phan_thuong":800_000,    "exp":800_000,    "cap_yeu":4,  "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"⚡ Lôi Thiên Cổ Thú",       "hp":12_000_000,    "sat_thuong":9_000,   "phan_thuong":1_200_000,  "exp":1_200_000,  "cap_yeu":5,  "img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "linh_gioi": [
        {"ten":"🌀 Hư Không Chi Thần",      "hp":20_000_000,    "sat_thuong":15_000,  "phan_thuong":2_000_000,  "exp":2_000_000,  "cap_yeu":7,  "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"🌊 Thương Hải Cự Linh",     "hp":35_000_000,    "sat_thuong":22_000,  "phan_thuong":3_500_000,  "exp":3_500_000,  "cap_yeu":8,  "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"🌑 Hắc Ám Thần Tôn",        "hp":50_000_000,    "sat_thuong":30_000,  "phan_thuong":5_000_000,  "exp":5_000_000,  "cap_yeu":9,  "img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "tien_gioi": [
        {"ten":"🌸 Thái Cổ Tiên Đế",        "hp":80_000_000,    "sat_thuong":40_000,  "phan_thuong":8_000_000,  "exp":8_000_000,  "cap_yeu":11, "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"⚔️ Tiên Kiếm Thánh Tôn",    "hp":120_000_000,   "sat_thuong":60_000,  "phan_thuong":12_000_000, "exp":12_000_000, "cap_yeu":12, "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"🦋 Hồ Tiên Cổ Thần",        "hp":180_000_000,   "sat_thuong":80_000,  "phan_thuong":18_000_000, "exp":18_000_000, "cap_yeu":14, "img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "thanh_gioi": [
        {"ten":"👑 Thánh Giới Chi Chủ",      "hp":300_000_000,   "sat_thuong":100_000, "phan_thuong":30_000_000, "exp":30_000_000, "cap_yeu":16, "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"🌌 Thiên Đạo Hiển Linh",     "hp":500_000_000,   "sat_thuong":150_000, "phan_thuong":50_000_000, "exp":50_000_000, "cap_yeu":20, "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"💎 Hồng Hoang Sáng Thế Thần","hp":800_000_000,   "sat_thuong":200_000, "phan_thuong":80_000_000, "exp":80_000_000, "cap_yeu":23, "img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "vu_tru": [
        {"ten":"☀️ Vô Thượng Thiên Đạo",    "hp":999_999_999,   "sat_thuong":300_000, "phan_thuong":100_000_000,"exp":100_000_000,"cap_yeu":26, "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"🔮 Hỗn Độn Sáng Thế Linh",  "hp":2_000_000_000, "sat_thuong":500_000, "phan_thuong":200_000_000,"exp":200_000_000,"cap_yeu":30, "img":"https://i.imgur.com/7Wh5G3N.png"},
        {"ten":"⚫ Vô Thượng Đại Đạo Thần", "hp":5_000_000_000, "sat_thuong":800_000, "phan_thuong":500_000_000,"exp":500_000_000,"cap_yeu":34, "img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "hon_don": [
        {"ten":"🌀 Hỗn Độn Ma Thần",        "hp":10_000_000_000,"sat_thuong":1_500_000,"phan_thuong":1_000_000_000,"exp":1_000_000_000,"cap_yeu":37,"img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "thai_co": [
        {"ten":"⚫ Thái Cổ Hung Thú",       "hp":50_000_000_000,"sat_thuong":5_000_000,"phan_thuong":5_000_000_000,"exp":5_000_000_000,"cap_yeu":50,"img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "than_thoai": [
        {"ten":"🌟 Thần Thoại Cổ Thần",     "hp":200_000_000_000,"sat_thuong":20_000_000,"phan_thuong":20_000_000_000,"exp":20_000_000_000,"cap_yeu":65,"img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
    "vo_thuong": [
        {"ten":"☀️ Vô Thượng Chí Tôn Thần", "hp":999_999_999_999,"sat_thuong":100_000_000,"phan_thuong":100_000_000_000,"exp":100_000_000_000,"cap_yeu":80,"img":"https://i.imgur.com/7Wh5G3N.png"},
    ],
}

def get_boss_hien_tai(gioi: str, idx: int = 0) -> dict:
    lst = BOSS_THE_GIOI_LIST.get(gioi, [])
    if not lst: return None
    return lst[idx % len(lst)]

# Compat cũ
BOSS_THE_GIOI = {k: v[0] for k, v in BOSS_THE_GIOI_LIST.items()}

# ══════════════════════════════════════════════════════════════
#  CÔNG PHÁP
# ══════════════════════════════════════════════════════════════
CONG_PHAP_TAN_CONG = {
    "Kiếm Khí Thuật":       {"sat_thuong":30,  "linh_luc":20,  "cap_yeu":1,  "phi":300},
    "Phong Nhận":           {"sat_thuong":45,  "linh_luc":25,  "cap_yeu":2,  "phi":500},
    "Lôi Pháp":             {"sat_thuong":60,  "linh_luc":40,  "cap_yeu":3,  "phi":800},
    "Băng Tiễn Thuật":      {"sat_thuong":70,  "linh_luc":45,  "cap_yeu":3,  "phi":900},
    "Thổ Linh Chấn":        {"sat_thuong":90,  "linh_luc":55,  "cap_yeu":4,  "phi":1200},
    "Hỏa Long Kiếm":        {"sat_thuong":100, "linh_luc":60,  "cap_yeu":5,  "phi":1500},
    "Huyết Ảnh Trảm":       {"sat_thuong":130, "linh_luc":80,  "cap_yeu":6,  "phi":2000},
    "Phong Lôi Kiếm Trận":  {"sat_thuong":170, "linh_luc":95,  "cap_yeu":7,  "phi":2500},
    "Thiên Lôi Nhất Kích":  {"sat_thuong":200, "linh_luc":120, "cap_yeu":8,  "phi":3000},
    "Diệt Thế Hỏa Liên":    {"sat_thuong":260, "linh_luc":150, "cap_yeu":9,  "phi":4000},
    "Thiên Hà Kiếm Ý":      {"sat_thuong":320, "linh_luc":180, "cap_yeu":10, "phi":5000},
    "Vạn Kiếm Quy Tông":    {"sat_thuong":400, "linh_luc":200, "cap_yeu":11, "phi":6000},
    "Thần Lôi Diệt Ma":     {"sat_thuong":500, "linh_luc":260, "cap_yeu":12, "phi":8000},
    "Tru Tiên Kiếm Trận":   {"sat_thuong":700, "linh_luc":350, "cap_yeu":14, "phi":12000},
    "Diệt Thế Thiên Phạt":  {"sat_thuong":900, "linh_luc":450, "cap_yeu":16, "phi":18000},
}

CONG_PHAP_PHONG_THU = {
    "Kim Cang Hộ Thể":    {"phong_thu_bonus":50,  "linh_luc":40,  "cap_yeu":3,  "phi":800,  "buff":"giam_damage"},
    "Thanh Tâm Quyết":    {"phong_thu_bonus":30,  "linh_luc":35,  "cap_yeu":4,  "phi":900,  "buff":"hoi_mana"},
    "Bất Diệt Kim Thân":  {"phong_thu_bonus":150, "linh_luc":120, "cap_yeu":9,  "phi":5000, "buff":"shield"},
    "Thiên Đạo Gia Hộ":   {"phong_thu_bonus":300, "linh_luc":200, "cap_yeu":13, "phi":15000,"buff":"bat_tu_1_luot"},
    "Thời Không Gia Tốc": {"phong_thu_bonus":100, "linh_luc":160, "cap_yeu":12, "phi":10000,"buff":"tang_toc_do"},
}

DAI_THAN_THONG = {
    "Pháp Thiên Tượng Địa":  {"sat_thuong":1200, "linh_luc":600,  "cap_yeu":18, "phi":50000},
    "Tam Thiên Lôi Kiếp":    {"sat_thuong":1500, "linh_luc":750,  "cap_yeu":20, "phi":80000},
    "Nhất Niệm Diệt Thế":    {"sat_thuong":2000, "linh_luc":900,  "cap_yeu":22, "phi":120000},
    "Thiên Đạo Tru Sát":     {"sat_thuong":2600, "linh_luc":1200, "cap_yeu":25, "phi":200000},
    "Đại Đạo Chi Thủ":       {"sat_thuong":3500, "linh_luc":1600, "cap_yeu":30, "phi":500000},
}

CONG_PHAP_PASSIVE = {
    "Thổ Nạp Tâm Pháp":      {"bonus_tuvi":50,  "cap_yeu":1,  "phi":500,   "mo_ta":"+50 Tu Vi/lần tu"},
    "Cửu Dương Thần Công":    {"bonus_damage":30,"cap_yeu":5,  "phi":2000,  "mo_ta":"+30% sát thương"},
    "Thái Cực Chân Quyết":    {"bonus_mana":2000,"cap_yeu":7,  "phi":3000,  "mo_ta":"+2000 Linh Lực tối đa"},
    "Bất Tử Trường Sinh Công":{"bonus_hp":10000, "cap_yeu":9,  "phi":5000,  "mo_ta":"+10,000 HP tối đa"},
    "Hỗn Độn Đạo Kinh":       {"bonus_all":200,  "cap_yeu":15, "phi":30000, "mo_ta":"+200 tất cả chỉ số"},
}

# ══════════════════════════════════════════════════════════════
#  ĐẠO & ĐẠO PHỤ
# ══════════════════════════════════════════════════════════════
DAO_CHINH = {
    "Kiếm Đạo":    {"mo_ta":"Con đường kiếm pháp vô song",       "bonus_atk":20, "cap_yeu":5,  "phi":5000},
    "Lôi Đạo":     {"mo_ta":"Nắm giữ sức mạnh thiên lôi",        "bonus_atk":15, "cap_yeu":5,  "phi":5000},
    "Hỏa Đạo":     {"mo_ta":"Thống lĩnh ngọn lửa hủy diệt",      "bonus_atk":15, "cap_yeu":5,  "phi":5000},
    "Băng Đạo":    {"mo_ta":"Đóng băng vạn vật thiên hạ",         "bonus_def":20, "cap_yeu":5,  "phi":5000},
    "Không Đạo":   {"mo_ta":"Hư không chi lực, thoắt ẩn thoắt hiện","bonus_exp":15,"cap_yeu":8, "phi":8000},
    "Thời Gian Đạo":{"mo_ta":"Kiểm soát dòng chảy thời gian",    "bonus_cd":30,  "cap_yeu":10, "phi":15000},
    "Sinh Tử Đạo": {"mo_ta":"Nắm quyền sinh tử chúng sinh",       "bonus_atk":30, "cap_yeu":15, "phi":30000},
    "Hỗn Độn Đạo": {"mo_ta":"Đạo của khởi nguồn vũ trụ",         "bonus_all":25, "cap_yeu":20, "phi":100000},
}

DAO_PHU = {
    "Đan Đạo":     {"mo_ta":"Luyện đan thần diệu", "bonus_dan":20,  "cap_yeu":3, "phi":2000},
    "Khí Đạo":     {"mo_ta":"Thuần hóa linh khí",  "bonus_mana":30, "cap_yeu":3, "phi":2000},
    "Trận Đạo":    {"mo_ta":"Bố trận pháp thiên địa","bonus_def":15, "cap_yeu":5, "phi":4000},
    "Cơ Khí Đạo":  {"mo_ta":"Chế tạo bảo khí",     "bonus_equip":1, "cap_yeu":5, "phi":4000},
    "Huyết Đạo":   {"mo_ta":"Dùng máu làm pháp lực","bonus_atk":20, "cap_yeu":7, "phi":6000},
}

# ══════════════════════════════════════════════════════════════
#  TRANG BỊ (14 phẩm chất)
# ══════════════════════════════════════════════════════════════
PHAM_CHAT = ["Phàm","Linh","Huyền","Địa","Thiên","Vương","Hoàng","Đế","Thánh","Tiên","Thần","Chí Tôn","Hồng Mông","Vô Thượng"]
PHAM_CHAT_ICON = ["⚪","🟢","🔵","🟣","🟡","🔶","🟠","🔴","⭐","💫","✨","🌟","🌈","☀️"]
PHAM_CHAT_BONUS = [1,2,3,5,8,12,18,26,36,50,70,100,150,200]

LOAI_TRANG_BI = ["Vũ Khí","Giáp","Mũ","Nhẫn","Vòng Tay","Đai Lưng","Hài","Áo Choàng"]
TRANG_BI_TEN = {
    "Vũ Khí":    ["Kiếm","Đao","Thương","Cung","Phủ Việt","Chùy","Tiêu","Trượng"],
    "Giáp":      ["Giáp Sắt","Linh Giáp","Huyền Giáp","Kim Cang Giáp","Tiên Giáp"],
    "Mũ":        ["Mũ Linh","Mũ Huyền","Kim Quan","Tiên Miện","Thần Quan"],
    "Nhẫn":      ["Nhẫn Linh","Nhẫn Pháp","Nhẫn Không Gian","Nhẫn Đạo"],
    "Vòng Tay":  ["Vòng Ngọc","Vòng Linh","Vòng Thần Lực","Vòng Tiên"],
    "Đai Lưng":  ["Đai Linh","Đai Huyền","Đai Thần Lực"],
    "Hài":       ["Hài Linh","Hài Phong","Hài Tiên"],
    "Áo Choàng": ["Áo Linh","Áo Huyền","Áo Tiên","Áo Thần"],
}

def gen_trang_bi(cap_yeu=0):
    pham = min(cap_yeu // 3, 13)
    pham = max(0, pham + random.randint(-1, 1))
    pham = max(0, min(13, pham))
    loai = random.choice(LOAI_TRANG_BI)
    ten_base = random.choice(TRANG_BI_TEN.get(loai, ["Bảo Khí"]))
    icon = PHAM_CHAT_ICON[pham]
    mul = PHAM_CHAT_BONUS[pham]
    atk = random.randint(5,15) * mul if loai == "Vũ Khí" else random.randint(0,5) * mul
    def_ = random.randint(5,12) * mul if loai != "Vũ Khí" else random.randint(0,3) * mul
    ten_day_du = f"{icon}{PHAM_CHAT[pham]} {ten_base}"
    return {
        "ten": ten_day_du, "loai": loai, "pham_chat": pham,
        "atk": atk, "def": def_,
        "gia_ban": mul * random.randint(100,300)
    }

# ══════════════════════════════════════════════════════════════
#  ĐAN DƯỢC
# ══════════════════════════════════════════════════════════════
DAN_DUOC = {
    # Hồi phục
    "Linh Thảo":          {"loai":"hoi_phuc","hp":50,   "gia":100,  "cap_yeu":0, "rare":"⚪"},
    "Hồi Linh Đan":       {"loai":"hoi_phuc","hp":200,  "gia":500,  "cap_yeu":2, "rare":"🟢"},
    "Đại Hồi Linh Đan":   {"loai":"hoi_phuc","hp":500,  "gia":1500, "cap_yeu":5, "rare":"🔵"},
    "Thần Hồi Đan":       {"loai":"hoi_phuc","hp":2000, "gia":8000, "cap_yeu":10,"rare":"🟣"},
    # Tu Vi
    "Tụ Linh Đan":        {"loai":"tu_vi",  "exp":500,  "gia":800,  "cap_yeu":2, "rare":"🟢"},
    "Tụ Nguyên Đan":      {"loai":"tu_vi",  "exp":2000, "gia":3000, "cap_yeu":6, "rare":"🔵"},
    "Thần Nguyên Đan":    {"loai":"tu_vi",  "exp":10000,"gia":15000,"cap_yeu":12,"rare":"🟣"},
    "Tiên Nguyên Đan":    {"loai":"tu_vi",  "exp":50000,"gia":80000,"cap_yeu":18,"rare":"🟡"},
    # Đột phá / Độ kiếp
    "Phá Cảnh Đan":       {"loai":"dot_pha","ti_le":30, "gia":2000, "cap_yeu":3, "rare":"🔵"},
    "Đại Phá Cảnh Đan":   {"loai":"dot_pha","ti_le":60, "gia":8000, "cap_yeu":8, "rare":"🟣"},
    "Độ Kiếp Đan":        {"loai":"do_kiep","giam_kien":50,"gia":20000,"cap_yeu":9,"rare":"🟡"},
    "Thiên Kiếp Phù":     {"loai":"do_kiep","giam_kien":80,"gia":80000,"cap_yeu":15,"rare":"⭐"},
    # Tăng chỉ số vĩnh viễn
    "Lực Nguyên Đan":     {"loai":"buff_atk","atk":5,   "gia":5000, "cap_yeu":5, "rare":"🔵"},
    "Hộ Thể Đan":         {"loai":"buff_def","def":5,   "gia":5000, "cap_yeu":5, "rare":"🔵"},
    "Thọ Mệnh Đan":       {"loai":"buff_hp", "hp_max":100,"gia":10000,"cap_yeu":8,"rare":"🟣"},
    "Cửu Chuyển Kim Đan": {"loai":"buff_all","all":10,  "gia":100000,"cap_yeu":15,"rare":"⭐"},
}

# ══════════════════════════════════════════════════════════════
#  CÂY LINH THẢO (trồng cây)
# ══════════════════════════════════════════════════════════════
CAY_LINH = {
    "Linh Thảo":      {"thoi_gian":60,   "so_luong":(1,3),  "gia_hat":50,   "cap_yeu":0},
    "Hỏa Liên":       {"thoi_gian":300,  "so_luong":(1,2),  "gia_hat":200,  "cap_yeu":2},
    "Băng Liên":      {"thoi_gian":600,  "so_luong":(1,2),  "gia_hat":300,  "cap_yeu":3},
    "Lôi Thảo":       {"thoi_gian":1800, "so_luong":(1,3),  "gia_hat":800,  "cap_yeu":5},
    "Thần Linh Thảo": {"thoi_gian":7200, "so_luong":(2,5),  "gia_hat":5000, "cap_yeu":10},
    "Tiên Đào":       {"thoi_gian":86400,"so_luong":(1,3),  "gia_hat":50000,"cap_yeu":15},
}

# ══════════════════════════════════════════════════════════════
#  CÂU CÁ
# ══════════════════════════════════════════════════════════════
CAU_CA_POOL = [
    {"ten":"Cá Chép Bạc",    "loai":"⚪ Thường",   "ty_le":28, "tu_vi":80,     "lt":200,    "mo_ta":"Cá thường"},
    {"ten":"Cá Vàng Linh",   "loai":"⚪ Thường",   "ty_le":22, "tu_vi":250,    "lt":600,    "mo_ta":"Có linh khí nhẹ"},
    {"ten":"Cá Ngọc Thạch",  "loai":"🟢 Hiếm",    "ty_le":16, "tu_vi":800,    "lt":2000,   "mo_ta":"Linh khí dồi dào"},
    {"ten":"Cá Hỏa Long",    "loai":"🟢 Hiếm",    "ty_le":12, "tu_vi":2000,   "lt":5000,   "mo_ta":"Vảy rực lửa"},
    {"ten":"Cá Băng Tinh",   "loai":"🔵 Quý",     "ty_le":8,  "tu_vi":6000,   "lt":15000,  "mo_ta":"Lạnh buốt xương"},
    {"ten":"Cá Thiên Lôi",   "loai":"🔵 Quý",     "ty_le":6,  "tu_vi":15000,  "lt":40000,  "mo_ta":"Điện chạy khắp thân"},
    {"ten":"Cá Cửu Âm",      "loai":"🟣 Thần",    "ty_le":4,  "tu_vi":50000,  "lt":150000, "mo_ta":"Âm khí cực nặng"},
    {"ten":"Cá Thần Long",   "loai":"🟡 Cổ Thần", "ty_le":2,  "tu_vi":200000, "lt":800000, "mo_ta":"Con cháu thần long"},
    {"ten":"Tiên Ngư",       "loai":"⭐ Tiên Phẩm","ty_le":1,  "tu_vi":1000000,"lt":5000000,"mo_ta":"Truyền thuyết"},
    {"ten":"Rác Rưởi",       "loai":"💀 Rác",      "ty_le":1,  "tu_vi":0,      "lt":0,      "mo_ta":"Vận đen"},
]

CAN_CAU_DATA = {
    "Đại Đạo Cần":    {"bonus":1.0, "mo_ta":"Cần câu cơ bản (mặc định)"},
    "Linh Ngư Câu":   {"bonus":1.3, "mo_ta":"+30% tỉ lệ cá hiếm", "phi":5000,   "cap_yeu":2},
    "Huyền Thiết Câu":{"bonus":1.6, "mo_ta":"+60% tỉ lệ cá hiếm", "phi":20000,  "cap_yeu":5},
    "Tiên Ngư Câu":   {"bonus":2.5, "mo_ta":"+150% tỉ lệ cá quý", "phi":100000, "cap_yeu":10},
}

# ══════════════════════════════════════════════════════════════
#  PET
# ══════════════════════════════════════════════════════════════
PET_DATA = {
    "Linh Thú Nhỏ":  {"icon":"🐱","bonus_exp":5,  "bonus_lc":500,    "phi":10000,   "cap_yeu":2},
    "Hỏa Hồ":        {"icon":"🦊","bonus_exp":10, "bonus_lc":2000,   "phi":50000,   "cap_yeu":5},
    "Lôi Điêu":      {"icon":"🦅","bonus_exp":15, "bonus_lc":8000,   "phi":200000,  "cap_yeu":8},
    "Băng Kỳ Lân":   {"icon":"🦄","bonus_exp":20, "bonus_lc":30000,  "phi":1000000, "cap_yeu":12},
    "Hắc Long":      {"icon":"🐲","bonus_exp":35, "bonus_lc":120000, "phi":5000000, "cap_yeu":18},
}

# ══════════════════════════════════════════════════════════════
#  TÔNG MÔN CẤP ĐỘ
# ══════════════════════════════════════════════════════════════
TONG_MON_CAP = {
    1:{"ten":"Tiểu Phái",  "max_tv":10,  "bonus_exp":0,  "exp_can":0},
    2:{"ten":"Trung Phái", "max_tv":20,  "bonus_exp":5,  "exp_can":10_000},
    3:{"ten":"Đại Phái",   "max_tv":30,  "bonus_exp":10, "exp_can":100_000},
    4:{"ten":"Tông Môn",   "max_tv":50,  "bonus_exp":15, "exp_can":1_000_000},
    5:{"ten":"Đại Tông",   "max_tv":100, "bonus_exp":20, "exp_can":10_000_000},
    6:{"ten":"Thánh Địa",  "max_tv":200, "bonus_exp":30, "exp_can":100_000_000},
}

# ══════════════════════════════════════════════════════════════
#  ĐAN DƯỢC MỞ RỘNG (thêm vào DAN_DUOC)
# ══════════════════════════════════════════════════════════════
DAN_DUOC.update({
    "Bát Ấn Xin":         {"loai":"tu_vi",  "exp":100,      "gia":100,         "cap_yeu":0,  "rare":"⚪"},
    "Huyền Tức Y":        {"loai":"tu_vi",  "exp":600,      "gia":5_000,       "cap_yeu":2,  "rare":"🟢"},
    "Hoa Liễn Châu":      {"loai":"tu_vi",  "exp":2_500,    "gia":20_000,      "cap_yeu":4,  "rare":"🔵"},
    "Lôi Minh Thảo":      {"loai":"tu_vi",  "exp":10_000,   "gia":100_000,     "cap_yeu":7,  "rare":"🟣"},
    "Thiên Lôi Ấn":       {"loai":"tu_vi",  "exp":40_000,   "gia":500_000,     "cap_yeu":10, "rare":"🟡"},
    "Vạn Kiếp Châu":      {"loai":"tu_vi",  "exp":150_000,  "gia":2_000_000,   "cap_yeu":15, "rare":"⭐"},
    "Hạo Nhiên Lệnh Bài": {"loai":"tu_vi",  "exp":600_000,  "gia":10_000_000,  "cap_yeu":20, "rare":"💫"},
    "Huyết Sát Lệnh":     {"loai":"tu_vi",  "exp":2_500_000,"gia":50_000_000,  "cap_yeu":25, "rare":"✨"},
    "Long Hồn Hộ Thân Ấn":{"loai":"buff_all","all":50,      "gia":200_000_000, "cap_yeu":15, "rare":"🌟"},
    "Đồng Hoàng Chuông":  {"loai":"hoi_phuc","hp":999999,   "gia":100_000,     "cap_yeu":0,  "rare":"🟢"},
    "Phá Giới Đan":       {"loai":"dot_pha", "ti_le":50,    "gia":50_000,      "cap_yeu":5,  "rare":"🔵"},
    "Khai Thiên Phù":     {"loai":"tu_vi",   "exp":10_000_000,"gia":5_000_000_000,"cap_yeu":30,"rare":"☀️"},
})

# ══════════════════════════════════════════════════════════════
#  THÀNH TÍCH
# ══════════════════════════════════════════════════════════════
THANH_TICH = {
    # Cơ bản
    "tan_dao":       {"ten":"⚔️ Tân Đạo",          "mo_ta":"Tạo nhân vật lần đầu"},
    # Tu luyện
    "tuluyen_10":     {"ten":"🧘 Siêng Năng",        "mo_ta":"Tu luyện 10 lần"},
    "tuluyen_100":    {"ten":"🔥 Khổ Tu",            "mo_ta":"Tu luyện 100 lần"},
    "tuluyen_500":    {"ten":"⛰️ Bế Quan Đại Sư",    "mo_ta":"Tu luyện 500 lần"},
    "tuluyen_1000":   {"ten":"🪨 Khổ Hạnh Giả",      "mo_ta":"Tu luyện 1000 lần"},
    "tuluyen_5000":   {"ten":"🌌 Vạn Cổ Khổ Tu",     "mo_ta":"Tu luyện 5000 lần"},
    # Boss
    "boss_1":        {"ten":"👹 Đồ Sát",            "mo_ta":"Giết boss đầu tiên"},
    "boss_50":       {"ten":"💀 Sát Thần",          "mo_ta":"Giết 50 boss"},
    "boss_100":      {"ten":"🩸 Diệt Yêu Sư",       "mo_ta":"Giết 100 boss"},
    "boss_500":      {"ten":"☠️ Tai Họa Nhân Gian",  "mo_ta":"Giết 500 boss"},
    "boss_1000":     {"ten":"🌋 Thiên Tai Diệt Thế", "mo_ta":"Giết 1000 boss"},
    # PvP
    "pvp_win_1":     {"ten":"🥊 Võ Đạo",            "mo_ta":"Thắng PvP lần đầu"},
    "pvp_win_10":    {"ten":"🏆 Chiến Thần",        "mo_ta":"Thắng 10 trận PvP"},
    "pvp_win_50":    {"ten":"⚔️ Bá Chủ Võ Lâm",     "mo_ta":"Thắng 50 trận PvP"},
    "pvp_win_100":   {"ten":"👑 Nhân Gian Vô Địch",  "mo_ta":"Thắng 100 trận PvP"},
    "pvp_win_500":   {"ten":"💀 Sát Thần PvP",       "mo_ta":"Thắng 500 trận PvP"},
    # Linh thạch
    "giau_co":       {"ten":"💰 Phú Gia Địch Quốc",  "mo_ta":"Tích lũy 100,000 Linh Thạch"},
    "linh_thach_1m": {"ten":"💎 Đại Phú Hào",        "mo_ta":"Sở hữu 1,000,000 Linh Thạch"},
    "linh_thach_10m":{"ten":"🏦 Linh Thạch Sơn",     "mo_ta":"Sở hữu 10,000,000 Linh Thạch"},
    "linh_thach_100m":{"ten":"🌍 Phú Khả Địch Giới", "mo_ta":"Sở hữu 100,000,000 Linh Thạch"},
    # Cảnh giới
    "canh_gioi_5":   {"ten":"🌟 Kỳ Tài",            "mo_ta":"Đạt Hóa Thần (Lv.5)"},
    "canh_gioi_10":  {"ten":"👑 Thiên Tài",          "mo_ta":"Đạt Độ Kiếp (Lv.10)"},
    "canh_gioi_15":  {"ten":"✨ Bán Thánh",          "mo_ta":"Đạt Thánh Nhân (Lv.15)"},
    "canh_gioi_20":  {"ten":"🌌 Chí Tôn",           "mo_ta":"Đạt Chí Tôn (Lv.20)"},
    "canh_gioi_25":  {"ten":"🪐 Thiên Đế",           "mo_ta":"Đạt Thiên Đạo (Lv.25)"},
    "canh_gioi_30":  {"ten":"🌀 Vĩnh Hằng",          "mo_ta":"Đạt Vĩnh Hằng (Lv.30)"},
    "canh_gioi_max": {"ten":"🕊️ Siêu Thoát",         "mo_ta":"Đạt Vô Thượng Đại Đạo"},
    # Cái chết
    "die_1":         {"ten":"💀 Tân Hồn",            "mo_ta":"Chết lần đầu"},
    "die_50":        {"ten":"🪦 Quen Thuộc",          "mo_ta":"Chết 50 lần"},
    "die_200":       {"ten":"👻 Âm Hồn Bất Tán",      "mo_ta":"Chết 200 lần"},
    # Đặc biệt
    "all_in":        {"ten":"🎰 Con Bạc",             "mo_ta":"Thua sạch linh thạch PvP"},
    "first_trade":   {"ten":"🤝 Thương Nhân",         "mo_ta":"Giao dịch lần đầu"},
    "ket_duyen":     {"ten":"💍 Đạo Lữ",              "mo_ta":"Kết duyên lần đầu"},
    "phi_thuong":    {"ten":"🚀 Phi Thăng",            "mo_ta":"Phi thăng sang giới mới"},
}

# ══════════════════════════════════════════════════════════════
#  KIẾM LINH
# ══════════════════════════════════════════════════════════════
KIEM_LINH_CAP = ["Sơ Sinh","Giác Ngộ","Trưởng Thành","Cường Hóa","Thức Tỉnh","Siêu Việt","Vô Thượng"]
KIEM_LINH_BONUS = [0, 25, 60, 120, 220, 400, 700]  # % bonus sát thương — ảo hơn nhiều!

# ══════════════════════════════════════════════════════════════
#  KHỞI TẠO
# ══════════════════════════════════════════════════════════════
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)
db_pool: asyncpg.Pool = None

# Channel ID để bot gửi thông báo boss thế giới — set qua !setchannel hoặc env var
BOSS_CHANNEL_ID = int(os.getenv("BOSS_CHANNEL_ID", "0"))
# Lưu message_id của thông báo boss đang active để edit
boss_event_messages: dict = {}  # gioi -> message_id

# ══════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=15)
    async with db_pool.acquire() as c:
        await c.execute("""
            CREATE TABLE IF NOT EXISTS nhanvat (
                user_id       BIGINT PRIMARY KEY,
                ten           TEXT NOT NULL,
                toc           TEXT DEFAULT 'Nhân Tộc',
                linh_can      TEXT DEFAULT 'Song Linh Căn',
                canh_gioi     INT  DEFAULT 0,
                exp           BIGINT DEFAULT 0,
                linh_luc      BIGINT DEFAULT 100,
                linh_luc_max  BIGINT DEFAULT 100,
                tan_cong      INT  DEFAULT 10,
                phong_thu     INT  DEFAULT 5,
                linh_thach    BIGINT DEFAULT 50,
                tu_vi         BIGINT DEFAULT 0,
                ban_do        TEXT DEFAULT 'nhan_gioi',
                dao_chinh     TEXT DEFAULT '',
                dao_phu       TEXT DEFAULT '',
                cong_phap     TEXT DEFAULT '[]',
                passive       TEXT DEFAULT '[]',
                trang_bi      TEXT DEFAULT '{}',
                kiem_linh_cap INT  DEFAULT 0,
                kiem_linh_exp INT  DEFAULT 0,
                dao_lu        BIGINT DEFAULT 0,
                so_chet       INT  DEFAULT 0,
                last_tuluyen   TIMESTAMPTZ,
                last_khampha  TIMESTAMPTZ,
                last_bequan   TIMESTAMPTZ,
                bequan_gio    INT  DEFAULT 0,
                tong_mon      TEXT DEFAULT '',
                created_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # ── Migration: tự động thêm cột thiếu cho bảng nhanvat cũ ──
        migrations = [
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS last_bequan   TIMESTAMPTZ",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS bequan_gio    INT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS last_khampha  TIMESTAMPTZ",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS toc           TEXT DEFAULT 'Nhân Tộc'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS linh_can      TEXT DEFAULT 'Song Linh Căn'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS tu_vi         BIGINT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS ban_do        TEXT DEFAULT 'nhan_gioi'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS dao_chinh     TEXT DEFAULT ''",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS dao_phu       TEXT DEFAULT ''",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS cong_phap     TEXT DEFAULT '[]'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS passive       TEXT DEFAULT '[]'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS trang_bi      TEXT DEFAULT '{}'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS kiem_linh_cap INT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS kiem_linh_exp INT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS dao_lu        BIGINT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS so_chet       INT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS da_chon_toc  BOOLEAN DEFAULT FALSE",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS tong_mon      TEXT DEFAULT ''",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS last_tuluyen  TIMESTAMPTZ",
        ]
        for sql in migrations:
            try:
                await c.execute(sql)
            except Exception as e:
                print(f"⚠️ Migration skip: {e}")
        print("✅ Migration hoàn tất!")

        await c.execute("""
            CREATE TABLE IF NOT EXISTS tui_do (
                user_id  BIGINT, vat_pham TEXT, so_luong INT DEFAULT 1,
                PRIMARY KEY (user_id, vat_pham)
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS tong_mon (
                ten TEXT PRIMARY KEY, chu_mon BIGINT,
                mo_ta TEXT DEFAULT '', linh_thach BIGINT DEFAULT 0, thanh_vien TEXT DEFAULT ''
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS thong_ke (
                user_id        BIGINT PRIMARY KEY REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                tong_tulyen    BIGINT DEFAULT 0, tong_exp  BIGINT DEFAULT 0,
                tong_boss_giet BIGINT DEFAULT 0, tong_pvp_thang INT DEFAULT 0,
                tong_pvp_thua  INT DEFAULT 0,    tong_lt_kiem BIGINT DEFAULT 0,
                tong_lt_tieu   BIGINT DEFAULT 0, dot_pha_count INT DEFAULT 0,
                updated_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Migration: đảm bảo cột tong_tulyen tồn tại
        try:
            await c.execute("ALTER TABLE thong_ke ADD COLUMN IF NOT EXISTS tong_tulyen BIGINT DEFAULT 0")
        except Exception as e:
            print(f"⚠️ Migration add tong_tulyen skip: {e}")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS nhat_ky (
                id BIGSERIAL PRIMARY KEY, user_id BIGINT REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                loai TEXT, noi_dung TEXT, created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await c.execute("CREATE INDEX IF NOT EXISTS idx_nk ON nhat_ky(user_id,created_at DESC)")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS lich_su_pvp (
                id BIGSERIAL PRIMARY KEY, nguoi_thang BIGINT, nguoi_thua BIGINT,
                ten_thang TEXT, ten_thua TEXT, lt_cuop BIGINT, created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS thanh_tich (
                user_id BIGINT REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                ma_tt TEXT, dat_duoc_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, ma_tt)
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS ket_duyen (
                user1 BIGINT, user2 BIGINT,
                ngay TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user1, user2)
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS vuon_cay (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                loai_cay TEXT, trong_luc TIMESTAMPTZ, thu_hoach_luc TIMESTAMPTZ,
                da_thu BOOLEAN DEFAULT FALSE
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS boss_the_gioi (
                gioi        TEXT PRIMARY KEY,
                hp_hien     BIGINT,
                last_reset  TIMESTAMPTZ DEFAULT NOW(),
                nguoi_giet  BIGINT DEFAULT 0,
                boss_idx    INT DEFAULT 0,
                trang_thai  TEXT DEFAULT 'chet',
                xuat_hien_luc TIMESTAMPTZ
            )
        """)
        # Migration thêm cột mới nếu chưa có
        for col in [
            "ALTER TABLE boss_the_gioi ADD COLUMN IF NOT EXISTS boss_idx INT DEFAULT 0",
            "ALTER TABLE boss_the_gioi ADD COLUMN IF NOT EXISTS trang_thai TEXT DEFAULT 'chet'",
            "ALTER TABLE boss_the_gioi ADD COLUMN IF NOT EXISTS xuat_hien_luc TIMESTAMPTZ",
            "ALTER TABLE boss_the_gioi ADD COLUMN IF NOT EXISTS so_lan_hom_nay INT DEFAULT 0",
            "ALTER TABLE boss_the_gioi ADD COLUMN IF NOT EXISTS ngay_reset DATE DEFAULT CURRENT_DATE",
        ]:
            try: await c.execute(col)
            except: pass
        await c.execute("""
            CREATE TABLE IF NOT EXISTS boss_damage_log (
                id          BIGSERIAL PRIMARY KEY,
                gioi        TEXT,
                user_id     BIGINT,
                ten_nv      TEXT,
                damage      BIGINT DEFAULT 0,
                boss_session TIMESTAMPTZ,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await c.execute("CREATE INDEX IF NOT EXISTS idx_bdl ON boss_damage_log(gioi, boss_session, damage DESC)")
        # Bảng đăng ký tham gia boss thế giới
        await c.execute("""
            CREATE TABLE IF NOT EXISTS boss_dangky (
                gioi        TEXT,
                user_id     BIGINT,
                dangky_luc  TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (gioi, user_id)
            )
        """)
        # Khởi tạo boss thế giới nếu chưa có
        for gioi in BOSS_THE_GIOI_LIST.keys():
            b = BOSS_THE_GIOI_LIST[gioi][0]
            await c.execute("""
                INSERT INTO boss_the_gioi (gioi, hp_hien, trang_thai) VALUES ($1,$2,'chet')
                ON CONFLICT (gioi) DO NOTHING
            """, gioi, b["hp"])
        await c.execute("""
            CREATE TABLE IF NOT EXISTS thap_thu_luyen (
                user_id  BIGINT PRIMARY KEY REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                tang_hien INT DEFAULT 1,
                last_thap TIMESTAMPTZ
            )
        """)
        # ── V4: Cột mới ──
        for col_sql in [
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS mana INT DEFAULT 100",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS mana_max INT DEFAULT 100",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS tho_nguyen BIGINT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS so_do_kiep INT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS ma_khi INT DEFAULT 0",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS pet TEXT DEFAULT ''",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS bi_canh TEXT DEFAULT ''",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS can_cau TEXT DEFAULT 'Đại Đạo Cần'",
            "ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS kiem_linh_active BOOLEAN DEFAULT FALSE",
            "ALTER TABLE tong_mon ADD COLUMN IF NOT EXISTS cap_do INT DEFAULT 1",
            "ALTER TABLE tong_mon ADD COLUMN IF NOT EXISTS exp_mon BIGINT DEFAULT 0",
        ]:
            await c.execute(col_sql)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS lich_su_cau (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                ten_ca TEXT, loai TEXT, gia_tri BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS dan_ngay (
                user_id BIGINT, ngay DATE DEFAULT CURRENT_DATE,
                so_dung INT DEFAULT 0,
                PRIMARY KEY (user_id, ngay)
            )
        """)
    print("✅ DB V3 sẵn sàng!")

# ══════════════════════════════════════════════════════════════
#  HELPER
# ══════════════════════════════════════════════════════════════
async def get_nv(uid): 
    if db_pool is None:
        raise RuntimeError("Database chưa kết nối! Kiểm tra DATABASE_URL.")
    async with db_pool.acquire() as c: 
        return await c.fetchrow("SELECT * FROM nhanvat WHERE user_id=$1", uid)

async def cap_nhat(uid, **kw):
    if not kw: return
    cols = ", ".join(f"{k}=${i+2}" for i,k in enumerate(kw))
    async with db_pool.acquire() as c:
        await c.execute(f"UPDATE nhanvat SET {cols} WHERE user_id=$1", uid, *kw.values())

async def them_nhat_ky(uid, loai, nd):
    async with db_pool.acquire() as c:
        await c.execute("INSERT INTO nhat_ky(user_id,loai,noi_dung) VALUES($1,$2,$3)", uid, loai, nd)
        await c.execute("DELETE FROM nhat_ky WHERE id IN (SELECT id FROM nhat_ky WHERE user_id=$1 ORDER BY created_at DESC OFFSET 50)", uid)

async def cap_nhat_tk(uid, **kw):
    if not kw: return
    async with db_pool.acquire() as c:
        await c.execute("INSERT INTO thong_ke(user_id) VALUES($1) ON CONFLICT(user_id) DO NOTHING", uid)
        cols = ", ".join(f"{k}={k}+${i+2}" for i,k in enumerate(kw))
        await c.execute(f"UPDATE thong_ke SET {cols},updated_at=NOW() WHERE user_id=$1", uid, *kw.values())

def exp_can(cg):
    # Nhân Giới (0-5): vài trăm lần tu = lên 1 cấp
    # Linh Giới (6-9): vài nghìn lần tu
    # Tiên Giới (10-14): chục nghìn lần tu
    # Thánh Giới (15-24): trăm nghìn lần tu
    # Vũ Trụ (25-36): triệu lần tu
    if cg == 0: return 2_000
    if cg < 6:  return int(2_000  * (3.2 ** cg))
    if cg < 10: return int(80_000 * (3.5 ** (cg - 6)))
    if cg < 15: return int(8_000_000 * (3.0 ** (cg - 10)))
    if cg < 25: return int(200_000_000 * (2.8 ** (cg - 15)))
    return int(20_000_000_000 * (3.5 ** (cg - 25)))

def embed_mau(title, desc, color=0xAA55FF):
    e = discord.Embed(title=title, description=desc, color=color)
    e.set_footer(text="⚡ Ta Tu Tiên | Vạn Cổ Trường Tồn")
    return e

async def paginate(ctx, pages, color=0xAA55FF):
    """Gửi 1 tin nhắn duy nhất có nút ◀ ▶ để lật trang.
    pages: list of (title, content_str)
    """
    if not pages:
        return
    if len(pages) == 1:
        await ctx.send(embed=discord.Embed(title=pages[0][0], description=pages[0][1], color=color).set_footer(text="⚡ Tu Tiên | Vạn Cổ Trường Tồn"))
        return

    page = 0
    total = len(pages)

    def make_embed(p):
        title, desc = pages[p]
        e = discord.Embed(title=title, description=desc, color=color)
        e.set_footer(text=f"⚡ Ta Tu Tiên | Trang {p+1}/{total} — Dùng ◀ ▶ để chuyển")
        return e

    msg = await ctx.send(embed=make_embed(0))
    await msg.add_reaction("◀️")
    await msg.add_reaction("▶️")

    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["◀️","▶️"] and reaction.message.id == msg.id

    while True:
        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=120, check=check)
            page = (page + (1 if str(reaction.emoji)=="▶️" else -1)) % total
            await msg.edit(embed=make_embed(page))
            try: await msg.remove_reaction(reaction, user)
            except: pass
        except asyncio.TimeoutError:
            try: await msg.clear_reactions()
            except: pass
            break

def cooldown_con(last, giay):
    if not last: return 0
    return max(0, giay - (datetime.now(last.tzinfo)-last).total_seconds())

async def kiem_tra_thanh_tich(ctx, uid, nv, tk):
    if not nv: return
    async with db_pool.acquire() as c:
        da_co = {r['ma_tt'] for r in await c.fetch("SELECT ma_tt FROM thanh_tich WHERE user_id=$1", uid)}
    moi = []
    dk = {
        "tan_dao": True,
        "tuluyen_10":    tk and tk['tong_tulyen']>=10,
        "tuluyen_100":   tk and tk['tong_tulyen']>=100,
        "tuluyen_500":   tk and tk['tong_tulyen']>=500,
        "tuluyen_1000":  tk and tk['tong_tulyen']>=1000,
        "tuluyen_5000":  tk and tk['tong_tulyen']>=5000,
        "boss_1":       tk and tk['tong_boss_giet']>=1,
        "boss_50":      tk and tk['tong_boss_giet']>=50,
        "boss_100":     tk and tk['tong_boss_giet']>=100,
        "boss_500":     tk and tk['tong_boss_giet']>=500,
        "boss_1000":    tk and tk['tong_boss_giet']>=1000,
        "pvp_win_1":    tk and tk['tong_pvp_thang']>=1,
        "pvp_win_10":   tk and tk['tong_pvp_thang']>=10,
        "pvp_win_50":   tk and tk['tong_pvp_thang']>=50,
        "pvp_win_100":  tk and tk['tong_pvp_thang']>=100,
        "pvp_win_500":  tk and tk['tong_pvp_thang']>=500,
        "giau_co":      nv['linh_thach']>=100000,
        "linh_thach_1m":nv['linh_thach']>=1000000,
        "linh_thach_10m":nv['linh_thach']>=10000000,
        "linh_thach_100m":nv['linh_thach']>=100000000,
        "canh_gioi_5":  nv['canh_gioi']>=5,
        "canh_gioi_10": nv['canh_gioi']>=10,
        "canh_gioi_15": nv['canh_gioi']>=15,
        "canh_gioi_20": nv['canh_gioi']>=20,
        "canh_gioi_25": nv['canh_gioi']>=25,
        "canh_gioi_30": nv['canh_gioi']>=30,
        "canh_gioi_max":nv['canh_gioi']>=len(CANH_GIOI)-1,
        "die_1":   nv['so_chet']>=1,
        "die_50":  nv['so_chet']>=50,
        "die_200": nv['so_chet']>=200,
    }
    async with db_pool.acquire() as c:
        for ma, ok in dk.items():
            if ok and ma not in da_co:
                await c.execute("INSERT INTO thanh_tich(user_id,ma_tt) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, ma)
                moi.append(THANH_TICH[ma]["ten"])
    if moi:
        await ctx.send(embed=embed_mau("🏅 Thành Tích Mới!", "\n".join(f"✨ **{t}** mở khóa!" for t in moi), 0xFFD700))

# ══════════════════════════════════════════════════════════════
#  LỆNH: TẠO NHÂN VẬT
# ══════════════════════════════════════════════════════════════
@bot.command(name="taonv", aliases=["dangky"])
async def tao_nv(ctx, *, ten: str = None):
    if not ten:
        await ctx.send(embed=embed_mau("❌","Dùng: `!taonv <tên>`",0xFF4444)); return
    if await get_nv(ctx.author.id):
        await ctx.send(embed=embed_mau("❌","Bạn đã có nhân vật!",0xFF4444)); return

    # Hiển thị chọn tộc
    desc = "**Chọn tộc của bạn:**\n\n"
    toc_list = list(TOC.keys())
    for i, (k,v) in enumerate(TOC.items(), 1):
        desc += f"`{i}` {v['icon']} **{k}** — {v['mo_ta']}\n"
        desc += f"   ⚔️+{v['bonus_tan_cong']} | 🛡️+{v['bonus_phong_thu']} | 💧+{v['bonus_hp']} | ✨EXP+{v['bonus_exp']}%\n\n"
    desc += "Gõ số từ 1-6 để chọn (30 giây):"
    await ctx.send(embed=embed_mau(f"🌟 Tạo Nhân Vật: {ten}", desc))

    def check(m): return m.author.id==ctx.author.id and m.content.strip() in [str(i) for i in range(1,7)]
    try:
        msg = await bot.wait_for("message", check=check, timeout=30)
        toc_chon = toc_list[int(msg.content)-1]
    except asyncio.TimeoutError:
        toc_chon = "Nhân Tộc"

    toc_info = TOC[toc_chon]
    linh_can = random_linh_can()
    lc_info = LINH_CAN[linh_can]
    hp_base = 10000 + toc_info["bonus_hp"]

    async with db_pool.acquire() as c:
        await c.execute("""
            INSERT INTO nhanvat(user_id,ten,toc,linh_can,linh_luc,linh_luc_max,tan_cong,phong_thu)
            VALUES($1,$2,$3,$4,$5,$5,$6,$7)
        """, ctx.author.id, ten, toc_chon, linh_can, hp_base,
           500+toc_info["bonus_tan_cong"], 200+toc_info["bonus_phong_thu"])
        await c.execute("INSERT INTO thong_ke(user_id) VALUES($1) ON CONFLICT DO NOTHING", ctx.author.id)
        await c.execute("INSERT INTO thap_thu_luyen(user_id) VALUES($1) ON CONFLICT DO NOTHING", ctx.author.id)

    await them_nhat_ky(ctx.author.id, "system", f"Nhập môn với tộc {toc_chon}, linh căn {linh_can}")
    nv = await get_nv(ctx.author.id)
    await kiem_tra_thanh_tich(ctx, ctx.author.id, nv, None)

    await ctx.send(embed=embed_mau("🌟 Nhập Môn Tu Tiên!", f"""
**{ten}** đã bước vào con đường tu tiên!
{toc_info['icon']} **Tộc:** {toc_chon} — _{toc_info['mo_ta']}_
{lc_info['icon']} **Linh Căn:** {linh_can} — _{lc_info['mo_ta']}_

🏔️ Cảnh Giới: **{CANH_GIOI[0]}** | 🗺️ Bản Đồ: **Nhân Giới**
💧 HP: **{hp_base}/{hp_base}** | ⚔️ {10+toc_info['bonus_tan_cong']} | 🛡️ {5+toc_info['bonus_phong_thu']}
💎 Linh Thạch: **50**

Dùng `!help` để xem tất cả lệnh!
    """, 0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: CHỌN TỘC (dành cho người chưa chọn tộc)
# ══════════════════════════════════════════════════════════════
@bot.command(name="chontoc", aliases=["ct","choc"])
async def chon_toc_cmd(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    # Kiểm tra đã chọn tộc thật sự chưa (khác Nhân Tộc mặc định = đã chọn)
    TOC_MAC_DINH = "Nhân Tộc"
    da_chon = nv.get('da_chon_toc', False)  # cờ riêng nếu có

    # Dùng cột da_chon_toc để xác định — nếu chưa có cột thì fallback check toc != mặc định ban đầu
    # Cách an toàn: kiểm tra trong DB có cờ không
    async with db_pool.acquire() as c:
        # Thêm cột da_chon_toc nếu chưa có
        await c.execute("ALTER TABLE nhanvat ADD COLUMN IF NOT EXISTS da_chon_toc BOOLEAN DEFAULT FALSE")
        row = await c.fetchrow("SELECT da_chon_toc FROM nhanvat WHERE user_id=$1", ctx.author.id)

    if row and row['da_chon_toc']:
        toc_info = TOC.get(nv['toc'], {})
        await ctx.send(embed=embed_mau(
            "🔒 Đã Chọn Tộc",
            f"Bạn đã là **{toc_info.get('icon','')} {nv['toc']}** — không thể thay đổi huyết mạch!\n"
            f"_{toc_info.get('mo_ta','')}_",
            0xFF4444
        )); return

    # Hiển thị danh sách tộc để chọn
    toc_list = list(TOC.keys())
    desc = "⚠️ **Lưu ý: Chỉ được chọn 1 lần duy nhất, không thể đổi lại!**\n\n"
    for i, (k, v) in enumerate(TOC.items(), 1):
        desc += f"`{i}` {v['icon']} **{k}**\n"
        desc += f"   _{v['mo_ta']}_\n"
        desc += f"   ⚔️+{v['bonus_tan_cong']} | 🛡️+{v['bonus_phong_thu']} | 💧HP+{v['bonus_hp']:,} | ✨EXP+{v['bonus_exp']}%\n\n"
    desc += "Gõ số **1-6** để chọn tộc (60 giây):"

    await ctx.send(embed=embed_mau("🐉 Chọn Huyết Mạch Của Bạn", desc, 0xAA55FF))

    def check(m): return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id and m.content.strip() in [str(i) for i in range(1, 7)]
    try:
        msg = await bot.wait_for("message", check=check, timeout=60)
        toc_chon = toc_list[int(msg.content.strip()) - 1]
    except asyncio.TimeoutError:
        await ctx.send(embed=embed_mau("⏰ Hết Giờ","Không chọn tộc! Dùng `!chontoc` để thử lại.",0xFF4444)); return

    # Xác nhận lần cuối
    toc_info = TOC[toc_chon]
    await ctx.send(embed=embed_mau(
        f"⚠️ Xác Nhận Chọn {toc_info['icon']} {toc_chon}",
        f"Bạn chắc chắn muốn chọn **{toc_chon}**?\n"
        f"_{toc_info['mo_ta']}_\n\n"
        f"Gõ **`xác nhận`** để đồng ý (20 giây):",
        0xFFAA00
    ))

    def check2(m): return m.author.id == ctx.author.id and m.channel.id == ctx.channel.id and m.content.strip().lower() in ["xác nhận", "xac nhan", "yes", "ok"]
    try:
        await bot.wait_for("message", check=check2, timeout=20)
    except asyncio.TimeoutError:
        await ctx.send(embed=embed_mau("❌ Đã Hủy","Không xác nhận — tộc chưa được chọn.",0x888888)); return

    # Áp dụng bonus tộc vào nhân vật
    async with db_pool.acquire() as c:
        await c.execute("""
            UPDATE nhanvat SET
                toc          = $2,
                tan_cong     = tan_cong + $3,
                phong_thu    = phong_thu + $4,
                linh_luc     = linh_luc + $5,
                linh_luc_max = linh_luc_max + $5,
                da_chon_toc  = TRUE
            WHERE user_id = $1
        """, ctx.author.id,
           toc_chon,
           toc_info["bonus_tan_cong"],
           toc_info["bonus_phong_thu"],
           toc_info["bonus_hp"])

    await them_nhat_ky(ctx.author.id, "system", f"Chọn tộc {toc_chon}")
    await ctx.send(embed=embed_mau(
        f"🎉 Huyết Mạch Thức Tỉnh!",
        f"{toc_info['icon']} **{toc_chon}** — _{toc_info['mo_ta']}_\n\n"
        f"⚔️ Tấn Công **+{toc_info['bonus_tan_cong']:,}**\n"
        f"🛡️ Phòng Thủ **+{toc_info['bonus_phong_thu']:,}**\n"
        f"💧 HP **+{toc_info['bonus_hp']:,}**\n"
        f"✨ EXP Bonus **+{toc_info['bonus_exp']}%**\n\n"
        f"🔒 Huyết mạch đã định — không thể thay đổi!",
        0xFFD700
    ))

# ══════════════════════════════════════════════════════════════
#  LỆNH: THÔNG TIN
# ══════════════════════════════════════════════════════════════
@bot.command(name="tt", aliases=["thongtin","info"])
async def thong_tin(ctx, member: discord.Member = None):
    target = member or ctx.author
    nv = await get_nv(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Chưa có nhân vật! Dùng `!taonv <tên>`",0xFF4444)); return

    async with db_pool.acquire() as c:
        tk = await c.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", target.id)
        tt_count = await c.fetchval("SELECT COUNT(*) FROM thanh_tich WHERE user_id=$1", target.id)
        mon_row = await c.fetchrow("SELECT ten, cap_do FROM tong_mon WHERE ten=$1", nv['tong_mon']) if nv['tong_mon'] else None
        thap_row = await c.fetchrow("SELECT tang_hien FROM thap_thu_luyen WHERE user_id=$1", target.id)
        bequan_row = None
        if nv['last_bequan'] and nv['bequan_gio'] > 0:
            end = nv['last_bequan'] + timedelta(hours=nv['bequan_gio'])
            now_utc = datetime.now(nv['last_bequan'].tzinfo)
            if now_utc < end:
                bequan_row = int((end - now_utc).total_seconds())

    cg  = nv['canh_gioi']
    lc  = tinh_luc_chien(nv)
    toc = TOC.get(nv['toc'], {})
    lci = LINH_CAN.get(nv['linh_can'], {})
    kl_cap_str = KIEM_LINH_CAP[min(nv['kiem_linh_cap'], len(KIEM_LINH_CAP)-1)]
    kl_active  = "🟢 Đã kích hoạt" if nv.get('kiem_linh_active') else "🔴 Chưa"
    pet_str    = PET_DATA.get(nv.get('pet',''), {}).get('icon','') + ' ' + nv.get('pet','Chưa có') if nv.get('pet') else "Chưa có"
    dao_lu_str = "Độc thân"
    if nv.get('dao_lu'):
        try:
            pu = await bot.fetch_user(int(nv['dao_lu']))
            dao_lu_str = pu.display_name
        except: dao_lu_str = "Đạo Lữ"

    bd = BAN_DO.get(nv['ban_do'], {})
    gioi_icon = bd.get('ten','').split()[0] if bd.get('ten') else '🌐'
    phi_str = f"🟢 Đã Phi Thăng {bd.get('ten','')}" if nv.get('phi_thuong_status') else f"⭕ Chưa Phi Thăng"

    tong_mon_str = "Không"
    if nv['tong_mon'] and mon_row:
        cap_info = TONG_MON_CAP.get(mon_row['cap_do'], {})
        tong_mon_str = f"{nv['tong_mon']} (Cấp {mon_row['cap_do']} — {cap_info.get('ten','')})"

    mana_hien = nv.get('mana', 100)
    mana_max  = nv.get('mana_max', 100)
    tho_nguyen = nv.get('tho_nguyen', 0)

    # Trang bị slot
    tb_dict = json.loads(nv.get('trang_bi','{}') or '{}')

    # Tính ngày tạo
    ngay_tao = nv['created_at'].strftime('%d/%m/%Y') if nv.get('created_at') else 'N/A'

    e = discord.Embed(
        title=f"Trạng Thái - {nv['ten']}",
        color=0x2B2D31
    )

    # Cột trái: Thông tin cơ bản
    thong_tin_co_ban = (
        f"{toc.get('icon','👤')} Tộc: **{nv['toc']}**\n"
        f"☯️ Đạo: **{nv['dao_chinh'] or 'Chưa ngộ'}**\n"
        f"📿 Đạo Phụ: **{nv['dao_phu'] or 'Chưa có'}**\n"
        f"{lci.get('icon','⭐')} Linh Căn: **{nv['linh_can']}**\n"
        f"🏯 Tông Môn: **{tong_mon_str}**\n"
        f"💍 Đạo Lữ: **{dao_lu_str}**"
    )

    # Cột giữa: Tu Vi & Cảnh Giới
    bar_len = 10
    exp_pct = min(1.0, nv['exp'] / exp_can(cg)) if exp_can(cg) > 0 else 1.0
    bar = "█" * int(exp_pct * bar_len) + "░" * (bar_len - int(exp_pct * bar_len))
    tu_vi_canh_gioi = (
        f"✨ Tu Vi: **{nv['tu_vi']:,}**\n"
        f"🏔️ Cảnh Giới: **{CANH_GIOI[cg]}**\n"
        f"({'Sơ ký' if exp_pct < 0.33 else 'Đại viên mãn' if exp_pct > 0.9 else 'Trung kỳ'})\n"
        f"⚡ Chiến Lực: **{lc:,}**\n"
        f"{luc_chien_rank(lc)}\n"
        f"🌐 Giới: {gioi_icon} {bd.get('ten','')}\n"
        f"🚀 Phi Thăng: {phi_str}"
    )

    # Cột phải: Sinh Tồn & Nghề
    sinh_ton = (
        f"🕰️ Thọ Nguyên: **{tho_nguyen:,}** năm\n"
        f"❤️ Trạng Thái: bình thường\n"
        f"💙 Mana (Câu cá):\n"
        f"**{mana_hien}/{mana_max}**\n"
        f"🌀 Ma Khí: **{nv.get('ma_khi',0)}**"
    )
    if bequan_row:
        h, m = bequan_row // 3600, (bequan_row % 3600) // 60
        sinh_ton += f"\n🧘 Đang bế quan: Còn **{h}h {m}m**"

    e.add_field(name="📋 Thông Tin Cơ Bản", value=thong_tin_co_ban, inline=True)
    e.add_field(name="🌀 Tu Vi & Cảnh Giới", value=tu_vi_canh_gioi, inline=True)
    e.add_field(name="💊 Sinh Tồn & Nghề", value=sinh_ton, inline=True)

    # Tài sản
    tai_san = (
        f"💎 Nguyên Thạch: **{nv['linh_thach']:,}**\n"
        f"💀 Số lần độ kiếp: **{nv.get('so_do_kiep', nv.get('so_chet',0))}**"
    )
    # Trang bị
    slot_icons = {
        "Công Pháp": "🔥", "Vũ Khí":"⚔️", "Giáp":"🛡️",
        "Pháp Bảo":"💎", "Bí Cảnh":"🌌", "Nhẫn":"💍",
        "Cần Câu":"🎣", "Kiếm Linh":"⚔️"
    }
    trang_bi_lines = []
    cp_list = json.loads(nv.get('cong_phap','[]') or '[]')
    if cp_list:
        trang_bi_lines.append(f"🔥 Công Pháp: **{cp_list[-1]}**")
    for loai, ten in tb_dict.items():
        icon = slot_icons.get(loai, "📦")
        trang_bi_lines.append(f"{icon} {loai}: **{ten}**")
    if nv.get('bi_canh'):
        trang_bi_lines.append(f"🌌 Bí Cảnh: **{nv['bi_canh']}**")
    can_cau_hien = nv.get('can_cau','Đại Đạo Cần')
    trang_bi_lines.append(f"🎣 Cần Câu: **{can_cau_hien}**")
    trang_bi_lines.append(f"⚔️ Kiếm Linh: {kl_active}")

    trang_bi_str = "\n".join(trang_bi_lines) if trang_bi_lines else "Chưa có"

    e.add_field(name="💰 Tài Sản", value=tai_san, inline=True)
    e.add_field(name="🎽 Trang Bị", value=trang_bi_str, inline=True)

    # Đặc biệt
    dac_biet_lines = []
    if bequan_row:
        h, m = bequan_row // 3600, (bequan_row % 3600) // 60
        dac_biet_lines.append(f"🧘 Đang bế quan: Còn **{h}h {m}m**")
    if nv.get('pet'):
        pi = PET_DATA.get(nv['pet'],{})
        dac_biet_lines.append(f"{pi.get('icon','🐾')} Pet: **{nv['pet']}** (+{pi.get('bonus_exp',0)}% EXP)")
    if dac_biet_lines:
        e.add_field(name="✨ Đặc Biệt", value="\n".join(dac_biet_lines), inline=False)

    e.set_footer(text=f"Tạo nhân vật: {ngay_tao} | Hoạt động cuối: {datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M')}")
    if target.avatar:
        e.set_thumbnail(url=target.avatar.url)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════════
#  LỆNH: TU LUYỆN
# ══════════════════════════════════════════════════════════════
@bot.command(name="tuluyen", aliases=["tl"])
async def tu_luyen(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    # Kiểm tra bế quan
    if nv['last_bequan'] and nv['bequan_gio'] > 0:
        end = nv['last_bequan'] + timedelta(hours=nv['bequan_gio'])
        if datetime.now(nv['last_bequan'].tzinfo) < end:
            con_lai = int((end - datetime.now(nv['last_bequan'].tzinfo)).total_seconds()) // 60
            await ctx.send(embed=embed_mau("🧘 Đang Bế Quan",f"Còn **{con_lai}** phút nữa!\n`!xuatquan` để xuất quan.",0xFFAA00)); return

    cd = cooldown_con(nv['last_tuluyen'], 15)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳",f"Còn **{cd:.0f}s** nữa!",0xFFAA00)); return

    lc_info  = LINH_CAN.get(nv['linh_can'], {})
    toc_info = TOC.get(nv['toc'], {})
    exp_bonus  = lc_info.get("bonus_exp", 0) + toc_info.get("bonus_exp", 0)
    tuvi_bonus = lc_info.get("bonus_tuluyen", 0)
    cg = nv['canh_gioi']

    # Passive bonus
    for p in json.loads(nv['cong_phap'] or '[]'):
        pi = CONG_PHAP_PASSIVE.get(p, {})
        if "bonus_tuvi" in pi: tuvi_bonus += pi["bonus_tuvi"]

    # Pet bonus
    pet_info = PET_DATA.get(nv.get('pet',''), {})
    pet_exp_bonus = pet_info.get("bonus_exp", 0)

    # Dao chinh bonus
    dao_info = DAO_CHINH.get(nv['dao_chinh'], {})
    dao_exp_bonus = dao_info.get("bonus_exp", 0) if 'bonus_exp' in dao_info else 0

    total_bonus = exp_bonus + pet_exp_bonus + dao_exp_bonus

    # EXP gain: ~0.6-1% exp cần mỗi cảnh giới/lần tu
    base_exp = max(100, int(exp_can(cg) * 0.008))
    exp_gain = int(random.randint(base_exp, int(base_exp * 1.4)) * (1 + total_bonus / 100))

    # Tu Vi gain cân bằng — tăng dần theo cảnh giới
    base_tv = int(50 * (1.6 ** min(cg, 18)) * (1 + max(0, cg - 18) * 0.3))
    tv_gain  = int(random.randint(base_tv, int(base_tv * 1.5)) + tuvi_bonus)

    # Thọ nguyên tăng mỗi lần tu
    tho_gain = random.randint(1, 3) * (cg + 1)

    # Mana hồi
    mana_hoi = random.randint(2, 8)

    # HP hồi
    ll_hoi = max(1, int(nv['linh_luc_max'] * 0.05 * random.uniform(0.5, 1.5)))

    # Kiếm Linh exp
    kl_exp  = random.randint(5, 15)
    new_kl_exp = nv['kiem_linh_exp'] + kl_exp
    new_kl_cap = nv['kiem_linh_cap']
    kl_threshold = (new_kl_cap + 1) * 300
    kl_msg = ""
    if new_kl_exp >= kl_threshold and new_kl_cap < len(KIEM_LINH_CAP) - 1:
        new_kl_exp -= kl_threshold
        new_kl_cap += 1
        kl_msg = f"\n⚔️ **Kiếm Linh → {KIEM_LINH_CAP[new_kl_cap]}**!"

    new_exp = nv['exp'] + exp_gain
    new_cg  = cg
    dp_msg  = ""
    dp_cnt  = 0
    tv_tru_msg = ""

    # Xử lý đột phá
    while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI) - 1:
        lc_bonus = {"Thiên Linh Căn":30,"Biến Linh Căn":20,"Tứ Linh Căn":15,
                    "Tam Linh Căn":10,"Song Linh Căn":5,"Đơn Linh Căn":5,"Phế Linh Căn":0}
        ti_le = min(99, max(20, 90 - new_cg * 2) + lc_bonus.get(nv['linh_can'], 0))
        if random.randint(1, 100) <= ti_le:
            new_exp -= exp_can(new_cg)
            new_cg  += 1
            dp_cnt  += 1
            dp_msg   = f"\n\n🎉 **ĐỘT PHÁ → {CANH_GIOI[new_cg]}** (tỉ lệ {ti_le}%)! 🎉"
        else:
            pct_tru = random.randint(15, 30)
            tru_tv  = int(nv['tu_vi'] * pct_tru / 100)
            new_exp = exp_can(new_cg) - 1
            tv_tru_msg = (f"\n\n💥 **ĐỘT PHÁ THẤT BẠI** (tỉ lệ {ti_le}%)!\n"
                          f"🌀 −{tru_tv:,} Tu Vi ({pct_tru}%) — Cảnh giới không giảm")
            await cap_nhat(ctx.author.id, tu_vi=max(0, nv['tu_vi'] - tru_tv))
            break

    # Phi thăng auto
    ban_do_hien = nv['ban_do']
    phi_msg = ""
    bd_info = BAN_DO[ban_do_hien]
    if new_cg > bd_info["cap_max"] and bd_info["phi_thuong"]:
        for bdk, bdv in BAN_DO.items():
            if bdv["cap_min"] <= new_cg <= bdv["cap_max"]:
                ban_do_hien = bdk
                phi_msg = f"\n🚀 **PHI THĂNG → {bdv['ten']}**! 🎊"
                async with db_pool.acquire() as c:
                    await c.execute("INSERT INTO thanh_tich(user_id,ma_tt) VALUES($1,'phi_thuong') ON CONFLICT DO NOTHING", ctx.author.id)
                break

    new_mana = min(nv.get('mana', 100) + mana_hoi, nv.get('mana_max', 100))
    new_tho  = nv.get('tho_nguyen', 0) + tho_gain

    await cap_nhat(ctx.author.id,
        exp=new_exp, tu_vi=nv['tu_vi'] + tv_gain, canh_gioi=new_cg,
        linh_luc=min(nv['linh_luc'] + ll_hoi, nv['linh_luc_max']),
        mana=new_mana, tho_nguyen=new_tho,
        kiem_linh_cap=new_kl_cap, kiem_linh_exp=new_kl_exp,
        ban_do=ban_do_hien, last_tuluyen=datetime.now(timezone.utc)
    )
    await cap_nhat_tk(ctx.author.id, tong_tulyen=1, tong_exp=exp_gain, dot_pha_count=dp_cnt)
    await them_nhat_ky(ctx.author.id, "tuluyen", f"+{exp_gain:,} EXP, +{tv_gain:,} Tu Vi → {CANH_GIOI[new_cg]}")

    nv2 = await get_nv(ctx.author.id)
    async with db_pool.acquire() as c:
        tk = await c.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", ctx.author.id)
    await kiem_tra_thanh_tich(ctx, ctx.author.id, nv2, tk)

    # Hiển thị bonus multipliers như ảnh mẫu
    bonus_lines = []
    if toc_info.get("bonus_exp"): bonus_lines.append(f"{nv['toc']} (+{toc_info['bonus_exp']}%)")
    lc_exp = lc_info.get("bonus_exp", 0)
    if lc_exp: bonus_lines.append(f"{nv['linh_can']} (x{1+lc_exp/100:.1f})")
    if nv['dao_chinh'] and dao_exp_bonus: bonus_lines.append(f"{nv['dao_chinh']} (x{1+dao_exp_bonus/100:.1f})")
    if pet_exp_bonus: bonus_lines.append(f"Pet Bonus (x{1+pet_exp_bonus/100:.1f})")
    # Tông môn bonus
    if nv['tong_mon']:
        async with db_pool.acquire() as c:
            mon = await c.fetchrow("SELECT cap_do FROM tong_mon WHERE ten=$1", nv['tong_mon'])
        if mon:
            mon_bonus = TONG_MON_CAP.get(mon['cap_do'], {}).get("bonus_exp", 0)
            if mon_bonus: bonus_lines.append(f"Tông Môn Cấp {mon['cap_do']} (x{1+mon_bonus/100:.1f})")

    bonus_str = "\n".join(f"  {b}" for b in bonus_lines) if bonus_lines else "  (không có)"
    ti_le_ke = max(20, min(99, 90 - new_cg * 2))

    color = 0x55FFAA if not tv_tru_msg else 0xFF6600
    await ctx.send(embed=discord.Embed(
        title="✨ Tu Luyện Thành Công!",
        description=f"""
{nv['ten']} thổ nạp linh khí, tu vi thăng hoa!

🔮 **Tu Vi** ✨ **Cảnh Giới** ⚡ **Chiến Lực (Ước tính)**
+{tv_gain:,} (Tổng: {nv['tu_vi']+tv_gain:,})  {CANH_GIOI[new_cg]}  {tinh_luc_chien(nv2):,}

📈 **Bonus**
{bonus_str}

🎯 Tỉ lệ ĐP tiếp: **{ti_le_ke}%** | Cần: **{max(0, exp_can(new_cg)-new_exp):,}** EXP
💧 Mana: {new_mana}/{nv.get('mana_max',100)} | 🕰️ Thọ Nguyên: +{tho_gain} năm
{dp_msg}{tv_tru_msg}{phi_msg}{kl_msg}
        """, color=color
    ).set_footer(text="⚡ Ta Tu Tiên | Vạn Cổ Trường Tồn"))

# ══════════════════════════════════════════════════════════════
#  LỆNH: BẾ QUAN
# ══════════════════════════════════════════════════════════════
@bot.command(name="bequan", aliases=["bq"])
async def be_quan(ctx, gio: int = None):
    """!bequan <giờ> — Bế quan 1-72 giờ, nhận EXP gấp 3 khi xuất quan"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    if nv['last_bequan'] and nv['bequan_gio']>0:
        end = nv['last_bequan'] + timedelta(hours=nv['bequan_gio'])
        now = datetime.now(nv['last_bequan'].tzinfo)
        if now < end:
            con_lai = int((end-now).total_seconds())
            h,m = con_lai//3600, (con_lai%3600)//60
            await ctx.send(embed=embed_mau("🧘 Đang Bế Quan",f"Còn **{h}h{m}m** nữa!\nDùng `!xuatquan` khi xong.",0xFFAA00)); return
        else:
            # Tự động xuất quan nếu đã hết giờ
            await _xuat_quan(ctx, nv); return

    if not gio:
        await ctx.send(embed=embed_mau("🧘 Bế Quan",
            "Dùng: `!bequan <giờ>` (1-72 giờ)\nNhận EXP gấp **3x** khi xuất quan!\n⚠️ Không thể tu luyện khi bế quan.")); return
    gio = max(1, min(72, gio))
    await cap_nhat(ctx.author.id, last_bequan=datetime.now(timezone.utc), bequan_gio=gio)
    await them_nhat_ky(ctx.author.id,"bequan",f"Bế quan {gio} giờ")
    await ctx.send(embed=embed_mau("🧘 Bắt Đầu Bế Quan!",f"""
Bế quan **{gio} giờ** bắt đầu!
✨ EXP nhận được x**3** khi xuất quan
⏰ Dùng `!xuatquan` sau {gio}h để nhận thưởng!
    """, 0xAA55FF))

@bot.command(name="xuatquan", aliases=["xq"])
async def xuat_quan_cmd(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv or not nv['last_bequan'] or nv['bequan_gio']<=0:
        await ctx.send(embed=embed_mau("❌","Bạn không đang bế quan!",0xFF4444)); return
    await _xuat_quan(ctx, nv)

async def _xuat_quan(ctx, nv):
    end = nv['last_bequan'] + timedelta(hours=nv['bequan_gio'])
    now = datetime.now(nv['last_bequan'].tzinfo)
    if now < end:
        con_lai = int((end-now).total_seconds())
        await ctx.send(embed=embed_mau("⏳",f"Còn **{con_lai//3600}h{(con_lai%3600)//60}m** nữa!",0xFFAA00)); return

    gio_thuc = min(nv['bequan_gio'], 72)
    lc_info = LINH_CAN.get(nv['linh_can'],{})
    exp_per_gio = (5000 + nv['canh_gioi']*500) * (1 + lc_info.get("bonus_exp",0)/100)
    exp_gain = int(exp_per_gio * gio_thuc * 3)
    tv_gain  = int(gio_thuc * 200)

    new_exp = nv['exp'] + exp_gain
    new_cg  = nv['canh_gioi']
    dp_msg  = ""
    while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI)-1:
        new_exp -= exp_can(new_cg); new_cg += 1
        dp_msg = f"\n🎉 **ĐỘT PHÁ → {CANH_GIOI[new_cg]}**!"

    await cap_nhat(ctx.author.id, exp=new_exp, tu_vi=nv['tu_vi']+tv_gain,
                   canh_gioi=new_cg, bequan_gio=0)
    await cap_nhat_tk(ctx.author.id, tong_tulyen=gio_thuc*6, tong_exp=exp_gain)
    await them_nhat_ky(ctx.author.id,"xuatquan",f"Xuất quan sau {gio_thuc}h, +{exp_gain} EXP")

    await ctx.send(embed=embed_mau("🌅 Xuất Quan Thành Công!", f"""
Bế quan **{gio_thuc} giờ** hoàn thành!
✨ **+{exp_gain:,} EXP** (x3 bonus!)
🌀 **+{tv_gain:,} Tu Vi**
📊 Cảnh Giới: **{CANH_GIOI[new_cg]}**
{dp_msg}
    """, 0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: ĐẠO
# ══════════════════════════════════════════════════════════════
@bot.command(name="chondao", aliases=["dao"])
async def chon_dao(ctx, *, ten_dao: str = None):
    """!chondao <tên đạo> — Chọn đạo chính"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if not ten_dao:
        lines = [f"**{k}** — {v['mo_ta']} | Cần Lv.{v['cap_yeu']} | 💎{v['phi']:,}" for k,v in DAO_CHINH.items()]
        await ctx.send(embed=embed_mau("☯️ Danh Sách Đạo Chính","\n".join(lines))); return
    dao = DAO_CHINH.get(ten_dao)
    if not dao:
        await ctx.send(embed=embed_mau("❌","Đạo không tồn tại! Dùng `!chondao` để xem danh sách.",0xFF4444)); return
    if nv['canh_gioi'] < dao['cap_yeu']:
        await ctx.send(embed=embed_mau("❌",f"Cần **{CANH_GIOI[dao['cap_yeu']]}**!",0xFF4444)); return
    if nv['linh_thach'] < dao['phi']:
        await ctx.send(embed=embed_mau("❌",f"Cần **{dao['phi']:,}** 💎",0xFF4444)); return
    if nv['dao_chinh']:
        await ctx.send(embed=embed_mau("❌",f"Đã ngộ **{nv['dao_chinh']}**! Không thể đổi đạo.",0xFF4444)); return
    await cap_nhat(ctx.author.id, dao_chinh=ten_dao, linh_thach=nv['linh_thach']-dao['phi'],
                   tan_cong=nv['tan_cong']+dao.get('bonus_atk',0),
                   phong_thu=nv['phong_thu']+dao.get('bonus_def',0))
    await ctx.send(embed=embed_mau("☯️ Ngộ Đạo Thành Công!",f"Đã bước vào **{ten_dao}**!\n_{dao['mo_ta']}_",0xAA55FF))

@bot.command(name="daophu")
async def dao_phu_cmd(ctx, *, ten_dao: str = None):
    """!daophu <tên> — Học đạo phụ"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if not ten_dao:
        lines = [f"**{k}** — {v['mo_ta']} | Cần Lv.{v['cap_yeu']} | 💎{v['phi']:,}" for k,v in DAO_PHU.items()]
        await ctx.send(embed=embed_mau("📿 Đạo Phụ","\n".join(lines))); return
    dao = DAO_PHU.get(ten_dao)
    if not dao:
        await ctx.send(embed=embed_mau("❌","Đạo phụ không tồn tại!",0xFF4444)); return
    if nv['canh_gioi']<dao['cap_yeu'] or nv['linh_thach']<dao['phi']:
        await ctx.send(embed=embed_mau("❌",f"Cần Lv.{dao['cap_yeu']} và {dao['phi']:,}💎",0xFF4444)); return
    await cap_nhat(ctx.author.id, dao_phu=ten_dao, linh_thach=nv['linh_thach']-dao['phi'])
    await ctx.send(embed=embed_mau("📿 Học Đạo Phụ",f"Đã học **{ten_dao}**!\n_{dao['mo_ta']}_",0x55AAFF))

# ══════════════════════════════════════════════════════════════
#  LỆNH: CÔNG PHÁP
# ══════════════════════════════════════════════════════════════
@bot.command(name="congphap", aliases=["cp"])
async def cong_phap_cmd(ctx, hanh_dong: str = None, *, ten: str = None):
    """!congphap — Xem | !congphap hoc <tên> — Học công pháp"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    if not hanh_dong or hanh_dong=="list":
        tan_cong_lines = ["**⚔️ Tấn Công:**"]
        for k,v in CONG_PHAP_TAN_CONG.items():
            tan_cong_lines.append(f"  `{k}` — 💥{v['sat_thuong']} | Lv.{v['cap_yeu']} | 💎{v['phi']}")

        phong_thu_lines = ["**🛡️ Phòng Thủ:**"]
        for k,v in CONG_PHAP_PHONG_THU.items():
            phong_thu_lines.append(f"  `{k}` — 🛡️+{v['phong_thu_bonus']} | Lv.{v['cap_yeu']} | 💎{v['phi']}")

        than_thong_lines = ["**🌀 Đại Thần Thông:**"]
        for k,v in DAI_THAN_THONG.items():
            than_thong_lines.append(f"  `{k}` — 💥{v['sat_thuong']} | Lv.{v['cap_yeu']} | 💎{v['phi']}")

        passive_lines = ["**✨ Passive:**"]
        for k,v in CONG_PHAP_PASSIVE.items():
            passive_lines.append(f"  `{k}` — {v['mo_ta']} | Lv.{v['cap_yeu']} | 💎{v['phi']}")

        cp_pages = [
            ("📚 Công Pháp (1/4) — Tấn Công", "\n".join(tan_cong_lines)),
            ("📚 Công Pháp (2/4) — Phòng Thủ", "\n".join(phong_thu_lines)),
            ("📚 Công Pháp (3/4) — Đại Thần Thông", "\n".join(than_thong_lines)),
            ("📚 Công Pháp (4/4) — Passive", "\n".join(passive_lines)),
        ]
        await paginate(ctx, cp_pages)
        return

    if hanh_dong == "hoc" and ten:
        all_cp = {**CONG_PHAP_TAN_CONG, **CONG_PHAP_PHONG_THU, **DAI_THAN_THONG, **CONG_PHAP_PASSIVE}
        cp = all_cp.get(ten)
        if not cp:
            await ctx.send(embed=embed_mau("❌","Công pháp không tồn tại!",0xFF4444)); return
        if nv['canh_gioi']<cp['cap_yeu']:
            await ctx.send(embed=embed_mau("❌",f"Cần **{CANH_GIOI[cp['cap_yeu']]}**!",0xFF4444)); return
        if nv['linh_thach']<cp['phi']:
            await ctx.send(embed=embed_mau("❌",f"Cần **{cp['phi']:,}** 💎",0xFF4444)); return
        cp_list = json.loads(nv['cong_phap'] or '[]')
        if ten in cp_list:
            await ctx.send(embed=embed_mau("⚠️","Đã học rồi!",0xFFAA00)); return
        cp_list.append(ten)
        updates = {"cong_phap": json.dumps(cp_list, ensure_ascii=False), "linh_thach": nv['linh_thach']-cp['phi']}
        # Áp dụng passive ngay
        if ten in CONG_PHAP_PASSIVE:
            pi = CONG_PHAP_PASSIVE[ten]
            if "bonus_hp" in pi: updates["linh_luc_max"] = nv['linh_luc_max']+pi["bonus_hp"]
            if "bonus_all" in pi:
                updates["tan_cong"] = nv['tan_cong']+pi["bonus_all"]
                updates["phong_thu"] = nv['phong_thu']+pi["bonus_all"]
        if ten in CONG_PHAP_PHONG_THU:
            updates["phong_thu"] = nv['phong_thu'] + CONG_PHAP_PHONG_THU[ten]['phong_thu_bonus']
        await cap_nhat(ctx.author.id, **updates)
        await ctx.send(embed=embed_mau("⚡ Học Công Pháp Thành Công!",f"Đã học **{ten}**!\n(-{cp['phi']:,} 💎)",0xAA55FF))

    elif hanh_dong == "xem":
        cp_list = json.loads(nv['cong_phap'] or '[]')
        if not cp_list:
            await ctx.send(embed=embed_mau("📚","Chưa học công pháp nào!")); return
        await ctx.send(embed=embed_mau("📚 Công Pháp Của Bạn", "\n".join(f"✅ **{k}**" for k in cp_list)))

# ══════════════════════════════════════════════════════════════
#  LỆNH: BOSS
# ══════════════════════════════════════════════════════════════
@bot.command(name="boss")
async def danh_boss(ctx, so_boss: int = None):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    if so_boss is None:
        gioi_hien = nv['ban_do']
        boss_trong_gioi = [(i+1,b) for i,b in enumerate(BOSS_LIST) if b.get('gioi')==gioi_hien]
        if not boss_trong_gioi:
            boss_trong_gioi = [(i+1,b) for i,b in enumerate(BOSS_LIST)]
        lines = []
        for idx, b in boss_trong_gioi[:10]:
            lock = "🔒" if nv['canh_gioi']<b['cap_yeu'] else "⚔️"
            lines.append(f"{lock} **{idx}. {b['ten']}** HP:{b['hp']:,} | Cần Lv.{b['cap_yeu']} | 💎{b['phan_thuong']:,}")
        await ctx.send(embed=embed_mau(f"👹 Boss — {BAN_DO[gioi_hien]['ten']}", "\n".join(lines) or "Không có boss phù hợp"))
        return

    if not (1<=so_boss<=len(BOSS_LIST)):
        await ctx.send(embed=embed_mau("❌",f"Boss 1-{len(BOSS_LIST)}",0xFF4444)); return
    boss = BOSS_LIST[so_boss-1]
    if nv['canh_gioi']<boss['cap_yeu']:
        await ctx.send(embed=embed_mau("❌",f"Cần **{CANH_GIOI[boss['cap_yeu']]}**!",0xFF4444)); return

    # Tính sát thương từ công pháp
    cp_list = json.loads(nv['cong_phap'] or '[]')
    atk_bonus = 0
    for cp in cp_list:
        if cp in CONG_PHAP_TAN_CONG: atk_bonus += CONG_PHAP_TAN_CONG[cp]['sat_thuong']
        if cp in DAI_THAN_THONG: atk_bonus += DAI_THAN_THONG[cp]['sat_thuong']
    kl_bonus = KIEM_LINH_BONUS[min(nv['kiem_linh_cap'],len(KIEM_LINH_BONUS)-1)]

    p_hp = nv['linh_luc']
    b_hp = boss['hp']
    rounds = []
    for turn in range(1,31):
        base_atk = nv['tan_cong'] + atk_bonus
        p_atk = max(1, int(random.randint(base_atk, base_atk*2) * (1+kl_bonus/100)) - boss['sat_thuong']//4)
        b_atk = max(1, random.randint(boss['sat_thuong']//2, boss['sat_thuong']) - nv['phong_thu'])
        b_hp -= p_atk; p_hp -= b_atk
        if turn<=3: rounds.append(f"Lượt {turn}: Bạn gây **{p_atk:,}** | Boss gây **{b_atk:,}**")
        if p_hp<=0 or b_hp<=0: break

    if p_hp>0:
        await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']+boss['phan_thuong'],
                       exp=nv['exp']+boss['exp'], linh_luc=max(1,p_hp))
        await cap_nhat_tk(ctx.author.id, tong_boss_giet=1, tong_lt_kiem=boss['phan_thuong'], tong_exp=boss['exp'])
        await them_nhat_ky(ctx.author.id,"boss",f"Hạ **{boss['ten']}** (+{boss['phan_thuong']:,}💎)")
        result = "\n".join(rounds)+f"\n...\n\n🏆 **CHIẾN THẮNG!**\n💎 +{boss['phan_thuong']:,} | ✨ +{boss['exp']:,} EXP"
        color = 0x55FF55
    else:
        await cap_nhat(ctx.author.id, linh_luc=1, so_chet=nv['so_chet']+1)
        await cap_nhat_tk(ctx.author.id, tong_tulyen=0)
        await them_nhat_ky(ctx.author.id,"boss",f"Bại trận trước **{boss['ten']}**")
        result = "\n".join(rounds)+"\n...\n\n💀 **THẤT BẠI!** Hồi phục rồi thử lại!"
        color = 0xFF4444

    nv2=await get_nv(ctx.author.id)
    async with db_pool.acquire() as c: tk=await c.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1",ctx.author.id)
    await kiem_tra_thanh_tich(ctx,ctx.author.id,nv2,tk)
    await ctx.send(embed=embed_mau(f"⚔️ Boss: {boss['ten']}", result, color))

# ══════════════════════════════════════════════════════════════
#  HELPER BOSS THẾ GIỚI
# ══════════════════════════════════════════════════════════════
def format_dmg_bar(hp_hien, hp_max, width=20):
    pct = max(0, min(1, hp_hien / max(hp_max, 1)))
    filled = int(pct * width)
    return "🟥" * filled + "⬛" * (width - filled)

async def gui_phan_thuong_boss(gioi: str, boss_info: dict, session_time):
    """Tính top damage, gửi kết quả và DM phần thưởng"""
    async with db_pool.acquire() as c:
        logs = await c.fetch("""
            SELECT user_id, ten_nv, SUM(damage) as tong_damage
            FROM boss_damage_log
            WHERE gioi=$1 AND boss_session=$2
            GROUP BY user_id, ten_nv
            ORDER BY tong_damage DESC
        """, gioi, session_time)

    if not logs: return

    total_hp = boss_info["hp"]
    phan_thuong_co_ban = boss_info["phan_thuong"]

    # Phần thưởng theo rank damage
    def reward_by_damage(dmg):
        pct = dmg / max(total_hp, 1) * 100
        if pct >= 20:    return {"lt": int(phan_thuong_co_ban * 0.5), "item": "Tiên Nguyên Đan"}
        elif pct >= 10:  return {"lt": int(phan_thuong_co_ban * 0.3), "item": "Thần Nguyên Đan"}
        elif pct >= 5:   return {"lt": int(phan_thuong_co_ban * 0.15),"item": "Tụ Nguyên Đan"}
        elif pct >= 1:   return {"lt": int(phan_thuong_co_ban * 0.05),"item": "Hồi Linh Đan"}
        else:            return {"lt": int(phan_thuong_co_ban * 0.01),"item": None}

    medals = ["🥇","🥈","🥉"] + ["🏅"]*50
    ke_tieu_diet = logs[0]

    # Build embed kết quả
    top3_lines = ""
    for i, row in enumerate(logs[:3]):
        top3_lines += f"{medals[i]} **#{i+1} — {row['ten_nv']}**\n💥 Sát thương: **{row['tong_damage']:,}**\n\n"

    # Phần thưởng top rank
    rank_reward_lines = "🥇 Top 1: +50% phần thưởng cơ bản\n🥈 Top 2-3: +30% phần thưởng cơ bản\n🏅 Top 4-10: +15% phần thưởng cơ bản"

    # Phần thưởng theo damage %
    dmg_reward_lines = (
        f"• ≥20% HP boss: Tiên Nguyên Đan + 50% LT\n"
        f"• ≥10% HP boss: Thần Nguyên Đan + 30% LT\n"
        f"• ≥5% HP boss: Tụ Nguyên Đan + 15% LT\n"
        f"• ≥1% HP boss: Hồi Linh Đan + 5% LT\n"
        f"• <1% HP boss: 1% LT"
    )

    result_embed = discord.Embed(
        title=f"💀 BOSS THẾ GIỚI ĐÃ BỊ TIÊU DIỆT! 💀",
        description=(
            f"**{boss_info['ten']}** — **{BAN_DO[gioi]['ten']}** đã bị đánh bại bởi các tu sĩ!\n\n"
            f"🗡️ **Kẻ tiêu diệt:** {ke_tieu_diet['ten_nv']}\n"
            f"⏰ **Thời gian tồn tại:** 2 giờ\n\n"
            f"🏆 **TOP 3 VINH DỰ**\n{top3_lines}"
            f"🎁 **HỆ THỐNG PHẦN THƯỞNG**\n✅ Phần thưởng đã được tự động phân phối!\n\n"
            f"🏅 **Top Damage Ranking:**\n{rank_reward_lines}\n\n"
            f"💥 **Phần thưởng theo Damage:**\n{dmg_reward_lines}\n\n"
            f"📩 Kiểm tra DM để xem phần thưởng của bạn!"
        ),
        color=0xFF0000
    )
    result_embed.set_image(url=boss_info.get("img",""))
    result_embed.set_footer(text="⚡ Ta Tu Tiên | Boss Thế Giới")

    # Gửi vào channel boss
    channel = bot.get_channel(BOSS_CHANNEL_ID)
    if channel:
        await channel.send(embed=result_embed)

    # Phát thưởng và DM từng người
    async with db_pool.acquire() as c:
        for i, row in enumerate(logs):
            uid = row['user_id']
            nv = await get_nv(uid)
            if not nv: continue

            dmg = row['tong_damage']
            reward = reward_by_damage(dmg)

            # Bonus top rank
            if i == 0:    bonus_lt = int(phan_thuong_co_ban * 0.5)
            elif i <= 2:  bonus_lt = int(phan_thuong_co_ban * 0.3)
            elif i <= 9:  bonus_lt = int(phan_thuong_co_ban * 0.15)
            else:         bonus_lt = 0

            total_lt = reward["lt"] + bonus_lt

            # Cập nhật linh thạch + exp
            await cap_nhat(uid,
                linh_thach=nv['linh_thach'] + total_lt,
                exp=nv['exp'] + boss_info["exp"] // max(len(logs), 1)
            )

            # Thêm item vào túi đồ
            if reward["item"]:
                await c.execute("""
                    INSERT INTO tui_do(user_id, vat_pham, so_luong) VALUES($1,$2,1)
                    ON CONFLICT(user_id, vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+1
                """, uid, reward["item"])

            # DM phần thưởng
            try:
                user = await bot.fetch_user(uid)
                dm_embed = discord.Embed(
                    title="🎁 BẠN ĐÃ NHẬN ĐƯỢC PHẦN THƯỞNG BOSS THẾ GIỚI!",
                    description=(
                        f"Chúc mừng bạn đã nhận được phần thưởng từ Boss Thế Giới!\n\n"
                        f"🗡️ **Tổng sát thương gây ra**\n```{dmg:,}```\n"
                        f"**🎁 Phần thưởng đã nhận**\n"
                        f"{'🥇 Top #'+str(i+1)+' Bonus: +'+f'{bonus_lt:,} Linh Thạch' + chr(10) if bonus_lt else ''}"
                        f"🎁 **+{total_lt:,} Linh Thạch**\n"
                        f"{'🎁 **'+reward['item']+'** × 1'+chr(10) if reward['item'] else ''}"
                        f"\n✅ **Thông báo**\n"
                        f"• Tất cả phần thưởng đã được tự động thêm vào inventory\n"
                        f"• Sử dụng lệnh `!tuido` để xem kho đồ\n"
                        f"• Cảm ơn bạn đã tham gia đánh boss!"
                    ),
                    color=0xFFD700
                )
                dm_embed.set_footer(text="Phần thưởng tự động thêm vào kho đồ")
                await user.send(embed=dm_embed)
            except: pass

# ══════════════════════════════════════════════════════════════
#  LỆNH: BOSS THẾ GIỚI
# ══════════════════════════════════════════════════════════════
@bot.command(name="bossthegioi", aliases=["btg","worldboss"])
async def boss_the_gioi_cmd(ctx, hanh_dong: str = None):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    gioi = nv['ban_do']
    if gioi not in BOSS_THE_GIOI_LIST:
        await ctx.send(embed=embed_mau("❌","Bản đồ của bạn chưa có Boss Thế Giới!",0xFF4444)); return

    async with db_pool.acquire() as c:
        boss_row = await c.fetchrow("SELECT * FROM boss_the_gioi WHERE gioi=$1", gioi)

    boss_idx = boss_row['boss_idx'] if boss_row else 0
    boss_info = get_boss_hien_tai(gioi, boss_idx)
    hp_hien = boss_row['hp_hien'] if boss_row else boss_info["hp"]
    trang_thai = boss_row['trang_thai'] if boss_row else 'chet'

    if not hanh_dong:
        # Hiển thị trạng thái boss
        if trang_thai == 'chet':
            async with db_pool.acquire() as c:
                await _reset_so_lan_neu_ngay_moi(c, gioi)
                so_lan = await c.fetchval("SELECT so_lan_hom_nay FROM boss_the_gioi WHERE gioi=$1", gioi) or 0
            next_spawn = gio_spawn_tiep_theo()
            now_vn = datetime.now(VN_TZ)
            con_lai = max(0, int((next_spawn - now_vn).total_seconds()))
            h, m, s = con_lai//3600, (con_lai%3600)//60, con_lai%60
            con_lan = BOSS_MAX_NGAY - so_lan
            if con_lan <= 0:
                mo_ta = (f"**Hôm nay boss đã xuất hiện đủ {BOSS_MAX_NGAY} lần!**\n"
                         f"⏰ Boss trở lại vào **00:00h** ngày mai.")
            else:
                mo_ta = (f"**Boss đang hồi sinh...**\n"
                         f"⏰ Xuất hiện lúc: **{next_spawn.strftime('%H:%M')}h** (sau {h}h {m}m {s}s)\n"
                         f"📅 Lịch: 0h·2h·4h·6h·8h·10h·12h·14h·16h·18h·20h·22h\n\n"
                         f"Boss tiếp theo: **{boss_info['ten']}**\n"
                         f"❤️ HP: **{boss_info['hp']:,}**\n\n"
                         f"📊 Hôm nay: **{so_lan}/{BOSS_MAX_NGAY}** lần | Còn lại: **{con_lan}** lần")
            await ctx.send(embed=embed_mau(f"💀 Boss Thế Giới — {BAN_DO[gioi]['ten']}", mo_ta, 0x888888)); return

        # Boss đang sống — tính thời gian còn lại 30p
        async with db_pool.acquire() as c:
            so_dangky = await c.fetchval("SELECT COUNT(*) FROM boss_dangky WHERE gioi=$1", gioi)
            da_dangky = await c.fetchval("SELECT 1 FROM boss_dangky WHERE gioi=$1 AND user_id=$2", gioi, ctx.author.id)
            so_lan    = await c.fetchval("SELECT so_lan_hom_nay FROM boss_the_gioi WHERE gioi=$1", gioi) or 0

        con_lai_s = 0
        if boss_row and boss_row['xuat_hien_luc']:
            xl = boss_row['xuat_hien_luc']
            now_vn = datetime.now(xl.tzinfo if xl.tzinfo else VN_TZ)
            con_lai_s = max(0, BOSS_TONTAI_GIAY - int((now_vn - xl).total_seconds()))
        m30, s30 = con_lai_s // 60, con_lai_s % 60

        pct_bar = format_dmg_bar(hp_hien, boss_info["hp"])
        dangky_str = f"✅ Đã đăng ký" if da_dangky else f"📋 Chưa đăng ký — dùng `!bossthegioi dangky`"
        e = discord.Embed(
            title=f"👑 Boss Thế Giới — {BAN_DO[gioi]['ten']}",
            description=(
                f"**{boss_info['ten']}**\n\n"
                f"❤️ HP: **{hp_hien:,}** / **{boss_info['hp']:,}**\n"
                f"{pct_bar}\n\n"
                f"⏰ Còn lại: **{m30}p {s30}s** trước khi boss rút lui!\n"
                f"💎 {boss_info['phan_thuong']:,} Linh Thạch | ✨ {boss_info['exp']:,} EXP\n"
                f"👥 Người đã đăng ký: **{so_dangky}**\n"
                f"Bạn: {dangky_str}\n"
                f"📊 Hôm nay: **{so_lan}/{BOSS_MAX_NGAY}** lần\n\n"
                f"Dùng `!bossthegioi tan` để tham chiến!"
            ),
            color=0xFF0000
        )
        e.set_image(url=boss_info.get("img",""))
        e.set_footer(text="⚡ Ta Tu Tiên | Boss Thế Giới")
        await ctx.send(embed=e)
        return

    if hanh_dong == "dangky":
        if trang_thai == 'chet':
            await ctx.send(embed=embed_mau("💀","Boss chưa xuất hiện! Không thể đăng ký.",0x888888)); return
        async with db_pool.acquire() as c:
            da_dangky = await c.fetchval("SELECT 1 FROM boss_dangky WHERE gioi=$1 AND user_id=$2", gioi, ctx.author.id)
            if da_dangky:
                await ctx.send(embed=embed_mau("✅ Đã Đăng Ký",f"Bạn đã đăng ký tham chiến **{boss_info['ten']}** rồi!\nDùng `!bossthegioi tan` để đánh!",0x55FFAA)); return
            await c.execute("INSERT INTO boss_dangky(gioi, user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", gioi, ctx.author.id)
            so_dangky = await c.fetchval("SELECT COUNT(*) FROM boss_dangky WHERE gioi=$1", gioi)
        await ctx.send(embed=embed_mau("📋 Đăng Ký Thành Công!",
            f"**{nv['ten']}** đã đăng ký tham chiến **{boss_info['ten']}**!\n"
            f"👥 Tổng người đăng ký: **{so_dangky}**\n\n"
            f"⚔️ Dùng `!bossthegioi tan` để bắt đầu đánh!",0x55FFAA)); return

    if hanh_dong == "tan":
        if trang_thai == 'chet':
            await ctx.send(embed=embed_mau("💀","Boss chưa xuất hiện! Chờ thông báo hồi sinh.",0x888888)); return
