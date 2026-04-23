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
from datetime import datetime, timedelta

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
    "Thiên Linh Căn":   {"icon":"🌟","mo_ta":"Vạn năm hiếm có một, thiên phú tuyệt thế vô song",   "bonus_exp":200,"bonus_tulyen":150,"ty_le":1},
    "Biến Linh Căn":    {"icon":"🌈","mo_ta":"5 hệ hỗn dung, tiến tốc kinh thiên động địa",          "bonus_exp":120,"bonus_tulyen":80, "ty_le":4},
    "Tứ Linh Căn":      {"icon":"💫","mo_ta":"4 hệ linh căn cân bằng, thiên địa chứng đạo",          "bonus_exp":80, "bonus_tulyen":50, "ty_le":10},
    "Tam Linh Căn":     {"icon":"✨","mo_ta":"3 hệ linh căn, khí vận phi thường",                    "bonus_exp":50, "bonus_tulyen":35, "ty_le":20},
    "Song Linh Căn":    {"icon":"⭐","mo_ta":"2 hệ linh căn, tài chất xuất chúng",                   "bonus_exp":30, "bonus_tulyen":20, "ty_le":30},
    "Đơn Linh Căn":     {"icon":"🔥","mo_ta":"1 hệ chuyên sâu, chuyên tinh hóa thần",               "bonus_exp":25, "bonus_tulyen":25, "ty_le":25},
    "Phế Linh Căn":     {"icon":"💀","mo_ta":"Vô căn nhưng ý chí thép, nghịch thiên cải mệnh",      "bonus_exp":5,  "bonus_tulyen":5,  "ty_le":10},
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
    "tulyen_10":     {"ten":"🧘 Siêng Năng",        "mo_ta":"Tu luyện 10 lần"},
    "tulyen_100":    {"ten":"🔥 Khổ Tu",            "mo_ta":"Tu luyện 100 lần"},
    "tulyen_500":    {"ten":"⛰️ Bế Quan Đại Sư",    "mo_ta":"Tu luyện 500 lần"},
    "tulyen_1000":   {"ten":"🪨 Khổ Hạnh Giả",      "mo_ta":"Tu luyện 1000 lần"},
    "tulyen_5000":   {"ten":"🌌 Vạn Cổ Khổ Tu",     "mo_ta":"Tu luyện 5000 lần"},
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
                last_tulyen   TIMESTAMPTZ,
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
    e.set_footer(text="⚡ Tu Tiên Bot V3 | Vạn Cổ Trường Tồn")
    return e

async def paginate(ctx, pages, color=0xAA55FF):
    """Gửi 1 tin nhắn duy nhất có nút ◀ ▶ để lật trang.
    pages: list of (title, content_str)
    """
    if not pages:
        return
    if len(pages) == 1:
        await ctx.send(embed=discord.Embed(title=pages[0][0], description=pages[0][1], color=color).set_footer(text="⚡ Tu Tiên Bot V3 | Vạn Cổ Trường Tồn"))
        return

    page = 0
    total = len(pages)

    def make_embed(p):
        title, desc = pages[p]
        e = discord.Embed(title=title, description=desc, color=color)
        e.set_footer(text=f"⚡ Tu Tiên Bot V3 | Trang {p+1}/{total} — Dùng ◀ ▶ để chuyển")
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
        "tulyen_10":    tk and tk['tong_tulyen']>=10,
        "tulyen_100":   tk and tk['tong_tulyen']>=100,
        "tulyen_500":   tk and tk['tong_tulyen']>=500,
        "tulyen_1000":  tk and tk['tong_tulyen']>=1000,
        "tulyen_5000":  tk and tk['tong_tulyen']>=5000,
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

    e.set_footer(text=f"Tạo nhân vật: {ngay_tao} | Hoạt động cuối: {datetime.utcnow().strftime('%d/%m/%Y %H:%M')}")
    if target.avatar:
        e.set_thumbnail(url=target.avatar.url)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════════
#  LỆNH: TU LUYỆN
# ══════════════════════════════════════════════════════════════
@bot.command(name="tulyen", aliases=["tl"])
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

    cd = cooldown_con(nv['last_tulyen'], 60)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳",f"Còn **{cd:.0f}s** nữa!",0xFFAA00)); return

    lc_info  = LINH_CAN.get(nv['linh_can'], {})
    toc_info = TOC.get(nv['toc'], {})
    exp_bonus  = lc_info.get("bonus_exp", 0) + toc_info.get("bonus_exp", 0)
    tuvi_bonus = lc_info.get("bonus_tulyen", 0)
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
        ban_do=ban_do_hien, last_tulyen=datetime.utcnow()
    )
    await cap_nhat_tk(ctx.author.id, tong_tulyen=1, tong_exp=exp_gain, dot_pha_count=dp_cnt)
    await them_nhat_ky(ctx.author.id, "tulyen", f"+{exp_gain:,} EXP, +{tv_gain:,} Tu Vi → {CANH_GIOI[new_cg]}")

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
    ).set_footer(text="⚡ Tu Tiên Bot V4 | Vạn Cổ Trường Tồn"))

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
    await cap_nhat(ctx.author.id, last_bequan=datetime.utcnow(), bequan_gio=gio)
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
    result_embed.set_footer(text="⚡ Tu Tiên Bot V3 | Boss Thế Giới")

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
            next_spawn = boss_row['last_reset'] + timedelta(hours=1) if boss_row and boss_row['last_reset'] else datetime.utcnow()
            now_utc = datetime.utcnow()
            if hasattr(next_spawn, 'tzinfo') and next_spawn.tzinfo:
                import pytz; now_utc = datetime.now(next_spawn.tzinfo)
            con_lai = max(0, int((next_spawn - now_utc).total_seconds()))
            h, m, s = con_lai//3600, (con_lai%3600)//60, con_lai%60
            await ctx.send(embed=embed_mau(
                f"💀 Boss Thế Giới — {BAN_DO[gioi]['ten']}",
                f"**Boss đang hồi sinh...**\n⏰ Xuất hiện sau: **{h}h {m}m {s}s**\n\n"
                f"Boss tiếp theo: **{boss_info['ten']}**\n"
                f"❤️ HP: **{boss_info['hp']:,}** | Cần Lv.**{boss_info['cap_yeu']}**",
                0x888888
            )); return

        pct_bar = format_dmg_bar(hp_hien, boss_info["hp"])
        e = discord.Embed(
            title=f"👑 Boss Thế Giới — {BAN_DO[gioi]['ten']}",
            description=(
                f"**{boss_info['ten']}**\n\n"
                f"❤️ HP: **{hp_hien:,}** / **{boss_info['hp']:,}**\n"
                f"{pct_bar}\n\n"
                f"⚔️ Cần Lv.**{boss_info['cap_yeu']}** | 💎 {boss_info['phan_thuong']:,} | ✨ {boss_info['exp']:,} EXP\n\n"
                f"Dùng `!bossthegioi tan` để tham chiến!"
            ),
            color=0xFF0000
        )
        e.set_image(url=boss_info.get("img",""))
        e.set_footer(text="⚡ Tu Tiên Bot V3 | Boss Thế Giới")
        await ctx.send(embed=e)
        return

    if hanh_dong == "tan":
        if trang_thai == 'chet':
            await ctx.send(embed=embed_mau("💀","Boss chưa xuất hiện! Chờ thông báo hồi sinh.",0x888888)); return
        if nv['canh_gioi'] < boss_info["cap_yeu"]:
            await ctx.send(embed=embed_mau("❌",f"Cần Lv.**{boss_info['cap_yeu']}** ({CANH_GIOI[boss_info['cap_yeu']]})!",0xFF4444)); return
        if hp_hien <= 0:
            await ctx.send(embed=embed_mau("💀","Boss đã bị hạ! Hồi sinh sau 1 giờ.",0x888888)); return

        cp_list = json.loads(nv['cong_phap'] or '[]')
        atk_bonus = sum(CONG_PHAP_TAN_CONG.get(cp,{}).get('sat_thuong',0) for cp in cp_list)
        atk_bonus += sum(DAI_THAN_THONG.get(cp,{}).get('sat_thuong',0) for cp in cp_list)
        kl_bonus = KIEM_LINH_BONUS[min(nv['kiem_linh_cap'],len(KIEM_LINH_BONUS)-1)]
        base_atk = nv['tan_cong'] + atk_bonus
        dmg = int(random.randint(base_atk*3, base_atk*10) * (1+kl_bonus/100))
        player_dmg = max(1, boss_info["sat_thuong"]//4 - nv['phong_thu'])

        new_hp = max(0, hp_hien - dmg)

        async with db_pool.acquire() as c:
            await c.execute("UPDATE boss_the_gioi SET hp_hien=$2 WHERE gioi=$1", gioi, new_hp)
            # Ghi damage log
            await c.execute("""
                INSERT INTO boss_damage_log(gioi, user_id, ten_nv, damage, boss_session)
                VALUES($1,$2,$3,$4,$5)
            """, gioi, ctx.author.id, nv['ten'], dmg, boss_row['xuat_hien_luc'])

        await cap_nhat(ctx.author.id, linh_luc=max(1, nv['linh_luc']-player_dmg))

        killed = new_hp <= 0
        if killed:
            next_idx = (boss_idx + 1) % len(BOSS_THE_GIOI_LIST[gioi])
            next_boss = get_boss_hien_tai(gioi, next_idx)
            async with db_pool.acquire() as c:
                await c.execute("""
                    UPDATE boss_the_gioi SET hp_hien=$2, trang_thai='chet', nguoi_giet=$3,
                    last_reset=NOW(), boss_idx=$4
                    WHERE gioi=$1
                """, gioi, next_boss["hp"], ctx.author.id, next_idx)
            # Phân phối phần thưởng
            await gui_phan_thuong_boss(gioi, boss_info, boss_row['xuat_hien_luc'])

        pct_bar2 = format_dmg_bar(new_hp, boss_info["hp"])
        color = 0xFFD700 if killed else 0xFF6600
        msg = (
            f"💥 Gây **{dmg:,}** sát thương!\n"
            f"🛡️ Boss phản đòn **{player_dmg:,}** sát thương\n\n"
            f"❤️ Boss HP: **{new_hp:,}** / **{boss_info['hp']:,}**\n"
            f"{pct_bar2}\n"
        )
        if killed:
            msg += f"\n💀 **BOSS THẾ GIỚI ĐÃ BỊ TIÊU DIỆT!**\n🏆 Phần thưởng đã gửi về DM!\n⏰ Boss tiếp theo xuất hiện sau **1 giờ**!"
        e2 = discord.Embed(title=f"⚔️ Tham Chiến Boss Thế Giới", description=msg, color=color)
        e2.set_footer(text="⚡ Tu Tiên Bot V3 | Vạn Cổ Trường Tồn")
        await ctx.send(embed=e2)

# ══════════════════════════════════════════════════════════════
#  LỆNH: SET CHANNEL BOSS
# ══════════════════════════════════════════════════════════════
@bot.command(name="setchannel", aliases=["setbosschannel"])
@commands.has_permissions(administrator=True)
async def set_channel(ctx, loai: str = "boss"):
    global BOSS_CHANNEL_ID
    BOSS_CHANNEL_ID = ctx.channel.id
    await ctx.send(embed=embed_mau("✅ Đã Thiết Lập",f"Kênh **#{ctx.channel.name}** sẽ nhận thông báo Boss Thế Giới!",0x55FF55))

# ══════════════════════════════════════════════════════════════
#  TASK: TỰ ĐỘNG THÔNG BÁO BOSS XUẤT HIỆN (mỗi 2 giờ thực)
# ══════════════════════════════════════════════════════════════
@tasks.loop(hours=2)
async def auto_boss_spawn():
    """Mỗi 2 giờ: spawn boss ở TẤT CẢ các giới cùng lúc, không block nhau"""
    if db_pool is None: return
    channel = bot.get_channel(BOSS_CHANNEL_ID)
    if not channel: return

    # Spawn tất cả các giới song song
    tasks_list = [_spawn_boss_gioi(gioi, bd_info, channel)
                  for gioi, bd_info in BAN_DO.items()
                  if gioi in BOSS_THE_GIOI_LIST]
    await asyncio.gather(*tasks_list, return_exceptions=True)

async def _spawn_boss_gioi(gioi: str, bd_info: dict, channel):
    """Spawn boss 1 giới: thông báo → đợi 2h → đóng nếu chưa chết"""
    try:
        async with db_pool.acquire() as c:
            boss_row = await c.fetchrow("SELECT * FROM boss_the_gioi WHERE gioi=$1", gioi)

        boss_idx = boss_row['boss_idx'] if boss_row else 0
        boss_info = get_boss_hien_tai(gioi, boss_idx)

        # Set trạng thái SỐNG
        async with db_pool.acquire() as c:
            await c.execute("""
                UPDATE boss_the_gioi SET hp_hien=$2, trang_thai='song',
                xuat_hien_luc=NOW(), last_reset=NOW() WHERE gioi=$1
            """, gioi, boss_info["hp"])

        # Embed thông báo xuất hiện
        e = discord.Embed(
            title=f"⚠️ BOSS THẾ GIỚI XUẤT HIỆN — {bd_info['ten']} ⚠️",
            description=(
                f"**{boss_info['ten']}** đã giáng lâm {bd_info['ten']}!\n\n"
                f"❤️ HP: **{boss_info['hp']:,}**\n"
                f"⚔️ Sát Thương: **{boss_info['sat_thuong']:,}**\n"
                f"💎 Phần Thưởng: **{boss_info['phan_thuong']:,}** Linh Thạch\n"
                f"✨ EXP: **{boss_info['exp']:,}**\n"
                f"🔑 Yêu cầu: Lv.**{boss_info['cap_yeu']}** "
                f"({CANH_GIOI[min(boss_info['cap_yeu'], len(CANH_GIOI)-1)]})\n\n"
                f"⏰ **Boss tồn tại 2 giờ!**\n"
                f"Dùng lệnh `!bossthegioi tan` để tham chiến!\n"
                f"Phần thưởng tính theo damage — đánh càng nhiều nhận càng nhiều!\n\n"
                f"🏆 Top damage nhận bonus đặc biệt!"
            ),
            color=0xFF0000
        )
        e.set_image(url=boss_info.get("img", ""))
        e.set_footer(text=f"⚡ Tu Tiên Bot V3 | Boss xuất hiện lúc {datetime.utcnow().strftime('%H:%M UTC')}")
        await channel.send(
            f"@everyone 🔔 **Boss Thế Giới xuất hiện tại {bd_info['ten']}!**",
            embed=e
        )

        # Đợi 2 giờ (không block task khác vì dùng gather)
        await asyncio.sleep(7200)

        # Kiểm tra còn sống không
        async with db_pool.acquire() as c:
            row = await c.fetchrow("SELECT trang_thai, hp_hien, xuat_hien_luc FROM boss_the_gioi WHERE gioi=$1", gioi)

        if row and row['trang_thai'] == 'song' and row['hp_hien'] > 0:
            # Boss chưa bị giết — đóng, chuyển sang boss tiếp theo
            next_idx = (boss_idx + 1) % len(BOSS_THE_GIOI_LIST[gioi])
            next_boss = get_boss_hien_tai(gioi, next_idx)
            async with db_pool.acquire() as c:
                await c.execute("""
                    UPDATE boss_the_gioi SET trang_thai='chet', hp_hien=$2,
                    boss_idx=$3, last_reset=NOW() WHERE gioi=$1
                """, gioi, next_boss["hp"], next_idx)

            # Phát thưởng cho người đã đánh
            if row['xuat_hien_luc']:
                await gui_phan_thuong_boss(gioi, boss_info, row['xuat_hien_luc'])

            await channel.send(embed=embed_mau(
                f"⏰ Boss Rút Lui — {bd_info['ten']}",
                f"**{boss_info['ten']}** đã rút lui sau 2 giờ!\n"
                f"Phần thưởng đã gửi về DM các tu sĩ đã tham chiến!\n"
                f"⏰ Boss mới sẽ xuất hiện sau **2 giờ**!",
                0x888888
            ))
    except Exception as err:
        print(f"❌ Lỗi spawn boss [{gioi}]: {err}")

# ══════════════════════════════════════════════════════════════
#  LỆNH: THÁP THỬ LUYỆN
# ══════════════════════════════════════════════════════════════
@bot.command(name="thap", aliases=["tower"])
async def thap_thu_luyen(ctx):
    """!thap — Leo tháp thử luyện, mỗi tầng tăng dần (2 phút/lần)"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    async with db_pool.acquire() as c:
        thap = await c.fetchrow("SELECT * FROM thap_thu_luyen WHERE user_id=$1", ctx.author.id)

    if not thap:
        async with db_pool.acquire() as c:
            await c.execute("INSERT INTO thap_thu_luyen(user_id) VALUES($1) ON CONFLICT DO NOTHING", ctx.author.id)
            thap = await c.fetchrow("SELECT * FROM thap_thu_luyen WHERE user_id=$1", ctx.author.id)

    cd = cooldown_con(thap['last_thap'], 120)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳",f"Còn **{cd:.0f}s** nữa!",0xFFAA00)); return

    tang = thap['tang_hien']
    # Mỗi tầng mạnh hơn tầng trước
    tang_hp = 500 * tang
    tang_atk = 10 * tang
    tang_def = 5 * tang
    tang_exp = 200 * tang
    tang_lt  = 100 * tang

    cp_list = json.loads(nv['cong_phap'] or '[]')
    atk_bonus = sum(CONG_PHAP_TAN_CONG.get(cp,{}).get('sat_thuong',0) for cp in cp_list)
    kl_bonus = KIEM_LINH_BONUS[min(nv['kiem_linh_cap'],len(KIEM_LINH_BONUS)-1)]

    p_hp = nv['linh_luc']
    e_hp = tang_hp
    for turn in range(1, 20):
        p_atk = max(1, int((nv['tan_cong']+atk_bonus) * (1+kl_bonus/100) * random.uniform(0.8,1.2)) - tang_def)
        e_atk = max(1, tang_atk - nv['phong_thu'])
        e_hp -= p_atk; p_hp -= e_atk
        if p_hp<=0 or e_hp<=0: break

    if p_hp > 0:
        new_tang = tang + 1
        async with db_pool.acquire() as c:
            await c.execute("UPDATE thap_thu_luyen SET tang_hien=$2, last_thap=NOW() WHERE user_id=$1", ctx.author.id, new_tang)
        await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']+tang_lt,
                       exp=nv['exp']+tang_exp, linh_luc=max(1,p_hp))
        await ctx.send(embed=embed_mau(f"🏯 Tháp Thử Luyện — Tầng {tang}", f"""
✅ **Vượt tầng {tang} thành công!**
💎 +{tang_lt:,} Linh Thạch | ✨ +{tang_exp:,} EXP
🏯 Tầng hiện tại: **{new_tang}**

Dùng `!thap` sau 2 phút để leo tiếp!
        """, 0x55FF55))
    else:
        # Thua thì về tầng 1
        async with db_pool.acquire() as c:
            await c.execute("UPDATE thap_thu_luyen SET tang_hien=1, last_thap=NOW() WHERE user_id=$1", ctx.author.id)
        await cap_nhat(ctx.author.id, linh_luc=1, so_chet=nv['so_chet']+1)
        await ctx.send(embed=embed_mau(f"🏯 Tháp Thử Luyện — Tầng {tang}", f"""
💀 **Thất bại ở tầng {tang}!**
Tầng cao nhất đạt được: **{tang}**
Trở về tầng 1... Hồi phục rồi thách thức lại!
        """, 0xFF4444))

# ══════════════════════════════════════════════════════════════
#  LỆNH: KẾT DUYÊN
# ══════════════════════════════════════════════════════════════
@bot.command(name="ketduyen", aliases=["kd","marry"])
async def ket_duyen(ctx, doi_tac: discord.Member = None):
    """!ketduyen @người — Cầu hôn kết đạo lữ"""
    nv1 = await get_nv(ctx.author.id)
    if not nv1:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if not doi_tac:
        await ctx.send(embed=embed_mau("💍","Dùng: `!ketduyen @người`")); return
    if doi_tac.id == ctx.author.id:
        await ctx.send(embed=embed_mau("❌","Không thể kết duyên với chính mình!",0xFF4444)); return
    nv2 = await get_nv(doi_tac.id)
    if not nv2:
        await ctx.send(embed=embed_mau("❌","Người kia chưa có nhân vật!",0xFF4444)); return

    async with db_pool.acquire() as c:
        da_co = await c.fetchrow("SELECT * FROM ket_duyen WHERE (user1=$1 AND user2=$2) OR (user1=$2 AND user2=$1)",
                                  ctx.author.id, doi_tac.id)
    if da_co:
        await ctx.send(embed=embed_mau("💍","Hai người đã kết duyên rồi!",0xFF4444)); return

    if nv1['linh_thach'] < 10000:
        await ctx.send(embed=embed_mau("❌","Cần **10,000** 💎 để kết duyên!",0xFF4444)); return

    await ctx.send(embed=embed_mau("💍 Cầu Hôn!", f"""
**{nv1['ten']}** muốn kết đạo lữ cùng **{nv2['ten']}**!
{doi_tac.mention} gõ `đồng ý` trong 60 giây!
💎 Phí kết duyên: **10,000** Linh Thạch
    """, 0xFF69B4))

    def check(m): return m.author.id==doi_tac.id and m.content.lower() in ["đồng ý","dong y","yes","accept","ok"]
    try:
        await bot.wait_for("message",check=check,timeout=60)
    except asyncio.TimeoutError:
        await ctx.send(embed=embed_mau("💔",f"{doi_tac.display_name} từ chối lời cầu hôn!",0x888888)); return

    async with db_pool.acquire() as c:
        await c.execute("INSERT INTO ket_duyen(user1,user2) VALUES($1,$2)", ctx.author.id, doi_tac.id)
    await cap_nhat(ctx.author.id, linh_thach=nv1['linh_thach']-10000, dao_lu=doi_tac.id)
    await cap_nhat(doi_tac.id, dao_lu=ctx.author.id)

    async with db_pool.acquire() as c:
        await c.execute("INSERT INTO thanh_tich(user_id,ma_tt) VALUES($1,'ket_duyen') ON CONFLICT DO NOTHING", ctx.author.id)
        await c.execute("INSERT INTO thanh_tich(user_id,ma_tt) VALUES($1,'ket_duyen') ON CONFLICT DO NOTHING", doi_tac.id)

    await ctx.send(embed=embed_mau("💍 Kết Duyên Thành Công!", f"""
💗 **{nv1['ten']}** & **{nv2['ten']}** đã kết thành đạo lữ!
Nguyện cùng nhau tu tiên vạn cổ trường tồn!

✨ **Bonus Đạo Lữ:** +5% EXP khi tu luyện cùng nhau!
    """, 0xFF69B4))

# ══════════════════════════════════════════════════════════════
#  LỆNH: TRỒNG CÂY
# ══════════════════════════════════════════════════════════════
@bot.command(name="trongcay", aliases=["tc","farm"])
async def trong_cay(ctx, hanh_dong: str = None, *, loai_cay: str = None):
    """!trongcay list | !trongcay trong <cây> | !trongcay thuhoach"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    if not hanh_dong or hanh_dong=="list":
        lines = [f"🌱 **{k}** — ⏱️{v['thoi_gian']//60}p | 💎{v['gia_hat']} hạt | Cần Lv.{v['cap_yeu']}" for k,v in CAY_LINH.items()]
        await ctx.send(embed=embed_mau("🌿 Vườn Linh Thảo","\n".join(lines)+"\n\nDùng `!trongcay trong <tên>` để trồng!")); return

    if hanh_dong=="trong" and loai_cay:
        cay = CAY_LINH.get(loai_cay)
        if not cay:
            await ctx.send(embed=embed_mau("❌","Loại cây không tồn tại!",0xFF4444)); return
        if nv['canh_gioi']<cay['cap_yeu']:
            await ctx.send(embed=embed_mau("❌",f"Cần Lv.{cay['cap_yeu']}!",0xFF4444)); return
        if nv['linh_thach']<cay['gia_hat']:
            await ctx.send(embed=embed_mau("❌",f"Cần **{cay['gia_hat']}** 💎 để mua hạt!",0xFF4444)); return

        async with db_pool.acquire() as c:
            dang_trong = await c.fetchval("SELECT COUNT(*) FROM vuon_cay WHERE user_id=$1 AND da_thu=FALSE", ctx.author.id)
        if dang_trong >= 5:
            await ctx.send(embed=embed_mau("❌","Vườn đầy rồi! Tối đa 5 ô. Thu hoạch trước!",0xFF4444)); return

        thu_hoach_luc = datetime.utcnow() + timedelta(seconds=cay['thoi_gian'])
        await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']-cay['gia_hat'])
        async with db_pool.acquire() as c:
            await c.execute("INSERT INTO vuon_cay(user_id,loai_cay,trong_luc,thu_hoach_luc) VALUES($1,$2,NOW(),$3)",
                            ctx.author.id, loai_cay, thu_hoach_luc)
        await ctx.send(embed=embed_mau("🌱 Đã Trồng!",f"**{loai_cay}** sẽ chín sau **{cay['thoi_gian']//60}** phút!\nDùng `!trongcay thuhoach` khi chín."))
        return

    if hanh_dong=="thuhoach":
        async with db_pool.acquire() as c:
            cay_chins = await c.fetch("SELECT * FROM vuon_cay WHERE user_id=$1 AND da_thu=FALSE AND thu_hoach_luc<=NOW()", ctx.author.id)
        if not cay_chins:
            async with db_pool.acquire() as c:
                cay_dang = await c.fetch("SELECT loai_cay,thu_hoach_luc FROM vuon_cay WHERE user_id=$1 AND da_thu=FALSE", ctx.author.id)
            if cay_dang:
                lines = [f"🌱 **{r['loai_cay']}** — chín lúc {r['thu_hoach_luc'].strftime('%H:%M:%S')}" for r in cay_dang]
                await ctx.send(embed=embed_mau("⏳ Chưa Chín","\n".join(lines)))
            else:
                await ctx.send(embed=embed_mau("🌿","Vườn trống. Dùng `!trongcay trong <tên>` để trồng!"))
            return

        ket_qua = []
        async with db_pool.acquire() as c:
            for row in cay_chins:
                cay_info = CAY_LINH.get(row['loai_cay'],{})
                sl = random.randint(*cay_info.get('so_luong',(1,2)))
                await c.execute("UPDATE vuon_cay SET da_thu=TRUE WHERE id=$1", row['id'])
                await c.execute("INSERT INTO tui_do(user_id,vat_pham,so_luong) VALUES($1,$2,$3) ON CONFLICT(user_id,vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+$3",
                                ctx.author.id, row['loai_cay'], sl)
                ket_qua.append(f"🌿 **{row['loai_cay']}** x{sl}")
        await ctx.send(embed=embed_mau("🌾 Thu Hoạch!","\n".join(ket_qua),0x55FFAA))

    if hanh_dong=="vuon":
        async with db_pool.acquire() as c:
            cay_dang = await c.fetch("SELECT loai_cay,thu_hoach_luc,da_thu FROM vuon_cay WHERE user_id=$1 ORDER BY trong_luc DESC LIMIT 10", ctx.author.id)
        if not cay_dang:
            await ctx.send(embed=embed_mau("🌿","Vườn trống!")); return
        lines=[]
        for r in cay_dang:
            status = "✅ Chín!" if r['da_thu']==False and r['thu_hoach_luc'] and r['thu_hoach_luc'].replace(tzinfo=None)<=datetime.utcnow() else ("🌱 Đang lớn" if not r['da_thu'] else "🍃 Đã thu")
            lines.append(f"{status} **{r['loai_cay']}**")
        await ctx.send(embed=embed_mau("🌿 Vườn Của Bạn","\n".join(lines)))

# ══════════════════════════════════════════════════════════════
#  LỆNH: DÙng ĐAN DƯỢC
# ══════════════════════════════════════════════════════════════
@bot.command(name="dung", aliases=["use"])
async def dung_item(ctx, *, ten: str):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    async with db_pool.acquire() as c:
        item = await c.fetchrow("SELECT * FROM tui_do WHERE user_id=$1 AND vat_pham=$2", ctx.author.id, ten)
    if not item or item['so_luong']<1:
        await ctx.send(embed=embed_mau("❌","Không có vật phẩm này trong túi!",0xFF4444)); return

    dan = DAN_DUOC.get(ten)
    if not dan:
        await ctx.send(embed=embed_mau("❌","Vật phẩm này không thể dùng!",0xFF4444)); return

    updates = {}
    msg = f"✅ Đã dùng **{ten}**!\n"

    if dan['loai'] == 'hoi_phuc':
        new_ll = min(nv['linh_luc']+dan['hp'], nv['linh_luc_max'])
        updates['linh_luc'] = new_ll
        msg += f"💧 +{dan['hp']} HP → {new_ll:,}/{nv['linh_luc_max']:,}"

    elif dan['loai'] == 'tu_vi':
        updates['exp'] = nv['exp'] + dan['exp']
        new_cg = nv['canh_gioi']
        new_exp = nv['exp'] + dan['exp']
        while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI)-1:
            new_exp -= exp_can(new_cg); new_cg += 1
        updates['exp'] = new_exp; updates['canh_gioi'] = new_cg
        msg += f"✨ +{dan['exp']:,} EXP → {CANH_GIOI[new_cg]}"

    elif dan['loai'] == 'dot_pha':
        msg += f"🔮 Tăng **{dan['ti_le']}%** tỉ lệ đột phá lần tu luyện kế!"

    elif dan['loai'] == 'do_kiep':
        msg += f"⚡ Giảm **{dan['giam_kien']}%** thiên kiếp khi độ kiếp!"

    elif dan['loai'] == 'buff_atk':
        updates['tan_cong'] = nv['tan_cong'] + dan['atk']
        msg += f"⚔️ Tấn Công vĩnh viễn +{dan['atk']}"

    elif dan['loai'] == 'buff_def':
        updates['phong_thu'] = nv['phong_thu'] + dan['def']
        msg += f"🛡️ Phòng Thủ vĩnh viễn +{dan['def']}"

    elif dan['loai'] == 'buff_hp':
        updates['linh_luc_max'] = nv['linh_luc_max'] + dan['hp_max']
        msg += f"💧 HP tối đa vĩnh viễn +{dan['hp_max']}"

    elif dan['loai'] == 'buff_all':
        updates['tan_cong']  = nv['tan_cong']  + dan['all']
        updates['phong_thu'] = nv['phong_thu'] + dan['all']
        updates['linh_luc_max'] = nv['linh_luc_max'] + dan['all']*5
        msg += f"⭐ Tất cả chỉ số vĩnh viễn +{dan['all']}"

    if updates: await cap_nhat(ctx.author.id, **updates)
    async with db_pool.acquire() as c:
        if item['so_luong']<=1: await c.execute("DELETE FROM tui_do WHERE user_id=$1 AND vat_pham=$2",ctx.author.id,ten)
        else: await c.execute("UPDATE tui_do SET so_luong=so_luong-1 WHERE user_id=$1 AND vat_pham=$2",ctx.author.id,ten)
    await ctx.send(embed=embed_mau("💊 Dùng Đan Dược",msg,0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: KHÁM PHÁ
# ══════════════════════════════════════════════════════════════
@bot.command(name="khampha", aliases=["kp"])
async def kham_pha(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    cd = cooldown_con(nv['last_khampha'],120)
    if cd>0:
        await ctx.send(embed=embed_mau("⏳",f"Còn **{cd:.0f}s**!",0xFFAA00)); return

    lt = random.randint(10,50) + nv['canh_gioi']*5
    results=[f"💎 +{lt} Linh Thạch"]
    vp_found=None

    # Trang bị ngẫu nhiên (~20%)
    if random.random()<0.2:
        tb = gen_trang_bi(nv['canh_gioi'])
        vp_found = tb['ten']
        results.append(f"⚔️ Nhặt được **{tb['ten']}** (+{tb['atk']} ATK, +{tb['def']} DEF)!")
        async with db_pool.acquire() as c:
            await c.execute("INSERT INTO tui_do(user_id,vat_pham,so_luong) VALUES($1,$2,1) ON CONFLICT(user_id,vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+1",ctx.author.id,vp_found)

    # Đan dược ngẫu nhiên (~30%)
    elif random.random()<0.3:
        dan_list = [k for k,v in DAN_DUOC.items() if v.get('cap_yeu',0)<=nv['canh_gioi']]
        if dan_list:
            vp_found = random.choice(dan_list)
            results.append(f"💊 Tìm được **{vp_found}**!")
            async with db_pool.acquire() as c:
                await c.execute("INSERT INTO tui_do(user_id,vat_pham,so_luong) VALUES($1,$2,1) ON CONFLICT(user_id,vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+1",ctx.author.id,vp_found)

    await cap_nhat(ctx.author.id,linh_thach=nv['linh_thach']+lt,last_khampha=datetime.utcnow())
    await cap_nhat_tk(ctx.author.id,tong_lt_kiem=lt)
    await them_nhat_ky(ctx.author.id,"khampha",f"+{lt}💎"+(f", tìm {vp_found}" if vp_found else ""))
    await ctx.send(embed=embed_mau("🗺️ Khám Phá","\n".join(results),0x55AAFF))

# ══════════════════════════════════════════════════════════════
#  LỆNH: PVP
# ══════════════════════════════════════════════════════════════
@bot.command(name="pvp")
async def pvp(ctx, doi_thu: discord.Member):
    if doi_thu.id==ctx.author.id:
        await ctx.send(embed=embed_mau("❌","Không thể tự đánh mình!",0xFF4444)); return
    nv1=await get_nv(ctx.author.id); nv2=await get_nv(doi_thu.id)
    if not nv1 or not nv2:
        await ctx.send(embed=embed_mau("❌","Một trong hai chưa có nhân vật!",0xFF4444)); return

    await ctx.send(embed=embed_mau("⚔️ Thách Đấu!",f"**{nv1['ten']}** (Lv.{nv1['canh_gioi']}) thách **{nv2['ten']}** (Lv.{nv2['canh_gioi']})!\n{doi_thu.mention} gõ `chấp nhận` trong 30s!"))
    def check(m): return m.author.id==doi_thu.id and m.content.lower() in ["chấp nhận","chap nhan","ok","accept"]
    try: await bot.wait_for("message",check=check,timeout=30)
    except asyncio.TimeoutError:
        await ctx.send(embed=embed_mau("⏰",f"{doi_thu.display_name} bỏ chạy!",0xFF4444)); return

    def get_atk(nv):
        cp_list = json.loads(nv['cong_phap'] or '[]')
        bonus = sum(CONG_PHAP_TAN_CONG.get(cp,{}).get('sat_thuong',0) for cp in cp_list)
        kl = KIEM_LINH_BONUS[min(nv['kiem_linh_cap'],len(KIEM_LINH_BONUS)-1)]
        return int((nv['tan_cong']+bonus)*(1+kl/100))

    hp1,hp2=nv1['linh_luc'],nv2['linh_luc']
    rounds=[]
    for i in range(1,15):
        a1=max(1,int(random.uniform(0.8,1.2)*get_atk(nv1))-nv2['phong_thu'])
        a2=max(1,int(random.uniform(0.8,1.2)*get_atk(nv2))-nv1['phong_thu'])
        hp2-=a1;hp1-=a2
        if i<=3: rounds.append(f"Lượt {i}: {nv1['ten']} -{a1:,} | {nv2['ten']} -{a2:,}")
        if hp1<=0 or hp2<=0: break

    win_id=ctx.author.id if hp2<=0 else doi_thu.id
    lose_id=doi_thu.id if hp2<=0 else ctx.author.id
    nv_w=nv1 if win_id==ctx.author.id else nv2
    nv_l=nv2 if win_id==ctx.author.id else nv1

    lt_cuop=min(random.randint(20,100), nv_l['linh_thach'])
    async with db_pool.acquire() as c:
        await c.execute("UPDATE nhanvat SET linh_thach=linh_thach+$2 WHERE user_id=$1",win_id,lt_cuop)
        await c.execute("UPDATE nhanvat SET linh_thach=GREATEST(0,linh_thach-$2),linh_luc=1 WHERE user_id=$1",lose_id,lt_cuop)
        await c.execute("INSERT INTO lich_su_pvp(nguoi_thang,nguoi_thua,ten_thang,ten_thua,lt_cuop) VALUES($1,$2,$3,$4,$5)",
                        win_id,lose_id,nv_w['ten'],nv_l['ten'],lt_cuop)

    await cap_nhat_tk(win_id, tong_pvp_thang=1, tong_lt_kiem=lt_cuop)
    await cap_nhat_tk(lose_id, tong_pvp_thua=1, tong_lt_tieu=lt_cuop)
    await cap_nhat(lose_id, so_chet=nv_l['so_chet']+1)
    await them_nhat_ky(win_id,"pvp",f"Thắng **{nv_l['ten']}** +{lt_cuop:,}💎")
    await them_nhat_ky(lose_id,"pvp",f"Thua **{nv_w['ten']}** -{lt_cuop:,}💎")

    # Kiểm tra all_in
    nv_l_new = await get_nv(lose_id)
    if nv_l_new and nv_l_new['linh_thach']==0:
        async with db_pool.acquire() as c:
            await c.execute("INSERT INTO thanh_tich(user_id,ma_tt) VALUES($1,'all_in') ON CONFLICT DO NOTHING", lose_id)

    for uid in [win_id,lose_id]:
        nv_c=await get_nv(uid)
        async with db_pool.acquire() as c: tk=await c.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1",uid)
        await kiem_tra_thanh_tich(ctx,uid,nv_c,tk)

    result="\n".join(rounds)+f"\n...\n\n🏆 **{nv_w['ten']} THẮNG!**\nCướp **{lt_cuop:,}** 💎"
    await ctx.send(embed=embed_mau("⚔️ Kết Quả PvP",result,0xFF8800))

# ══════════════════════════════════════════════════════════════
#  LỆNH: TRANG BỊ
# ══════════════════════════════════════════════════════════════
@bot.command(name="mac", aliases=["equip"])
async def mac_trang_bi(ctx, *, ten_tb: str):
    """!mac <tên trang bị> — Mặc trang bị từ túi đồ"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    async with db_pool.acquire() as c:
        item = await c.fetchrow("SELECT * FROM tui_do WHERE user_id=$1 AND vat_pham=$2", ctx.author.id, ten_tb)
    if not item:
        await ctx.send(embed=embed_mau("❌","Không có trang bị này trong túi!",0xFF4444)); return

    # Parse trang bị từ tên (đơn giản: +atk nếu có "Kiếm/Đao/Thương")
    trang_bi_hien = json.loads(nv['trang_bi'] or '{}')
    # Xác định loại từ tên
    loai = "Vũ Khí"
    for lt in LOAI_TRANG_BI:
        for t in TRANG_BI_TEN.get(lt,[]):
            if t.lower() in ten_tb.lower():
                loai = lt; break

    trang_bi_hien[loai] = ten_tb
    await cap_nhat(ctx.author.id, trang_bi=json.dumps(trang_bi_hien, ensure_ascii=False))
    await ctx.send(embed=embed_mau("🎽 Mặc Trang Bị", f"Đã trang bị **{ten_tb}** vào ô **{loai}**!", 0x55FFAA))

@bot.command(name="trangbi", aliases=["tb","gear"])
async def xem_trang_bi(ctx, member: discord.Member = None):
    """!trangbi — Xem trang bị hiện tại"""
    target = member or ctx.author
    nv = await get_nv(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Chưa có nhân vật!",0xFF4444)); return
    tb = json.loads(nv['trang_bi'] or '{}')
    if not tb:
        await ctx.send(embed=embed_mau("👕 Chưa Có Trang Bị","Tìm trang bị bằng `!khampha`!")); return
    lines = [f"**{loai}:** {ten}" for loai,ten in tb.items()]
    await ctx.send(embed=embed_mau(f"🎽 Trang Bị — {nv['ten']}", "\n".join(lines)))

# ══════════════════════════════════════════════════════════════
#  LỆNH: CỬA HÀNG
# ══════════════════════════════════════════════════════════════
@bot.command(name="shop", aliases=["muahang","ch"])
async def shop(ctx, trang: str = None, *, ten: str = None):
    """!shop — Xem shop | !shop dan | !shop trangbi | !shop mua <tên>"""
    nv = await get_nv(ctx.author.id)
    if not nv and trang=="mua":
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    if trang=="dan" or not trang:
        icon_map={"hoi_phuc":"💧","tu_vi":"✨","dot_pha":"🔮","do_kiep":"⚡","buff_atk":"⚔️","buff_def":"🛡️","buff_hp":"💧","buff_all":"⭐"}
        all_lines=[f"{icon_map.get(v['loai'],'💊')} **{k}** {v['rare']} — {v['gia']:,}💎 | Lv.{v.get('cap_yeu',0)}" for k,v in DAN_DUOC.items()]
        chunk = 10
        shop_pages = [
            (f"🏪 Cửa Hàng Đan Dược ({i//chunk+1}/{(len(all_lines)-1)//chunk+1})", "\n".join(all_lines[i:i+chunk]))
            for i in range(0, len(all_lines), chunk)
        ]
        await paginate(ctx, shop_pages)
        return

    if trang=="mua" and ten:
        dan = DAN_DUOC.get(ten)
        if not dan:
            await ctx.send(embed=embed_mau("❌","Vật phẩm không tồn tại!",0xFF4444)); return
        if nv['canh_gioi']<dan.get('cap_yeu',0):
            await ctx.send(embed=embed_mau("❌",f"Cần Lv.{dan['cap_yeu']}!",0xFF4444)); return
        if nv['linh_thach']<dan['gia']:
            await ctx.send(embed=embed_mau("❌",f"Cần **{dan['gia']:,}** 💎",0xFF4444)); return
        await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']-dan['gia'])
        await cap_nhat_tk(ctx.author.id, tong_lt_tieu=dan['gia'])
        async with db_pool.acquire() as c:
            await c.execute("INSERT INTO tui_do(user_id,vat_pham,so_luong) VALUES($1,$2,1) ON CONFLICT(user_id,vat_pham) DO UPDATE SET so_luong=tui_do.so_luong+1",ctx.author.id,ten)
        async with db_pool.acquire() as c:
            await c.execute("INSERT INTO thanh_tich(user_id,ma_tt) VALUES($1,'first_trade') ON CONFLICT DO NOTHING",ctx.author.id)
        await ctx.send(embed=embed_mau("✅ Mua Thành Công!",f"**{ten}** (-{dan['gia']:,} 💎)",0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: TÚI ĐỒ
# ══════════════════════════════════════════════════════════════
@bot.command(name="tuido", aliases=["bag","td"])
async def tui_do(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    async with db_pool.acquire() as c:
        items = await c.fetch("SELECT * FROM tui_do WHERE user_id=$1 ORDER BY vat_pham", ctx.author.id)
    if not items:
        await ctx.send(embed=embed_mau("🎒 Trống","Dùng `!khampha` để tìm vật phẩm!",0x888888)); return
    lines=[]
    for it in items:
        dan = DAN_DUOC.get(it['vat_pham'],{})
        icon = dan.get('rare','📦')
        lines.append(f"{icon} **{it['vat_pham']}** x{it['so_luong']}")
    chunk = 15
    td_pages = [
        (f"🎒 Túi Đồ — {nv['ten']} ({i//chunk+1}/{(len(lines)-1)//chunk+1})", "\n".join(lines[i:i+chunk]))
        for i in range(0, len(lines), chunk)
    ]
    await paginate(ctx, td_pages)

# ══════════════════════════════════════════════════════════════
#  LỆNH: HÀNH TRANG (túi đồ chi tiết đẹp như ảnh)
# ══════════════════════════════════════════════════════════════
@bot.command(name="hanhtrang", aliases=["ht","inventory","inv"])
async def hanh_trang(ctx, member: discord.Member = None):
    """!hanhtrang — Xem hành trang chi tiết theo từng loại"""
    target = member or ctx.author
    nv = await get_nv(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Chưa có nhân vật!",0xFF4444)); return

    async with db_pool.acquire() as c:
        items = await c.fetch("SELECT vat_pham, so_luong FROM tui_do WHERE user_id=$1 ORDER BY vat_pham", target.id)

    if not items:
        await ctx.send(embed=embed_mau(f"🎒 Hành Trang — {nv['ten']}","Túi đồ trống! Dùng `!khampha` để tìm vật phẩm.",0x888888)); return

    # Phân loại vật phẩm
    trang_bi_lines   = []
    vat_lieu_lines   = []
    su_dung_lines    = []
    khac_lines       = []

    # Icon phân loại
    LOAI_ICON = {
        "hoi_phuc": "💊", "tu_vi": "✨", "dot_pha": "🔮", "do_kiep": "⚡",
        "buff_atk": "⚔️", "buff_def": "🛡️", "buff_hp": "💧", "buff_all": "⭐",
    }

    for it in items:
        ten = it['vat_pham']
        sl  = it['so_luong']
        khoa = "🔒" if sl <= 0 else ""

        # Trang bị (có phẩm chất icon)
        is_tb = any(pc in ten for pc in PHAM_CHAT)
        if is_tb:
            trang_bi_lines.append(f"⚔️ **{ten}** {'(khóa)' if khoa else '(không khóa)'} — Số lượng: {sl}")
            continue

        # Đan dược / vật phẩm sử dụng
        dan = DAN_DUOC.get(ten)
        if dan:
            icon = LOAI_ICON.get(dan['loai'], '💊')
            su_dung_lines.append(f"{icon} **{ten}** {'(khóa)' if khoa else '(không khóa)'} — Số lượng: {sl}")
            continue

        # Cây linh thảo → nguyên liệu
        if ten in CAY_LINH:
            vat_lieu_lines.append(f"🌿 **{ten}** {'(khóa)' if khoa else '(không khóa)'} — Số lượng: {sl}")
            continue

        # Còn lại
        khac_lines.append(f"📦 **{ten}** — Số lượng: {sl}")

    total = len(items)
    pages = []

    def build_section(title, lines, page_size=12):
        result = []
        for i in range(0, max(len(lines), 1), page_size):
            chunk = lines[i:i+page_size]
            result.append((title, "\n".join(chunk) if chunk else "_Trống_"))
        return result

    # Trang tổng quan
    overview = (
        f"💎 **Linh Thạch:** {nv['linh_thach']:,}\n"
        f"🔑 **KNB:** {nv.get('ma_khi', 0):,}\n\n"
        f"⚔️ **Trang Bị:** {len(trang_bi_lines)} vật phẩm\n"
        f"🌿 **Vật Liệu:** {len(vat_lieu_lines)} vật phẩm\n"
        f"💊 **Vật Phẩm Sử Dụng:** {len(su_dung_lines)} vật phẩm\n"
        f"📦 **Khác:** {len(khac_lines)} vật phẩm\n\n"
        f"📊 Tổng vật phẩm (trong túi): **{total}**"
    )
    pages.append((f"🎒 KHÔNG GIAN TRỮ VẬT GIỚI CỦA {nv['ten'].upper()}", overview))

    if trang_bi_lines:
        pages += build_section("⚔️ TRANG BỊ:", trang_bi_lines)
    if vat_lieu_lines:
        pages += build_section("🌿 VẬT LIỆU:", vat_lieu_lines)
    if su_dung_lines:
        pages += build_section("💊 VẬT PHẨM SỬ DỤNG:", su_dung_lines)
    if khac_lines:
        pages += build_section("📦 KHÁC:", khac_lines)

    await paginate(ctx, pages, color=0x2B2D31)
# ══════════════════════════════════════════════════════════════
#  LỆNH: CÂU CÁ
# ══════════════════════════════════════════════════════════════
@bot.command(name="cau", aliases=["fishing","fish"])
async def cau_ca(ctx, so_luong: int = 1):
    """!cau [số] — Câu cá tiêu mana, nhận tu vi và linh thạch (30s/lần)"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    # Cooldown 30s
    async with db_pool.acquire() as c:
        last_cau = await c.fetchval("SELECT MAX(created_at) FROM lich_su_cau WHERE user_id=$1", ctx.author.id)
    if last_cau:
        cd = cooldown_con(last_cau, 30)
        if cd > 0:
            await ctx.send(embed=embed_mau("⏳",f"Còn **{cd:.0f}s** nữa mới câu được!",0xFFAA00)); return

    so_luong = max(1, min(so_luong, 10))
    mana_hien = nv.get('mana', 100)
    mana_moi_lan = 10
    tong_mana_can = mana_moi_lan * so_luong

    if mana_hien < mana_moi_lan:
        await ctx.send(embed=embed_mau("💙 Hết Mana",f"Cần **{mana_moi_lan}** Mana/lần câu. Hiện có: **{mana_hien}**\nTu luyện để hồi mana!",0xFF4444)); return

    so_luong = min(so_luong, mana_hien // mana_moi_lan)
    can_cau_hien = nv.get('can_cau', 'Đại Đạo Cần')
    can_info = CAN_CAU_DATA.get(can_cau_hien, {"bonus": 1.0})
    bonus_mul = can_info["bonus"]

    # Xây pool câu cá với bonus cần
    pool_goc = []
    for ca in CAU_CA_POOL:
        ty_le_thuc = max(1, int(ca["ty_le"] * (bonus_mul if "Quý" in ca["loai"] or "Thần" in ca["loai"] else 1.0)))
        pool_goc.extend([ca] * ty_le_thuc)

    ket_qua = []
    tong_tv = 0; tong_lt = 0
    async with db_pool.acquire() as c:
        for _ in range(so_luong):
            ca = random.choice(pool_goc)
            tv = int(ca["tu_vi"] * random.uniform(0.8, 1.3))
            lt = int(ca["lt"] * random.uniform(0.8, 1.3))
            tong_tv += tv; tong_lt += lt
            ket_qua.append(f"{ca['loai']} **{ca['ten']}** — +{tv:,} Tu Vi, +{lt:,} 💎")
            await c.execute("INSERT INTO lich_su_cau(user_id,ten_ca,loai,gia_tri) VALUES($1,$2,$3,$4)",
                            ctx.author.id, ca['ten'], ca['loai'], lt)

    new_mana = max(0, mana_hien - mana_moi_lan * so_luong)
    await cap_nhat(ctx.author.id,
        tu_vi=nv['tu_vi'] + tong_tv,
        linh_thach=nv['linh_thach'] + tong_lt,
        mana=new_mana
    )
    await them_nhat_ky(ctx.author.id, "cau_ca", f"Câu {so_luong} lần, +{tong_tv:,} Tu Vi, +{tong_lt:,} 💎")

    await ctx.send(embed=discord.Embed(
        title=f"🎣 Câu Cá — {nv['ten']}",
        description="\n".join(ket_qua) + f"\n\n**Tổng:** +{tong_tv:,} Tu Vi | +{tong_lt:,} 💎\n💙 Mana: {new_mana}/{nv.get('mana_max',100)}",
        color=0x55AAFF
    ).set_footer(text="⚡ Tu Tiên Bot V4"))

@bot.command(name="lichsucau", aliases=["lsc"])
async def lich_su_cau_cmd(ctx):
    """!lichsucau — Xem 10 lần câu gần nhất"""
    async with db_pool.acquire() as c:
        rows = await c.fetch("SELECT ten_ca,loai,gia_tri,created_at FROM lich_su_cau WHERE user_id=$1 ORDER BY created_at DESC LIMIT 10", ctx.author.id)
    if not rows:
        await ctx.send(embed=embed_mau("🎣","Chưa câu lần nào!")); return
    lines = [f"{r['loai']} **{r['ten_ca']}** — {r['gia_tri']:,}💎 | `{r['created_at'].strftime('%d/%m %H:%M')}`" for r in rows]
    nv = await get_nv(ctx.author.id)
    await ctx.send(embed=embed_mau(f"🎣 Lịch Sử Câu — {nv['ten'] if nv else '?'}", "\n".join(lines)))

# ══════════════════════════════════════════════════════════════
#  LỆNH: PET
# ══════════════════════════════════════════════════════════════
@bot.command(name="pet")
async def pet_cmd(ctx, hanh_dong: str = None, *, ten_pet: str = None):
    """!pet — Xem | !pet mua <tên> — Mua pet | !pet info — Xem pet hiện tại"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    if not hanh_dong or hanh_dong == "list":
        lines = []
        for k,v in PET_DATA.items():
            lock = "🔒" if nv['canh_gioi'] < v['cap_yeu'] else "✅"
            lines.append(f"{lock} {v['icon']} **{k}** — +{v['bonus_exp']}% EXP | 💎{v['phi']:,} | Cần Lv.{v['cap_yeu']}")
        await ctx.send(embed=embed_mau("🐾 Danh Sách Pet", "\n".join(lines))); return

    if hanh_dong == "info":
        pet_ten = nv.get('pet','')
        if not pet_ten:
            await ctx.send(embed=embed_mau("🐾","Chưa có pet! Dùng `!pet mua <tên>`")); return
        pi = PET_DATA.get(pet_ten, {})
        await ctx.send(embed=embed_mau(f"🐾 Pet — {pi.get('icon','')} {pet_ten}",
            f"📈 Bonus EXP: +{pi.get('bonus_exp',0)}%\n⚡ Bonus Chiến Lực: +{pi.get('bonus_lc',0):,}")); return

    if hanh_dong == "mua" and ten_pet:
        pi = PET_DATA.get(ten_pet)
        if not pi:
            await ctx.send(embed=embed_mau("❌","Pet không tồn tại!",0xFF4444)); return
        if nv['canh_gioi'] < pi['cap_yeu']:
            await ctx.send(embed=embed_mau("❌",f"Cần Lv.{pi['cap_yeu']}!",0xFF4444)); return
        if nv['linh_thach'] < pi['phi']:
            await ctx.send(embed=embed_mau("❌",f"Cần **{pi['phi']:,}** 💎",0xFF4444)); return
        await cap_nhat(ctx.author.id, pet=ten_pet, linh_thach=nv['linh_thach']-pi['phi'])
        await ctx.send(embed=embed_mau(f"🐾 Đã Nhận Pet!", f"{pi['icon']} **{ten_pet}** đã theo bạn!\n+{pi['bonus_exp']}% EXP mỗi lần tu luyện.", 0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: TÔNG MÔN NÂNG CẤP
# ══════════════════════════════════════════════════════════════
@bot.command(name="lapmon")
async def lap_mon(ctx, *, ten_mon: str = None):
    """!lapmon <tên> — Lập tông môn (Lv.3+, 1000💎)"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if not ten_mon:
        await ctx.send(embed=embed_mau("❌","Dùng: `!lapmon <tên tông môn>`",0xFF4444)); return
    if nv['canh_gioi'] < 3:
        await ctx.send(embed=embed_mau("❌","Cần đạt **Kim Đan** mới lập tông môn!",0xFF4444)); return
    if nv['linh_thach'] < 1000:
        await ctx.send(embed=embed_mau("❌","Cần **1,000** 💎!",0xFF4444)); return
    async with db_pool.acquire() as c:
        ex = await c.fetchrow("SELECT ten FROM tong_mon WHERE ten=$1", ten_mon)
        if ex:
            await ctx.send(embed=embed_mau("❌","Tông môn này đã tồn tại!",0xFF4444)); return
        await c.execute("INSERT INTO tong_mon(ten,chu_mon,thanh_vien,cap_do,exp_mon) VALUES($1,$2,$3,1,0)",
                        ten_mon, ctx.author.id, str(ctx.author.id))
    await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']-1000, tong_mon=ten_mon)
    await ctx.send(embed=embed_mau("🏯 Lập Tông Môn!", f"**{ten_mon}** — Tiểu Phái (Cấp 1)\n👑 Chưởng Môn: **{nv['ten']}**\nDùng `!nangcapmon` khi đủ điều kiện!", 0xFFD700))

@bot.command(name="nangcapmon", aliases=["ncm"])
async def nang_cap_mon(ctx):
    """!nangcapmon — Nâng cấp tông môn"""
    nv = await get_nv(ctx.author.id)
    if not nv or not nv['tong_mon']:
        await ctx.send(embed=embed_mau("❌","Bạn chưa có tông môn!",0xFF4444)); return
    async with db_pool.acquire() as c:
        mon = await c.fetchrow("SELECT * FROM tong_mon WHERE ten=$1", nv['tong_mon'])
    if not mon or mon['chu_mon'] != ctx.author.id:
        await ctx.send(embed=embed_mau("❌","Chỉ chưởng môn mới nâng cấp được!",0xFF4444)); return

    cap_hien = mon['cap_do']
    if cap_hien >= 6:
        await ctx.send(embed=embed_mau("🏯","Tông môn đã đạt cấp tối đa (Thánh Địa)!",0xFFD700)); return

    cap_moi_info = TONG_MON_CAP.get(cap_hien + 1, {})
    exp_can_nc = cap_moi_info.get("exp_can", 0)
    phi_nc = exp_can_nc // 10  # phí linh thạch = 1/10 EXP cần

    tv_mon = [r for r in mon['thanh_vien'].split(',') if r] if mon['thanh_vien'] else []

    if len(tv_mon) < TONG_MON_CAP[cap_hien]["max_tv"] // 2:
        await ctx.send(embed=embed_mau("❌",f"Cần ít nhất **{TONG_MON_CAP[cap_hien]['max_tv']//2}** thành viên!",0xFF4444)); return
    if nv['linh_thach'] < phi_nc:
        await ctx.send(embed=embed_mau("❌",f"Cần **{phi_nc:,}** 💎 để nâng cấp!",0xFF4444)); return

    async with db_pool.acquire() as c:
        await c.execute("UPDATE tong_mon SET cap_do=cap_do+1 WHERE ten=$1", nv['tong_mon'])
    await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']-phi_nc)
    await ctx.send(embed=embed_mau("🏯 Nâng Cấp Tông Môn!", f"**{nv['tong_mon']}** → **{cap_moi_info['ten']}** (Cấp {cap_hien+1})\n✨ Thành viên EXP Bonus: +{cap_moi_info['bonus_exp']}%", 0xFFD700))

@bot.command(name="thamgia")
async def tham_gia(ctx, *, ten_mon: str):
    """!thamgia <tên> — Gia nhập tông môn"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if nv['tong_mon']:
        await ctx.send(embed=embed_mau("❌",f"Đã ở tông môn **{nv['tong_mon']}**!",0xFF4444)); return
    async with db_pool.acquire() as c:
        mon = await c.fetchrow("SELECT * FROM tong_mon WHERE ten=$1", ten_mon)
    if not mon:
        await ctx.send(embed=embed_mau("❌","Tông môn không tồn tại!",0xFF4444)); return

    tv_list = [r for r in mon['thanh_vien'].split(',') if r] if mon['thanh_vien'] else []
    max_tv = TONG_MON_CAP.get(mon['cap_do'],{}).get("max_tv", 10)
    if len(tv_list) >= max_tv:
        await ctx.send(embed=embed_mau("❌",f"Tông môn đã đầy! (Tối đa {max_tv} người)",0xFF4444)); return

    tv_list.append(str(ctx.author.id))
    async with db_pool.acquire() as c:
        await c.execute("UPDATE tong_mon SET thanh_vien=$2 WHERE ten=$1", ten_mon, ",".join(tv_list))
    await cap_nhat(ctx.author.id, tong_mon=ten_mon)
    cap_info = TONG_MON_CAP.get(mon['cap_do'],{})
    await ctx.send(embed=embed_mau("🏯 Gia Nhập Thành Công!",
        f"**{nv['ten']}** đã gia nhập **{ten_mon}** ({cap_info.get('ten','')} Cấp {mon['cap_do']})\n"
        f"✨ Bonus EXP: +{cap_info.get('bonus_exp',0)}%", 0x55FFAA))

# ══════════════════════════════════════════════════════════════
#  LỆNH: DÙNG NHANH ĐAN (,an <tên> <số>)
#  Giới hạn 5 lần/ngày, giảm hiệu quả sau mốc
# ══════════════════════════════════════════════════════════════
@bot.command(name="an", aliases=["uong"])
async def an_dan(ctx, *, args: str = None):
    """!an <tên đan> [số] — Dùng đan nhanh, giảm hiệu quả sau 5 lần/ngày"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if not args:
        await ctx.send(embed=embed_mau("❌","Dùng: `!an <tên đan> [số lượng]`",0xFF4444)); return

    # Parse tên và số lượng
    parts = args.rsplit(' ', 1)
    ten_dan = parts[0].strip()
    try:
        so_luong = int(parts[1]) if len(parts) > 1 else 1
    except ValueError:
        ten_dan = args.strip()
        so_luong = 1
    so_luong = max(1, min(so_luong, 99))

    dan = DAN_DUOC.get(ten_dan)
    if not dan:
        await ctx.send(embed=embed_mau("❌",f"Đan dược **{ten_dan}** không tồn tại!\nDùng `!shop` để xem danh sách.",0xFF4444)); return

    # Kiểm tra túi đồ
    async with db_pool.acquire() as c:
        item = await c.fetchrow("SELECT so_luong FROM tui_do WHERE user_id=$1 AND vat_pham=$2", ctx.author.id, ten_dan)
    if not item or item['so_luong'] < 1:
        await ctx.send(embed=embed_mau("❌",f"Không có **{ten_dan}** trong túi!",0xFF4444)); return

    so_luong = min(so_luong, item['so_luong'])

    # Kiểm tra giới hạn ngày (5 lần/ngày, giảm hiệu quả)
    async with db_pool.acquire() as c:
        dan_row = await c.fetchrow("SELECT so_dung FROM dan_ngay WHERE user_id=$1 AND ngay=CURRENT_DATE", ctx.author.id)
    so_da_dung = dan_row['so_dung'] if dan_row else 0

    # Hệ số giảm hiệu quả theo số lần dùng trong ngày
    hieu_qua_map = {0:1.0, 1:1.0, 2:1.0, 3:1.0, 4:1.0, 5:0.8, 6:0.5, 7:0.3, 8:0.1}
    hieu_qua = hieu_qua_map.get(so_da_dung, 0.05)

    vuot_moc_msg = ""
    if so_da_dung >= 5:
        vuot_moc_msg = f"\n⚠️ Đã vượt mốc 5 lần/ngày, hiệu quả còn **{int(hieu_qua*100)}%**: 80% → 50% → 30% → 10% → 5%"

    # Tính hiệu ứng
    updates = {}
    hieu_ung_lines = []
    total_dung = min(so_luong, 99)

    for _ in range(total_dung):
        if dan['loai'] == 'tu_vi':
            exp_raw = dan.get('exp', 0)
            exp_thuc = int(exp_raw * hieu_qua)
            updates['exp'] = updates.get('exp', nv['exp']) + exp_thuc
            hieu_ung_lines.append(f"✨ Tu Vi: +{exp_thuc:,} (gốc {exp_raw:,})")

        elif dan['loai'] == 'hoi_phuc':
            hp_raw = dan.get('hp', 0)
            hp_thuc = int(hp_raw * hieu_qua)
            new_ll = min(updates.get('linh_luc', nv['linh_luc']) + hp_thuc, nv['linh_luc_max'])
            updates['linh_luc'] = new_ll
            hieu_ung_lines.append(f"💧 HP: +{hp_thuc:,}")

        elif dan['loai'] == 'dot_pha':
            hieu_ung_lines.append(f"🔮 Tăng **{dan.get('ti_le',0)}%** tỉ lệ ĐP (x{hieu_qua:.1f})")

        elif dan['loai'] == 'buff_all':
            v = int(dan.get('all',0) * hieu_qua)
            updates['tan_cong'] = updates.get('tan_cong', nv['tan_cong']) + v
            updates['phong_thu'] = updates.get('phong_thu', nv['phong_thu']) + v
            hieu_ung_lines.append(f"⭐ Tất cả chỉ số: +{v}")

        elif dan['loai'] == 'buff_atk':
            v = int(dan.get('atk',0) * hieu_qua)
            updates['tan_cong'] = updates.get('tan_cong', nv['tan_cong']) + v
            hieu_ung_lines.append(f"⚔️ Tấn Công: +{v}")

        elif dan['loai'] == 'buff_def':
            v = int(dan.get('def',0) * hieu_qua)
            updates['phong_thu'] = updates.get('phong_thu', nv['phong_thu']) + v
            hieu_ung_lines.append(f"🛡️ Phòng Thủ: +{v}")

        elif dan['loai'] == 'buff_hp':
            v = int(dan.get('hp_max',0) * hieu_qua)
            updates['linh_luc_max'] = updates.get('linh_luc_max', nv['linh_luc_max']) + v
            hieu_ung_lines.append(f"💧 HP Max: +{v:,}")

        so_da_dung += 1
        hieu_qua = hieu_qua_map.get(so_da_dung, 0.05)

    # Xử lý lên cảnh giới nếu có EXP
    if 'exp' in updates:
        new_cg = nv['canh_gioi']
        new_exp = updates['exp']
        dp_msg = ""
        while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI) - 1:
            new_exp -= exp_can(new_cg); new_cg += 1
            dp_msg = f"\n🎉 **ĐỘT PHÁ → {CANH_GIOI[new_cg]}**!"
        updates['exp'] = new_exp
        updates['canh_gioi'] = new_cg
        if dp_msg: hieu_ung_lines.append(dp_msg)
        await cap_nhat_tk(ctx.author.id, tong_exp=updates.get('exp',0) - nv['exp'])

    if updates: await cap_nhat(ctx.author.id, **updates)

    # Trừ túi đồ
    async with db_pool.acquire() as c:
        if item['so_luong'] <= total_dung:
            await c.execute("DELETE FROM tui_do WHERE user_id=$1 AND vat_pham=$2", ctx.author.id, ten_dan)
        else:
            await c.execute("UPDATE tui_do SET so_luong=so_luong-$3 WHERE user_id=$1 AND vat_pham=$2", ctx.author.id, ten_dan, total_dung)
        # Cập nhật đan ngày
        await c.execute("""
            INSERT INTO dan_ngay(user_id,ngay,so_dung) VALUES($1,CURRENT_DATE,$2)
            ON CONFLICT(user_id,ngay) DO UPDATE SET so_dung=dan_ngay.so_dung+$2
        """, ctx.author.id, total_dung)

    lc_moi = tinh_luc_chien(await get_nv(ctx.author.id))

    await ctx.send(embed=discord.Embed(
        title=f"🟢 Đã dùng x{total_dung} {ten_dan}",
        description=(
            f"**Hiệu ứng:**\n" +
            "\n".join(hieu_ung_lines[:5]) +
            f"\n⚡ Chiến Lực: +{lc_moi - tinh_luc_chien(nv):,}" +
            vuot_moc_msg
        ),
        color=0x00AA44
    ).set_footer(text="⚡ Tu Tiên Bot V4"))

# ══════════════════════════════════════════════════════════════
#  LỆNH: MUA CẦN CÂU
# ══════════════════════════════════════════════════════════════
@bot.command(name="muacancau", aliases=["mcc"])
async def mua_can_cau(ctx, *, ten_can: str = None):
    """!muacancau — Xem | !muacancau <tên> — Mua cần câu"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    if not ten_can:
        lines = [f"🎣 **{k}** — {v['mo_ta']} | 💎{v.get('phi',0):,}" for k,v in CAN_CAU_DATA.items()]
        await ctx.send(embed=embed_mau("🎣 Cần Câu","\n".join(lines))); return
    can = CAN_CAU_DATA.get(ten_can)
    if not can or 'phi' not in can:
        await ctx.send(embed=embed_mau("❌","Cần câu không tồn tại hoặc không thể mua!",0xFF4444)); return
    if nv['linh_thach'] < can['phi']:
        await ctx.send(embed=embed_mau("❌",f"Cần **{can['phi']:,}** 💎",0xFF4444)); return
    if nv['canh_gioi'] < can.get('cap_yeu',0):
        await ctx.send(embed=embed_mau("❌",f"Cần Lv.{can['cap_yeu']}!",0xFF4444)); return
    await cap_nhat(ctx.author.id, can_cau=ten_can, linh_thach=nv['linh_thach']-can['phi'])
    await ctx.send(embed=embed_mau("🎣 Đã Mua Cần Câu!", f"**{ten_can}** — {can['mo_ta']}", 0x55FFAA))

@bot.command(name="bxh", aliases=["rank"])
async def bang_xep_hang(ctx):
    async with db_pool.acquire() as c:
        rows = await c.fetch("SELECT ten,toc,canh_gioi,tu_vi,tan_cong,phong_thu,linh_luc_max FROM nhanvat ORDER BY canh_gioi DESC,tu_vi DESC LIMIT 10")
    medals=["🥇","🥈","🥉"]+["🏅"]*7
    lines=[]
    for i,r in enumerate(rows):
        lc = tinh_luc_chien(r)
        toc_icon = TOC.get(r['toc'],{}).get('icon','👤')
        lines.append(f"{medals[i]} {toc_icon}**{r['ten']}** — {CANH_GIOI[r['canh_gioi']]} | ⚡{lc:,}")
    await ctx.send(embed=embed_mau("🏆 Bảng Xếp Hạng","\n".join(lines) or "Chưa có ai!"))

@bot.command(name="thongke", aliases=["stats"])
async def thong_ke(ctx, member: discord.Member = None):
    target = member or ctx.author
    nv = await get_nv(target.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Chưa có nhân vật!",0xFF4444)); return
    async with db_pool.acquire() as c:
        tk = await c.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1", target.id)
    if not tk:
        await ctx.send(embed=embed_mau("📊","Chưa có dữ liệu!")); return
    pvp_t=tk['tong_pvp_thang']+tk['tong_pvp_thua']
    wr=f"{int(tk['tong_pvp_thang']/pvp_t*100)}%" if pvp_t>0 else "N/A"
    async with db_pool.acquire() as c:
        thap = await c.fetchrow("SELECT tang_hien FROM thap_thu_luyen WHERE user_id=$1",target.id)
    await ctx.send(embed=embed_mau(f"📊 Thống Kê — {nv['ten']}",f"""
🧘 Tu Luyện: {tk['tong_tulyen']:,} lần | 🔥 Đột Phá: {tk['dot_pha_count']} lần
✨ Tổng EXP: {tk['tong_exp']:,}
👹 Boss đã giết: {tk['tong_boss_giet']:,}
⚔️ PvP: {tk['tong_pvp_thang']}T/{tk['tong_pvp_thua']}B ({wr})
💎 LT tích lũy: {tk['tong_lt_kiem']:,} | Đã tiêu: {tk['tong_lt_tieu']:,}
💀 Số lần chết: {nv['so_chet']}
🏯 Tầng tháp cao nhất: {thap['tang_hien']-1 if thap else 0}
    """))

@bot.command(name="nhatky",aliases=["nk","log"])
async def nhat_ky_cmd(ctx):
    nv=await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return
    async with db_pool.acquire() as c:
        rows=await c.fetch("SELECT loai,noi_dung,created_at FROM nhat_ky WHERE user_id=$1 ORDER BY created_at DESC LIMIT 15",ctx.author.id)
    if not rows:
        await ctx.send(embed=embed_mau("📖 Trống","Hãy bắt đầu tu luyện!")); return
    icons={"tulyen":"🧘","khampha":"🗺️","boss":"👹","pvp":"⚔️","bequan":"🧘","xuatquan":"🌅","system":"📌"}
    lines=[f"{icons.get(r['loai'],'📝')} `{r['created_at'].strftime('%d/%m %H:%M')}` {r['noi_dung']}" for r in rows]
    await ctx.send(embed=embed_mau(f"📖 Nhật Ký — {nv['ten']}","\n".join(lines)))

@bot.command(name="lichsupvp",aliases=["lspvp"])
async def ls_pvp(ctx, member: discord.Member = None):
    target=member or ctx.author
    async with db_pool.acquire() as c:
        rows=await c.fetch("SELECT * FROM lich_su_pvp WHERE nguoi_thang=$1 OR nguoi_thua=$1 ORDER BY created_at DESC LIMIT 10",target.id)
    if not rows:
        await ctx.send(embed=embed_mau("⚔️","Chưa có trận đấu!",0x888888)); return
    lines=[]
    for r in rows:
        w=r['nguoi_thang']==target.id
        lines.append(f"{'🏆 THẮNG' if w else '💀 THUA'} vs **{r['ten_thua'] if w else r['ten_thang']}** | {'+' if w else '-'}{r['lt_cuop']:,}💎 | `{r['created_at'].strftime('%d/%m %H:%M')}`")
    nv=await get_nv(target.id)
    await ctx.send(embed=embed_mau(f"⚔️ Lịch Sử PvP — {nv['ten'] if nv else '?'}","\n".join(lines)))

@bot.command(name="thanhtich",aliases=["tt2","achievement"])
async def thanh_tich_cmd(ctx, member: discord.Member = None):
    target=member or ctx.author
    async with db_pool.acquire() as c:
        da_co={r['ma_tt']:r['dat_duoc_at'] for r in await c.fetch("SELECT ma_tt,dat_duoc_at FROM thanh_tich WHERE user_id=$1",target.id)}
    lines=[]
    for ma,info in THANH_TICH.items():
        if ma in da_co:
            tg=da_co[ma].strftime("%d/%m/%Y")
            lines.append(f"✅ {info['ten']} — _{info['mo_ta']}_ `({tg})`")
        else:
            lines.append(f"🔒 ~~{info['ten']}~~ — _{info['mo_ta']}_")
    nv=await get_nv(target.id); ten=nv['ten'] if nv else target.display_name
    chunk = 15
    tt_pages = [
        (f"🏅 Thành Tích — {ten} ({len(da_co)}/{len(THANH_TICH)}) — Trang {i//chunk+1}/{(len(lines)-1)//chunk+1}",
         "\n".join(lines[i:i+chunk]))
        for i in range(0, len(lines), chunk)
    ]
    await paginate(ctx, tt_pages)

# ══════════════════════════════════════════════════════════════
#  LỆNH: HELP (pagination 1 tin nhắn, dùng reaction ◀ ▶)
# ══════════════════════════════════════════════════════════════
HELP_PAGES = [
    ("📖 Hướng Dẫn (1/4) — Nhân Vật & Tu Luyện", """
**👤 Nhân Vật**
`!taonv <tên>` — Tạo nhân vật
`!chontoc` — Chọn tộc *(1 lần, không đổi!)*
`!tt [@người]` — Xem thông tin nhân vật
`!thongke` — Thống kê tổng hợp
`!bxh` — Bảng xếp hạng lực chiến

**⚡ Tu Luyện**
`!tulyen` — Tu luyện *(60s cooldown)*
`!khampha` — Khám phá tìm đồ *(120s)*
`!bequan <giờ>` — Bế quan 1-72h *(EXP x3)*
`!xuatquan` — Xuất quan nhận thưởng

**🌿 Trồng Cây**
`!trongcay list` — Danh sách cây
`!trongcay trong <tên>` — Trồng cây
`!trongcay thuhoach` — Thu hoạch
`!trongcay vuon` — Xem vườn
"""),
    ("📖 Hướng Dẫn (2/4) — Chiến Đấu & Đạo", """
**⚔️ Chiến Đấu**
`!boss` — Xem boss bản đồ hiện tại
`!boss <số>` — Đánh boss
`!bossthegioi` — Xem boss thế giới
`!bossthegioi tan` — Tấn công boss thế giới
`!pvp @người` — Thách đấu PvP
`!thap` — Tháp thử luyện *(120s)*

**☯️ Đạo & Công Pháp**
`!chondao` — Xem / chọn đạo chính
`!daophu` — Học đạo phụ
`!congphap list` — Xem danh sách công pháp
`!congphap hoc <tên>` — Học công pháp
`!congphap xem` — Công pháp đã học
"""),
    ("📖 Hướng Dẫn (3/4) — Vật Phẩm & Trang Bị", """
**🎒 Túi Đồ & Trang Bị**
`!tuido` — Xem túi đồ
`!trangbi` — Xem trang bị đang mặc
`!mac <tên trang bị>` — Mặc trang bị
`!dung <tên>` — Dùng đan dược

**🏪 Cửa Hàng**
`!shop` — Xem đan dược
`!shop mua <tên>` — Mua vật phẩm

**📊 Lịch Sử**
`!nhatky` — Nhật ký hoạt động
`!lichsupvp` — Lịch sử PvP
`!thanhtich` — Xem thành tích
"""),
    ("📖 Hướng Dẫn (4/4) — Xã Hội & Thông Tin", """
**💍 Kết Duyên**
`!ketduyen @người` — Cầu hôn đạo lữ *(10,000💎)*

**🏯 Tông Môn**
`!lapmon <tên>` — Lập tông môn *(Lv.3+, 1000💎)*
`!thamgia <tên>` — Gia nhập tông môn

**🗺️ Bản Đồ**
🟢 Nhân Giới (Lv.0-5) → 🔵 Linh Giới (Lv.6-9)
🟣 Tiên Giới (Lv.10-14) → 🟡 Thánh Giới (Lv.15-24)
🔴 Vũ Trụ Cấp (Lv.25-36) → 🌀 Hỗn Độn (Lv.37-49)
⚫ Thái Cổ (Lv.50-64) → 🌟 Thần Thoại (Lv.65-79)
☀️ Vô Thượng (Lv.80-100)
*Phi thăng tự động khi đủ cảnh giới!*

**🐉 Tộc:** Long, Thần, Nhân, Tiên, Ma, Thú
**36 Cảnh Giới** từ Phàm Nhân → Vô Thượng Đại Đạo
"""),
]

@bot.command(name="help", aliases=["hd","huongdan"])
async def help_cmd(ctx):
    await paginate(ctx, HELP_PAGES)

# ══════════════════════════════════════════════════════════════
#  KHỞI ĐỘNG
# ══════════════════════════════════════════════════════════════
@bot.event
async def on_ready():
    print(f"🔌 Bot Tu Tiên V3 đã kết nối Discord: {bot.user}")
    await bot.change_presence(activity=discord.Game(name="⏳ Đang kết nối DB..."))
    
    # Retry kết nối DB tối đa 5 lần
    for attempt in range(1, 6):
        try:
            print(f"🗄️ Đang kết nối database... (lần {attempt}/5)")
            await init_db()
            print(f"✅ Bot Tu Tiên V3 MEGA online: {bot.user}")
            await bot.change_presence(activity=discord.Game(name="Tu Tiên V3 | !help | Lv.100"))
            if not auto_boss_spawn.is_running():
                auto_boss_spawn.start()
                print("✅ Task auto_boss_spawn đã khởi động!")
            return
        except Exception as e:
            print(f"❌ Lỗi kết nối DB (lần {attempt}/5): {type(e).__name__}: {e}")
            if attempt < 5:
                print(f"⏳ Thử lại sau 10 giây...")
                await asyncio.sleep(10)
            else:
                print("💀 Không thể kết nối database sau 5 lần thử!")
                print(f"   DB_URL = {DB_URL[:40] if DB_URL else 'KHÔNG CÓ'}...")
                await bot.change_presence(activity=discord.Game(name="❌ Lỗi DB - Kiểm tra DATABASE_URL"))

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(embed=embed_mau("❌ Thiếu Tham Số","Dùng `!help` để xem hướng dẫn!",0xFF4444))
    elif isinstance(error, commands.CommandNotFound):
        pass
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send(embed=embed_mau("❌","Không tìm thấy người dùng!",0xFF4444))
    elif isinstance(error, commands.CommandInvokeError):
        original = error.original
        print(f"❌ Lỗi lệnh [{ctx.command}]: {type(original).__name__}: {original}")
        await ctx.send(embed=embed_mau("❌ Lỗi Hệ Thống", f"Có lỗi xảy ra khi thực thi lệnh!\n```{type(original).__name__}: {str(original)[:200]}```", 0xFF4444))
    else:
        print(f"Lỗi: {type(error).__name__}: {error}")
        await ctx.send(embed=embed_mau("❌ Lỗi",f"```{type(error).__name__}: {str(error)[:200]}```",0xFF4444))

bot.run(TOKEN)