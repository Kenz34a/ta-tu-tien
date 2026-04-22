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
    "Sáng Thế Chủ","Toàn Năng","Toàn Tri","Siêu Việt","Vô Cực","Vô Thượng Đại Đạo"
]

BAN_DO = {
    "nhan_gioi":  {"ten":"🟢 Nhân Giới",  "cap_min":0,  "cap_max":5,  "mo_ta":"Early game — Phàm Nhân → Hóa Thần",     "phi_thuong":6},
    "linh_gioi":  {"ten":"🔵 Linh Giới",  "cap_min":6,  "cap_max":9,  "mo_ta":"Mid game — Luyện Hư → Độ Kiếp",        "phi_thuong":10},
    "tien_gioi":  {"ten":"🟣 Tiên Giới",  "cap_min":10, "cap_max":14, "mo_ta":"Late game — Tiên Nhân → Thánh Nhân",    "phi_thuong":15},
    "thanh_gioi": {"ten":"🟡 Thánh Giới", "cap_min":15, "cap_max":24, "mo_ta":"End game — Thánh Nhân → Thiên Đạo",    "phi_thuong":25},
    "vu_tru":     {"ten":"🔴 Vũ Trụ Cấp", "cap_min":25, "cap_max":36, "mo_ta":"Ultra end — Siêu Thoát → Vô Thượng",   "phi_thuong":None},
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
    base = nv['tan_cong']*50 + nv['phong_thu']*35 + nv['linh_luc_max']*3
    cg_bonus = nv['canh_gioi'] * 50000
    tv_bonus = nv['tu_vi'] // 10
    return base + cg_bonus + tv_bonus

def luc_chien_rank(lc: int) -> str:
    if lc < 100000:     return "⚪ Phàm"
    if lc < 500000:     return "🟢 Tinh Anh"
    if lc < 2000000:    return "🔵 Cường Giả"
    if lc < 10000000:   return "🟣 Tôn Giả"
    if lc < 50000000:   return "🟡 Hoàng Giả"
    if lc < 500000000:  return "🔴 Đế Giả"
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

# Boss thế giới (1 boss/giới, hồi sinh 24h)
BOSS_THE_GIOI = {
    "nhan_gioi":  {"ten":"👑 Ma Đế Thiên Tuyệt",   "hp":5000000,  "sat_thuong":5000,  "phan_thuong":500000,  "exp":500000,  "cap_yeu":3},
    "linh_gioi":  {"ten":"👑 Hư Không Chi Thần",    "hp":20000000, "sat_thuong":15000, "phan_thuong":2000000, "exp":2000000, "cap_yeu":7},
    "tien_gioi":  {"ten":"👑 Thái Cổ Tiên Đế",      "hp":80000000, "sat_thuong":40000, "phan_thuong":8000000, "exp":8000000, "cap_yeu":11},
    "thanh_gioi": {"ten":"👑 Thánh Giới Chi Chủ",   "hp":300000000,"sat_thuong":100000,"phan_thuong":30000000,"exp":30000000,"cap_yeu":16},
    "vu_tru":     {"ten":"👑 Vô Thượng Thiên Đạo",  "hp":999999999,"sat_thuong":300000,"phan_thuong":100000000,"exp":100000000,"cap_yeu":26},
}

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
                gioi TEXT PRIMARY KEY,
                hp_hien BIGINT, last_reset TIMESTAMPTZ DEFAULT NOW(),
                nguoi_giet BIGINT DEFAULT 0
            )
        """)
        # Khởi tạo boss thế giới nếu chưa có
        for gioi, b in BOSS_THE_GIOI.items():
            await c.execute("""
                INSERT INTO boss_the_gioi (gioi, hp_hien) VALUES ($1,$2)
                ON CONFLICT (gioi) DO NOTHING
            """, gioi, b["hp"])
        await c.execute("""
            CREATE TABLE IF NOT EXISTS thap_thu_luyen (
                user_id  BIGINT PRIMARY KEY REFERENCES nhanvat(user_id) ON DELETE CASCADE,
                tang_hien INT DEFAULT 1,
                last_thap TIMESTAMPTZ
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

def exp_can(cg): return int((cg+1) * 500 * (1.15**cg))

def embed_mau(title, desc, color=0xAA55FF):
    e = discord.Embed(title=title, description=desc, color=color)
    e.set_footer(text="⚡ Tu Tiên Bot V3 | Vạn Cổ Trường Tồn")
    return e

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
        dao_lu_row = await c.fetchrow("SELECT user1,user2 FROM ket_duyen WHERE user1=$1 OR user2=$1", target.id)

    cg  = nv['canh_gioi']
    lc  = tinh_luc_chien(nv)
    bar = "█"*int((nv['exp']/exp_can(cg))*10)+"░"*(10-int((nv['exp']/exp_can(cg))*10))
    toc = TOC.get(nv['toc'],{})
    lc_info = LINH_CAN.get(nv['linh_can'],{})
    pvp_total = (tk['tong_pvp_thang']+tk['tong_pvp_thua']) if tk else 0
    wr = f"{int(tk['tong_pvp_thang']/pvp_total*100)}%" if pvp_total>0 else "N/A"
    kl_cap = KIEM_LINH_CAP[min(nv['kiem_linh_cap'], len(KIEM_LINH_CAP)-1)]
    dao_lu_str = "Chưa có"
    if dao_lu_row:
        pid = dao_lu_row['user2'] if dao_lu_row['user1']==target.id else dao_lu_row['user1']
        try:
            pu = await bot.fetch_user(pid); dao_lu_str = pu.display_name
        except: dao_lu_str = "Đạo Lữ"

    e = embed_mau(f"📜 {nv['ten']}", f"""
{toc.get('icon','👤')} **Tộc:** {nv['toc']} | {lc_info.get('icon','⭐')} **Linh Căn:** {nv['linh_can']}
🏔️ **Cảnh Giới:** {CANH_GIOI[cg]} (Lv.{cg}) | 🗺️ {BAN_DO[nv['ban_do']]['ten']}
📊 **EXP:** {nv['exp']:,}/{exp_can(cg):,} [{bar}]
⚡ **Lực Chiến:** {lc:,} {luc_chien_rank(lc)}

💧 **HP:** {nv['linh_luc']:,}/{nv['linh_luc_max']:,}
⚔️ **Tấn Công:** {nv['tan_cong']:,} | 🛡️ **Phòng Thủ:** {nv['phong_thu']:,}
💎 **Linh Thạch:** {nv['linh_thach']:,}
✨ **Tu Vi:** {nv['tu_vi']:,}

⚔️ **Đạo Chính:** {nv['dao_chinh'] or 'Chưa ngộ đạo'}
📿 **Đạo Phụ:** {nv['dao_phu'] or 'Chưa có'}
🗡️ **Kiếm Linh:** {kl_cap}
💍 **Đạo Lữ:** {dao_lu_str}
🏯 **Tông Môn:** {nv['tong_mon'] or 'Vô Môn'}
🏅 **Thành Tích:** {tt_count}/{len(THANH_TICH)}
👹 Boss: {tk['tong_boss_giet'] if tk else 0} | ⚔️ PvP: {tk['tong_pvp_thang'] if tk else 0}T/{tk['tong_pvp_thua'] if tk else 0}B ({wr})
    """)
    await ctx.send(embed=e)

# ══════════════════════════════════════════════════════════════
#  LỆNH: TU LUYỆN
# ══════════════════════════════════════════════════════════════
@bot.command(name="tulyen", aliases=["tl"])
async def tu_luyen(ctx):
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    # Kiểm tra đang bế quan
    if nv['last_bequan'] and nv['bequan_gio'] > 0:
        end = nv['last_bequan'] + timedelta(hours=nv['bequan_gio'])
        if datetime.now(nv['last_bequan'].tzinfo) < end:
            con_lai = (end - datetime.now(nv['last_bequan'].tzinfo)).seconds // 60
            await ctx.send(embed=embed_mau("🧘 Đang Bế Quan",f"Còn **{con_lai}** phút nữa mới xuất quan!",0xFFAA00)); return

    cd = cooldown_con(nv['last_tulyen'], 60)
    if cd > 0:
        await ctx.send(embed=embed_mau("⏳","Còn **{:.0f}s**!".format(cd),0xFFAA00)); return

    lc_info = LINH_CAN.get(nv['linh_can'],{})
    toc_info = TOC.get(nv['toc'],{})
    exp_bonus = lc_info.get("bonus_exp",0) + toc_info.get("bonus_exp",0)
    tuvi_bonus = lc_info.get("bonus_tulyen",0)

    # Passive bonus
    passives = json.loads(nv['cong_phap'] or '[]')
    for p in passives:
        pi = CONG_PHAP_PASSIVE.get(p,{})
        if "bonus_tuvi" in pi: tuvi_bonus += pi["bonus_tuvi"]

    exp_gain = int((random.randint(200,800) + nv['canh_gioi']*100) * (1 + exp_bonus/100))
    tv_gain  = random.randint(80,300) + tuvi_bonus
    ll_hoi   = random.randint(50,200)
    kl_exp   = random.randint(10,30)

    new_exp = nv['exp'] + exp_gain
    new_cg  = nv['canh_gioi']
    dp_msg  = ""
    dp_cnt  = 0

    while new_exp >= exp_can(new_cg) and new_cg < len(CANH_GIOI)-1:
        new_exp -= exp_can(new_cg)
        new_cg += 1
        dp_cnt += 1
        dp_msg = f"\n\n🎉 **ĐỘT PHÁ → {CANH_GIOI[new_cg]}**! 🎉"

    # Kiếm Linh exp
    new_kl_exp = nv['kiem_linh_exp'] + kl_exp
    new_kl_cap = nv['kiem_linh_cap']
    kl_threshold = (new_kl_cap+1) * 200
    if new_kl_exp >= kl_threshold and new_kl_cap < len(KIEM_LINH_CAP)-1:
        new_kl_exp -= kl_threshold; new_kl_cap += 1
        dp_msg += f"\n⚔️ **Kiếm Linh thức tỉnh: {KIEM_LINH_CAP[new_kl_cap]}**!"

    # Phi thăng
    ban_do_hien = nv['ban_do']
    bd_info = BAN_DO[ban_do_hien]
    phi_thuong_msg = ""
    if new_cg > bd_info["cap_max"] and bd_info["phi_thuong"]:
        # Tự động chuyển bản đồ
        for bdk, bdv in BAN_DO.items():
            if bdv["cap_min"] <= new_cg <= bdv["cap_max"]:
                ban_do_hien = bdk
                phi_thuong_msg = f"\n🚀 **PHI THĂNG → {bdv['ten']}**!"
                break

    await cap_nhat(ctx.author.id,
        exp=new_exp, tu_vi=nv['tu_vi']+tv_gain, canh_gioi=new_cg,
        linh_luc=min(nv['linh_luc']+ll_hoi, nv['linh_luc_max']),
        kiem_linh_cap=new_kl_cap, kiem_linh_exp=new_kl_exp,
        ban_do=ban_do_hien, last_tulyen=datetime.utcnow()
    )
    await cap_nhat_tk(ctx.author.id, tong_tulyen=1, tong_exp=exp_gain, dot_pha_count=dp_cnt)
    await them_nhat_ky(ctx.author.id,"tulyen",f"+{exp_gain} EXP, +{tv_gain} Tu Vi → {CANH_GIOI[new_cg]}")

    nv2 = await get_nv(ctx.author.id)
    async with db_pool.acquire() as c: tk=await c.fetchrow("SELECT * FROM thong_ke WHERE user_id=$1",ctx.author.id)
    await kiem_tra_thanh_tich(ctx, ctx.author.id, nv2, tk)

    await ctx.send(embed=embed_mau("🧘 Tu Luyện", f"""
✨ **+{exp_gain} EXP** | 🌀 **+{tv_gain} Tu Vi** | 💧 **+{ll_hoi} HP**
📊 EXP: {new_exp:,}/{exp_can(new_cg):,} | **{CANH_GIOI[new_cg]}**
{dp_msg}{phi_thuong_msg}
    """, 0x55FFAA))

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
        lines = ["**⚔️ Tấn Công:**"]
        for k,v in CONG_PHAP_TAN_CONG.items():
            lines.append(f"  `{k}` — 💥{v['sat_thuong']} | Cần Lv.{v['cap_yeu']} | 💎{v['phi']}")
        lines.append("\n**🛡️ Phòng Thủ:**")
        for k,v in CONG_PHAP_PHONG_THU.items():
            lines.append(f"  `{k}` — 🛡️+{v['phong_thu_bonus']} | Cần Lv.{v['cap_yeu']} | 💎{v['phi']}")
        lines.append("\n**🌀 Đại Thần Thông:**")
        for k,v in DAI_THAN_THONG.items():
            lines.append(f"  `{k}` — 💥{v['sat_thuong']} | Cần Lv.{v['cap_yeu']} | 💎{v['phi']}")
        lines.append("\n**✨ Passive:**")
        for k,v in CONG_PHAP_PASSIVE.items():
            lines.append(f"  `{k}` — {v['mo_ta']} | Cần Lv.{v['cap_yeu']} | 💎{v['phi']}")
        # Chia trang nếu quá dài
        text = "\n".join(lines)
        for i in range(0, len(text), 1900):
            await ctx.send(embed=embed_mau("📚 Danh Sách Công Pháp", text[i:i+1900]))
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
#  LỆNH: BOSS THẾ GIỚI
# ══════════════════════════════════════════════════════════════
@bot.command(name="bossthegioi", aliases=["btg","worldboss"])
async def boss_the_gioi_cmd(ctx, hanh_dong: str = None):
    """!bossthegioi — Xem | !bossthegioi tan — Đánh boss thế giới"""
    nv = await get_nv(ctx.author.id)
    if not nv:
        await ctx.send(embed=embed_mau("❌","Dùng `!taonv <tên>` trước!",0xFF4444)); return

    gioi = nv['ban_do']
    boss_info = BOSS_THE_GIOI[gioi]

    async with db_pool.acquire() as c:
        boss_row = await c.fetchrow("SELECT * FROM boss_the_gioi WHERE gioi=$1", gioi)

    # Reset boss sau 24h
    if boss_row and (datetime.now(boss_row['last_reset'].tzinfo)-boss_row['last_reset']).total_seconds() > 86400:
        async with db_pool.acquire() as c:
            await c.execute("UPDATE boss_the_gioi SET hp_hien=$2, last_reset=NOW(), nguoi_giet=0 WHERE gioi=$1",
                            gioi, boss_info["hp"])
        boss_row = await (db_pool.acquire().__aenter__()).__aenter__()
        async with db_pool.acquire() as c:
            boss_row = await c.fetchrow("SELECT * FROM boss_the_gioi WHERE gioi=$1", gioi)

    hp_hien = boss_row['hp_hien'] if boss_row else boss_info["hp"]

    if not hanh_dong:
        pct = int(hp_hien/boss_info["hp"]*20)
        bar = "🟥"*pct + "⬛"*(20-pct)
        await ctx.send(embed=embed_mau(f"👑 Boss Thế Giới — {BAN_DO[gioi]['ten']}", f"""
**{boss_info['ten']}**
❤️ HP: {hp_hien:,}/{boss_info['hp']:,}
{bar}
Cần Lv.**{boss_info['cap_yeu']}** | 💎{boss_info['phan_thuong']:,} | ✨{boss_info['exp']:,} EXP

Dùng `!bossthegioi tan` để tham chiến!
        """, 0xFF0000)); return

    if hanh_dong == "tan":
        if nv['canh_gioi']<boss_info["cap_yeu"]:
            await ctx.send(embed=embed_mau("❌",f"Cần Lv.{boss_info['cap_yeu']}!",0xFF4444)); return
        if hp_hien <= 0:
            await ctx.send(embed=embed_mau("💀","Boss đã bị hạ! Hồi sinh sau 24h.",0x888888)); return

        cp_list = json.loads(nv['cong_phap'] or '[]')
        atk_bonus = sum(CONG_PHAP_TAN_CONG.get(cp,{}).get('sat_thuong',0) for cp in cp_list)
        atk_bonus += sum(DAI_THAN_THONG.get(cp,{}).get('sat_thuong',0) for cp in cp_list)
        kl_bonus = KIEM_LINH_BONUS[min(nv['kiem_linh_cap'],len(KIEM_LINH_BONUS)-1)]
        base_atk = nv['tan_cong'] + atk_bonus
        dmg = int(random.randint(base_atk*5, base_atk*15) * (1+kl_bonus/100))
        player_dmg = max(1, boss_info["sat_thuong"]//2 - nv['phong_thu'])

        new_hp = max(0, hp_hien - dmg)
        async with db_pool.acquire() as c:
            await c.execute("UPDATE boss_the_gioi SET hp_hien=$2 WHERE gioi=$1", gioi, new_hp)

        reward_msg = ""
        if new_hp <= 0:
            phan_thuong = boss_info["phan_thuong"]
            await cap_nhat(ctx.author.id, linh_thach=nv['linh_thach']+phan_thuong,
                           exp=nv['exp']+boss_info["exp"])
            await cap_nhat_tk(ctx.author.id, tong_boss_giet=1, tong_lt_kiem=phan_thuong)
            reward_msg = f"\n\n👑 **BOSS THẾ GIỚI ĐÃ BỊ HẠ!**\n💎 +{phan_thuong:,} | ✨ +{boss_info['exp']:,} EXP"

        pct2 = int(new_hp/boss_info["hp"]*20)
        bar2 = "🟥"*pct2+"⬛"*(20-pct2)
        await cap_nhat(ctx.author.id, linh_luc=max(1, nv['linh_luc']-player_dmg))
        await ctx.send(embed=embed_mau(f"⚔️ Tham Chiến Boss Thế Giới", f"""
💥 Gây **{dmg:,}** sát thương!
Boss nhận **{player_dmg:,}** sát thương ngược lại.
❤️ Boss HP: {new_hp:,}/{boss_info['hp']:,}
{bar2}
{reward_msg}
        """, 0xFF6600 if new_hp>0 else 0xFFD700))

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
        lines=[f"{icon_map.get(v['loai'],'💊')} **{k}** {v['rare']} — {v['gia']:,}💎 | Cần Lv.{v.get('cap_yeu',0)}" for k,v in DAN_DUOC.items()]
        for i in range(0,len(lines),15):
            await ctx.send(embed=embed_mau("🏪 Cửa Hàng Đan Dược","\n".join(lines[i:i+15])))
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
    for i in range(0,len(lines),20):
        await ctx.send(embed=embed_mau(f"🎒 Túi Đồ — {nv['ten']}","\n".join(lines[i:i+20])))

# ══════════════════════════════════════════════════════════════
#  LỆNH: BXH & STATS
# ══════════════════════════════════════════════════════════════
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
    for i in range(0,len(lines),20):
        await ctx.send(embed=embed_mau(f"🏅 Thành Tích — {ten} ({len(da_co)}/{len(THANH_TICH)})","\n".join(lines[i:i+20])))

# ══════════════════════════════════════════════════════════════
#  LỆNH: HELP
# ══════════════════════════════════════════════════════════════
@bot.command(name="help",aliases=["hd","huongdan"])
async def help_cmd(ctx):
    pages = [
        ("📖 Hướng Dẫn (1/4) — Nhân Vật & Tu Luyện", """
**👤 Nhân Vật**
`!taonv <tên>` — Tạo nhân vật (chọn tộc)
`!tt [@người]` — Xem thông tin
`!thongke` — Thống kê tổng hợp
`!bxh` — Bảng xếp hạng lực chiến

**⚡ Tu Luyện**
`!tulyen` — Tu luyện (60s)
`!khampha` — Khám phá tìm đồ (120s)
`!bequan <giờ>` — Bế quan 1-72h (EXP x3)
`!xuatquan` — Xuất quan nhận thưởng

**🌿 Trồng Cây**
`!trongcay list` — Xem danh sách cây
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
`!thap` — Tháp thử luyện (120s)

**☯️ Đạo & Công Pháp**
`!chondao` — Xem / chọn đạo chính
`!daophu` — Học đạo phụ
`!congphap list` — Xem công pháp
`!congphap hoc <tên>` — Học công pháp
`!congphap xem` — Xem công pháp đã học
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
`!ketduyen @người` — Cầu hôn đạo lữ (10,000💎)

**🏯 Tông Môn**
`!lapmon <tên>` — Lập tông môn (Lv.3+, 1000💎)
`!thamgia <tên>` — Gia nhập tông môn

**🗺️ Bản Đồ**
🟢 Nhân Giới (Lv.0-5) → 🔵 Linh Giới (Lv.6-9)
🟣 Tiên Giới (Lv.10-14) → 🟡 Thánh Giới (Lv.15-24)
🔴 Vũ Trụ Cấp (Lv.25-36)
*Phi thăng tự động khi đủ cảnh giới!*

**🐉 Tộc:** Long, Thần, Nhân, Tiên, Ma, Thú
**36 Cảnh Giới** từ Phàm Nhân → Vô Thượng Đại Đạo
        """),
    ]
    for title, desc in pages:
        await ctx.send(embed=embed_mau(title, desc))

# ══════════════════════════════════════════════════════════════
#  KHỞI ĐỘNG
# ══════════════════════════════════════════════════════════════
@bot.event
async def on_ready():
    await init_db()
    print(f"✅ Bot Tu Tiên V3 MEGA online: {bot.user}")
    await bot.change_presence(activity=discord.Game(name="Tu Tiên V3 | !help | 36 Cảnh Giới"))

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
