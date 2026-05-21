# Ta Tu Tiên

Bot Discord RPG chủ đề **tu tiên**, có hệ thống nhân vật, cảnh giới, tông môn, boss thế giới, trang bị, đan dược, pet, bí cảnh, câu cá và nhiều hoạt động để server Discord có thể chơi lâu dài.

## Tính năng chính

- Tạo nhân vật tu tiên với tộc, linh căn, cảnh giới, tu vi và lực chiến.
- Tu luyện, bế quan, đột phá cảnh giới và phi thăng qua nhiều giới.
- Hệ thống bản đồ: Nhân Giới, Linh Giới, Tiên Giới, Thánh Giới, Vũ Trụ, Hỗn Độn, Thái Cổ, Thần Thoại, Vô Thượng.
- Boss thường và Boss Thế Giới có ảnh, HP, top damage, bảng xếp hạng và phần thưởng.
- Boss Thế Giới có nút tương tác:
  - ⚔️ Tự động đánh
  - 📊 Trạng thái
  - 🏆 Bảng xếp hạng
- Tông môn, linh mạch tông môn và nâng cấp tông môn.
- Công pháp, đạo chính, đạo phụ, kiếm linh.
- Trang bị nhiều phẩm chất, rèn đồ, mặc/tháo trang bị.
- Đan dược, túi đồ, trồng cây, câu cá, pet thám hiểm.
- Nhiệm vụ ngày, giờ hoàng đạo, bí cảnh phó bản.
- PvP, bảng xếp hạng, thống kê, nhật ký và thành tích.

## Yêu cầu

- Python 3.10+
- PostgreSQL database
- Discord Bot Token
- Các package trong `requirements.txt`

##  Lệnh cơ bản
Lệnh:
!taonv <tên>	Tạo nhân vật
!tt	Xem thông tin nhân vật
!tuluyen	Tu luyện
!bequan <giờ>	Bế quan
!xuatquan	Xuất quan
!khampha	Khám phá tìm đồ
!nhiemvu	Xem nhiệm vụ ngày
!nhiemvu nhan	Nhận thưởng nhiệm vụ ngày
!giohoangdao	Xem giờ hoàng đạo

##  Chiến đấu
Lệnh:
!boss	Xem boss thường
!boss <số>	Đánh boss thường
!bossthegioi	Xem Boss Thế Giới
!bossthegioi lich	Xem lịch Boss Thế Giới
!bossthegioi dangky	Đăng ký đánh boss
!bossthegioi tan	Tấn công boss
!bossthegioi bxh	Bảng xếp hạng damage
!pvp @người	Thách đấu PvP
!thap	Leo tháp thử luyện
!bicanh	Xem bí cảnh
!bicanh vao <tên>	Vào bí cảnh

##  Trang bị và vật phẩm
Lệnh:
!tuido	Xem túi đồ
!hanhtrang	Xem hành trang chi tiết
!trangbi	Xem trang bị đang mặc
!mac <tên>	Mặc trang bị
!thao <slot>	Tháo trang bị
!ren	Rèn trang bị
!shop	Xem cửa hàng
!shop mua <tên>	Mua vật phẩm
!dung <tên>	Dùng vật phẩm
!an <tên> [số]	Dùng đan nhanh

##  Tông môn và xã hội
Lệnh	Mô tả
!lapmon <tên>	Lập tông môn
!thamgia <tên>	Gia nhập tông môn
!nangcapmon	Nâng cấp tông môn
!linhmach	Xem linh mạch tông môn
!linhmach tu	Tu luyện linh mạch
!ketduyen @người	Kết đạo lữ

##  Nghề và hoạt động phụ
Lệnh: 
!trongcay list	Xem cây có thể trồng
!trongcay trong <tên>	Trồng cây
!trongcay thuhoach	Thu hoạch
!cau [số]	Câu cá
!muacancau	Mua cần câu
!pet	Xem pet
!pet mua <tên>	Mua pet
!pet phai <giờ>	Phái pet đi thám hiểm
!pet thu	Nhận thưởng pet thám hiểm

##  Admin
Lệnh :  
!setchannel	Đặt kênh thông báo Boss Thế Giới
