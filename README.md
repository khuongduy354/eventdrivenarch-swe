Software Architecture Assignment

> Implementation planning: see [PROTOTYPE_PLAN.md](./PROTOTYPE_PLAN.md).

# Bài tập: Event-Driven Architecture

Thiết kế và hiện thực một prototype nhỏ để chứng minh cách áp dụng Event-Driven Architecture trong bài toán xử lý sau khi đơn hàng được tạo.

Thời gian

120 phút

Hình thức

Làm theo nhóm

Số thành viên

3 sinh viên

## Phần 1. Tự tìm hiểu lý thuyết

Event-Driven Architecture

Nhóm tự nghiên cứu Event-Driven Architecture ở mức đủ để có thể giải thích và áp dụng vào bài toán ở Phần 2.

Khi trình bày, nhóm cần làm rõ tối thiểu:

-   Event-Driven Architecture giải quyết loại vấn đề gì trong thiết kế hệ thống.
-   Các thành phần cơ bản trong một hệ thống hướng sự kiện và cách chúng tương tác.
-   Sự khác nhau giữa synchronous communication và asynchronous communication.
-   Một sự kiện có thể được nhiều thành phần xử lý như thế nào.
-   Những vấn đề có thể phát sinh khi message/event không được xử lý như mong đợi.

Nhóm được tự quyết định cách tổ chức phần trình bày. Không bắt buộc trình bày theo danh sách định nghĩa.

## Phần 2. Bài toán

Sales System

Một hệ thống bán hàng hiện có chức năng tiếp nhận đơn hàng. Sau khi một đơn hàng được tạo thành công, hệ thống cần thực hiện thêm một số tác vụ liên quan đến đơn hàng đó.

Các tác vụ này không cần hoàn thành trước khi người dùng nhận được kết quả tạo đơn hàng. Một tác vụ có thể xử lý chậm, tạm thời không hoạt động hoặc gặp lỗi mà không nên làm chức năng tạo đơn hàng thất bại.

Trong tương lai, hệ thống có thể cần bổ sung thêm các chức năng khác cũng phản ứng khi đơn hàng được tạo, chẳng hạn phục vụ chăm sóc khách hàng, thống kê kinh doanh, đồng bộ dữ liệu hoặc các nghiệp vụ mở rộng khác.

Yêu cầu thực hiện:

1.  Đề xuất kiến trúc sử dụng Event-Driven Architecture cho bài toán trên.
2.  Implement một prototype chứng minh kiến trúc đề xuất hoạt động.
3.  Thể hiện được asynchronous service calling: thao tác tạo đơn hàng không phải chờ toàn bộ tác vụ xử lý sau đó hoàn tất.
4.  Thể hiện được fan-out: một sự kiện hoặc thông điệp liên quan đến việc tạo đơn hàng có thể được nhiều thành phần độc lập xử lý.
5.  Thể hiện được việc một thành phần xử lý chậm hoặc lỗi không làm toàn bộ luồng tạo đơn hàng thất bại.

Nhóm tự quyết định:

-   Cách chia service/component trong prototype.
-   Event/message cần sử dụng và nội dung dữ liệu đi kèm.
-   Thành phần nào tạo ra event/message và thành phần nào xử lý event/message.
-   Công nghệ hoặc cơ chế dùng để truyền event/message.
-   Cách chứng minh luồng xử lý là bất đồng bộ và có khả năng mở rộng thêm thành phần xử lý mới.

## Yêu cầu demo

-   Chạy được prototype trên máy khi demo.
-   Tạo được một đơn hàng mẫu và quan sát được các xử lý phát sinh sau đó.
-   Chứng minh thao tác tạo đơn hàng trả kết quả trước khi các xử lý sau đó hoàn tất.
-   Chứng minh một lần tạo đơn hàng có thể kích hoạt nhiều xử lý độc lập.
-   Chứng minh khi một xử lý bị chậm hoặc lỗi, các phần còn lại vẫn có thể tiếp tục theo thiết kế của nhóm.

## Deliverables

-   Source code prototype.
-   File README hướng dẫn cách chạy và cách demo.
-   Sơ đồ hoặc mô tả ngắn kiến trúc nhóm đề xuất.
-   Mô tả các quyết định chính: cách chia component, event/message, cơ chế truyền message và cách xử lý khi có lỗi.
-   Minh chứng demo: log, ảnh chụp màn hình hoặc video ngắn thể hiện các yêu cầu demo.
