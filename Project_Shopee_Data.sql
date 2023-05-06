--Normalize Data/Standardlize data

select avg(STANDARDIZE_DATA), stdev(STANDARDIZE_DATA) 
from
(
	select ([Grand Total] - (select avg([Grand Total]) from shopee_data))/
	(select STDEV([Grand Total]) from shopee_data)  STANDARDIZE_DATA
	from shopee_data
) t

-- Normalize data: (X-min(X))/(max(X) - min(X))
with sub as 
(
	select ([Grand Total] - (select avg([Grand Total]) from shopee_data))/
	(select STDEV([Grand Total]) from shopee_data)  STANDARDIZE_DATA
	from shopee_data
) 
select (STANDARDIZE_DATA - (select min(STANDARDIZE_DATA) from sub))/
	((select max(STANDARDIZE_DATA) from sub) - (select min(STANDARDIZE_DATA) from sub))from sub

-- Extract trường City  + Province từ Recipient Shipping Address

select [Recipient shipping address]
	, reverse(substring(reverse([Recipient shipping address]),1,charindex(',',reverse([Recipient shipping address]))-2)) CITY
	, case when charindex(',',SUBSTRING(reverse([Recipient shipping address]),
	charindex(',',reverse([Recipient shipping address])) +1,
	len([Recipient shipping address]))) <2 then null else reverse(substring(SUBSTRING(reverse([Recipient shipping address]),
	charindex(',',reverse([Recipient shipping address])) +1,
	len([Recipient shipping address])),1,charindex(',',SUBSTRING(reverse([Recipient shipping address]),
	charindex(',',reverse([Recipient shipping address])) +1,
	len([Recipient shipping address])))-2)) end DISTRICT
	from shopee_data

--Top 10 danh sách Sell_name và Buyer_name

select top 10 [Seller name] ,sum([Grand Total]) as Sum_grand_total
from shopee_data
group by [Seller name] 
order by sum([Grand Total]) desc

select top 10 [Buyer name] ,sum([Grand Total]) as Sum_grand_total
from shopee_data
group by [Buyer name] 
order by sum([Grand Total]) desc

--Seller Category nào có số lượng Seller nhiều nhất
select  [Seller Category], count(distinct([Seller name]))
from shopee_data
group by [Seller Category]
order by count(distinct([Seller name])) desc

--Seller Category nào được ưa chuộng nhất 

select top 1[Seller Category], count([Order ID])
from shopee_data
group by [Seller Category]
order by count([Order ID]) desc 

--cosine 
--5. Giá trị GD theo từng mặt hàng, Sức mua theo khu vực

select [Seller Category], sum([Grand Total])
from shopee_data
group by [Seller Category]
order by sum([Grand Total]) desc

select city_province
	, sum([Grand Total])
from
(
select [Recipient shipping address]
	, reverse(substring(reverse([Recipient shipping address]),1,charindex(',',reverse([Recipient shipping address]))-2)) CITY_PROVINCE
	,[Grand Total]
from shopee_data
) as A
group by city_province
order by sum([Grand Total]) desc

--Xét tính chu kỳ theo số tuần trong tháng week num (1, 2, 3, 4) Order number & add scalar function trả ra thứ trong tuần

CREATE FUNCTION Week_Number (@date as datetime)
RETURNS VARCHAR(30)
WITH RETURNS NULL ON NULL INPUT
AS
BEGIN 
	RETURN CASE WHEN datepart(day, @date) <=7 then 'Week 1'
	when datepart(day, @date) between 8 and 14 then 'Week 2'
	when datepart(day, @date) between 15 and 21 then 'Week 3'
	else 'Week 4' end
END
GO
select Month, Week_number, count([Order ID]) as Total_order from
(
	select convert(varchar(6), [Purchased on] ,112) as Month
		, dbo.Week_Number([Purchased on]) AS Week_number
		, [Order ID]
	from shopee_data
) as A
group by Month, Week_number
order by Month, Total_order desc
-- Chu kỳ theo số tuần trong tháng:
-- Trong 3 tuần đầu của tháng, SL order không có thay đổi đáng kể (gần như là đường thẳng). Tuy nhiên peak thấp nhất vào tuần thứ 3
-- Total order tăng mạnh ở tuần 4 (tuần cuối cùng của tháng)

--Phân bổ của Seller Category:
select avg(Total_order) from
(
	select convert(varchar(6), [Purchased on] ,112) as Month
		, dbo.Week_Number ([Purchased on] ) as Week_number
		, [Seller Category]
		, count([Order ID]) as Total_order
		, ROW_NUMBER () over (partition by convert(varchar(6), [Purchased on] ,112), dbo.Week_Number ([Purchased on] ) order by count([Order ID]) desc) as Row_num
	from shopee_data
	group by convert(varchar(6), [Purchased on] ,112), dbo.Week_Number ([Purchased on] ), [Seller Category]
) as A 
where Row_num = 1 
select Month, Week_number, [Seller Category], Total_order  from 
(
	select convert(varchar(6), [Purchased on] ,112) as Month
		, dbo.Week_Number ([Purchased on] ) as Week_number
		, [Seller Category]
		, count([Order ID]) as Total_order
		, ROW_NUMBER () over (partition by convert(varchar(6), [Purchased on] ,112), dbo.Week_Number ([Purchased on] ) order by count([Order ID]) desc) as Row_num
	from shopee_data
	group by convert(varchar(6), [Purchased on] ,112), dbo.Week_Number ([Purchased on] ), [Seller Category]
) as A 
where Row_num = 1 
order by Week_number
-- Từ tháng 08 - 12, Health & Beauty là ngành hàng được mua chủ yếu trên shopee.
-- Total order cao nhất là tuần cuối cùng của tháng, đều vượt 500 đơn hàng.
-- Tháng 11, Women Clothes có số lượng đơn hàng cao nhất (có thể là do Black Friday)
-- Hầu như chênh lệch đơn hàng giữa các tuần trong các tháng khác nhau không có sự chênh lệch quá cao (với cùng 1 ngành hàng)

--Chiến lược sale từ trường này
-- Sale chủ yếu đổ dồn vào tuần cuối cùng của tháng, các sản phẩm thuộc nhóm Health & Beauty.
-- Ưu tiên cho nhóm Health & Beauty ở banner đầu tiên của trang chủ 
-- Đối với 3 tuần đầu của tháng: 
-- vẫn duy trì giá của sản phẩm, đẩy các mã khuyến mãi như freeship, giảm 8k cho số lượng khách hàng nhất định (vd (500 KH/ngày)
-- giữ nguyên giá hiển thị của seller.
-- Hiển thị banner để người dùng của shopee biết ngày campaign cuối tháng là ngày nào
-- Đối với tuần thứ 4:
-- Tăng băng thông cho máy chủ/tối ưu hóa mã nguồn để hạn chế trường hợp sập app.
-- Chọn 1 ngày làm ngày chạy campaign & đẩy mạnh khuyến mãi cho KH + tăng giá của seller.
-- Deal với KH để áp dụng giảm giá SP; sàn mở thêm nhiều mã như freeship, giảm tiền theo đơn hàng (vd như đơn 200k được giảm 30k...)
-- Tăng giá hiện thị của seller lên x%:
	-- vd như key word "sữa rửa mặt" bình thường để xuất hiện ở top 1 tìm kiếm của shopee, seller phải trả 10k cho 1 lượt search của KH, nhưng vào những
	-- ngày chạy flash sale, key word để seller hiện top đầu của sàn có thể tăng lên x%.


--Xây dựng bảng master theo levey buyer name
select * from shopee_data
select top 4 [Seller Category], count(*)
from shopee_data
group by [Seller Category] -- Healthy & Beaty; WomenClothes; Baby&Toy
order by count(*) desc
select [Buyer name] from shopee_data
group by [Buyer name] -- 19750 dòng

--Detail feature of top 3 best-selling category:
select T.[Buyer name], isnull(Healthy_Beauty,0) Healthy_Beauty
	, isnull(Women_Clothes,0) Women_Clothes
	, isnull(Baby_Toys,0) Baby_Toys
	, isnull(Other_Seller_Category,0) Other_Seller_Category
	, isnull(Healthy_Beauty,0) + isnull(Women_Clothes,0) + isnull(Baby_Toys,0) + isnull(Other_Seller_Category,0) as Total_Grand_Category
	, Buyer_name_cycle_trans
	, Buyer_Transaction_age
	, Buyer_Status
	, Prefre_Category
from
(
select [Buyer name],
	case when [Seller Category] = 'Health & Beauty' then 'Healthy_Beauty'
	when [Seller Category] = 'Women Clothes' then 'Women_Clothes'
	when [Seller Category] = 'Baby & Toys' then 'Baby_Toys'
	else 'Other_Seller_Category'
	end Group_Seller
	, [Grand Total]
from shopee_data
) pv pivot
(
	sum([Grand Total]) 
	for Group_Seller in ([Healthy_Beauty], [Women_Clothes], [Baby_Toys], [Other_Seller_Category])
) as T
-- transaction cycle of buyer name:
LEFT JOIN 
(
	select [Buyer name], avg(Interval) as Buyer_name_cycle_trans
	from
	(
		select [Buyer name]
			, [Purchased on]
			, lead([Purchased on],1) over (partition by [Buyer name] order by [Purchased on]) as Next_trans_date
			, case when lead([Purchased on],1) over (partition by [Buyer name] order by [Purchased on]) is null then 
			datediff(day, [Purchased on], [Purchased on])
			else datediff(day, [Purchased on], lead([Purchased on],1) over (partition by [Buyer name] order by [Purchased on])) end as Interval
		from shopee_data
	) as A
	group by [Buyer name]
) T1
	on T.[Buyer name] = T1.[Buyer name]
-- Buyer age:
left join
(
	select [Buyer name]
		, min([Purchased on]) as First_Transaction
		, max([Purchased on]) as Last_Transaction
		, datediff(day, min([Purchased on]), max([Purchased on])) as Buyer_Transaction_age
	from shopee_data
	group by [Buyer name]
) T2 
	on T.[Buyer name] = T2.[Buyer name]
--Buyer Status: active or inactive:
LEFT JOIN
(
	select [Buyer name]
		, case when Last_Transaction > Date_Add then 'ACTIVE'
		else 'INACTIVE' end Buyer_Status
	from
	(
		select [Buyer name]
			, max([Purchased on]) as Last_Transaction
			, dateadd(day, -60, '12-31-2016') as Date_Add
		from shopee_data
		group by [Buyer name]
	) as A
) T3
	on T.[Buyer name] = T3.[Buyer name]
--Prefer Category of buyer:
LEFT JOIN
(
	select [Buyer name]
		, Prefre_Category
	from
	(
		select [Buyer name]
			, [Seller Category] as Prefre_Category
			, ROW_NUMBER () over (partition by [Buyer name] order by count([Seller Category]) desc) as Row_num
		from shopee_data
		group by [Buyer name], [Seller Category]
	) as A
	where Row_num = 1
) T4
on T.[Buyer name] = T4.[Buyer name]




