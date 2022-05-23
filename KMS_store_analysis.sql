select * from KMS_Orders
--Datacleaning
--Check for duplicates

select *,
	ROW_NUMBER() over(partition by [customer name],[row id],[order id],sales order by [order date])
from KMS_Orders
--no duplicates

--Case 1: Which product category had the highest sales?

;with sales_cat as
(select distinct [Product Category], 
	round(sum(Sales) over (partition by [product category]),2) as SumOfSales
from KMS_Orders),

salescatrnk as
(select *, RANK() over(order by sumofsales desc) as rank
from sales_cat)

select [Product Category],SumOfSales
from salescatrnk
where salescatrnk.rank=1

--Case 2: What are the Top 3 and Bottom 3 Regions with regards to sales?
with sales_reg as
(select distinct Region, round(sum(sales) over (partition by region),2) as SumOfSales
from KMS_Orders),

rank_sales_tab as
(select *,
DENSE_RANK() over(order by sumofsales desc) as rnkhigh,
DENSE_RANK() over(order by sumofsales asc) as rnklow
from sales_reg),

topbottom_sales as
(select *,
	case when
		rnkhigh<4 then 'Top three region' 
	when rnklow<4 then 'Bottom three region' else null
	end as salesflag
from rank_sales_tab)

select Region,SumOfSales,salesflag
from topbottom_sales
where salesflag is not null
order by 2 desc

--Case 3: What was the total sales of appliances in Ontario?

select distinct [Product Sub-Category],region,sum(Sales) over(partition by [Product Sub-Category]) as SumOfSales
from KMS_Orders
where [Product Sub-Category]='Appliances' and Region='ontario'

--Case 4: Advise the management of KMS on what to to do to increase
--the revenue from the bottom 10 customers

--Case 5: KMS incurred the most shipping cost using which shipping
--method?

select distinct [Ship Mode], 
	round(sum([Shipping Cost]) over(partition by [ship mode]),2) as SumOfShippingCost
from KMS_Orders
order by 2 desc

--Case 6: Who are the most valuable customers and what do they
--purchase?

select top(10)[Customer Name],[Order Quantity],Sales,[Product Name]
from KMS_Orders
order by 3 desc

--b. Who are the most profitable customers and what do they
----purchase?

select top(10) [Customer Name],[Order Quantity],Sales, Profit, [Product Name]
from KMS_Orders
order by Profit desc

--Case 7: If the delivery truck is the most economical but the slowest
--shipping method and Express Air is the fastest but the most expensive
--one, do you think the company appropriately spent shipping costs
--based on the Order Priority? Explain your answer

select distinct [Ship Mode],round(sum([Shipping Cost]) over(partition by [ship mode]),2) as SumShippingCost
from KMS_Orders
order by 2 desc

select distinct [Order Priority],round(sum([Shipping Cost]) over(partition by [order priority]),2) as SumShippingCost
from KMS_Orders
order by 2 desc

--do you think the company appropriately spent shipping costs
----based on the Order Priority? Explain your answer

select distinct [Ship Mode],[Order Priority],round(sum([Shipping Cost]) over(partition by [order priority]),2) as ShippingCostSum
from KMS_Orders
--group by [Ship Mode],[Order Priority]
order by 3 desc

--Case 8: Which small business customer had the highest sales?

select distinct [Customer Name],round(sum(Sales) over (partition by [customer name]),2) as SalesSum
from KMS_Orders
where [Customer Segment] like 'small business'
order by 2 desc

---Case 9: Which Corporate Customer placed the most number of orders
--in 2009 – 2012? How many orders were placed by the Corporate
--customer?

with ordertab as
(select year(cast([Order Date] as date)) as year,[Customer Name],[Order Quantity]
from KMS_Orders
where [Customer Segment] like 'Corporate')

select * from ordertab
where year in (2009,2010,2011,2012)
order by [Order Quantity] desc

--Case 10: Which consumer customer was the most profitable one?

select [Customer Name],Profit
from KMS_Orders
order by 2 desc

--Case 11: Which customer returned items and what segment do they
--belong?

with orderstatustab as
(select [Customer Name],[Customer Segment],Status,
	case when Status is null then 'Not Returned'
	else 'Returned'
	end as OrderStatus
from KMS_Orders as ord
left join KMS_Returns as ret
on ord.[Order ID]=ret.[Order ID])

select distinct [Customer Name],[Customer Segment],OrderStatus
from orderstatustab
where OrderStatus='returned'

--Case 12: Which product sub-category has the most sales?

select distinct [Product Sub-Category],round(sum(Sales) over(partition by [product sub-category]),2) as Revenue
from KMS_Orders
order by 2 desc

--Case 13: Which customer segment sold the most?

select distinct[Customer Segment], round(sum(Sales) over(partition by [customer segment]),2) as Revenue
from KMS_Orders
order by 2 desc

--Case 13: Which product were less profitable?

select distinct[Product Name],round(sum(Profit) over(partition by [product name]),2) as loss
from
	(select [Customer Name], [Product Name],Profit
	from KMS_Orders
	where Profit<=0) as less_profit
order by 2 

--Case 14: Which year did they make the most sales?

select distinct YEAR([order date]) as year, round(sum(Sales) over(partition by YEAR([order date])),2)
from KMS_Orders
order by 2 desc

--Case 15: What was the best month for sales in a specific year?

select distinct month([Order Date]) as month,
	count([Order Quantity]) over (partition by month([Order Date])) as OrderFreq,
	round(sum(Sales) over (partition by month([Order Date])),2) as Revenue
from KMS_Orders
where year([order date])=2012
order by 3 desc

---May is our best month in 2012, which product category sold the most that month?
select distinct [Product Category],
	count([Order Quantity]) over (partition by ([product category])) as OrderFreq,
	round(sum(Sales) over(partition by ([product category])),2) as Revenue
from KMS_Orders
where MONTH([order date])=5
order by 3 desc

--Case 16: Who is our Best customer? (Using RFM Analysis)?
--- Who is our best customer?
--We will use the RFM technique,
--RFM is an indexing technique that uses past purchase behaviour to segment
--customers
--RFM uses 3 key metrics
--1. Recency - Last order date (how long ago was their last purchase)
--2. Frequency - Count of Total Order (how often did they purchase)
--3. Monetary value - Total spend (how much they spent)

select * from KMS_Orders

drop table if exists #rfmtab
;with rfm_tab as
(select [Customer Name],
	round(sum([sales]),2) as tot_spend,
	count([Order ID]) as ord_freq,
	max([order date]) as cust_recent_order_date,
	(select max([order date]) from KMS_Orders) as recent_order_date,
	DATEDIFF(dd,max([order date]),(select max([order date]) from KMS_Orders)) as recency
from KMS_Orders
group by [Customer Name]),

rfm_grp as
(select *,
	NTILE(4) over(order by recency desc) as rfm_recency,
	NTILE(4) over(order by ord_freq) as rfm_freq,
	NTILE(4) over(order by tot_spend) as rfm_monetary
from rfm_tab)

select *,
	rfm_recency+rfm_freq+rfm_monetary as rfm_cell,
	cast(rfm_recency as varchar)+cast(rfm_freq as varchar)+cast(rfm_monetary as varchar) as rfm_cell_string
into #rfmtab
from rfm_grp

select [Customer Name],rfm_recency,rfm_freq,rfm_monetary,
	case 
		when rfm_cell_string in (111, 112 , 113, 121, 122, 123, 132, 211, 212, 114, 141) then 'lost_customers'  --lost customers, they purchased a long time ago in small quantities
		when rfm_cell_string in (124, 133, 134, 143, 234, 244, 334, 343, 344, 144) then 'slipping away, cannot afford to loose them' -- (Big spenders who haven’t purchased lately)
		when rfm_cell_string in (311, 411, 331) then 'new customers'
		when rfm_cell_string in (222, 223, 224, 233, 322) then 'potential churners'
		when rfm_cell_string in (323, 333,321, 422, 332, 432) then 'active' --(Customers who buy often & recently, but at low price points)
		when rfm_cell_string in (424, 433, 434, 443, 444) then 'loyal'
	end rfm_segment
into #rfmtable
from #rfmtab

select * from #rfmtable

---Case 17: What products are most often sold together?
drop table if exists #prodtab
;with prod_tab as
(select distinct[Order ID],STUFF(

	(select ',' + [Product Name]
	from KMS_Orders t2
	where [Order ID] in
	(	select [Order ID]
		from(
			select [Order ID],COUNT([Order ID]) as order_count
			from KMS_Orders
			group by [Order ID])as t1
			where order_count=2)
	and t2.[Order ID]=t3.[Order ID]
	for xml path(''))
	,1,1,'') product

from KMS_Orders t3)

select *
into #prodtab
from prod_tab

select *
from #prodtab
where product is not null
order by 2