SELECT 
	SOD.SalesOrderID
	,SOH.OrderDate
	,SOH.DueDate
	,SOH.ShipDate
	,CASE
		WHEN SOH.Status = 1 THEN 'In Process'
		WHEN SOH.Status = 2 THEN 'Approved'
		WHEN SOH.Status = 3 THEN 'Backordered'
		WHEN SOH.Status = 4 THEN 'Rejected'
		WHEN SOH.Status = 5 THEN 'Shipped'
		WHEN SOH.Status = 6 THEN 'Cancelled'
	END AS [Order Status]
	,CONCAT(PER.FirstName, ' ', PER.LastName) AS [Sales Person Name]
	,SOD.SalesOrderDetailID
	,SOD.OrderQty
	,SOD.UnitPrice
	,SOD.UnitPriceDiscount
	,SOD.LineTotal
	,SalesOrderTotal = SUM(SOD.LineTotal) OVER(PARTITION BY SOD.SalesOrderID)
	,ST.Name AS [Territory Name]
	,ST."Group" AS [Territory Group]
	,CR.Name AS [Country Region Name]
	,P.Name
	,P.StandardCost
	,P.ListPrice
	,P.DaysToManufacture
	,CASE
		WHEN P.ProductLine = 'R' THEN 'Road'
		WHEN P.ProductLine = 'M' THEN 'Mountain'
		WHEN P.ProductLine = 'T' THEN 'Touring'
		WHEN P.ProductLine = 'S' THEN 'Standard'
	END AS [Product Line]
	,CASE 
		WHEN P.Class = 'H' THEN 'High'
		WHEN P.Class = 'M' THEN 'Medium'
		WHEN P.Class = 'L' THEN 'Low'
	END AS [Product Class]
	,CASE 
		WHEN P.Style = 'W' THEN 'Womens'
		WHEN P.Style = 'M' THEN 'Mens'
		WHEN P.Style = 'U' THEN 'Universal'
	END AS [Product Style]
	,PSC.Name AS [Product Subcategory]
	,PC.Name AS [Product Category]
FROM 
	Sales.SalesOrderDetail SOD
JOIN Sales.SalesOrderHeader SOH
	ON SOD.SalesOrderID = SOH.SalesOrderID
JOIN Production.Product P 
	ON P.ProductID = SOD.ProductID
JOIN Production.ProductSubcategory PSC
	ON PSC.ProductSubcategoryID = P.ProductSubcategoryID
JOIN Production.ProductCategory PC
	ON PC.ProductCategoryID = PSC.ProductCategoryID
LEFT JOIN Sales.SalesPerson SP
	ON SOH.SalesPersonID = SP.BusinessEntityID
LEFT JOIN HumanResources.Employee EMP
	ON SP.BusinessEntityID = EMP.BusinessEntityID
LEFT JOIN Person.Person PER
	ON PER.BusinessEntityID = EMP.BusinessEntityID
LEFT JOIN Sales.SalesTerritory ST
	ON SP.TerritoryID = ST.TerritoryID
LEFT JOIN Person.CountryRegion CR
	ON ST.CountryRegionCode = CR.CountryRegionCode
