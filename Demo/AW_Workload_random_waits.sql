USE AdventureWorks;
GO
SELECT *
FROM Production.Product
ORDER BY Name ASC;


USE AdventureWorks;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay

SELECT p.*
FROM Production.Product AS p
ORDER BY Name ASC;
GO

USE AdventureWorks;
GO
SELECT Name, ProductNumber, ListPrice AS Price
FROM Production.Product 
ORDER BY Name ASC;
GO

USE AdventureWorks;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay


SELECT Name, ProductNumber, ListPrice AS Price
FROM Production.Product 
WHERE ProductLine = 'R' 
AND DaysToManufacture < 4
ORDER BY Name ASC;
GO

USE AdventureWorks;
GO
SELECT p.Name AS ProductName, 
NonDiscountSales = (OrderQty * UnitPrice),
Discounts = ((OrderQty * UnitPrice) * UnitPriceDiscount)
FROM Production.Product AS p 
INNER JOIN Sales.SalesOrderDetail AS sod
ON p.ProductID = sod.ProductID 
ORDER BY ProductName DESC;
GO

USE AdventureWorks;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay


SELECT 'Total income is', ((OrderQty * UnitPrice) * (1.0 - UnitPriceDiscount)), ' for ',
p.Name AS ProductName 
FROM Production.Product AS p 
INNER JOIN Sales.SalesOrderDetail AS sod
ON p.ProductID = sod.ProductID 
ORDER BY ProductName ASC;
GO

USE AdventureWorks;
GO
SELECT DISTINCT JobTitle
FROM HumanResources.Employee
ORDER BY JobTitle;
GO

USE tempdb;
GO
IF OBJECT_ID (N'#Bicycles',N'U') IS NOT NULL
DROP TABLE #Bicycles;
GO
SELECT * 
INTO #Bicycles
FROM AdventureWorks.Production.Product
WHERE ProductNumber LIKE 'BK%';
GO

USE AdventureWorks;
GO
IF OBJECT_ID('dbo.NewProducts', 'U') IS NOT NULL
    DROP TABLE dbo.NewProducts;
GO


SELECT * INTO dbo.NewProducts
FROM Production.Product
WHERE ListPrice > $25 
AND ListPrice < $100;
GO

USE AdventureWorks;
GO
SELECT DISTINCT Name
FROM Production.Product AS p 
WHERE EXISTS
    (SELECT *
     FROM Production.ProductModel AS pm 
     WHERE p.ProductModelID = pm.ProductModelID
           AND pm.Name LIKE 'Long-Sleeve Logo Jersey%');
GO

-- OR

USE AdventureWorks;
GO
SELECT DISTINCT Name
FROM Production.Product
WHERE ProductModelID IN
    (SELECT ProductModelID 
     FROM Production.ProductModel
     WHERE Name LIKE 'Long-Sleeve Logo Jersey%');
GO

USE AdventureWorks;
GO
SELECT DISTINCT p.LastName, p.FirstName 
FROM Person.Person AS p 
JOIN HumanResources.Employee AS e
    ON e.BusinessEntityID = p.BusinessEntityID WHERE 5000.00 IN
    (SELECT Bonus
     FROM Sales.SalesPerson AS sp
     WHERE e.BusinessEntityID = sp.BusinessEntityID);
GO

USE AdventureWorks;
GO
SELECT p1.ProductModelID
FROM Production.Product AS p1
GROUP BY p1.ProductModelID
HAVING MAX(p1.ListPrice) >= ALL
    (SELECT AVG(p2.ListPrice)
     FROM Production.Product AS p2
     WHERE p1.ProductModelID = p2.ProductModelID);
GO

USE AdventureWorks;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay


SELECT DISTINCT pp.LastName, pp.FirstName 
FROM Person.Person pp JOIN HumanResources.Employee e
ON e.BusinessEntityID = pp.BusinessEntityID WHERE pp.BusinessEntityID IN 
(SELECT SalesPersonID 
FROM Sales.SalesOrderHeader
WHERE SalesOrderID IN 
(SELECT SalesOrderID 
FROM Sales.SalesOrderDetail
WHERE ProductID IN 
(SELECT ProductID 
FROM Production.Product p 
WHERE ProductNumber = 'BK-M68B-42')));
GO

USE AdventureWorks;
GO
SELECT SalesOrderID, SUM(LineTotal) AS SubTotal
FROM Sales.SalesOrderDetail
GROUP BY SalesOrderID
ORDER BY SalesOrderID;
GO

USE AdventureWorks;
GO
SELECT ProductID, SpecialOfferID, AVG(UnitPrice) AS [Average Price], 
    SUM(LineTotal) AS SubTotal
FROM Sales.SalesOrderDetail
GROUP BY ProductID, SpecialOfferID
ORDER BY ProductID;
GO

USE AdventureWorks;
GO
SELECT ProductModelID, AVG(ListPrice) AS [Average List Price]
FROM Production.Product
WHERE ListPrice > $1000
GROUP BY ProductModelID
ORDER BY ProductModelID;
GO

USE AdventureWorks;
GO
SELECT AVG(OrderQty) AS [Average Quantity], 
NonDiscountSales = (OrderQty * UnitPrice)
FROM Sales.SalesOrderDetail
GROUP BY (OrderQty * UnitPrice)
ORDER BY (OrderQty * UnitPrice) DESC;
GO

USE AdventureWorks;
GO
SELECT ProductID, AVG(UnitPrice) AS [Average Price]
FROM Sales.SalesOrderDetail
WHERE OrderQty > 10
GROUP BY ProductID
ORDER BY AVG(UnitPrice);
GO

USE AdventureWorks;
GO
SELECT ProductID 
FROM Sales.SalesOrderDetail
GROUP BY ProductID
HAVING AVG(OrderQty) > 5
ORDER BY ProductID;
GO

USE AdventureWorks ;
GO
SELECT SalesOrderID, CarrierTrackingNumber 
FROM Sales.SalesOrderDetail
GROUP BY SalesOrderID, CarrierTrackingNumber
HAVING CarrierTrackingNumber LIKE '4BD%'
ORDER BY SalesOrderID ;
GO

USE AdventureWorks;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay


SELECT ProductID 
FROM Sales.SalesOrderDetail
WHERE UnitPrice < 25.00
GROUP BY ProductID
HAVING AVG(OrderQty) > 5
ORDER BY ProductID;
GO

USE AdventureWorks;
GO
SELECT ProductID, AVG(OrderQty) AS AverageQuantity, SUM(LineTotal) AS Total
FROM Sales.SalesOrderDetail
GROUP BY ProductID
HAVING SUM(LineTotal) > $1000000.00
AND AVG(OrderQty) < 3;
GO

USE AdventureWorks;
GO
SELECT ProductID, Total = SUM(LineTotal)
FROM Sales.SalesOrderDetail
GROUP BY ProductID
HAVING SUM(LineTotal) > $2000000.00;
GO

USE AdventureWorks;
GO
SELECT ProductID, SUM(LineTotal) AS Total
FROM Sales.SalesOrderDetail
GROUP BY ProductID
HAVING COUNT(*) > 1500;
GO

USE AdventureWorks;
GO
SELECT pp.FirstName, pp.LastName, e.NationalIDNumber
FROM HumanResources.Employee AS e WITH (INDEX(AK_Employee_NationalIDNumber))
JOIN Person.Person AS pp on e.BusinessEntityID = pp.BusinessEntityID
WHERE LastName = 'Johnson';
GO

-- Force a table scan by using INDEX = 0.
USE AdventureWorks;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay


SELECT pp.LastName, pp.FirstName, e.JobTitle
FROM HumanResources.Employee AS e WITH (INDEX = 0) JOIN Person.Person AS pp
ON e.BusinessEntityID = pp.BusinessEntityID
WHERE LastName = 'Johnson';
GO

USE AdventureWorks;
GO
SELECT ProductID, OrderQty, SUM(LineTotal) AS Total
FROM Sales.SalesOrderDetail
WHERE UnitPrice < $5.00
GROUP BY ProductID, OrderQty
ORDER BY ProductID, OrderQty
OPTION (HASH GROUP, FAST 10);
GO

USE AdventureWorks;
GO
SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours
FROM HumanResources.Employee AS e1
UNION
SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours
FROM HumanResources.Employee AS e2
OPTION (MERGE UNION);
GO

USE AdventureWorks;
GO
IF OBJECT_ID ('dbo.Gloves', 'U') IS NOT NULL
DROP TABLE dbo.Gloves;
GO
-- Create Gloves table.
SELECT ProductModelID, Name
INTO dbo.Gloves
FROM Production.ProductModel
WHERE ProductModelID IN (3, 4);
GO

-- Here is the simple union.
USE AdventureWorks;
GO
SELECT ProductModelID, Name
FROM Production.ProductModel
WHERE ProductModelID NOT IN (3, 4)
UNION
SELECT ProductModelID, Name
FROM dbo.Gloves
ORDER BY Name;
GO

USE AdventureWorks;
GO
IF OBJECT_ID ('dbo.ProductResults', 'U') IS NOT NULL
DROP TABLE dbo.ProductResults;
GO
IF OBJECT_ID ('dbo.Gloves', 'U') IS NOT NULL
DROP TABLE dbo.Gloves;
GO
-- Create Gloves table.
SELECT ProductModelID, Name
INTO dbo.Gloves
FROM Production.ProductModel
WHERE ProductModelID IN (3, 4);
GO

USE AdventureWorks;
GO
SELECT ProductModelID, Name
INTO dbo.ProductResults
FROM Production.ProductModel
WHERE ProductModelID NOT IN (3, 4)
UNION
SELECT ProductModelID, Name
FROM dbo.Gloves;
GO

SELECT ProductModelID, Name 
FROM dbo.ProductResults;

USE AdventureWorks;
GO
IF OBJECT_ID ('dbo.Gloves', 'U') IS NOT NULL
DROP TABLE dbo.Gloves;
GO
-- Create Gloves table.
SELECT ProductModelID, Name
INTO dbo.Gloves
FROM Production.ProductModel
WHERE ProductModelID IN (3, 4);
GO

/* CORRECT */
USE AdventureWorks;
GO
SELECT ProductModelID, Name
FROM Production.ProductModel
WHERE ProductModelID NOT IN (3, 4)
UNION
SELECT ProductModelID, Name
FROM dbo.Gloves
ORDER BY Name;
GO

USE AdventureWorks;
GO
IF OBJECT_ID ('dbo.EmployeeOne', 'U') IS NOT NULL
DROP TABLE dbo.EmployeeOne;
GO
IF OBJECT_ID ('dbo.EmployeeTwo', 'U') IS NOT NULL
DROP TABLE dbo.EmployeeTwo;
GO
IF OBJECT_ID ('dbo.EmployeeThree', 'U') IS NOT NULL
DROP TABLE dbo.EmployeeThree;
GO

SELECT pp.LastName, pp.FirstName, e.JobTitle 
INTO dbo.EmployeeOne
FROM Person.Person AS pp JOIN HumanResources.Employee AS e
ON e.BusinessEntityID = pp.BusinessEntityID
WHERE LastName = 'Johnson';
GO
SELECT pp.LastName, pp.FirstName, e.JobTitle 
INTO dbo.EmployeeTwo
FROM Person.Person AS pp JOIN HumanResources.Employee AS e
ON e.BusinessEntityID = pp.BusinessEntityID
WHERE LastName = 'Johnson';
GO
SELECT pp.LastName, pp.FirstName, e.JobTitle 
INTO dbo.EmployeeThree
FROM Person.Person AS pp JOIN HumanResources.Employee AS e
ON e.BusinessEntityID = pp.BusinessEntityID
WHERE LastName = 'Johnson';
GO
-- Union ALL
SELECT LastName, FirstName, JobTitle
FROM dbo.EmployeeOne
UNION ALL
SELECT LastName, FirstName ,JobTitle
FROM dbo.EmployeeTwo
UNION ALL
SELECT LastName, FirstName,JobTitle 
FROM dbo.EmployeeThree;
GO

DECLARE @i varchar(2)
DECLARE @delay varchar(8)

SET @i = ABS(Checksum(NewID()) % 5) + 1
SET @delay = '00:00:' + @i

WAITFOR DELAY @delay

SELECT LastName, FirstName,JobTitle
FROM dbo.EmployeeOne
UNION 
SELECT LastName, FirstName, JobTitle 
FROM dbo.EmployeeTwo
UNION 
SELECT LastName, FirstName, JobTitle 
FROM dbo.EmployeeThree;
GO

SELECT LastName, FirstName,JobTitle 
FROM dbo.EmployeeOne
UNION ALL
(
SELECT LastName, FirstName, JobTitle 
FROM dbo.EmployeeTwo
UNION
SELECT LastName, FirstName, JobTitle 
FROM dbo.EmployeeThree
);
GO