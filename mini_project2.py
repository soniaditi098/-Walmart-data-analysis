### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error
import datetime

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    regions = set()
    with open(data_filename, 'r') as f:
        next(f) 
        for line in f:
            fields = line.strip().split('\t')
            region = fields[4]
            regions.add(region)
    regions = sorted(list(regions))
    
    conn = create_connection(normalized_database_filename)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS region (
            RegionID INTEGER PRIMARY KEY,
            Region TEXT NOT NULL
        );
    """
    create_table(conn, create_table_sql)

    # Insert into region table
    insert_sql = "INSERT INTO region (Region) VALUES (?)"
    values = [(region,) for region in regions]
    conn.executemany(insert_sql, values)


    conn.commit()
    conn.close()

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)

    select_sql = "SELECT RegionID, Region FROM region"
    cursor = conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()

    region_to_id = {row[1]: row[0] for row in rows}

    conn.close()

    return region_to_id


def step3_create_country_table(data_filename, normalized_database_filename):
    countries = set()
    country_region = []
    region_to_id = step2_create_region_to_regionid_dictionary(normalized_database_filename)
    with open(data_filename, 'r') as f:
        next(f) 
        for line in f:
            fields = line.strip().split('\t')
            country, region = fields[3], fields[4]
            if country in countries and region in region_to_id:
                continue
            countries.add(country)
            if region in region_to_id:
                country_region.append((country, region_to_id[region]))

    conn = create_connection(normalized_database_filename)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS country (
            CountryID INTEGER PRIMARY KEY,
            Country TEXT NOT NULL,
            RegionID INTEGER NOT NULL,
            FOREIGN KEY (RegionID) REFERENCES region (RegionID)
        );
    """
    create_table(conn, create_table_sql)

    # Insert into country table
    insert_sql = "INSERT INTO country (Country, RegionID) VALUES (?, ?)"
    values = [(country_reg[0], country_reg[1]) for country_reg in country_region if country_reg[0] in countries]
    values.sort(key=lambda x: x[0])    
    conn.executemany(insert_sql, values)

    conn.commit()
    conn.close()



def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)

    select_sql = "SELECT CountryID, Country FROM country"
    cursor = conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()

    country_to_id = {row[1]: row[0] for row in rows}

    conn.close()

    return country_to_id
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    country_to_id = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    with open(data_filename, 'r') as f:
        next(f) 
        values = []
        for line in f:
            fields = line.strip().split('\t')
            name = fields[0].split(' ')
            firstname, lastname = name[0], ' '.join(name[1:])
            address, city, country = fields[1], fields[2], fields[3]
            if country in country_to_id:
                values.append((firstname, lastname, address, city, country_to_id[country]))
        

    conn = create_connection(normalized_database_filename)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS customer (
            CustomerID INTEGER PRIMARY KEY,
            FirstName TEXT NOT NULL,
            LastName TEXT NOT NULL,
            Address TEXT NOT NULL,
            City TEXT NOT NULL,
            CountryID INTEGER NOT NULL,
            FOREIGN KEY (CountryID) REFERENCES country (CountryID)
        );
    """
    create_table(conn, create_table_sql)

    # Insert into customer table
    insert_sql = "INSERT INTO customer (FirstName, LastName, Address, City, CountryID) VALUES (?, ?, ?, ?, ?)"
    values = sorted(values, key=lambda x: x[0])
    conn.executemany(insert_sql, values)

    conn.commit()
    conn.close()


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)

    select_sql = "SELECT CustomerID, FirstName ||' '|| LastName as Name FROM customer"
    cursor = conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()

    customer_to_id = {row[1]: row[0] for row in rows}

    conn.close()

    return customer_to_id
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    with open(data_filename, 'r') as f:
        next(f) # skip header line
        ProductCategories=set()
        ProductCategoryDescriptions=set()
        ProductCategoryValue=[]
        for line in f:
            fields = line.strip().split('\t')
            ProductCategoryList, ProductCategoryDescriptionList = fields[6].split(';'), fields[7].split(';')
            ProductCategorydict = dict(zip(ProductCategoryList, ProductCategoryDescriptionList))
            
    for key,value in ProductCategorydict.items():
        a=tuple((key,value))
        ProductCategoryValue.append(a)
            
            

    conn = create_connection(normalized_database_filename)

    # Create customer table
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS productcategory (
            ProductCategoryID INTEGER PRIMARY KEY,
            ProductCategory TEXT NOT NULL,
            ProductCategoryDescription TEXT NOT NULL
        );
    """
    create_table(conn, create_table_sql)

    # Insert data into productcategory table
    insert_sql = "INSERT INTO productcategory (ProductCategory, ProductCategoryDescription) VALUES (?, ?)"
    ProductCategoryValue = sorted(ProductCategoryValue, key=lambda x: x[0])
    conn.executemany(insert_sql, ProductCategoryValue)

    conn.commit()
    conn.close()

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    conn = create_connection(normalized_database_filename)

    select_sql = "SELECT ProductCategoryID, ProductCategory FROM productcategory"
    cursor = conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()

    ProductCategory_to_id = {row[1]: row[0] for row in rows}

    conn.close()

    return ProductCategory_to_id
        

def step9_create_product_table(data_filename, normalized_database_filename):
    ProductCategory_to_id = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    values = set()

    with open(data_filename, 'r') as f:
        next(f)
        for line in f:
            fields = line.strip().split('\t')
            #ProductName, ProductUnitPrice, ProductCategoryList = fields[5].split(';'), fields[8].split(';'), fields[6].split(';')
            #prod_dict=dict(zip(ProductName, ProductUnitPrice))
            #print(prod_dict)
            ProductName, ProductUnitPrice, ProductCategoryList = fields[5].split(';'), fields[8].split(';'), fields[6].split(';')
            for i, prod_name in enumerate(ProductName):
                values.add((prod_name.strip(), float(ProductUnitPrice[i]), ProductCategory_to_id[ProductCategoryList[i].strip()]))
    conn = create_connection(normalized_database_filename)

    # Create customer table
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS product (
            ProductID INTEGER PRIMARY KEY,
            ProductName TEXT NOT NULL,
            ProductUnitPrice REAL NOT NULL,
            ProductCategoryID INTEGER NOT NULL,
            FOREIGN KEY (ProductCategoryID) REFERENCES productcategory (ProductCategoryID)
        );
    """
    create_table(conn, create_table_sql)

    insert_sql = "INSERT INTO product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?, ?, ?)"
    values = sorted(values, key=lambda x: x[0])
    conn.executemany(insert_sql, values)

    conn.commit()
    conn.close()


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)

    select_sql = "SELECT ProductID, ProductName FROM product"
    cursor = conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()

    Product_to_id = {row[1]: row[0] for row in rows}

    conn.close()

    return Product_to_id
    
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    Customer_to_id = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    Product_to_id = step10_create_product_to_productid_dictionary(normalized_database_filename)
    #Product_to_id={'Alice Mutton': 1, 'Aniseed Syrup': 2, 'Boston Crab Meat': 3, 'Camembert Pierrot': 4, 'Carnarvon Tigers': 5, 'Chai': 6, 'Chang': 7, 'Chartreuse verte': 8, "Chef Anton's Cajun Seasoning": 9, "Chef Anton's Gumbo Mix": 10, 'Chocolade': 11, 'Cote de Blaye': 12, 'Escargots de Bourgogne': 13, 'Filo Mix': 14, 'Flotemysost': 15, 'Geitost': 16, 'Genen Shouyu': 17, 'Gnocchi di nonna Alice': 18, 'Gorgonzola Telino': 19, "Grandma's Boysenberry Spread": 20, 'Gravad lax': 21, 'Guarana Fantastica': 22, 'Gudbrandsdalsost': 23, 'Gula Malacca': 24, 'Gumbar Gummibarchen': 25, "Gustaf's Knackebrod": 26, 'Ikura': 27, 'Inlagd Sill': 28, 'Ipoh Coffee': 29, "Jack's New England Clam Chowder": 30, 'Konbu': 31, 'Lakkalikoori': 32, 'Laughing Lumberjack Lager': 33, 'Longlife Tofu': 34, 'Louisiana Fiery Hot Pepper Sauce': 35, 'Louisiana Hot Spiced Okra': 36, 'Manjimup Dried Apples': 37, 'Mascarpone Fabioli': 38, 'Maxilaku': 39, 'Mishi Kobe Niku': 40, 'Mozzarella di Giovanni': 41, 'Nord-Ost Matjeshering': 42, 'Northwoods Cranberry Sauce': 43, 'NuNuCa Nu-Nougat-Creme': 44, 'Original Frankfurter grune Soe': 45, 'Outback Lager': 46, 'Pate chinois': 47, 'Pavlova': 48, 'Perth Pasties': 49, 'Queso Cabrales': 50, 'Queso Manchego La Pastora': 51, 'Raclette Courdavault': 52, 'Ravioli Angelo': 53, 'Rhonbrau Klosterbier': 54, 'Rod Kaviar': 55, 'Rogede sild': 56, 'Rossle Sauerkraut': 57, 'Sasquatch Ale': 58, 'Schoggi Schokolade': 59, 'Scottish Longbreads': 60, 'Singaporean Hokkien Fried Mee': 61, "Sir Rodney's Marmalade": 62, "Sir Rodney's Scones": 63, "Sirop d'erable": 64, 'Spegesild': 65, 'Steeleye Stout': 66, 'Tarte au sucre': 67, 'Teatime Chocolate Biscuits': 68, 'Thuringer Rostbratwurst': 69, 'Tofu': 70, 'Tourtiere': 71, 'Tunnbrod': 72, "Uncle Bob's Organic Dried Pears": 73, 'Valkoinen suklaa': 74, 'Vegie-spread': 75, 'Wimmers gute Semmelknodel': 76, 'Zaanse koeken': 77}
    values = []
    with open(data_filename, 'r') as f:
        next(f) # skip header line
        for line in f:
            fields = line.strip().split('\t')
            CustomerName, OrderDate, ProductName, QuantityOrdered = fields[0], fields[10].split(';'), fields[5].split(';'), fields[9].split(';')
            for ProdName in ProductName:
                p_id=Product_to_id[ProdName]
                
            for i, ProdName in enumerate(ProductName):
                values.append((Customer_to_id[CustomerName], Product_to_id[ProdName], datetime.datetime.strptime(OrderDate[i], '%Y%m%d').strftime('%Y-%m-%d') , QuantityOrdered[i]))

    conn = create_connection(normalized_database_filename)

    # Create customer table
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS OrderDetail (
            OrderID INTEGER PRIMARY KEY,
            CustomerID INTEGER NOT NULL,
            ProductID INTEGER NOT NULL,
            OrderDate TEXT NOT NULL,
            QuantityOrdered INTEGER NOT NULL

        );
    """
    create_table(conn, create_table_sql)

    # Insert data into customer table
    insert_sql = "INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?, ?, ?, ?)"
    values = sorted(values, key=lambda x: x[0])
    conn.executemany(insert_sql, values)

    conn.commit()
    conn.close()


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    Customer_to_id = step6_create_customer_to_customerid_dictionary('normalized.db')
    customer_id = Customer_to_id[CustomerName]

    sql_statement = """
    select 
        c.FirstName ||' '|| c.LastName as Name,
        p.ProductName as ProductName,
        od.OrderDate as OrderDate,
        p.ProductUnitPrice as ProductUnitPrice,
        od.QuantityOrdered as QuantityOrdered,
        round(p.ProductUnitPrice * od.QuantityOrdered,2) as Total
    from OrderDetail od 
    join customer c on od.CustomerID = c.CustomerID
    join product p on od.ProductID = p.ProductID
    where od.CustomerID={}
    """.format(customer_id)
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    Customer_to_id = step6_create_customer_to_customerid_dictionary('normalized.db')
    customer_id = Customer_to_id[CustomerName]
    sql_statement = """

    SELECT 
        FirstName || ' ' || LastName AS Name, 
        round(sum(ProductUnitPrice * QuantityOrdered), 2) as Total
    from OrderDetail od 
    join customer c on od.CustomerID = c.CustomerID
    join product p on od.ProductID = p.ProductID
    where od.CustomerID={}
    """.format(customer_id)
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
   
    sql_statement = """

    SELECT 
        FirstName || ' ' || LastName AS Name, 
        round(sum(ProductUnitPrice * QuantityOrdered), 2) as Total
    from OrderDetail od 
    join customer c on od.CustomerID = c.CustomerID
    join product p on od.ProductID = p.ProductID
    group by od.CustomerID
    order by Total desc
    """

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    SELECT 
        r.Region, 
        round(sum(p.ProductUnitPrice * od.QuantityOrdered), 2) as Total
    from OrderDetail od 
    join customer c on od.CustomerID = c.CustomerID
    join product p on od.ProductID = p.ProductID
    join country co ON c.CountryID = co.CountryID
    join region r on co.RegionID=r.RegionID
    GROUP BY r.Region
    order by Total desc
    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """
    select co.Country, round(sum(p.ProductUnitPrice * od.QuantityOrdered), 0) AS CountryTotal
    from OrderDetail od
    join customer c ON od.CustomerID = c.CustomerID
    join product p ON od.ProductID = p.ProductID
    join country co ON c.CountryID = co.CountryID
    group by co.Country
    order by CountryTotal DESC
    """
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """
    with region_country_total as
    (
        select r.Region as Region,
        co.Country as Country,
        round(sum(p.ProductUnitPrice * od.QuantityOrdered),0) as CountryTotal
        from OrderDetail od
        join Customer c on od.CustomerID=c.CustomerID
        join Country co on c.CountryID=co.CountryID
        join Region r on co.RegionID=r.RegionID
        join Product p on od.ProductID=p.ProductID
        group by r.Region,co.Country
    ),
    ranked_country_total as
    (
        select *,
        dense_rank() over(partition by Region order by CountryTotal desc) as CountryRegionalRank
        from region_country_total
    )
    select Region, Country, CountryTotal, CountryRegionalRank
    from ranked_country_total
    order by Region ASC

    """


    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """
    with region_country_total as
    (
        select r.Region as Region,
        co.Country as Country,
        round(sum(p.ProductUnitPrice * od.QuantityOrdered),0) as CountryTotal
        from OrderDetail od
        join Customer c on od.CustomerID=c.CustomerID
        join Country co on c.CountryID=co.CountryID
        join Region r on co.RegionID=r.RegionID
        join Product p on od.ProductID=p.ProductID
        group by r.Region,co.Country
    ),
    ranked_country_total as
    (
        select *,
        dense_rank() over(partition by Region order by CountryTotal desc) as CountryRegionalRank
        from region_country_total
    )
    select Region, Country, CountryTotal, CountryRegionalRank
    from ranked_country_total
    where CountryRegionalRank=1
    order by Region ASC
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """
    With SalesByQuarter AS (
        select 
            strftime('%Y', OrderDate) AS Year, 
            CASE 
                WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1'
                WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2'
                WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3'
                ELSE 'Q4'
            END AS Quarter, 
            CustomerID, 
            ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 0) AS Total
        from OrderDetail od
        join Product p ON od.ProductID = p.ProductID
        group by Year, Quarter, CustomerID
    )
    select Quarter, CAST(Year AS INTEGER) AS Year, CustomerID, Total
    from SalesByQuarter
    order by Year, Quarter

       
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """
    
    With SalesByQuarter AS (
        select 
            strftime('%Y', OrderDate) AS Year, 
            CASE 
                WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1'
                WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2'
                WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3'
                ELSE 'Q4'
            END AS Quarter, 
            CustomerID, 
            ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 0) AS Total
        from OrderDetail od
        join Product p ON od.ProductID = p.ProductID
        group by Year, Quarter, CustomerID
    ),
    Top5Customers AS (
        select 
            Quarter, 
            CAST(Year AS INTEGER) AS Year, 
            CustomerID, 
            Total, 
            RANK() OVER (PARTITION BY Quarter, Year ORDER BY Total DESC) AS CustomerRank
        from SalesByQuarter
    )
    select Quarter, Year, CustomerID, Total,CustomerRank
    from Top5Customers
    where CustomerRank <= 5
    order by Year, Quarter
    

    
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """
    	With SalesByMonth AS (
			select 
				CASE 
					WHEN strftime('%m', OrderDate) = '01' THEN 'January'
					WHEN strftime('%m', OrderDate) = '02' THEN 'February'
					WHEN strftime('%m', OrderDate) = '03' THEN 'March'
					WHEN strftime('%m', OrderDate) = '04' THEN 'April'
					WHEN strftime('%m', OrderDate) = '05' THEN 'May'
					WHEN strftime('%m', OrderDate) = '06' THEN 'June'
					WHEN strftime('%m', OrderDate) = '07' THEN 'July'
					WHEN strftime('%m', OrderDate) = '08' THEN 'August'
					WHEN strftime('%m', OrderDate) = '09' THEN 'September'
					WHEN strftime('%m', OrderDate) = '10' THEN 'October'
					WHEN strftime('%m', OrderDate) = '11' THEN 'November'
					WHEN strftime('%m', OrderDate) = '12' THEN 'December'
				END AS Month, 
				sum(ROUND((p.ProductUnitPrice * od.QuantityOrdered))) AS Total
			from OrderDetail od
			join Product p ON od.ProductID = p.ProductID
			group by Month
		),
		 MonthlySalesRank AS (
			SELECT 
				Month, 
				Total, 
				RANK() OVER (ORDER BY Total DESC) AS TotalRank
			FROM SalesByMonth
		)
		select Month,Total,TotalRank
		from MonthlySalesRank
      
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """
    WITH OrderDates AS (
        SELECT 
			CustomerID, 
            OrderDate, 
            LAG(OrderDate, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousOrderDate,
            JULIANDAY(OrderDate) - JULIANDAY(LAG(OrderDate, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate)) AS DaysSinceLastOrder
        FROM 
            OrderDetail
			)
    
    SELECT 
        distinct od.CustomerID, 
        FirstName, 
        LastName, 
        Country, 
        OrderDate, 
        PreviousOrderDate, 
        max(DaysSinceLastOrder) AS MaxDaysWithoutOrder
    FROM OrderDates od   
    JOIN Customer ON od.CustomerID = Customer.CustomerID
	join country on Customer.CountryID =country.CountryID
	group by od.CustomerID
	order by MaxDaysWithoutOrder desc
    

    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

