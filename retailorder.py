import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import matplotlib.pyplot as plt

#Database connection
def get_db_connection():
    conn = psycopg2.connect(
    dbname="mp1",
    user="postgres",
    password="awsrootkey",
    host="retaildb.c9uucaq6k26m.ap-south-1.rds.amazonaws.com",
    port="5432" 
) 
    return conn


intro=st.sidebar.radio('Main Menu',["Home page","Retail Order's Business Analysis"])
if intro=='Home page':
    #project introduction
    st.title("RETAIL ORDER DATA ANALYSIS")
    st.subheader("Overview of Retail Order Data trends using Kaggle API, Google colab,Postgresql and streamlit")
     
    st.image("""C:/Users/91701/Downloads/DALL·E 2024-12-24 02.37.01 - A professional and detailed visualization of retail sales data analysis, showcasing bar charts, line graphs, and pie charts representing key sales met.webp""")

    st.write("""
             In this project, the objective is to evaluate and enhance sales performance by uncovering critical patterns, 
             high-performing products, and potential areas for expansion using a dataset of sales transactions. 
             The aim is to extract valuable insights that can support strategic decisions and foster business growth.
             """)
    
    st.write("""
             **Tech Stack and Tools Applied:**
             - **Kaggle API**: For getting retail order dataset directly from Kaggle.
             - **Python**: Utilized for data extraction, cleaning, and analysis.
             - **SQL**: Used for storing and querying the cleaned dataset.
             - **Streamlit**: For building a real-time, interactive dashboard to display insights.
             """)
    #project goals
    st.write("""
             1. **Identify Top-Performing Products**: Analyze the products and categories that contribute the most to revenue and profit.
        2. **Sales Trend Analysis**: Perform Year-over-Year (YoY) and Month-over-Month (MoM) comparisons to identify sales trends.
        3. **Profit Margin Analysis**: Highlight the subcategories with the highest profit margins to guide future sales strategies.
        4. **Regional Analysis**: Explore sales data by region to identify high-performing areas.
        5. **Discount Impact**: Analyze the effects of discounts on product sales, especially those with more than 20 percentage off.
    """)

    st.write("""
        This project integrates multiple components:
        - **Data Extraction**: Using Kaggle API to fetch raw sales data.
        - **Data Cleaning**: Applying transformations to standardize and prepare the data for analysis.
        - **SQL Database Integration**: Moving the cleaned data into SQL for efficient querying and analysis.
        - **Business Insights**: Extracting valuable insights using SQL queries to answer key business questions.
        - **Interactive Dashboard**: Building a real-time dashboard in Streamlit to visualize the findings.
    """)

    # Tools Overview
    st.write("""
        **Streamlit Features for This Project:**
        
        1. **Real-Time Data Queries**: Query your SQL database and display live results directly in the Streamlit interface.
        2. **Dynamic Data Filtering**: Allow users to filter and analyze the dataset based on various criteria such as product category, region, and sales trends.
        3. **Visualization**: Use charts and tables to present insights visually for better interpretation and decision-making.
    """)

    # Brief Explanation of the Flow
    st.subheader("Project Flow")
    st.write("""
        1. **Data Extraction**: Fetch raw data from Kaggle API.
        2. **Data Cleaning**: Clean the dataset to handle missing values, standardize column names, and derive new columns (e.g., discount, sale price).
        3. **SQL Integration**: Load cleaned data into a SQL database for efficient querying.
        4. **SQL Queries**: Run advanced queries to generate business insights and trends.
        5. **Streamlit Dashboard**: Build an interactive dashboard to display the results dynamically.
    """)

    # Conclusion
    st.write("""
        This project aims to provide meaningful insights into the retail order data, helping businesses make informed decisions about 
        their products, sales strategies, and market opportunities. By combining data extraction, cleaning, SQL analysis, and Streamlit 
        visualization, we offer a comprehensive approach to analyzing retail sales performance.
    """)
else:
    # Business Analysis Section
    st.title("Retail Order's Business Analysis")
    st.subheader("Explore the detailed business insights of retail orders")

    

    def top_10_highest_revenue_generating_products():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT products1.product_id, SUM(products1.sale_price * products1.quantity) AS total_revenue
        FROM products1 GROUP BY products1.product_id ORDER BY total_revenue DESC LIMIT 10"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Product ID", "Total Revenue"])
        return df
    
    def Top_5_Cities_Highest_Profit():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.city, SUM(products1.profit) AS total_profit FROM orders1
        JOIN products1 ON orders1.order_id = products1.order_id GROUP BY orders1.city ORDER BY 
        total_profit DESC LIMIT 5"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["City", "Total Profit"])
        return df
    
    def Total_Discount_given_foreach_category():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.category, SUM(products1.discount) AS total_discount FROM 
        orders1 JOIN products1 ON orders1.order_id = products1.order_id GROUP BY orders1.category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Category", "Total Discount"])
        return df
    
    def Average_sale_price_per_product():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.category, AVG(products1.sale_price) AS avg_sale_price FROM orders1 JOIN 
        products1 ON orders1.order_id = products1.order_id GROUP BY orders1.category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Category", "Average Sale Price"])
        return df
    
    def Region_with_Highest_Average_Sale_Price():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.region, AVG(products1.sale_price) AS avg_sale_price FROM orders1
        JOIN products1 ON orders1.order_id = products1.order_id GROUP BY orders1.region 
        ORDER BY avg_sale_price DESC LIMIT 1"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Region", "Average Sale Price"])
        return df
    
    def Total_Profit_Per_Category():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.category, SUM(products1.profit) AS total_profit FROM 
        orders1 JOIN products1 ON orders1.order_id = products1.order_id GROUP BY orders1.category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Category", "Total Profit"])
        return df
    
    def Top_3_Segments_with_Highest_Quantityof_Orders():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.segment, SUM(products1.quantity) AS total_quantity FROM 
        orders1 JOIN products1 ON orders1.order_id = products1.order_id GROUP BY 
        orders1.segment ORDER BY total_quantity DESC LIMIT 3"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Segment", "Total Quantity"])
        return df
    
    def Average_Discount_Percentage_Given_per_Region():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.region, AVG(products1.discount_percent) AS avg_discount_percentage 
        FROM orders1 JOIN products1 ON orders1.order_id = products1.order_id GROUP BY orders1.region;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Region", "Average Discount Percentage"])
        return df
    
    def Product_Category_with_Highest_Total_Profit():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT orders1.category, SUM(products1.profit) AS total_profit FROM 
        orders1 JOIN products1 ON orders1.order_id = products1.order_id GROUP BY orders1.category ORDER BY 
        total_profit DESC LIMIT 1"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Category", "Total Profit"])
        return df
    
    def Total_Revenue_Generated_Per_Year():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT DATE_PART('year', o.order_date) AS year, SUM(p.sale_price * p.quantity) AS total_revenue 
        FROM orders1 o JOIN products1 p ON o.order_id = p.order_id 
        GROUP BY DATE_PART('year', o.order_date) ORDER BY year;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Year", "Total Revenue"])
        return df
    
    def popular_subcategory_by_total_quantity():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT sub_category, SUM(quantity) AS total_sold
        FROM products1 GROUP BY sub_category ORDER BY total_sold DESC LIMIT 1;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Sub_Category", "Total"])
        return df
    
    def orders_shipping_standardclass_mode():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT * FROM orders1 WHERE ship_mode = 'Standard Class';"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["OPrder_id","Order_date","Shipping_mode","Segment","Country","City","State","Postal_code","Region","Category","Sub_category"])
        return df    
    
    def total_no_of_products_soldby_categories():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT category, SUM(quantity) AS total_quantity FROM products1 GROUP BY category;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Category", "Total Quatity"])
        return df
    
    def products_with_discount_20_percent():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT * FROM products1 WHERE discount_percent > 20;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results)
        st.write("There is no such records")
        return df
    
    def total_revenue_for_each_region():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT o.region, SUM(p.sale_price * p.quantity) AS total_revenue FROM orders1 o 
        JOIN products1 p ON o.order_id = p.order_id GROUP BY  o.region;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Region", "Total Revenue"])
        return df
    
    def furniture_category_with_saleprice_500():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT * FROM products1 WHERE category = 'Furniture' AND sale_price > 500;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Product ID","Order ID","Category","Sub Category","Cost Price","List price","Quantity","Discount percent","Discount","Sale price","profit"])
        return df
    
    def total_discount_on_all_products():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT SUM(discount) AS total_discount FROM products1;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Total Discount"])
        return df
    
    def popular_subcategory_sold_quantity():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""SELECT sub_category, SUM(quantity) AS total_sold FROM products1 GROUP BY sub_category 
        ORDER BY total_sold DESC LIMIT 1;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Sub Category", "Total Sold"])
        return df
    
    def orders_in_descember2023():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""select * from orders1 where order_date between '2023-12-01' and '2023-12-31';"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Order ID","Order Date","Shipment Mode","Segment","Country","city","State","Posstal Code","Region","Category","Sub Category"])
        return df
    
    def frequently_ordered_product():
        conn = get_db_connection()
        mycursor=conn.cursor()
        query="""select product_id, category, sum(quantity) as total_quantity from products1
        group by product_id, category order by total_quantity desc limit 1"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        conn.close()
        df = pd.DataFrame(results, columns=["Product ID", "Category","Total Quantity"])
        return df
    
    col1, col2= st.columns([3,5])
    
    with col1:
        st.markdown(' #### Discover the leading products, categories, and regional insights driving growth')

    with col2:
        selected_question = st.selectbox("Select a Question to View Analysis", [
            "Find top 10 highest revenue generating products",
            "Find the top 5 cities with the highest profit margins",
            "Calculate the total discount given for each category",
            "Find the average sale price per product category",
            "Find the region with the highest average sale price",
            "Find the total profit per category",
            "Identify the top 3 segments with the highest quantity of orders",
            "Determine the average discount percentage given per region",
            "Find the product category with the highest total profit",
            "Calculate the total revenue generated per year"
        ])

        if selected_question == "Find top 10 highest revenue generating products":
            st.write(top_10_highest_revenue_generating_products())
        elif selected_question == "Find the top 5 cities with the highest profit margins":
            st.write(Top_5_Cities_Highest_Profit())
        elif selected_question == "Calculate the total discount given for each category":
            st.write(Total_Discount_given_foreach_category())
        elif selected_question == "Find the average sale price per product category":
            st.write(Average_sale_price_per_product())
        elif selected_question == "Find the region with the highest average sale price":
            st.write(Region_with_Highest_Average_Sale_Price())
        elif selected_question == "Find the total profit per category":
            st.write(Total_Profit_Per_Category())
        elif selected_question == "Identify the top 3 segments with the highest quantity of orders":
            st.write(Top_3_Segments_with_Highest_Quantityof_Orders())
        elif selected_question == "Determine the average discount percentage given per region":
            st.write(Average_Discount_Percentage_Given_per_Region())
        elif selected_question == "Find the product category with the highest total profit":
            st.write(Product_Category_with_Highest_Total_Profit())
        elif selected_question == "Calculate the total revenue generated per year":
            st.write(Total_Revenue_Generated_Per_Year())

    colA, colB = st.columns([3,5])
    
    with colA:
        st.markdown(' #### Discover the Basic Analytics: product performance, category trends, and regional patterns')
    with colB:
        own_queries = st.selectbox("Select a question to view analysis",[
            "Determine the most popular sub-category by total quantity sold",
            "Find all orders shipped using Standard Class mode",
            "Get the total number of products sold by category",
            "List all products with a discount greater than 20%",
            "Calculate the total revenue for each region",
            "Find all products in the Furniture category with a sale price greater than $500",
            "Calculate the total discount given on all products",
            "Determine the most popular sub-category by total quantity sold",
            "List all orders placed in December 2023",
            "Identify the most frequently ordered product",
            ])
        
        if own_queries =="Determine the most popular sub-category by total quantity sold":
            st.write(popular_subcategory_by_total_quantity())
        elif own_queries == "Find all orders shipped using Standard Class mode":
            st.write(orders_shipping_standardclass_mode())
        elif own_queries == "Get the total number of products sold by category":
            st.write(total_no_of_products_soldby_categories())
        elif own_queries == "List all products with a discount greater than 20%":
            st.write(products_with_discount_20_percent())
        elif own_queries == "Calculate the total revenue for each region":
            st.write(total_revenue_for_each_region())
        elif own_queries == "Find all products in the Furniture category with a sale price greater than $500":
            st.write(furniture_category_with_saleprice_500())
        elif own_queries == "Calculate the total discount given on all products":
            st.write(total_discount_on_all_products())
        elif own_queries == "Determine the most popular sub-category by total quantity sold":
            st.write(popular_subcategory_sold_quantity())
        elif own_queries == "List all orders placed in December 2023":
            st.write(orders_in_descember2023())
        elif own_queries == "Identify the most frequently ordered product":
            st.write(frequently_ordered_product())
    
    




        

        

        


















    






           
