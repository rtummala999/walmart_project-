#!/usr/bin/env python
# coding: utf-8

# In[10]:


#RAMGOPAL PUSID:918450087
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio


# In[11]:


#connecting to postgres sql
conn =psycopg2.connect(database="postgres",user="postgres",password="Chinnu.969",host="localhost",port="5432")


# In[4]:


#checking the connection
sql='''SELECT datname FROM pg_database'''
conn.autocommit=True
cursor=conn.cursor()
cursor.execute(sql)


# In[38]:


df=sqlio.read_sql_query(sql,conn)
df


# ## 1.Find the sales of the department beds on date 2010-10-15 only store_type A

# In[22]:


import pandas as pd
sql='''SELECT s.store_id,s.week_sales,d.department,st.type_id
FROM department d 
INNER JOIN sales s ON d.department_id=s.dept_id 
INNER JOIN store st ON s.store_id=st.store_id 
WHERE d.department_id=(select department_id from department where department ='beds') and st.type_id='A' AND s.date='2010-10-15' '''

df=sqlio.read_sql_query(sql,conn)
df


# ## 2. Get the avg unemployment during the working days from all store_type village 

# In[23]:



sql='''SELECT AVG(unemployment),e.store_id,st.type_id
FROM unemployment e
INNER JOIN isholiday da ON e.date=da.date
INNER JOIN store st ON st.store_id=e.store_id 
WHERE da.holiday='f' and st.type_id=(select distinct(st.type_id) from store st join store_type stt on st.type_id=stt.type_id where type='village')
GROUP BY e.store_id,st.type_id ,da.holiday
ORDER BY e.store_id
'''
df=sqlio.read_sql_query(sql,conn)
df


# # 3 .Get the difference In weekly sales on date 2010-02-05 if store_id=1 and store_id =2 only department id with  5

# In[24]:


sql='''Select distinct(select week_sales from sales where store_id=1 and date='2010-02-05' and dept_id=5) as store_id1,
 (select week_sales from sales where store_id=2 and date='2010-02-05' and dept_id=5) as store_id2, 
(select week_sales from sales where store_id=1 and date='2010-02-05' and dept_id=5)-(select week_sales from sales where store_id=2 and date='2010-02-05' and dept_id=5) as difference
From sales;'''
df=sqlio.read_sql_query(sql,conn)
df


# # 4. Compare the stock price during the high sales and low sales dates and there difference  

# In[26]:


sql='''Select
(select st.value from stockvalue st where st.date=(select date from sales order by week_sales desc limit 1)) as low,
 (select st.date from stockvalue st where st.date=(select date from sales order by week_sales desc limit 1)) as date, 
 (select st.value as low from stockvalue st where st.date=(select date from sales order by week_sales asc limit 1)) as high, 
(select st.date from stockvalue st where st.date=(select date from sales order by week_sales asc limit 1)) as date,  
 (select st.value  from stockvalue st where st.date=(select date from sales order by week_sales desc limit 1)) - (select st.value as low from stockvalue st where st.date=(select date from sales order by week_sales asc limit 1)) as difference  ;
'''
df=sqlio.read_sql_query(sql,conn)
df


# # 5 Get all the sum of all products for each store with starting with A for each store 

# In[28]:


sql='''SELECT sum(s.week_sales),s.store_id,d.department From sales s join department d On s.dept_id=d.department_id  Where   d.department LIKE 'A%' or d.department LIKE 'a%' group by s.store_id,d.department;


'''
df=sqlio.read_sql_query(sql,conn)
df


# # 6. Get the stock values during holiday time 

# In[13]:


sql='''select s.value,ih.date,ih.holiday
from stockvalue s join isholiday ih
on s.date=ih.date
where ih.holiday='t';
'''
df=sqlio.read_sql_query(sql,conn)
df


# # 7. Get the temperature at store_id 20 during holidays 

# In[32]:


sql='''select un.temperature,un.store_id,ih.holiday,ih.date
from unemployment un join isholiday ih
on un.date=ih.date
where ih.holiday='t' and un.store_id=17;
'''
df=sqlio.read_sql_query(sql,conn)
df


# # 8. get the sales data of all department of city type during working days on 2010-02-12

# In[35]:


sql='''select s.week_sales,s.dept_id,s.store_id
from sales s join store st on s.store_id=st.store_id
where st.type_id=(select type_id from store_type where type='city') and s.date='2010-02-12';
'''
df=sqlio.read_sql_query(sql,conn)
df


# # Get the count  of each department with starting letters from a to z

# In[36]:


sql='''select count(*),substring(department,1,1) from department group by substring(department,1,1);
'''
df=sqlio.read_sql_query(sql,conn)
df


# In[ ]:





# # 10. Get the avg fuel price in all village  store during holidays 

# In[16]:


sql='''Select avg(fuel_price),ih.date
From unemployment un join store st 
On un.store_id=st.store_id
Join isholiday ih on ih.date=un.date
where st.type_id=(select type_id from store_type where type='village') and ih.holiday='t' group by un.date,ih.date;
'''
df=sqlio.read_sql_query(sql,conn)
df


# In[ ]:




