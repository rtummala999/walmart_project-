#!/usr/bin/env python
# coding: utf-8

# In[2]:


#RAMGOPAL PUSID:918450087
import psycopg2
import pandas as pd


# In[3]:


#connecting to postgres sql
conn =psycopg2.connect(database="postgres",user="postgres",password="Chinnu.969",host="localhost",port="5432")


# In[4]:


#checking the connection
sql='''SELECT datname FROM pg_database'''
conn.autocommit=True
cursor=conn.cursor()
cursor.execute(sql)


# In[18]:


#prinitng results 
for i in cursor.fetchall():
    print(i)


# ## 1.Find the sales of the department beds on date 2010-10-15 only store_type A

# In[14]:



sql='''SELECT s.store_id,s.week_sales,d.department,st.type_id
FROM department d 
INNER JOIN sales s ON d.department_id=s.dept_id 
INNER JOIN store st ON s.store_id=st.store_id 
WHERE d.department_id=(select department_id from department where department ='beds') and st.type_id='A' AND s.date='2010-10-15' '''
cursor.execute(sql)

for i in cursor.fetchall():
    print(i)


# # 2. Get the avg unemployment during the working days from all store_type village 

# In[12]:



sql='''SELECT AVG(unemployment),e.store_id,st.type_id
FROM unemployment e
INNER JOIN isholiday da ON e.date=da.date
INNER JOIN store st ON st.store_id=e.store_id 
WHERE da.holiday='f' and st.type_id=(select distinct(st.type_id) from store st join store_type stt on st.type_id=stt.type_id where type='village')
GROUP BY e.store_id,st.type_id ,da.holiday
ORDER BY e.store_id
'''


# In[13]:


cursor.execute(sql)
for i in cursor.fetchall():
    print(i)


# In[ ]:




