# -*- coding: utf-8 -*-
"""
Created on Thu May  6 10:47:27 2021

@author: HYU
"""

import psycopg2
from tqdm import tqdm

conn = psycopg2.connect("host=localhost dbname=Gyeonggi_taxi user=postgres password=Stl200301")
cur = conn.cursor()

for i in tqdm(range(171201,171232)):
    with open("Z:\\STL_LAB_NAS\\DTG\\GYEONGGI\\Taxi\\merge\\" + str(i) +".csv") as f_i:
        next(f_i)
        cur.copy_from(f_i, 'dtg_t_' + str(i), sep=',')
        conn.commit()

conn.close()