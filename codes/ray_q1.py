"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:47
 	 LastEditTime: 2023-09-11 10:37:24
 	 FilePath: /codes/pandas_q1.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import time
import numpy as np


@ray.remote
def process_chunk(chunk, start_date, end_date):
    #_ = [np.sin(x) for x in range(10000)] 
    chunk['l_shipdate'] = pd.to_datetime(chunk['l_shipdate'])

    filtered_chunk = chunk[
        (chunk['l_shipdate'] >= start_date) &
        (chunk['l_shipdate'] < end_date) &
        (chunk['l_discount'] >= 0.06 - 0.01) &
        (chunk['l_discount'] <= 0.06 + 0.010001) &
        (chunk['l_quantity'] < 24)
    ]
    revenue = (filtered_chunk['l_extendedprice'] * filtered_chunk['l_discount']).sum()
    return revenue


def ray_q1(time_1: str, lineitem:pd.DataFrame) -> float:
    
    ray.init(ignore_reinit_error=True)

    start_date = pd.to_datetime(time_1)
    end_date = start_date + pd.DateOffset(years=1)
    

    
    # Divide the data
    chunks = np.array_split(lineitem, 4)  # Adjust the number of chunks as needed
    
    # Distribute the chunks to the processing function
    futures = [process_chunk.remote(chunk, start_date, end_date) for chunk in chunks]
    
    # Gather and sum the results
    revenues = ray.get(futures)
    total_revenue = sum(revenues)
    ray.shutdown()

    return total_revenue


if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # run the test
    replication_factor = 10
    lineitem_large = pd.concat([lineitem]*replication_factor, ignore_index=True)
    lineitem_large['l_shipdate'] = pd.date_range(start='1992-01-01', periods=len(lineitem_large), freq='T')



    start_time = time.time()
    result = ray_q1("1994-01-01", lineitem)
    print("duration =", time.time() - start_time)


    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")
