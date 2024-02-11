"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import tempfile
import numpy as np
import pandas as pd
import ray
import typing
import util.judge_df_equal


@ray.remote
def process_orders_chunk(orders_chunk, valid_keys):
    # Filter orders based on valid_lineitems
    valid_orders_chunk = orders_chunk[orders_chunk['o_orderkey'].isin(valid_keys)]
    
    # Group by o_orderpriority and count
    grouped = valid_orders_chunk.groupby('o_orderpriority').size().reset_index(name='order_count')
    
    return grouped


def ray_q4(time: str, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    # Convert strings to datetime
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_commitdate'] = pd.to_datetime(lineitem['l_commitdate'])
    lineitem['l_receiptdate'] = pd.to_datetime(lineitem['l_receiptdate'])

    # Define the date range
    start_date = pd.to_datetime(time)
    end_date = start_date + pd.DateOffset(months=3)

    filtered_orders = orders[(orders['o_orderdate'] >= start_date) & (orders['o_orderdate'] < end_date)]

    # Identify lineitems with l_commitdate < l_receiptdate
    valid_lineitems = lineitem[lineitem['l_commitdate'] < lineitem['l_receiptdate']]['l_orderkey'].unique()
    
    # Broadcast the valid keys to all actors
    valid_keys_id = ray.put(valid_lineitems)

    # Split the orders DataFrame into chunks
    orders_chunks = np.array_split(filtered_orders, 4)  # The number of chunks can be adjusted

    # Distribute the computation across the chunks
    futures = [process_orders_chunk.remote(chunk, valid_keys_id) for chunk in orders_chunks]
    
    # Collect and combine the results
    partial_results = ray.get(futures)
    combined_results = pd.concat(partial_results).groupby('o_orderpriority').sum().reset_index()

    # Order by o_orderpriority
    ordered_results = combined_results.sort_values(by='o_orderpriority')
    ray.shutdown()
    return ordered_results



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q4("1993-9-01",orders,lineitem)
    result.to_csv("test2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q4.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
