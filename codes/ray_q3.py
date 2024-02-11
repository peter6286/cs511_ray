"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import numpy as np
import typing

import util.judge_df_equal

ray.init(ignore_reinit_error=True)

def calculate_revenue(group):
    return pd.Series({
        'revenue': (group['l_extendedprice'] * (1 - group['l_discount'])).sum()
    })


@ray.remote
def process_chunk(orders_filtered, lineitem_chunk, cutoff_date):
    # Join the orders with the chunk of lineitem
    merged_df = pd.merge(orders_filtered, lineitem_chunk, left_on='o_orderkey', right_on='l_orderkey')
    
    # Filter lineitems further if necessary
    filtered_lineitem = merged_df[merged_df['l_shipdate'] > cutoff_date]

    if not filtered_lineitem.empty:
        # Group by and calculate revenue based on the condition
        grouped = filtered_lineitem.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).apply(calculate_revenue).reset_index()
        return grouped
    else:
        return pd.DataFrame(columns=['l_orderkey', 'o_orderdate', 'o_shippriority', 'revenue'])



def ray_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    # Preprocessing steps
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    cutoff_date = pd.Timestamp('1995-03-15')

    # Filter orders based on segment and date
    filtered_customers = customer[customer['c_mktsegment'] == segment]
    filtered_orders = pd.merge(filtered_customers, orders[orders['o_orderdate'] < cutoff_date], left_on='c_custkey', right_on='o_custkey')

    # Broadcast the filtered orders DataFrame
    orders_filtered_id = ray.put(filtered_orders)

    # Split the lineitem DataFrame into chunks
    lineitem_chunks = np.array_split(lineitem, 4)  # The number of chunks can be adjusted

    # Distribute the computation across the chunks
    futures = [process_chunk.remote(orders_filtered_id, chunk, cutoff_date) for chunk in lineitem_chunks]
    
    # Collect the results
    partial_results = ray.get(futures)
    
    # Combine the partial results and perform the final aggregation
    combined_results = pd.concat(partial_results, ignore_index=True) if partial_results else pd.DataFrame()
    combined_results = combined_results.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'], as_index=False).agg(
            revenue=('revenue', 'sum')
        )
    # Assuming calculate_revenue needs to be applied after grouping, not shown here
    final_result = combined_results.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True]).head(10)

    ray.shutdown()
    return final_result
    




if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")


    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q3('BUILDING', customer, orders, lineitem)
    #result.to_csv("test.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
