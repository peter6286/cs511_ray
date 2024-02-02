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
import typing

import util.judge_df_equal

def aggregate_revenue(group):
    revenue = (group['l_extendedprice'] * (1 - group['l_discount'])).sum()
    return pd.Series({'revenue': revenue})


def pandas_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    # Convert date columns to datetime
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    
    # Filter DataFrames based on conditions
    filtered_customers = customer[customer['c_mktsegment'] == segment]
    filtered_orders = orders[orders['o_orderdate'] < pd.Timestamp('1995-03-15')]
    filtered_lineitem = lineitem[lineitem['l_shipdate'] > pd.Timestamp('1995-03-15')]
    
    # Merge DataFrames
    cust_orders = pd.merge(filtered_customers, filtered_orders, left_on='c_custkey', right_on='o_custkey')
    merged_df = pd.merge(cust_orders, filtered_lineitem, left_on='o_orderkey', right_on='l_orderkey')
    
    # Group by, aggregate, and calculate revenue
    grouped = merged_df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'])

    result = grouped.apply(aggregate_revenue).reset_index()

    # Order and select top 10
    result = result.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True]).head(10)
    
    return result

    #end of your codes



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
    result = pandas_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/pandas_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
