"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import util.judge_df_equal
import numpy as np
import tempfile

ray.init(ignore_reinit_error=True)


def calculate_sum_disc_price(group):
    """Calculate the sum of discounted prices."""
    return (group['l_extendedprice'] * (1 - group['l_discount'])).sum()

def calculate_sum_charge(group):
    """Calculate the sum of charged prices after discount and tax."""
    return (group['l_extendedprice'] * (1 - group['l_discount']) * (1 + group['l_tax'])).sum()

@ray.remote
def process_chunk(chunk, date):
    filtered_chunk = chunk[chunk['l_shipdate'] <= date]
    grouped = filtered_chunk.groupby(['l_returnflag', 'l_linestatus'])
    
    result_chunk = pd.DataFrame({
        'sum_qty': grouped['l_quantity'].sum(),
        'sum_base_price': grouped['l_extendedprice'].sum(),
        'sum_disc_price': grouped.apply(calculate_sum_disc_price),
        'sum_charge': grouped.apply(calculate_sum_charge),
        'avg_qty': grouped['l_quantity'].mean(),
        'avg_price': grouped['l_extendedprice'].sum(),
        'avg_disc': grouped['l_discount'].mean(),
        'count_order': grouped.size()
    }).reset_index()

    return result_chunk

def aggregate_results(futures):

    partial_results = ray.get(futures)
    combined_result = pd.concat(partial_results, ignore_index=True)
    
    # Need to aggregate again since the same groups may be split across different chunks
    final_grouped = combined_result.groupby(['l_returnflag', 'l_linestatus'])
    
    final_result = pd.DataFrame({
        'sum_qty': final_grouped['sum_qty'].sum(),
        'sum_base_price': final_grouped['sum_base_price'].sum(),
        'sum_disc_price': final_grouped['sum_disc_price'].sum(),
        'sum_charge': final_grouped['sum_charge'].sum(),
        'avg_qty': final_grouped['avg_qty'].mean(),
        'avg_price': final_grouped['avg_price'].sum() / final_grouped['count_order'].sum(),
        'avg_disc': final_grouped['avg_disc'].mean(),  # Note: This is an approximation
        'count_order': final_grouped['count_order'].sum()
    }).reset_index().sort_values(by=['l_returnflag', 'l_linestatus'])

    return final_result


def ray_q2(timediff:int, lineitem:pd.DataFrame) -> pd.DataFrame:
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    cutoff_date = pd.Timestamp('1998-12-01') - pd.Timedelta(days=timediff)
    
    chunks = np.array_split(lineitem, 2)  # Adjust the number of chunks based on your setup
    
    futures = [process_chunk.remote(chunk, cutoff_date) for chunk in chunks]
    
    final_result = aggregate_results(futures)
    
    ray.shutdown()
    return final_result.reset_index(drop=True)



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
    result = ray_q2(90, lineitem)
    #result.to_csv("test.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")


