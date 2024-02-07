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
import tempfile


def calculate_sum_disc_price(group):
    return (group['l_extendedprice'] * (1 - group['l_discount'])).sum()

def calculate_sum_charge(group):
    return (group['l_extendedprice'] * (1 - group['l_discount']) * (1 + group['l_tax'])).sum()



def pandas_q2(timediff, lineitem):
    #TODO: your codes begin
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    date = pd.Timestamp('1998-12-01') - pd.Timedelta(days=timediff)
    filtered_df = lineitem[lineitem['l_shipdate'] <= date]
    grouped = filtered_df.groupby(['l_returnflag', 'l_linestatus'])

    # Calculate each metric
    result_df = pd.DataFrame({
        'sum_qty': grouped['l_quantity'].sum(),
        'sum_base_price': grouped['l_extendedprice'].sum(),
        'sum_disc_price': grouped.apply(calculate_sum_disc_price),
        'sum_charge': grouped.apply(calculate_sum_charge),
        'avg_qty': grouped['l_quantity'].mean(),
        'avg_price': grouped['l_extendedprice'].mean(),
        'avg_disc': grouped['l_discount'].mean(),
        'count_order': grouped.size()
    }).reset_index()

    result_df = result_df.sort_values(by=['l_returnflag', 'l_linestatus'])
    
    return result_df

    #end of your codes



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
    result = pandas_q2(90, lineitem)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/pandas_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")


