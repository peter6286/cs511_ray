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


def pandas_q1(time: str, lineitem:pd.DataFrame) -> float:
    # TODO: your codes begin
    return -1
    # end of your codes




if __name__ == "__main__":
    # import the logger to output message
    # it is shuorong
    import logging
    logger = logging.getLogger()

    # read the data
    import pandas as pd
    from datetime import datetime
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    def pandas_q1(time,lineitem):
        start_date = datetime.strptime(time, '%Y-%m-%d')
        lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
        end_date = start_date.replace(year=start_date.year + 1)

        # create new data frame and filter the DataFrame
        filtered_df = lineitem[
        (lineitem['l_shipdate'] >= start_date) &
        (lineitem['l_shipdate'] < end_date) &
        (lineitem['l_discount'] >= 0.06 - 0.01) &
        (lineitem['l_discount'] <= 0.06 + 0.010001) &
        (lineitem['l_quantity'] < 24)]
        # Calculate the revenue
        revenue = (filtered_df['l_extendedprice'] * filtered_df['l_discount']).sum()

        return revenue
        
    # run the test
    result = pandas_q1("1994-01-01", lineitem)    
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")
    