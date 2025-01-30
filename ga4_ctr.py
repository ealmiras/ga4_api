from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest, Filter, FilterExpression, FilterExpressionList, NumericValue
import pandas as pd
import numpy as np
import os, sys, logging, pathlib, time
from datetime import datetime, timedelta

username = str(pathlib.Path.home())

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logging.info("Start downloadGA4data")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = username + '\\Desktop\\Config\\ga4-config.json'

client = BetaAnalyticsDataClient()

property_id = '***'
brand = '***'
success = False
max_attempts = 5 
attempts = 0
row_limit = 10000

def get_data(dimensions, metrics, start_date, end_date, dimension_filter = None, metric_filter = None):
    offset = 0
    all_rows = []

    if start_date > end_date:
        logging.info("No new data added!")
    
    else:
        while True:
            attempts = 0
            success = False

            while not success and attempts < max_attempts:
                attempts += 1        
                try:
                    if brand == '***':
                        
                        print(max(int(offset/10000) + 1, 1))
                        request = RunReportRequest(
                            property=f"properties/{property_id}",
                            dimensions=dimensions,
                            metrics=metrics,
                            date_ranges=[DateRange(start_date = datetime.strftime(start_date,'%Y-%m-%d'),
                                                    end_date = datetime.strftime(end_date,'%Y-%m-%d'))],
                            dimension_filter = dimension_filter,
                            metric_filter = metric_filter,
                            offset = offset,
                            limit = row_limit
                        )

                        response = client.run_report(request)
                        success = True      
                    
                except Exception as exception:
                    print('GA4 API did not respond, trying again')
                    print(exception)
                    time.sleep(3 ** attempts)
                    if attempts == max_attempts:
                        print('Error - GA4 API did not respond')

            if success:
                for row in response.rows:
                    row_data = []

                    for dimension_value in row.dimension_values:
                        row_data.append(dimension_value.value)
                    for metric_value in row.metric_values:
                        row_data.append(metric_value.value)
        
                    all_rows.append(row_data)

                if len(response.rows) < row_limit:
                    break

                offset += row_limit
            
            else:
                print('Failed to retrive data from GA4 API')
                break
    
    return all_rows

items_viewed = FilterExpression(
    filter=Filter(
        field_name='screenPageViews',
        numeric_filter=Filter.NumericFilter(
            value=NumericValue(int64_value=500),
            operation=Filter.NumericFilter.Operation.GREATER_THAN
        )
    )
)

view_filter = FilterExpression(
    filter=Filter(
        field_name='itemsViewed',
        numeric_filter=Filter.NumericFilter(
            value=NumericValue(int64_value=0),
            operation=Filter.NumericFilter.Operation.GREATER_THAN
        )
    )
)

cust_id_filter = FilterExpression(
    filter=Filter(
        field_name = 'customUser:customer_id',
        string_filter=Filter.StringFilter(
            value = '(not set)',
            match_type = Filter.StringFilter.MatchType.EXACT
        )
    )
)

platform_filter = FilterExpression(
    not_expression = FilterExpression(
        filter=Filter(
            field_name = 'platform',
            string_filter=Filter.StringFilter(
                value="iOS",
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        )
    )
)

resolution_filter = FilterExpression(
    not_expression = FilterExpression(
        filter=Filter(
            field_name = 'screenResolution',
            string_filter=Filter.StringFilter(
                value='800x600',
                match_type=Filter.StringFilter.MatchType.EXACT
            )
        )
    )
)

bot_filter = FilterExpression(
    and_group=FilterExpressionList(expressions=[platform_filter, resolution_filter])) 


## ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
start_date = datetime(2024, 5, 1).date()
end_date = datetime.today().date() - timedelta(days=2)

## ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## BOT Data
print('Loading BOT customers!')

dimensions_bot1 = [Dimension(name="date"), Dimension(name="customUser:customer_id"), Dimension(name="customEvent:list_category_id")]
metrics_bot1 = [Metric(name="screenPageViews")]
column_names1 = ["Date", "CustomerID", "list_category_id", "PageView"]

all_rows_bot1 = get_data(dimensions=dimensions_bot1, metrics=metrics_bot1, start_date=start_date, end_date=end_date, metric_filter=items_viewed, dimension_filter=cust_id_filter)
bot_df1 = pd.DataFrame(all_rows_bot1, columns = column_names1)
bot_df1['key'] = bot_df1['Date'] + bot_df1['CustomerID'] + bot_df1['list_category_id']
bots1 = set(bot_df1['key'])

time.sleep(2)  

# --
dimensions_bot2 = [Dimension(name="date"), Dimension(name="customUser:customer_id"), Dimension(name="screenResolution")]
metrics_bot2 = [Metric(name="screenPageViews")]
column_names2 = ["Date", "CustomerID", 'Resolution', "PageView"]

all_rows_bot2 = get_data(dimensions=dimensions_bot2, metrics=metrics_bot2, start_date=start_date, end_date=end_date, dimension_filter=resolution_filter)
bot_df2 = pd.DataFrame(all_rows_bot2, columns = column_names2)
bot_df2['PageView'] = bot_df2['PageView'].astype('int')
days_between = (datetime.strptime(max(bot_df2['Date']), "%Y%m%d").date() - datetime.strptime(min(bot_df2['Date']), "%Y%m%d").date()).days + 1

bot_df2 = bot_df2[['CustomerID', 'PageView']].groupby(['CustomerID']).sum().reset_index(drop=False)
bot_df2['AvgView'] = bot_df2['PageView'] / days_between
bot_criteria = (bot_df2['CustomerID'] != '(not set)') & (bot_df2['AvgView'] >= 1000)
bot_df2['bot_flag'] = np.where(bot_criteria, 'Y', 'N')
bot_df2 = bot_df2.loc[bot_df2['bot_flag'] == 'Y']
bots2 = set(bot_df2['CustomerID'])
print(bots2)

time.sleep(2)

## ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## CTR Data

filename = username + '\\***\\ga_ctr_data.csv'

ctr_db = pd.read_csv(filename, parse_dates=[0], header=None)
last_date = max(ctr_db[0]).date()
first_date = min(ctr_db[0]).date()

if start_date <= first_date:
    end_date_ctr = first_date - timedelta(days=1)
    start_date_ctr = start_date

if end_date > last_date:
    start_date_ctr = last_date + timedelta(days=1)
    end_date_ctr = end_date

print('CTR data dates between: ', start_date_ctr, end_date_ctr)

print('Loading CTR data!')
dimensions_ctr = [Dimension(name="date"), Dimension(name="ItemId"), Dimension(name="customEvent:list_category_id"), Dimension(name="customUser:customer_id")]
metrics_ctr = [Metric(name="itemsClickedInList"), Metric(name="itemsViewedInList"), Metric(name="itemsAddedToCart"), Metric(name="itemsPurchased")]
column_names = ["Date", "Item ID", "list_category_id", "customerId", "Items clicked in list", "Items viewed in list", "Added to cart", "Purchased"]

all_rows_ctr = get_data(dimensions=dimensions_ctr, metrics=metrics_ctr, start_date=start_date_ctr, end_date=end_date_ctr)
ctr_df = pd.DataFrame(all_rows_ctr, columns= column_names)

ctr_df = ctr_df.astype({'Items clicked in list': 'int32', 'Items viewed in list': 'int32', 'Added to cart': 'int32', 'Purchased': 'int32'})

ctr_df['key1'] = ctr_df['Date'] + ctr_df['customerId'] + ctr_df['list_category_id']
ctr_df['bot1'] = ctr_df['key1'].isin(bots1).astype(int)
ctr_df['bot2'] = ctr_df['customerId'].isin(bots2).astype(int)
ctr_df = ctr_df.loc[(ctr_df['bot1'] == 0)]

ctr_df = ctr_df[['Date', "Item ID", "list_category_id", 'Items clicked in list', 'Items viewed in list', "Added to cart", "Purchased"]].groupby(['Date', "Item ID", "list_category_id"]).sum().reset_index(drop=False)

print(ctr_df[['Items clicked in list', 'Items viewed in list']].sum())
time.sleep(2) 

ctr_df.to_csv(filename, encoding='utf-8', index=False, header=False, mode='a') #

## ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## ADD TO CART & PURCHASED

filename = username + '\\***\\ga_purchase_data.csv'

cart_db = pd.read_csv(filename, parse_dates=[0], header=None)
last_date = max(cart_db[0]).date()
first_date = min(cart_db[0]).date()

if start_date <= first_date:
    end_date_cart = first_date - timedelta(days=1)
    start_date_cart = start_date

if end_date > last_date:
    start_date_cart = last_date + timedelta(days=1)
    end_date_cart = end_date

print('Purchase data dates between: ', start_date_cart, end_date_cart)

print('Loading Add to cart/Purchased data!')
dimensions_purc = [Dimension(name="date"), Dimension(name="ItemId"), Dimension(name="customUser:customer_id")]
metrics_purc = [Metric(name="itemsViewed"), Metric(name="itemsAddedToCart"), Metric(name="itemsPurchased")]
column_names = ['Date', 'Item ID', 'customerId', 'Items viewed', 'Added to cart', 'Purchased']

all_rows_purc = get_data(metrics=metrics_purc, dimensions=dimensions_purc, start_date=start_date_cart, end_date=end_date_cart, dimension_filter=platform_filter)
purchase_df = pd.DataFrame(all_rows_purc, columns=column_names)

purchase_df = purchase_df.astype({'Items viewed': 'int32', 'Added to cart': 'int32', 'Purchased': 'int32'})
purchase_df['key'] = purchase_df['Date'] + purchase_df['customerId']
purchase_df['funnel_check'] = np.where((purchase_df['Items viewed'] >= purchase_df['Added to cart']) & (purchase_df['Added to cart'] >= purchase_df['Purchased']), "Y", "N")
purchase_df['key'] = purchase_df['Date'] + purchase_df['customerId']

purchase_df['bot'] = purchase_df['customerId'].isin(bots2).astype(int)
 
purchase_df = purchase_df.loc[(purchase_df['bot'] == 0)& (purchase_df['funnel_check'] == "Y") & (purchase_df['Items viewed'] > 0)]
print(purchase_df[['Added to cart', 'Purchased']].sum())

purchase_df = purchase_df[['Date', 'Item ID', 'Items viewed', 'Added to cart', 'Purchased']].groupby(['Date', 'Item ID']).sum().reset_index(drop=False)

purchase_df.to_csv(filename, encoding='utf-8', index=False, header=False, mode='a') #

logging.info(" Finish download GA4sessions")