import pymongo
from pymongo import MongoClient, ReturnDocument
from datetime import datetime
from config import parameters as conf

# connection = MongoClient(host=conf["host"],
#                          port=conf["port"],
#                          w=conf["write_concern"],
#                          readConcernLevel=conf["read_concern"])
#db = connection[conf["database"]]
db = [] # for not trying to connnect not exist server
###############################################################################
#
# Utility Function(s)
#
###############################################################################
import pprint

def output(dictionary):
    output_form = conf["output_form"]
    if output_form == "RAW_PRINT":
        print(dictionary)
        return None
    elif output_form == "PRETTY_PRINT":
        pprint.pprint(dictionary)
        return None
    elif output_form == "SILENT":
        return
    elif output_form == "NONE":
        return dictionary

def flatten(array):
    # Equivalent to:
    #
    # for sublist in array:
    # for item in sublist:
    #     flat_list.append(item)
    flat_list = [item for sublist in array for item in sublist]
    return flat_list

###############################################################################
#
# TRANSACTION 1
#
# Comment: Assume items is a list of items in the order
#
###############################################################################
def new_order_transaction(c_id, w_id, d_id, M, items, session):
    districts = session.district
    customers = session.customer
    stocks = session.stock
    orders = session.orders

    # Read and update district
    district = districts.find_one_and_update({"w_id": w_id, "d_id": d_id}, {'$inc': {"d_next_o_id": 1}})
    district_id = "%02d" % d_id
    o_id = district['d_next_o_id']
    w_tax = district['w_tax']
    d_tax = district['d_tax']

    # Retrieve customer info
    customer = customers.find_one({"w_id": w_id, "d_id": d_id, "c_id": c_id}, projection = {'_id':False})
    c_name = customer['c_name']
    c_last = c_name['c_last']
    c_credit = customer['c_credit']
    c_discount = customer['c_discount']

    # Prepare the new order
    o_entry_d = datetime.utcnow()
    order = {
        "w_id": w_id,
        "d_id": d_id,
        "o_id": o_id,
        "c_id": c_id,
        "o_entry_d": o_entry_d,
        "o_ol_cnt": M,
        "c_name": c_name
    }
    orderlines = []
    total_amount = 0.0
    ordered_items = []
    ordered_items_info = []
    popular_items = []
    popular_item_qty = 0
    popular_items_name = []
    is_all_local = True

    # Prepare an orderline for each item
    for ol_number in range(0, M):
        item = items[ol_number]
        ol_i_id = item[0]
        ol_supply_w_id = item[1]
        ol_quantity = item[2]
        # Retrieve stock and item information
        stock = stocks.find_one({"w_id": ol_supply_w_id, "i_id": ol_i_id})
        s_quantity = stock['s_quantity']
        i_name = stock['i_name']
        i_price = stock['i_price']

        ol_dist_info = stock['district_info']['s_dist_' + district_id]
        ol_amount = ol_quantity * i_price
        # Update stock
        adjusted_qty = s_quantity - ol_quantity
        adjusted_qty = adjusted_qty + 100 if adjusted_qty < 10 else adjusted_qty
        is_remote = 1 if ol_supply_w_id != w_id else 0
        is_all_local = False if is_remote == 1 else is_all_local
        change_in_qty = adjusted_qty - s_quantity
        stocks.update_one(
            {"w_id": ol_supply_w_id, "i_id": ol_i_id},
            {
                "$inc": {
                    "s_ytd": ol_quantity, 
                    "s_order_cnt": 1, 
                    "s_remote_cnt": is_remote, 
                    "s_quantity": change_in_qty
                }
            }
        )
        s_quantity = adjusted_qty
        # Update popular item and ordered_items
        if (ol_quantity > popular_item_qty):
            popular_item_qty = ol_quantity
            popular_items = [ol_i_id]
            popular_items_name = [i_name]
        elif (ol_quantity == popular_item_qty):
            popular_items.append(ol_i_id)
            popular_items_name.append(i_name)

        ordered_items.append(ol_i_id)
        ordered_item_info = {
            'item_number': ol_i_id,
            'i_name': i_name,
            'supplier_warehouse': ol_supply_w_id,
            'quantity': ol_quantity,
            'ol_amount': ol_amount,
            's_quantity': s_quantity
        }
        ordered_items_info.append(ordered_item_info)

        # Add new order line
        orderline = {
            "ol_number": ol_number,
            "ol_i_id": ol_i_id,
            "ol_amount": ol_amount,
            "ol_supply_w_id": ol_supply_w_id,
            "ol_quantity": ol_quantity,
            "ol_dist_info": ol_dist_info
        }
        orderlines.append(orderline)
        total_amount += ol_amount

    # Update order information
    final_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
    order["o_total_amt"] = final_amount
    order["orderline"] = orderlines
    order["popular_items"] = popular_items
    order["popular_items_name"] = popular_items_name
    order["ordered_items"] = ordered_items
    order["popular_item_qty"] = popular_item_qty
    order["o_all_local"] = is_all_local

    # Insert order
    orders.insert_one(order)

    # Process output
    result = {
        'w_id': w_id,
        'd_id': d_id,
        'c_id': c_id,
        'c_last': c_last,
        'c_credit': c_credit,
        'c_discount': c_discount,
        'w_tax': w_tax,
        'd_tax': d_tax,
        'o_id': o_id,
        'o_entry_d': o_entry_d,
        'num_items': M,
        'total_amount': final_amount,
        'ordered_item': ordered_items_info
    }
    return output(result)

###############################################################################
#
# TRANSACTION 2
#
###############################################################################
def payment_transaction(c_w_id, c_d_id, c_id, payment, session):
    customers = session.customer
    districts = session.district
    warehouses = session.warehouse
    
    result = {}
    # Read and update Warehouse
    warehouse = warehouses.find_one_and_update(
        {"w_id": c_w_id},
        {"$inc": {"w_ytd": payment}},
        projection = {'w_address': True, '_id': False}
    )
    result.update(warehouse)
    # Read and update District
    district = districts.find_one_and_update(
        {"w_id": c_w_id, "d_id": c_d_id},
        {"$inc": {"d_ytd": payment}},
        projection = {'d_address': True, '_id': False}
    )
    result.update(district)
    # Read and update Customer
    customer = customers.find_one_and_update(
        {"w_id": c_w_id, "d_id": c_d_id, "c_id": c_id},
        {
            "$inc": {
                "c_balance": -payment,
                "c_ytd_payment": payment,
                "c_payment_cnt": 1
            }
        },
        projection = {
            'c_ytd_payment': False,
            'c_payment_cnt': False,
            'c_delivery_cnt': False,
            'c_data': False,
            '_id': False
        },
        return_document=ReturnDocument.AFTER
    )
    result.update(customer)

    return output(result)

###############################################################################
#
# TRANSACTION 3
#
###############################################################################
def delivery_transaction(w_id, carrier_id, session):
    for d_id in range(1, 11):
        timestamp = datetime.utcnow()
        orders = session.orders.find_one_and_update(
            {"w_id": w_id, "d_id": d_id, "o_carrier_id": None},
            {"$set": {"o_carrier_id": carrier_id, "o_delivery_d": timestamp}},
            sort = [('o_id', 1)],
            projection = {"o_id": 1, "ol_amount": 1, "c_id": 1, "o_total_amt": 1, "_id": 0}
        )
        if orders is None:
            continue
        o_total_amt = orders['o_total_amt']
        c_id = orders['c_id']

        #update customer table
        session.customer.update(
            {"w_id": w_id, "d_id": d_id,"c_id": c_id},
            { "$inc": {"c_delivery_cnt": 1, "c_balance": o_total_amt}}
        )


###############################################################################
#
# TRANSACTION 4
#
###############################################################################
def order_status_transaction(c_w_id, c_d_id, c_id, session):
    result = {}
    #1. get last order of a customer
    orders = session.orders.find(
        {"w_id": c_w_id, "d_id": c_d_id, "c_id": c_id},
        {"o_id": 1, "orderline": 1, "o_delivery_d": 1, "o_entry_d": 1, "o_carrier_id":1, '_id': 0}
    ).sort([("o_id", -1)]).limit(1)
    if orders.count() <= 0:
        return result
    #2. get the customer info from customer table
    customers = session.customer.find(
        {"w_id": c_w_id, "d_id": c_d_id, "c_id": c_id},
        {"c_name": 1, "c_balance": 1}
    ).limit(1)
    #3. get orderline info
    #print(orders[0])
    ol_delivery_d = orders[0].get('o_delivery_d', None)
    o_items = {}
    for each_orderline in orders[0]['orderline']:
        o_items[each_orderline['ol_number']] = {}
        o_items[each_orderline['ol_number']]['ol_i_id'] = each_orderline['ol_i_id']
        o_items[each_orderline['ol_number']]['ol_supply_w_id'] = each_orderline['ol_supply_w_id']
        o_items[each_orderline['ol_number']]['ol_quantity'] = each_orderline['ol_quantity']
        o_items[each_orderline['ol_number']]['ol_delivery_d'] = ol_delivery_d
    result['items'] = o_items
    result['c_name'] = customers[0]['c_name']
    result['c_balance'] = customers[0]['c_balance']
    result['o_id'] = orders[0]['o_id']
    result['o_entry_d'] = orders[0]['o_entry_d']
    result['o_carrier_id'] = orders[0].get('o_carrier_id', None)
    return output(result)


###############################################################################
#
# TRANSACTION 5
#
###############################################################################
def stock_level_transaction(w_id, d_id, T, L, session):
    # 1. Select last L orders of a district from the order table
    orders = session.orders.find(
        {"w_id": w_id, "d_id": d_id},
        {"_id": 0, "ordered_items": 1}
    ).sort([("o_id", -1)]).limit(L)
    # 2. Find the set of items in all the orderlines
    if orders.count() <= 0:
        return {}
    all_item_id = set()
    for order in orders:
        all_item_id = all_item_id | set(list(order["ordered_items"]))
    # 3. Look up warehouse table to check for stock level of
    #    each item in the set
    all_item_id = list(all_item_id)
    stocks_below_threshold = session.stock.find(
        {"w_id": w_id, "i_id": {"$in": all_item_id}, "s_quantity": {"$lt": T}}
    ).count()
    result = {'number in S': stocks_below_threshold}
    return output(result)


###############################################################################
#
# TRANSACTION 6
#
###############################################################################
def popular_item_transaction(i, w_id, d_id, L, session):
    main_output = {"w_id": w_id, "d_id": d_id, "L": L}
    # 1. Select last L orders of a district from the order table
    results = session.orders.find({"w_id": w_id, "d_id": d_id},
                             {"_id": 0,
                              "c_name": 1,
                              "o_id": 1,
                              "o_entry_d": 1,
                              "popular_items": 1,
                              "popular_items_name": 1,
                              "popular_item_qty": 1,
                              "ordered_items": 1})\
                      .sort([("o_id", -1)])\
                      .limit(L)
    number_of_orders = results.count()
    if number_of_orders <= 0:
        return {}
    orders = []
    # Following lists will be used to calculate part II
    orders_i_id = []
    popular_i_id = []
    popular_i_name = []
    for result in results:
        number_of_orders += 1
        # 2. Obtain last L order's information
        order = {
            "o_id": result["o_id"],
            "o_entry_d": result["o_entry_d"],
            "c_name": result["c_name"],
            "popular_item": {
                "i_name": result["popular_items_name"],
                "ol_quantity": result["popular_item_qty"]
            }
        }
        orders.append(order)
        # 3. Get popular items raw data
        orders_i_id.append(result["ordered_items"])
        popular_i_id.append(result["popular_items"])
        popular_i_name.append(result["popular_items_name"])
    # 3a. Get the popular item percentage count
    popular_percentage = []
    # 3b. Create distinct popular id with their name
    popular_i_id = flatten(popular_i_id)
    popular_i_name = flatten(popular_i_name)
    popular_items = list(zip(popular_i_id, popular_i_name))
    distinct_popular_item = list(set(popular_items))
    # 3c. Perform percentage count
    raw_count = [[(item_id in single_ordered_items)
                  for single_ordered_items in orders_i_id].count(True)
                 for item_id, item_name in distinct_popular_item]
    popular_percentage = [{"i_name": item[1],
                           "percentage": float(item_count) / number_of_orders}
                          for item, item_count in
                          zip(distinct_popular_item, raw_count)]
    # Append result to main_output
    main_output["orders"] = orders
    main_output["popular_percentage"] = popular_percentage
    return(output(main_output))


###############################################################################
#
# TRANSACTION 7
#
###############################################################################
def top_balance_transaction(session, query_limit=10):
    customer_list = []
    results = session.customer.find({},
                                    {"_id": 0,
                                     "c_name": 1,
                                     "c_balance": 1,
                                     "w_name": 1,
                                     "d_name": 1})\
                              .sort([("c_balance", -1)])\
                              .limit(query_limit)
    for result in results:
        customer_list.append(result)
    return(output(customer_list))
