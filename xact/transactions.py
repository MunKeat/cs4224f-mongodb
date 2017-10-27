
import pymongo
from datetime import datetime

###############################################################################
#
# Utility Function(s)
#
###############################################################################
import pprint

def output(dictionary):
    #TODO: change output form
    output_form = "RAW_PRINT"
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

###############################################################################
#
# TRANSACTION 1
#
# Comment: Assume items is a list of items in the order
#
###############################################################################
def new_order_transaction(c_id, w_id, d_id, M, items, db):
    districts = db.district
    customers = db.customer
    stocks = db.stock
    orders = db.order

    # Retrieve tax rate and order id from district
    district = districts.find_one({"w_id": w_id, "d_id", d_id})
    district_id = district._id
    o_id = district.d_next_o_id
    w_tax = district.w_id
    d_tax = district.d_id

    # Retrieve customer info
    customer = customers.find_one({"w_id": w_id, "d_id", d_id, "c_id": c_id}, fields = {'_id':False})
    c_name = customer.c_name
    c_last = c_name.c_last
    c_credit = customer.c_credit
    c_discount = customer.c_discount

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
    ordered_items_id = []
    ordered_items = {}
    popular_item_quantity = 0
    popular_items_name = []
    is_all_local = True

    if (d_id == 10):
        s_dist_col_name = 's_dist_info_' + str(d_id)
    elif (d_id < 10):
        s_dist_col_name = 's_dist_info_0' + str(d_id)
    # Prepare an orderline for each item
    for ol_number in (0, M):
        item = items[ol_number]
        ol_i_id = item[0]
        ol_supply_w_id = item[1]
        ol_quantity = item[2]
        # Retrieve stock and item information
        stock = stocks.find_one({"w_id": ol_supply_w_id, "i_id": ol_i_id})
        s_quantity = stock.s_quantity
        i_name = stock.i_name
        i_price = stock.i_price
        district_id = "%02d" % d_id
        ol_dist_info = stock['s_dist_' + district_id]
        ol_amount = ol_quantity * i_price
        # Update stock
        adjusted_qty = s_quantity - ol_quantity
        adjusted_qty = adjusted_qty + 100 if adjusted_qty < 10 else adjusted_qty
        is_remote = 1 if ol_supply_w_id != w_id else 0
        is_all_local = False if is_remote == 1
        stock.update_one(
            {"w_id": ol_supply_w_id, "i_id": ol_i_id},
            {
                "$inc": {"s_ytd": ol_quantity, "s_order_cnt": 1, "s_remote_cnt": is_remote},
                "$set": {"s_quantity": adjusted_qty}
            }
        )
        s_quantity = adjusted_qty
        # Update popular item and ordered_items
        if (ol_quantity > popular_item_qty):
            popular_item_qty = ol_quantity
            popular_items_id = [ol_i_id]
            popular_items_name = [i_name]
        elif (ol_quantity == popular_item_qty):
            popular_items_id.append(ol_i_id)
            popular_items_name.append(i_name)
        
        ordered_items_id.append(ol_i_id)
        ordered_item_info = {
            'item_number': ol_i_id, 
            'i_name': i_name, 
            'supplier_warehouse': ol_supply_w_id,
            'quantity': ol_quantity,
            'ol_amount': ol_amount,
            's_quantity': s_quantity
        }
        ordered_items.append(ordered_item_info)

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
    order["o_total_amount"] = final_amount
    order["orderline"] = orderlines
    order["popular_items"] = popular_item_id
    order["popular_items_name"] = popular_item_name
    order["ordered_items"] = ordered_items
    order["popular_item_qty"] = popular_item_quantity
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
        'ordered_item': ordered_items
    }
    return output(result)

###############################################################################
#
# TRANSACTION 2
#
###############################################################################
def payment_transaction(c_w_id, c_d_id, c_id, payment, db):
    customers = db.customer
    districts = db.district
    warehouses = db.warehouse
    # Update Warehouse, District, Customer
    warehouses.update_one({"w_id": c_w_id}, {"$inc": {"w_ytd": payment}})
    districts.update_one({"w_id": c_w_id, "d_id": c_d_id}, {"$inc": {"d_ytd": payment}})
    customers.update_one(
        {"w_id": c_w_id, "d_id": c_d_id,, "c_id": c_id},
        {
            "$inc": {
                "c_balance": -payment
                "c_ytd_payment": payment
                "c_payment_cnt": 1
            }
        }
    )

    result = {}
    # Retrieve customer information
    customer = customers.find_one(
        {"w_id": c_w_id, "d_id": c_d_id, "c_id": c_id},
        fields = {
            'c_ytd_payment': False, 
            'c_payment_cnt': False,
            'c_delivery_cnt': False,
            'c_data': False,
            '_id': False
        }
    )
    result.update(customer)
    # Retrieve warehouse information
    warehouses = warehouses.find_one(
        {"w_id": c_w_id}, 
        fields = {'w_address': True, '_id': False}
    )
    result.update(warehouses)
    # Retrieve district information
    district = districts.find_one(
        {"w_id": c_w_id, "d_id": c_d_id},
        fields = {'d_address': True, '_id': False}
    )
    result.update(district)

    return output(result)


###############################################################################
#
# TRANSACTION 3
#
###############################################################################
def delivery_transaction(w_id, carrier_id, db):
    for d_id in range(1, 11):
        #1. retrieve the smallest undelivered order
        orders = db.order.find(
            {"w_id": w_id, "d_id": d_id, "o_carrier_id": None},
            {"o_id": 1, "ol_amount": 1, "c_id": 1}
        ).sort("o_id": 1).limit(1)
        if len(orders) == 0:
            continue
        o_id = orders[0].o_id
        o_total_amt = orders[0].o_total_amt
        c_id = orders[0].c_id

        #2. update the order entry
        timestamp = datetime.utcnow()
        db.order.update_one(
            {"w_id": w_id, "d_id": d_id, "o_id": o_id},
            { "$set": {"o_carrier_id": carrier_id, "o_delivery_d": timestamp}}
        )

        #3. update customer table
        db.customer.update(
            {"w_id": w_id, "d_id": d_id,"c_id": c_id},
            { "$inc": {"c_delivery_cnt": 1, "c_balance": o_total_amt}}
        )



###############################################################################
#
# TRANSACTION 4
#
###############################################################################
def order_status_transaction(c_w_id, c_d_id, c_id, db):
    result = {}
    #1. get last order of a customer
    orders = db.order.find(
        {"w_id": c_w_id, "d_id": c_d_id, "c_id": c_id},
        {"o_id": 1, "orderline": 1, "o_delivery_d": 1}
    ).sort("o_id": -1).limit(1)
    if len(orders) == 0:
        return result
    #2. get the customer info from customer table
    customers = db.customer.find(
        {"w_id": c_w_id, "d_id": c_d_id, "c_id": c_id},
        {"c_name": 1, "c_balance": 1}
    ).limit(1)
    #3. get orderline info
    ol_delivery_d = orders[0].o_delivery_d
    o_items = {}
    for each_orderline in orders[0].orderline:
        o_items[each_orderline.ol_number] = {}
        o_items[each_orderline.ol_number]['ol_i_id'] = each_orderline.ol_i_id
        o_items[each_orderline.ol_number]['ol_supply_w_id'] = each_orderline.ol_supply_w_id
        o_items[each_orderline.ol_number]['ol_quantity'] = each_orderline.ol_quantity
        o_items[each_orderline.ol_number]['ol_delivery_d'] = ol_delivery_d
    result['items'] = o_items
    result['c_name'] = customers[0].c_name
    result['c_balance'] = customers[0].c_balance
    result['o_id'] = orders[0].o_id
    result['o_entry_d'] = orders[0].o_entry_d
    result['o_carrier_id'] = orders[0].o_carrier_id
    return output(result)



###############################################################################
#
# TRANSACTION 5
#
# Comment: WIP
#
###############################################################################
def stock_level_transaction(w_id, d_id,T, L, db):
    #1. select last L orders of a district from the order table
    orders = db.order.find(
        {"w_id": w_id, "d_id": d_id},
        {"o_id": 1, "ordered_items": 1}
    ).sort("o_id": -1).limit(L)
    #2. find the set of items in all the orderlines
    all_item_id = set()
    for order in orders:
        all_item_id = all_item_id | order.ordered_items
    #3. look up warehouse table to check for stock level of each item in the set
    stocks = db.stock.find(
        {"w_id": w_id, "i_id": {"$in": all_item_id}, "s_quantity": {"$lt": T}},
        {"w_id": 1, "i_id": 1, "i_name": 1}
    )
    result = {}
    result['number in S'] = len(stocks)
    return output(result)


###############################################################################
#
# TRANSACTION 6
#
###############################################################################
def popular_item_transaction(i, w_id, d_id, L, db):
    #1. get all the L orders for the district from order table
    orders = db.order.find(
        {"w_id": w_id, "d_id": d_id},
        {"o_id":1, "popular_items": 1, "popular_items_name": 1, "popular_item_qty": 1, "ordered_items": 1}
    ).sort("o_id": -1).limit(L)

    output_1 = []
    output_2 = []
    number_of_orders = 0
    popular_item_id = []
    popular_item_name = []
    order_item_id = []
    for order in orders:
        number_of_orders += 1
        popular_quantity = order.popular_item_qty
        popular_items = [{'i_name': name, 'ol_quantity': popular_quantity} for name in popular_item_name]
        output_1.append({'w_id': order.w_id, 'd_id': order.d_id, 'o_id': order.o_id,
                         'o_entry_d': order.o_entry_d, 'c_name': order.c_name, 'popular_items': popular_items})
        popular_item_id.extend(order.popular_item_id)
        popular_item_name.extend(order.popular_item_name)
        order_item_id.append(order.ordered_items)

    # Get distinct popular items
    distinct_popular_item = list(set([tuple([id, name]) for id, name in zip(popular_item_id, popular_item_name)]))
    # Perform percentage count
    raw_count = [[(item_id in single_ordered_items)
                  for single_ordered_items in order_item_id].count(True)
                 for item_id, item_name in distinct_popular_item]
    output_2 = [{"i_name": item[1], "percentage": float(item_count) / number_of_orders} for item, item_count in
                zip(distinct_popular_item, raw_count)]
    return (output(output_1), output(output_2))



###############################################################################
#
# TRANSACTION 7
#
###############################################################################
def top_balance_transaction(db):
    #1. find list of w_id
    list_of_distinct_wid = []
    distinct_wid = db.warehouse.find(
        {},
        {"w_id": 1}
    )
    for result in distinct_wid:
        list_of_distinct_wid.append(result.w_id)
    # Begin transaction

    customers = db.customer.find(
        {},
        {"w_name":1, "d_name": 1, "c_name":1, "c_balance":1}
    ).sort("c_balance": -1).limit(10)
    return output(customers)
