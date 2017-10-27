
import pymongo
from datetime import datetime

###############################################################################
#
# TRANSACTION 1
#
# Comment: Assume items is a list of items in the order
#
###############################################################################
def new_order_transaction(c_id, w_id, d_id, M, items, db):
    pass

###############################################################################
#
# TRANSACTION 2
#
###############################################################################
def payment_transaction(c_w_id, c_d_id, c_id, payment, db):
    pass


###############################################################################
#
# TRANSACTION 3
#
###############################################################################
def delivery_transaction(w_id, carrier_id, db):
    for d_id in range(1, 11):
        #1. retrieve the smallest undelivered order
        orders = db.orders.find(
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
        db.orders.updateOne(
            {"w_id": w_id, "d_id": d_id, "o_id": o_id},
            { $set: {"o_carrier_id": carrier_id, "o_delivery_d": timestamp}}
        )

        #3. update customer table
        db.customer.update(
            {"w_id": w_id, "d_id": d_id,"c_id": c_id},
            { $inc: {"c_delivery_cnt": 1, "c_balance": o_total_amt}}
        )



###############################################################################
#
# TRANSACTION 4
#
###############################################################################
def order_status_transaction(c_w_id, c_d_id, c_id, db):
    result = {}
    #1. get last order of a customer
    orders = db.orders.find(
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
    return result



###############################################################################
#
# TRANSACTION 5
#
# Comment: WIP
#
###############################################################################
def stock_level_transaction(w_id, d_id,T, L, db):
    pass


###############################################################################
#
# TRANSACTION 6
#
###############################################################################
def popular_item_transaction(i, w_id, d_id, L, db):
    pass


###############################################################################
#
# TRANSACTION 7
#
###############################################################################
def top_balance_transaction(db):
    pass