import pymongo
from pymongo import MongoClient
from config import parameters as conf

import os

def test_state(current_session):
    a=list(db.warehouse.aggregate([{"$group":{
                        "_id":"null",
                        "w_ytd_sum":{"$sum":"$w_ytd"}
                    }
                }
            ]
        )
    )
    print "select sum(W_YTD) from Warehouse : %f" % a[0]["w_ytd_sum"]


    a=list(db.district.aggregate([{"$group":{
                        "_id":"null",
                        "d_ytd_sum":{"$sum":"$d_ytd"},
                        "d_next_o_id_sum":{"$sum":"$d_next_o_id"}
                    }
                }
            ]
        )
    )
    print "select sum(D_YTD), sum(D_NEXT_O_ID) from District : %f , %d" % (a[0]["d_ytd_sum"],a[0]["d_next_o_id_sum"])
    

    a=list(db.customer.aggregate([{"$group":{
                        "_id":"null",
                        "C_BALANCE_sum":{"$sum":"$c_balance"},
                        "C_YTD_PAYMENT_sum":{"$sum":"$c_ytd_payment"},
                        "C_PAYMENT_CNT_sum":{"$sum":"$c_payment_cnt"},
                        "C_DELIVERY_CNT_sum":{"$sum":"$c_delivery_cnt"}
                    }
                }
            ]
        )
    )
    print "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer: %f , %f , %d , %d" % (
        a[0]["C_BALANCE_sum"],a[0]["C_YTD_PAYMENT_sum"],a[0]["C_PAYMENT_CNT_sum"],a[0]["C_DELIVERY_CNT_sum"]
        )


    a=list(db.orders.aggregate([{"$group":{
                        "_id":"null",
                        "O_ID_max":{"$max":"$o_id"},
                        "O_OL_CNT_sum":{"$sum":"$o_ol_cnt"}
                    }
                }
            ]
        )
    )
    print "select max(O_ID), sum(O_OL_CNT) from Order : %d , %d" % (a[0]["O_ID_max"],a[0]["O_OL_CNT_sum"]) 
    

    a=list(db.orders.aggregate([
                    {"$unwind":"$orderline"},
                    {"$group":{
                        "_id":"null",
                        "OL_AMOUNT_sum":{"$sum":"$orderline.ol_amount"},
                        "OL_QUANTITY_sum":{"$sum":"$orderline.ol_quantity"}
                    }
                }
            ]
        )
    )
    print "select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order-Line : %f , %d" % (a[0]["OL_AMOUNT_sum"],a[0]["OL_QUANTITY_sum"]) 
    

    a=list(db.stock.aggregate([{"$group":{
                        "_id":"null",
                        "S_QUANTITY_sum":{"$sum":"$s_quantity"},
                        "S_YTD_sum":{"$sum":"$s_ytd"},
                        "S_ORDER_CNT_sum":{"$sum":"$s_order_cnt"},
                        "S_REMOTE_CNT_sum":{"$sum":"$s_remote_cnt"}
                    }
                }
            ]
        )
    )
    print "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock : %d , %f , %d , %d" % (
        a[0]["S_QUANTITY_sum"],a[0]["S_YTD_sum"],a[0]["S_ORDER_CNT_sum"],a[0]["S_REMOTE_CNT_sum"])


if __name__ == '__main__':
    connection = MongoClient(host='localhost',port=47017,w=conf["write_concern"],readConcernLevel=conf["read_concern"])
    db = connection[conf["database"]]
    
    test_state(db)


