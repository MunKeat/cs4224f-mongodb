import pymongo
from pymongo import MongoClient
from datetime import datetime
from config import parameters as conf

import os
import sys

import transactions

import time


def act_test(session):
	line = sys.stdin.readline()
	num_tran=0
	while line:
		para=line.strip().split(",")
		if para[0]=="N":
			num_item=0
			new_order_c_id=int(para[1])
			new_order_w_id=int(para[2])
			new_order_d_id=int(para[3])
			new_order_m=int(para[4])
			new_order_line=list()
			num_tran+=1
		elif para[0]=="P":
			a=transactions.payment_transaction(c_w_id=int(para[1]), c_d_id=int(para[2]), c_id=int(para[3]), payment=float(para[4]), db=session)
			num_tran+=1
		elif para[0]=="D":
			a=transactions.delivery_transaction(db=session, w_id=int(para[1]), carrier_id=int(para[2]))
			num_tran+=1
		elif para[0]=="O":
			a=transactions.order_status_transaction(db=session, c_w_id=int(para[1]), c_d_id=int(para[2]), c_id=int(para[3]))
			num_tran+=1
		elif para[0]=="S":
			a=transactions.stock_level_transaction(w_id=int(para[1]),d_id=int(para[2]),T=int(para[3]), L=int(para[4]), db=session)
			num_tran+=1
		elif para[0]=="I":
			a=transactions.popular_item_transaction(i="I", w_id=int(para[1]), d_id=int(para[2]),L=int(para[3]), db=session)
			num_tran+=1
		elif para[0]=="T":
			a=transactions.top_balance_transaction(db=session)
			num_tran+=1
		else: #order line in the order
			new_order_line.append([int(para[0]),int(para[1]),int(para[2])])
			num_item += 1
			if num_item == new_order_m:
				a=transactions.new_order_transaction(c_id=new_order_c_id, w_id=new_order_w_id, d_id=new_order_d_id,M=new_order_m, items=new_order_line, db=session)

		line = sys.stdin.readline()
	
	return num_tran

if __name__ == '__main__':
	
	connection = MongoClient(host='localhost',port=47017,w=conf["write_concern"],readConcernLevel=conf["read_concern"])
	db = connection[conf["database"]]
	
	num_tran=0
	before_time=time.time()
	
	num_tran=act_test(db)
	
	after_time=time.time()

	total_time=after_time-before_time

	sys.stderr.write("Total number of transactions processed:%d\n" % num_tran)
	sys.stderr.write("Total elapsed time for processing the transactions:%f\n" % total_time)
	sys.stderr.write("Transaction throughput:%f\n" % (num_tran/total_time))

