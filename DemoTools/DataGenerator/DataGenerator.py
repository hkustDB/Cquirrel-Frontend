import os

prefix = input("Please input the file path: ")

window_size = input("Please input the window size: ")
scale_factor = input("Please input the scale factor: ")

isLineitem = True if input("Is Lineitem included in the stream? (Y for yes, default: No): ") == "Y" else False
isOrders = True if input("Is Orders included in the stream? (Y for yes, default: No): ") == "Y" else False
isCustomer = True if input("Is Customer included in the stream? (Y for yes, default: No): ") == "Y" else False
isPartSupp = True if input("Is PartSupp included in the stream? (Y for yes, default: No): ") == "Y" else False
isPart = True if input("Is Part included in the stream? (Y for yes, default: No): ") == "Y" else False
isSupplier = True if input("Is Supplier included in the stream? (Y for yes, default: No): ") == "Y" else False
isNation = True if input("Is Nation included in the stream? (Y for yes, default: No): ") == "Y" else False

lineitem = open(prefix+"/lineitem.tbl", "r")
orders = open(prefix + "/orders.tbl", "r")
partsupp = open(prefix + "/partsupp.tbl", "r")
part = open(prefix + "/part.tbl", "r")
supplier = open(prefix + "/supplier.tbl", "r")
customer = open(prefix + "/customer.tbl", "r")
nation = open(prefix + "/nation.tbl", "r")
region = open(prefix + "/region.tbl", "r")

lineitem_size = scale_factor * 6000000
orders_size = scale_factor * 1500000
nation_size = 25
region_size = 5
partsupp_size = scale_factor * 800000
part_size = scale_factor * 200000
supplier_size = scale_factor * 10000
customer_size = scale_factor * 150000

delete = (window_size != 0)
lineitem_d = open(prefix+"/lineitem.tbl", "r")
orders_d = open(prefix + "/orders.tbl", "r")
partsupp_d = open(prefix + "/partsupp.tbl", "r")
part_d = open(prefix + "/part.tbl", "r")
supplier_d = open(prefix + "/supplier.tbl", "r")
customer_d = open(prefix + "/customer.tbl", "r")
nation_d = open(prefix + "/nation.tbl", "r")
region_d = open(prefix + "/region.tbl", "r")

output = open(prefix + "../testdata.csv", "w")

count = 0
delete_count = 0 - window_size
part_count = 0
part_delete_count = 0
supplier_count = 0
supplier_delete_count = 0
orders_count = 0
orders_delete_count = 0
customer_count = 0
customer_delete_count = 0
partsupp_count = 0
partsupp_delete_count = 0
nation_count = 0
nation_delete_count = 0

line_lineitem = lineitem.readline()
line_lineitem_d = lineitem_d.readline()
line_orders = orders.readline()
line_orders_d = orders_d.readline()
line_partsupp = partsupp.readline()
line_partsupp_d = partsupp_d.readline()
line_part = part.readline()
line_part_d = part_d.readline()
line_supplier = supplier.readline()
line_supplier_d = supplier_d.readline()
line_customer = customer.readline()
line_customer_d = customer_d.readline()
line_nation = nation.readline()
line_nation_d = nation_d.readline()
line_region = region.readline()
line_region_d = region_d.readline()

while (line_lineitem):
    count = count + 1
    delete_count = delete_count + 1
    if (isLineitem): output.write("+LI"+line_lineitem)
    line_lineitem = lineitem.readline()
    if (delete_count > 0) :
        if (isLineitem): output.write("-LI"+line_lineitem_d)
        line_lineitem_d = lineitem.readline()


    if (count * orders_size / lineitem_size > orders_count and line_orders):
        orders_count = orders_count + 1
        if (isOrders) : output.write("+OR"+line_orders)
        line_orders = orders.readline()
        if (delete_count * orders_size / lineitem_size  > orders_delete_count and line_orders_d):
            orders_delete_count = orders_delete_count + 1
            if (isOrders) : output.write("-OR"+line_orders_d)
            line_orders_d = orders_d.readline()

    if (count * customer_size / lineitem_size > customer_count and line_customer):
        customer_count = customer_count + 1
        if (isCustomer) : output.write("+CU"+line_customer)
        line_customer = customer.readline()
        if (delete_count * customer_size / lineitem_size  > customer_delete_count and line_customer_d):
            customer_delete_count = customer_delete_count + 1
            if (isCustomer) : output.write("-CU"+line_customer_d)
            line_customer_d = customer_d.readline()

    if (count * part_size / lineitem_size > part_count and line_part):
        part_count = part_count + 1
        if (isPart) : output.write("+PA"+line_part)
        line_part = part.readline()
        if (delete_count * part_size / lineitem_size  > part_delete_count and line_part_d):
            part_delete_count = part_delete_count + 1
            if (isPart) : output.write("-PA"+line_part_d)
            line_part_d = part_d.readline()

    if (count * supplier_size / lineitem_size > supplier_count and line_supplier):
        supplier_count = supplier_count + 1
        if (isSupplier) : output.write("+SU"+line_supplier)
        line_supplier = supplier.readline()
        if (delete_count * supplier_size / lineitem_size  > supplier_delete_count and line_supplier_d):
            supplier_delete_count = supplier_delete_count + 1
            if (isSupplier) : output.write("-SU"+line_supplier_d)
            line_supplier_d = supplier_d.readline()

    if (count * partsupp_size / lineitem_size > partsupp_count and line_partsupp):
        partsupp_count = partsupp_count + 1
        if (isPartSupp) : output.write("+PS"+line_partsupp)
        line_partsupp = partsupp.readline()
        if (delete_count * partsupp_size / lineitem_size  > partsupp_delete_count and line_partsupp_d):
            partsupp_delete_count = partsupp_delete_count + 1
            if (isPartSupp) : output.write("-PS"+line_partsupp_d)
            line_partsupp_d = partsupp_d.readline()

    if (count * nation_size / lineitem_size > nation_count and line_nation):
        nation_count = nation_count + 1
        if (isNation) : output.write("+NA"+line_nation)
        line_nation = nation.readline()
        if (delete_count * nation_size / lineitem_size  > nation_delete_count and line_nation_d):
            nation_delete_count = nation_delete_count + 1
            if (isNation) : output.write("-NA"+line_nation_d)
            line_nation_d = nation_d.readline()



while (line_lineitem_d):
    delete_count = delete_count + 1
    if (isLineitem) : output.write("-LI"+line_lineitem_d)
    line_lineitem_d = lineitem_d.readline()

    if (delete_count * orders_size / lineitem_size > orders_delete_count and line_orders_d):
        orders_delete_count = orders_delete_count + 1
        if (isOrders) : output.write("-OR"+line_orders_d)
        line_orders_d = orders_d.readline()

    if (delete_count * customer_size / lineitem_size > customer_delete_count and line_customer_d):
        customer_delete_count = customer_delete_count + 1
        if (isCustomer) : output.write("-CU"+line_customer_d)
        line_customer_d = customer_d.readline()

    if (delete_count * part_size / lineitem_size > part_delete_count and line_part_d):
        part_delete_count = part_delete_count + 1
        if (isPart) : output.write("-PA"+line_part_d)
        line_part_d = part_d.readline()

    if (delete_count * supplier_size / lineitem_size > supplier_delete_count and line_supplier_d):
        supplier_delete_count = supplier_delete_count + 1
        if (isSupplier) : output.write("-SU"+line_supplier_d)
        line_supplier_d = supplier_d.readline()

    if (delete_count * partsupp_size / lineitem_size > partsupp_delete_count and line_partsupp_d):
        partsupp_delete_count = partsupp_delete_count + 1
        if (isPartSupp) : output.write("-PS"+line_partsupp_d)
        line_partsupp_d = partsupp_d.readline()

    if (delete_count * nation_size / lineitem_size > nation_delete_count and line_nation_d):
        nation_delete_count = nation_delete_count + 1
        if (isNation) : output.write("-NA"+line_nation_d)
        line_nation_d = nation_d.readline()



output.close()



