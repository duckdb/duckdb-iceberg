update default.lineitem_sf1_deletes
set l_orderkey=NULL,
    l_partkey=NULL,
    l_suppkey=NULL,
    l_linenumber=NULL,
    l_quantity=NULL,
    l_extendedprice=NULL,
    l_discount=NULL,
    l_shipdate=NULL,
    l_comment=NULL
where l_partkey % 2 = 0;