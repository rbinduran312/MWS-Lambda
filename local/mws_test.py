from pyathena import connect
from Config import config2
from Config import merchants


def get_mapped_sku(sku):
    try:
        cursor = connect(aws_access_key_id=config2["aws_access_key_id"],
                         aws_secret_access_key=config2["aws_secret_access_key"],
                         s3_staging_dir=config2["s3_staging_dir"],
                         region_name=config2["region_name"]).cursor()
        cursor.execute("SELECT seller_sku, seller FROM optivations.master_product_list where sc_sku = %(sku)s ",
                       {"sku": str(sku)})

        # print(cursor.description)
        result = cursor.fetchall()
        for row in result:
            return {'Cross-Reference No': row[0], 'brand': row[1]}

    except Exception as e:
        print(e)
        return {}
    return {}


def get_sku(seller_sku, sc_sku, seller):
    try:
        cursor = connect(aws_access_key_id=config2["aws_access_key_id"],
                         aws_secret_access_key=config2["aws_secret_access_key"],
                         s3_staging_dir=config2["s3_staging_dir"],
                         region_name=config2["region_name"]).cursor()
        cursor.execute("SELECT seller_sku FROM optivations.master_product_list where sc_sku = %(sku)s ",
                       {"sku": str(sku)})

        # print(cursor.description)
        # print(cursor.fetchall())
        for row in cursor:
            return (row[0])
    except Exception as e:
        print(e)
        return False
    return True


def add_sku(sc_sku, seller_sku, seller):
    try:
        cursor = connect(aws_access_key_id=config2["aws_access_key_id"],
                         aws_secret_access_key=config2["aws_secret_access_key"],
                         s3_staging_dir=config2["s3_staging_dir"],
                         region_name=config2["region_name"]).cursor()
        cursor.execute("INSERT INTO optivations.master_product_list VALUES ( %(scsku)s, %(sellersku)s, %(seller)s )",
                       {"scsku": str(sc_sku), "sellersku": str(seller_sku), "seller": str(seller)})

        return (cursor.description)
        # print(cursor.fetchall())
        # for row in cursor:
        #     return (row[0])
    except Exception as e:
        print(e)
        return False
    return True
# print(add_sku('test', 'test', 'Adean'))
# result = (get_mapped_sku('HDS-3571'))
# print(result['Cross-Reference No'])
