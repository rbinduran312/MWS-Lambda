import csv

import psycopg2

from local.mws_test import get_mapped_sku


def update_db_raw(config, order_list_):
    order_list = {}
    select_query = ""
    select_query_base = "SELECT * from " + config["schema"] + "." + config["table"] + " WHERE 0 "
    for row in order_list_:
        order_list[row["order-id"]] = row
        select_query = select_query + " OR " + " external_document_no = '" + row["order-id"] + "'"
        # 'order-id': order['AmazonOrderId']['value'],
        # 'order-item-id': item['OrderItemId']['value'],
        # 'purchase-date': order['PurchaseDate']['value'],
        # 'payments-date': order['PurchaseDate']['value'],
        # 'buyer-email': order['BuyerEmail']['value'],
        # 'buyer-name': order['BuyerName'][
        #     'value'] if 'BuyerName' in order else '',
        # 'buyer-phone-number': order['ShippingAddress']['Phone'][
        #     'value'] if 'Phone' in order['ShippingAddress'] else '',
        # 'sku': item['SellerSKU']['value'],
        # 'product-name': item['Title']['value'],
        # 'quantity-purchased': item['QuantityOrdered']['value'],
        # 'currency': item['ItemPrice']['CurrencyCode']['value'],
        # 'item-price': item['ItemPrice']['Amount']['value'],
        # 'item-tax': item['ItemTax']['Amount']['value'],
        # 'shipping-price': item['ShippingPrice']['Amount'][
        #     'value'] if 'ShippingPrice' in item else '',
        # 'shipping-tax': item['ShippingTax']['Amount'][
        #     'value'] if 'ShippingTax' in item else '',
        # 'ship-service-level': order['ShipServiceLevel']['value'],
        # 'recipient-name': order['ShippingAddress']['Name']['value'],
        # 'ship-address-1': order['ShippingAddress']['AddressLine1']['value'],
        # 'ship-address-2': order['ShippingAddress']['AddressLine2'][
        #     'value'] if 'AddressLine2' in order['ShippingAddress'] else '',
        # 'ship-address-3': order['ShippingAddress']['AddressLine3'][
        #     'value'] if 'AddressLine3' in order['ShippingAddress'] else '',
        # 'ship-city': order['ShippingAddress']['City']['value'],
        # 'ship-state': order['ShippingAddress']['StateOrRegion']['value'],
        # 'ship-postal-code': order['ShippingAddress']['PostalCode']['value'],
        # 'ship-country': order['ShippingAddress']['CountryCode']['value'],
        # 'ship-phone-number': order['ShippingAddress']['Phone'][
        #     'value'] if 'Phone' in order['ShippingAddress'] else '',
        # 'item-promotion-discount': item['PromotionDiscount']['Amount'][
        #     'value'],
        # 'item-promotion-id': ' '.join(map(str, item['PromotionIds'][
        #     'value'])) if 'PromotionIds' in item else '',
        # 'ship-promotion-discount': item['PromotionDiscount']['Amount'][
        #     'value'],
        # 'ship-promotion-id': ' '.join(map(str, item['PromotionIds'][
        #     'value'])) if 'PromotionIds' in item else '',
        # 'delivery-start-date': '',
        # 'delivery-end-date': '',
        # 'delivery-time-zone': '',
        # 'delivery-Instructions': '',
        # 'sales-channel': order['SalesChannel']['value'],
        # 'earliest-ship-date': order['EarliestShipDate']['value'],
        # 'latest-ship-date': order['LatestShipDate']['value'],
        # 'earliest-delivery-date': order['EarliestDeliveryDate']['value'],
        # 'latest-delivery-date': order['LatestDeliveryDate']['value'],
        # 'is-business-order': order['IsBusinessOrder']['value'],
        # 'purchase-order-number': order['PurchaseOrderNumber'][
        #     'value'] if 'PurchaseOrderNumber' in order else '',
        # 'price-designation': item['PriceDesignation'][
        #     'value'] if 'PriceDesignation' in order else '',
        # 'is-prime': order['IsPrime']['value'],
        # 'tax-collection-model': item['TaxCollection']['Model'][
        #     'value'] if 'TaxCollection' in item else '',
        # 'tax-collection-responsible-party':
        # item['TaxCollection']['ResponsibleParty'][
        #     'value'] if 'TaxCollection' in item else ''

    # with open(csv_name, 'r') as f:
    #     reader = csv.reader(f)
    #     header = next(reader)  # Skip the header row.
    #

    select_query_base = select_query_base + select_query
    query = "INSERT INTO " + config["schema"] + "." + config["table"] + "(order_date, external_document_no, " \
                                                                        "ship_to_name, ship_to_name_2, " \
                                                                        "ship_to_address, ship_to_address_2, " \
                                                                        "ship_to_city, ship_to_county, " \
                                                                        "ship_to_post_code, " \
                                                                        "ship_to_country_region_code, brand, " \
                                                                        "quantity, cross_reference_no, " \
                                                                        "unit_price, sales_tax_amount, " \
                                                                        "shipping_agent_code, " \
                                                                        "shipping_agent_service_code, " \
                                                                        "e_ship_agent_service)" + \
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    connection = None
    duplication = []
    try:
        connection = psycopg2.connect(user=config["user"],
                                      password=config["password"],
                                      host=config["host"],
                                      port=config["port"],
                                      database=config["database"])
        cur = connection.cursor()
        try:
            cur.execute(select_query_base)
            duplication = cur.fetchall()
            if duplication is not None:
                for duplicate_e in duplication:
                    order_list.pop(duplicate_e[1], None)
        except Exception as e:
            pass
        values = []
        for key in order_list.keys():
            row = order_list[key]
            cross_ref = get_mapped_sku(row['sku'])  # sku
            value = (row['purchase-date'],
                     row['order-id'],
                     row['buyer-name'],
                     "",
                     row['ship-address-1'] + " " + row['ship-address-2'] + " " + row['ship-address-3'],  # address 1
                     "UNABLE TO VARIFY",  # address 2
                     row['ship-city'],  # ship city
                     row['ship-state'],  # ship state
                     row['ship-postal-code'],  # postal code
                     row['ship-country'],  # region code
                     cross_ref["brand"],
                     int(row['quantity-purchased']),
                     cross_ref["Cross-Reference No"],
                     float(row["item-price"]),
                     float(row["item-tax"]) + 0 if row["shipping-tax"] == "" else float(row["shipping-tax"]),
                     "FDXH",
                     "FDXH",
                     "FDXH")
            values.append(value)
        cur.executemany(query, tuple(values))
        cur.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while processing= ", error)
    finally:
        # closing database connection.
        if connection:
            connection.commit()
            connection.close()
            print("PostgreSQL connection is closed")

    return duplication


def get_fresh_list(config, order_list_): # ready.csv
    order_list = {}
    select_query = ""
    select_query_base = "SELECT * from " + config["schema"] + "." + config["table"] + " WHERE 0 "
    for row in order_list_:
        order_list[row["External Document No."]] = row
        select_query = select_query + " OR " + " external_document_no = '" + row["External Document No."] + "'"

    select_query_base = select_query_base + select_query
    connection = None
    duplication = []
    new_list = []
    try:
        connection = psycopg2.connect(user=config["user"],
                                      password=config["password"],
                                      host=config["host"],
                                      port=config["port"],
                                      database=config["database"])
        cur = connection.cursor()
        cur.execute(select_query_base)
        duplication = cur.fetchall()
        if duplication is not None:
            for duplicate_e in duplication:
                order_list.pop(duplicate_e[1], None)

        for key in order_list.keys():
            new_list.append(order_list[key])
    except (Exception, psycopg2.Error) as error:
        print("Error while processing= ", error)
    finally:
        # closing database connection.
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")

    return duplication, new_list
