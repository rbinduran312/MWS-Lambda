import mws
from datetime import datetime, timedelta
import time
import dateutil.parser
# from bs4 import BeautifulSoup
import csv
import logging
import boto3
from botocore.exceptions import ClientError
from local.smarty_test import varify_address
from local.mws_test import get_mapped_sku
from local.dgmanage import update_db_raw, get_fresh_list
import os
from Config import config2, merchants, postgres_db

# from requests.exceptions import ConnectionError
s3_client = boto3.client(service_name='s3', region_name="us-east-1",
                         aws_access_key_id=config2["aws_access_key_id"],
                         aws_secret_access_key=config2["aws_secret_access_key"]
                         )


class ImportOrder():

    def __init__(self, account):
        """
        Args: account: merchant account details
        """
        self.report = None
        self.account = account
        self.orders_api = mws.Orders(
            account_id=account['merchant_id'],
            access_key=account['access_key'],
            secret_key=account['secret_key'],
            region=account['marketplace'],
            auth_token=account['mws_auth_token']
        )
        self.last_updated_after = ImportOrder.get_posted_after_date()
        self.last_updated_before = ImportOrder.get_posted_before()
        self.order_list = []

    def get_processed_orders_list(self):
        return self.order_list

    @staticmethod
    def get_posted_before(delta=timedelta(minutes=3)):
        """Return correctly formatted timestamp for now + 6 Hours for timezone"""
        now = datetime.now()
        # ft = (now + timedelta(hours=6) + timedelta(minutes=57))
        ft = now - delta
        return ft.isoformat()
        # return ft
        # return ft.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def get_posted_after_date(delta=timedelta(hours=4)):
        """Return correctly formatted timestamp for now + 6 Hours for timezone"""
        now = datetime.now()
        # ft = now - timedelta(hours=1)
        ft = now - delta
        print(ft)
        return ft.isoformat()
        # ft = (now + timedelta(hours=2) + timedelta(minutes=57))
        # return ft.strftime("%Y-%m-%dT%H:%M:%SZ")

    def write_orders(self, order, item):
        # TODO SAVE LINE WITH ORDER INFO + ORDER ITEM INFO
        '''order-id	order-item-id
        purchase-date	payments-date
        buyer-email	buyer-name	buyer-phone-number
        sku	product-name	quantity-purchased	currency
        item-price	item-tax	shipping-price	shipping-tax	ship-service-level
        recipient-name	ship-address-1	ship-address-2	ship-address-3
        ship-city	ship-state	ship-postal-code	ship-country
        ship-phone-number	item-promotion-discount	item-promotion-id
        ship-promotion-discount	ship-promotion-id	delivery-start-date	delivery-end-date
        delivery-time-zone	delivery-Instructions	sales-channel	earliest-ship-date
        latest-ship-date	earliest-delivery-date	latest-delivery-date
        is-business-order	purchase-order-number	price-designation
        is-prime	tax-collection-model
        tax-collection-responsible-party'''
        try:
            self.order_list.append(
                {
                    'order-id': order['AmazonOrderId']['value'],
                    'order-item-id': item['OrderItemId']['value'],
                    'purchase-date': order['PurchaseDate']['value'],
                    'payments-date': order['PurchaseDate']['value'],
                    'buyer-email': order['BuyerEmail']['value'],
                    'buyer-name': order['BuyerName'][
                        'value'] if 'BuyerName' in order else '',
                    'buyer-phone-number': order['ShippingAddress']['Phone'][
                        'value'] if 'Phone' in order['ShippingAddress'] else '',
                    'sku': item['SellerSKU']['value'],
                    'product-name': item['Title']['value'],
                    'quantity-purchased': item['QuantityOrdered']['value'],
                    'currency': item['ItemPrice']['CurrencyCode']['value'],
                    'item-price': item['ItemPrice']['Amount']['value'],
                    'item-tax': item['ItemTax']['Amount']['value'],
                    'shipping-price': item['ShippingPrice']['Amount'][
                        'value'] if 'ShippingPrice' in item else '',
                    'shipping-tax': item['ShippingTax']['Amount'][
                        'value'] if 'ShippingTax' in item else '',
                    'ship-service-level': order['ShipServiceLevel']['value'],
                    'recipient-name': order['ShippingAddress']['Name']['value'],
                    'ship-address-1': order['ShippingAddress']['AddressLine1']['value'],
                    'ship-address-2': order['ShippingAddress']['AddressLine2'][
                        'value'] if 'AddressLine2' in order['ShippingAddress'] else '',
                    'ship-address-3': order['ShippingAddress']['AddressLine3'][
                        'value'] if 'AddressLine3' in order['ShippingAddress'] else '',
                    'ship-city': order['ShippingAddress']['City']['value'],
                    'ship-state': order['ShippingAddress']['StateOrRegion']['value'],
                    'ship-postal-code': order['ShippingAddress']['PostalCode']['value'],
                    'ship-country': order['ShippingAddress']['CountryCode']['value'],
                    'ship-phone-number': order['ShippingAddress']['Phone'][
                        'value'] if 'Phone' in order['ShippingAddress'] else '',
                    'item-promotion-discount': item['PromotionDiscount']['Amount'][
                        'value'],
                    'item-promotion-id': ' '.join(map(str, item['PromotionIds'][
                        'value'])) if 'PromotionIds' in item else '',
                    'ship-promotion-discount': item['PromotionDiscount']['Amount'][
                        'value'],
                    'ship-promotion-id': ' '.join(map(str, item['PromotionIds'][
                        'value'])) if 'PromotionIds' in item else '',
                    'delivery-start-date': '',
                    'delivery-end-date': '',
                    'delivery-time-zone': '',
                    'delivery-Instructions': '',
                    'sales-channel': order['SalesChannel']['value'],
                    'earliest-ship-date': order['EarliestShipDate']['value'],
                    'latest-ship-date': order['LatestShipDate']['value'],
                    'earliest-delivery-date': order['EarliestDeliveryDate']['value'],
                    'latest-delivery-date': order['LatestDeliveryDate']['value'],
                    'is-business-order': order['IsBusinessOrder']['value'],
                    'purchase-order-number': order['PurchaseOrderNumber'][
                        'value'] if 'PurchaseOrderNumber' in order else '',
                    'price-designation': item['PriceDesignation'][
                        'value'] if 'PriceDesignation' in order else '',
                    'is-prime': order['IsPrime']['value'],
                    'tax-collection-model': item['TaxCollection']['Model'][
                        'value'] if 'TaxCollection' in item else '',
                    'tax-collection-responsible-party':
                        item['TaxCollection']['ResponsibleParty'][
                            'value'] if 'TaxCollection' in item else ''
                })
            item_processed = True
            print('single order added')
            print(len(self.order_list))
        except Exception as e:
            print('single item failed', e)
            item_processed = False
        return item_processed

    @staticmethod
    def new_order_freight_v2(order, order_list, current_index, address_object, cross_ref):
        index = current_index
        sum_tax = sum_price = 0.0
        while index >= 0 and order_list[index]["order-id"] == order["order-id"]:
            if order_list[index]["shipping-price"] != '':
                sum_price = sum_price + float(order_list[index]["shipping-price"])
            if order_list[index]["shipping-tax"] != '':
                sum_tax = sum_tax + float(order_list[index]["shipping-tax"])
            index = index - 1
        new_att = {
            # 'Order Date': datetime.strptime(order['purchase-date'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime(
            #     '%m/%d/%Y'),
            "Order Date": dateutil.parser.parse(order['purchase-date']).strftime('%m/%d/%Y'),
            'External Document No.': order['order-id'],
            'Ship-to Name': order['buyer-name'],
            'Ship-to Name 2': '',
            'Ship-to Address': address_object['Ship-to Address'],
            'Ship-to Address 2': address_object['Ship-to Address 2'],
            'Ship-to City': address_object['Ship-to City'],
            'Ship-to County': address_object['Ship-to County'],
            'Ship-to Post-code': address_object['Ship-to Post-code'],
            'Ship-to Country/Region Code': address_object['Ship-to Country/Region Code'],
            'Cross-Reference No.': cross_ref['Cross-Reference No'],
            'Brand': cross_ref['brand'],
            'Quantity': order['quantity-purchased'],
            'Unit Price': sum_price,
            'Sales Tax Amount': sum_tax,
            'Shipping Agent Code': 'FDXH',
            'Shipping Agent Service Code': 'FDXH',
            'E-Ship Agent Service': 'FDXH'
        }
        return new_att

    @staticmethod
    def new_order_item_v2(order, address_object, cross_ref, tax_rate, is_fright):
        unit_price = order['item-price']
        cross_refer = cross_ref['Cross-Reference No']
        sales_tax_amount = order['item-tax']
        if is_fright:
            unit_price = 4.99
            sales_tax_amount = round(4.99 * tax_rate, 2)
            cross_refer = "FREIGHT"

        new_item = {
            "Order Date": dateutil.parser.parse(order['purchase-date']).strftime('%m/%d/%Y'),
            'External Document No.': order['order-id'],
            'Ship-to Name': order['buyer-name'],
            'Ship-to Name 2': '',
            'Ship-to Address': address_object['Ship-to Address'],
            'Ship-to Address 2': address_object['Ship-to Address 2'],
            'Ship-to City': address_object['Ship-to City'],
            'Ship-to County': address_object['Ship-to County'],
            'Ship-to Post-code': address_object['Ship-to Post-code'],
            'Ship-to Country/Region Code': address_object['Ship-to Country/Region Code'],
            'Brand': cross_ref['brand'],
            'Quantity': order['quantity-purchased'],

            'Cross-Reference No.': cross_refer,
            'Unit Price': unit_price,
            'Sales Tax Amount': sales_tax_amount,

            'Shipping Agent Code': 'FDXH',
            'Shipping Agent Service Code': 'FDXH',
            'E-Ship Agent Service': 'FDXH'
        }
        return new_item

    def parse_orders(self, orders):
        # print(type(orders))
        ##################################################################
        t_count = 0
        order_count = 0
        print(len(orders))
        time.sleep(5)
        for order in orders:
            try:
                order_status = order['OrderStatus']['value']
            except Exception as e:
                print(e)
                pass
            ###########################################################
            # print(order['OrderStatus'])
            item_processed = False
            item_connection_err_retry_count = 1
            order_count = order_count + 1
            while not item_processed and item_connection_err_retry_count <= 3:
                try:
                    ordered_items_result = self.orders_api.list_order_items(
                        order['AmazonOrderId']['value'], None)
                    t_count += 1
                    if t_count > 30:
                        print('avoiding throttle')
                        time.sleep(5)
                    ordered_items = ordered_items_result.parsed['OrderItems']
                    # print(ordered_items)
                    # time.sleep(10)
                    if not isinstance(ordered_items['OrderItem'], list):
                        print('single item order')
                        item = ordered_items['OrderItem']
                        item_processed = self.write_orders(order, item)
                    else:
                        print('multi item order')
                        for item in ordered_items['OrderItem']:
                            # print(item)
                            # TODO SAVE LINE WITH ORDER INFO + ORDER ITEM INFO
                            item_processed = self.write_orders(order, item)
                except Exception as e:
                    print(e)
                    item_processed = True
                    item_connection_err_retry_count = item_connection_err_retry_count + 1
            ###########################################################
        return self.order_list

    @staticmethod
    def save_files(order_list_, file_ready_path):
        try:
            keys = order_list_[0].keys()
            with open(file_ready_path, 'w', newline='') as output_file_v2:
                dict_writer = csv.DictWriter(output_file_v2, keys)
                dict_writer.writeheader()
                dict_writer.writerows(order_list_)
        except Exception as e:
            print(f'write order_list_v2 == Exception {e}')
            pass
        return file_ready_path

    def upload_2s3(self, upload_file, file_name, s3_sub_path):
        # '_raw.csv', '_ready.csv'
        with open(upload_file, "rb") as f:
            try:
                upload_status = s3_client.upload_fileobj(f,
                                                         "optivations-fbm194823-master",
                                                         s3_sub_path + file_name)
                print(upload_status)
                os.remove(upload_file)
                return True
            except ClientError as e:
                logging.error(e)
                return False

    def main(self):
        order_list = []
        order_list_v2 = []
        order_list_v2_needs_att = []
        next_token = None
        first_time = True
        self.order_list = []
        while first_time or next_token is not None:
            first_time = False
            error = True
            connection_err_retry_count = 1
            while error and connection_err_retry_count <= 3:
                try:
                    if next_token is not None:
                        # orders = self.orders_api.list_orders(marketplaceids=[marketplace_usa], created_after='2017-07-07')
                        result = self.orders_api.list_orders(next_token=next_token)
                    else:
                        # orders = self.orders_api.list_orders(marketplaceids=[marketplace_usa], created_after='2018-04-07')
                        result = self.orders_api.list_orders(
                            marketplaceids=[self.account['marketplace_id']],
                            fulfillment_channels=['MFN'],
                            # created_after=self.lastupdatedafter,
                            lastupdatedafter=self.last_updated_after,
                            # lastupdatedbefore=self.lastupdatedbefore,
                            # orderstatus="Shipped"
                            orderstatus='Unshipped'
                        )
                    # print(result.parsed)
                    if 'Order' in result.parsed['Orders']:
                        orders = result.parsed['Orders']['Order']
                        order_list = self.parse_orders(orders)

                    if 'NextToken' in result.parsed:
                        print('has next token')
                        # print(result.parsed['NextToken'])
                        next_token = result.parsed['NextToken']['value']
                        # time.sleep(80)
                    else:
                        next_token = None
                    error = False
                except ConnectionError as ex:
                    print(ex)
                    print("retry count ", connection_err_retry_count, "in 20 secs")
                    time.sleep(20)
                    error = True
                    connection_err_retry_count = connection_err_retry_count + 1

            if error:
                raise Exception("Connection Error exceed retry.")
        print('order list length', len(order_list))
        # Split two brands
        cross_ref_dict = {}
        order_list_brand = {}
        for index in range(len(order_list)):
            order = order_list[index]
            try:
                # {'Cross-Reference No': '3571', 'brand': 'Halo'}
                _cross_ref = get_mapped_sku(order['sku'])
                cross_ref_dict[order["sku"]] = _cross_ref
                brand = _cross_ref["brand"]
                if brand in order_list_brand.keys():
                    order_list_brand[brand].append(order)
                else:
                    order_list_brand[brand] = [order]

            except Exception as e:
                print(e)
                _cross_ref = "MISSING SKU"

        brand_list = ["Halo", "Aden"]
        for _brand_ in brand_list:
            if _brand_ not in order_list_brand.keys():
                continue
            brand_order_list = order_list_brand[_brand_]
            order_list_v2 = []
            order_list_v2_needs_att = []
            for index in range(len(brand_order_list)):
                order = brand_order_list[index]
                cross_ref = cross_ref_dict[order['sku']]
                if cross_ref == "MISSING SKU":
                    continue

                address_string = " ".join(
                    [str(order['ship-address-1']), str(order['ship-address-2']), str(order['ship-address-3']),
                     str(order['ship-postal-code'])])
                address_object = varify_address(address_string)
                try:
                    print('getting tax rate')
                    tax_rate = round(float(order['item-tax']) / float(order['item-price']), 2)
                    time.sleep(10)
                except Exception as e:
                    tax_rate = 0
                    print(e)
                if not address_object:
                    if 'Cross-Reference No' not in cross_ref:
                        continue
                    address_object = {
                        'Ship-to Address': " ".join([str(order['ship-address-1']),
                                                     str(order['ship-address-2']),
                                                     str(order['ship-address-3'])]),
                        'Ship-to Address 2': "UNABLE TO VARIFY",
                        'Ship-to City': order['ship-city'],
                        'Ship-to County': order['ship-state'],
                        'Ship-to Post-code': order['ship-postal-code'],
                        'Ship-to Country/Region Code': "US"}

                    new_att = ImportOrder.new_order_item_v2(order=order,
                                                            address_object=address_object,
                                                            cross_ref=cross_ref,
                                                            tax_rate=tax_rate,
                                                            is_fright=False)
                    order_list_v2_needs_att.append(new_att)
                    print('adding freight')
                    new_att_fright = ImportOrder.new_order_item_v2(order=order,
                                                                   address_object=address_object,
                                                                   cross_ref=cross_ref,
                                                                   tax_rate=tax_rate,
                                                                   is_fright=True)
                    order_list_v2_needs_att.append(new_att_fright)
                    continue

                new_order_v2_item = ImportOrder.new_order_item_v2(order=order,
                                                                  address_object=address_object,
                                                                  cross_ref=cross_ref,
                                                                  tax_rate=tax_rate, is_fright=False)
                order_list_v2.append(new_order_v2_item)

                print('adding freight')
                last_order_index = index + 1
                if index == len(brand_order_list) - 1 or \
                        order['order-id'] != brand_order_list[last_order_index]['order-id']:
                    new_order_freight = ImportOrder.new_order_freight_v2(order=order,
                                                                         order_list=brand_order_list,
                                                                         current_index=index,
                                                                         address_object=address_object,
                                                                         cross_ref=cross_ref)
                    order_list_v2.append(new_order_freight)

            # SAVE FILES #
            if len(order_list_v2):
                duplicated, new_order_list_v2 = get_fresh_list(config=postgres_db, order_list_=order_list_v2)
                ImportOrder.save_files(order_list_=new_order_list_v2, file_ready_path="./ready.csv")
                ImportOrder.save_files(order_list_=duplicated, file_ready_path="./" + self.last_updated_after + "duplicated.csv")
            if len(order_list):
                ImportOrder.save_files(order_list_=order_list, file_ready_path="./raw.csv")
                update_db_raw(config=postgres_db,  order_list_=order_list)

            # Upload raw file to raw folder in S3
            self.upload_2s3(upload_file="raw.csv",
                            file_name=str(self.account['merchant_id']) + "_" + str(
                                self.last_updated_after) + '_raw.csv',
                            s3_sub_path='public/raw/' + _brand_ + "/")
            self.upload_2s3(upload_file="ready.csv",
                            file_name=str(self.account['merchant_id']) + "_" + str(
                                self.last_updated_after) + '_ready.csv',
                            s3_sub_path='public/ready/' + _brand_ + "/")
            # Save and Upload needs att file to needs_attention folder in S3
            print('need attention')
            print(order_list_v2_needs_att)
            if len(order_list_v2_needs_att) > 0:
                ImportOrder.save_files(order_list_=order_list_v2_needs_att, file_ready_path="./needs_att.csv")
                self.upload_2s3(upload_file="needs_att.csv",
                                file_name=str(self.account['merchant_id']) + "_" + str(self.last_updated_after) +
                                          "_na.csv",
                                s3_sub_path='public/needs_attention/' + _brand_ + "/")


if __name__ == '__main__':
    fi = ImportOrder(merchants['Aden'])
    fi.main()
    # cross_ref = {"brand": "Halo", "Cross-Reference No": "123"}
