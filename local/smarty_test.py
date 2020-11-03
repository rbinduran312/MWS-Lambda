import os

from smartystreets_python_sdk import StaticCredentials, ClientBuilder
from smartystreets_python_sdk.us_extract import Lookup as ExtractLookup


def varify_address(address_string):
    auth_id = "c7bce886-bfac-637c-31f2-dff34a511bf7"
    auth_token = "r895zvHWv7B9Ew1cmwTA"

    # We recommend storing your secret keys in environment variables instead---it's safer!
    # auth_id = os.environ['SMARTY_AUTH_ID']
    # auth_token = os.environ['SMARTY_AUTH_TOKEN']

    credentials = StaticCredentials(auth_id, auth_token)

    client = ClientBuilder(credentials).build_us_extract_api_client()

    text = address_string

    # Documentation for input fields can be found at:
    # https://smartystreets.com/docs/cloud/us-extract-api#http-request-input-fields

    lookup = ExtractLookup()
    lookup.text = text
    lookup.aggressive = True
    lookup.addresses_have_line_breaks = False
    lookup.addresses_per_line = 1

    result = client.send(lookup)

    metadata = result.metadata
    # print('Found {} addresses.'.format(metadata.address_count))
    # print('{} of them were valid.'.format(metadata.verified_count))
    # print()

    addresses = result.addresses

    for address in addresses:
        print('"{}"\n'.format(address.text))
        print('Verified? {}'.format(address.verified))
        data={}
        if len(address.candidates) > 0:
            try:
                data['Ship-to Address'] = (address.candidates[0].components.primary_number +' '+ address.candidates[0].components.street_name +' '+ address.candidates[0].components.street_suffix)
                try:
                    data['Ship-to Address 2'] = (address.candidates[0].components.secondary_designator +' '+ address.candidates[0].components.secondary_number)
                except Exception as e:
                    print('error in add 2')
                    print(e)
                    data['Ship-to Address 2'] = "" 
                data['Ship-to City'] = (address.candidates[0].components.city_name)
                data['Ship-to County'] =  (address.candidates[0].components.state_abbreviation)
                data['Ship-to Post-code'] = (address.candidates[0].components.zipcode + '-' + address.candidates[0].components.plus4_code)
                data['Ship-to Country/Region Code'] = ('US')
            except Exception as e:
                return False
            return data
    return False
# if __name__ == "__main__":
#     run()