import decimal
import json
import urllib

import simplejson as simplejson
from sqlalchemy import engine, text, create_engine

engine = create_engine('mysql+pymysql://mall_tiny:%s@localhost:3306/mall_ums' % urllib.parse.quote('1233rs!@d'))


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        super(DecimalEncoder, self).default(o)


with engine.connect() as connection:
    json_data = {'data': [], 'en_title': []}
    sel_zh_title = 'select col_desc from {0} where col_def in ({1})'
    result = connection.execute(text(
        "select oi,oi_ccy,ts from cc_swap_open_interest  where channel = 'open-interest' and inst_id = 'BTC-USDT-SWAP' limit 500"))
    data = result.fetchall()
    list_data = list()
    for d in data:
        list_data.append(list(d))

    json_data['en_title'] = ['oi', 'oi_ccy', 'ts']
    json_data['data'] = list_data
    with open('./data.txt', "w") as file:
        file.write("data = '")
        file.write(json.dumps(json_data, cls=DecimalEncoder))
        file.write("'")
