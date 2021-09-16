# coding=utf-8

from pandas.core.frame import DataFrame
from pytdx.parser.base import BaseParser
from pytdx.helper import get_datetime, get_volume, get_price
from collections import OrderedDict
import struct


class GetSecurityList(BaseParser):

    def setParams(self, market, start):
        pkg = bytearray.fromhex(u'0c 01 18 64 01 01 06 00 06 00 50 04')
        pkg_param = struct.pack("<HH", market, start)
        pkg.extend(pkg_param)
        self.send_pkg = pkg

    def parseResponse(self, body_buf):

        pos = 0
        (num, ) = struct.unpack("<H", body_buf[:2])
        pos += 2
        stocks = []
        for i in range(num):

            # b'880023d\x00\xd6\xd0\xd0\xa1\xc6\xbd\xbe\xf9.9\x04\x00\x02\x9a\x99\x8cA\x00\x00\x00\x00'
            # 880023 100 中小平均 276782 2 17.575001 0 80846648

            one_bytes = body_buf[pos: pos + 29]

            (code, volunit,
             name_bytes, reversed_bytes1, decimal_point,
             pre_close_raw, reversed_bytes2) = struct.unpack("<6sH8s4sBI4s", one_bytes)

            code = code.decode("utf-8")
            name = name_bytes.decode("gbk").rstrip("\x00")
            pre_close = get_volume(pre_close_raw)
            pos += 29

            one = OrderedDict(
                [
                    ('code', code),
                    ('volunit', volunit),
                    ('decimal_point', decimal_point),
                    ('name', name),
                    ('pre_close', pre_close),
                ]
            )

            stocks.append(one)


        return stocks

if __name__ == '__main__':

    #from pytdx.util.best_ip import select_best_ip
    from pytdx.hq import TdxHq_API
    api = TdxHq_API()
    with api.connect("119.147.212.81", 7709):
        # 11 扩缩股
        start = 0
        limit = 1000
        datalist = DataFrame()
        market = 0
        while True:
            df = api.to_df(api.get_security_list(market, start))
            if datalist.shape[0] == 0 and df.shape[0] > 1:
                datalist = df
            else:
                datalist = datalist.append(df)
            start += limit
            print(df.shape[0])
            if df.shape[0] < 1000:
                break
        market = 1
        start = 0
        while True:
            df = api.to_df(api.get_security_list(market, start))
            datalist = datalist.append(df)
            start += limit
            print(df.shape[0])
            if df.shape[0] < 1000:
                break
        datalist.to_csv("security_list.csv",  index=False)