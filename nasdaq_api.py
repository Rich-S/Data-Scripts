import csv
import requests
import json


class Nasdaq_Api_Dataset:
    def __init__(self, str):
        raw = list(csv.reader(str.split('\r\n')))
        self.header = raw[0]
        self.data = self.__clean(raw[1:])

    def dictioned_data(self):
        obj = []
        for nested_list in self.data:
            d = {}
            for indx, element in enumerate(self.header):
                d[element] = nested_list[indx]
            obj.append(d)
        return obj

    def jsoned_data(self):
        return json.dumps(self.dictioned_data())

    def aggr_mktcap(self):
        data = self.column('MarketCap')
        return sum([float(i) for i in data])

    def column(self, label):
        if label not in self.header:
            raise RuntimeError(label + " does not exist in the data.")
        column_index_of_label = self.header.index(label)
        return [row[column_index_of_label] for row in self.data]

    def __clean(self, l):
        l = [[j.strip() for j in i] for i in l if (len(i) != 0) and ("TEST STOCK" not in "".join(i))]
        return l


class Nasdaq_Api:
    def __init__(self):
        self.url = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?'
        self.markets = ['AMEX', 'NASDAQ', 'NYSE']

    def universe(self):
        universe = Nasdaq_Api_Dataset('"Symbol","Name","LastSale","MarketCap","ADR TSO","IPOyear","Sector","Industry","Summary Quote"')
        for i in self.markets:
            universe.data += self.__get_data(i).data
        return universe

    def amex(self):
        return self.__get_data(self.markets[0])

    def nasdaq(self):
        return self.__get_data(self.markets[1])

    def nyse(self):
        return self.__get_data(self.markets[2])

    def __get_data(self, market):
        url = self.url + 'exchange=' + market + '&render=download'
        request = requests.get(url)
        content = str(request.content,'utf-8')
        return Nasdaq_Api_Dataset(content)


nasdaq_api = Nasdaq_Api()
universe = nasdaq_api.universe()
universe_mktcap = universe.aggr_mktcap()
universe_jsoned_data = universe.jsoned_data()
print(universe_jsoned_data)

#####

#   or get specific market

#   amex = nasdaq_api.amex()
#   amex.dictioned_data()
#   amex.jsoned_data()
#   amex.column('Symbol')
#   amex.aggr_mktcap()


