import dateutil.parser
import pytz

class Transaction:
    def __init__(self, date=None, vendor_id=None, amount=None):
        if isinstance(date, str):
            self.date = dateutil.parser.parse(date).astimezone(pytz.utc)
        else:
            self.date = date
        self.vendor_id = vendor_id
        self.amount = str(amount)

    def __eq__(self, other):
        return self.date == other.date and \
            self.vendor_id == other.vendor_id and \
            self.amount == other.amount

    def __repr__(self):
        return 'Transaction(date=%s, vendor_id=%s, amount=%s)' % (self.date_str, self.vendor_id, self.amount)
    
    @property
    def complete(self):
        return all([self.date, self.vendor_id, self.amount])

    @property
    def date_str(self):
        return self.date.strftime('%Y-%m-%d %H:%M:%S %z')