import logging
import datetime
import pytz
import dateutil.parser
import re
from contextlib import closing

from controller import get_user_db, create_gmail_service
from model import Transaction
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class Account():    
    def __init__(self, username, gmail_address, timezone):
        self.username = username
        self.gmail_address = gmail_address
        self.timezone = timezone

        self.gmail_service = create_gmail_service()
        gmail_service_profile = self.gmail_service.users().getProfile(userId='me').execute()
        assert(gmail_service_profile['emailAddress'] == self.gmail_address)

    def retrieve_last_transaction(self):
        # Get the latest expense from db
        with closing(get_user_db(self.username)) as db:
            cursor = db.cursor()
            cursor.execute('SELECT datetime, vendor_id, amount FROM expenses ORDER BY datetime DESC LIMIT 1') # What if there are multiple eligible last rows?
            last_date, last_vendor_id, last_amount = cursor.fetchone() # What if table is empty or D.N.E.?
            last_transaction = Transaction(last_date, last_vendor_id, last_amount)
            logger.debug('Last Transaction: %s', last_transaction)
            return last_transaction

    def update_transactions(self, dry_run=True):
        last_transaction = self.retrieve_last_transaction()
        last_transaction_date_usertz = last_transaction.date.astimezone(pytz.timezone(self.timezone))
        after_date = last_transaction_date_usertz - datetime.timedelta(1) # TODO: remove -1, gmail after: is inclusive

        messages = self._get_all_transaction_messages(after_date)
        transactions = self._process_messages(messages, last_transaction)
        self._insert_transactions(transactions, dry_run=dry_run)

    def _get_all_transaction_messages(self, after_date=None):
        # Get all messages
        messages = list()
        gmail_query = 'from:(no.reply.alerts@chase.com) subject:(Your * transaction with *)'
        if after_date:
            gmail_query += ' after:' + after_date.strftime('%Y/%m/%d')
        logger.debug('Querying gmail with q=\'%s\'' % gmail_query)
        result = self.gmail_service.users().messages().list(userId='me', q=gmail_query).execute()
        
        next_page_token = result.get('nextPageToken', None)
        messages += result['messages']

        while next_page_token:
            result = self.gmail_service.users().messages().list(userId='me', pageToken=next_page_token, q='from:(no.reply.alerts@chase.com) subject:(Your * transaction with *)').execute()
            messages += result['messages']
            next_page_token = result.get('nextPageToken', None)

        logger.debug('Retrieved %d transaction messages', len(messages))
        return messages

    def _process_messages(self, messages, last_transaction):
        transactions = list()
        logger.debug('Processing Messages...')
        for message in messages:
            res = self.gmail_service.users().messages().get(userId='me', id=message['id']).execute()
            transaction = Transaction()
            for header in res['payload']['headers']:
                if header['name'] == 'Subject':
                    mo = re.match('^Your \$(?P<amount>[0-9]+\.[0-9]{2}) transaction with (?P<vendor_id>.*)$', header['value'])
                    transaction.amount = mo.group('amount')
                    transaction.vendor_id = mo.group('vendor_id')
                elif header['name'] == 'Date':
                    date_string = header['value']
                    date = dateutil.parser.parse(date_string).astimezone(pytz.utc)
                    transaction.date = date
                if transaction.complete:
                    if transaction != last_transaction and transaction.date < last_transaction.date: # TODO: Make sure this logic only gets the latest message
                        logger.debug('Keeping  %s', Transaction) # TODO: fix typo
                        transactions.append(transaction)
                    else:
                        logger.debug('Skipping %s', transaction)
                    break
            else:
                logger.warning('Could not process message with incomplete transaction data! Message id=%s', message['id'])
        return transactions

    def _insert_transactions(self, transactions, dry_run=True):
        # populate expenses table
    
        with closing(get_user_db(self.username)) as db:
            cursor = db.cursor()
            for transaction in transactions:
                sql = 'INSERT INTO expenses (datetime, vendor_id, amount) VALUES (%s, %s, %s)'
                row = (transaction.date_str, transaction.vendor_id, transaction.amount)
                cursor.execute(sql, row)

            if not dry_run:
                db.commit()