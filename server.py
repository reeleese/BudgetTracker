import logging
import datetime
import pytz
import dateutil.parser
import re

from controller import get_user_db, create_gmail_service
from model import Transaction

def get_last_transaction(db):
    # Get the latest expense from db
    cursor = db.cursor()
    cursor.execute('SELECT datetime, vendor_id, amount FROM expenses ORDER BY datetime DESC LIMIT 1') # What if there are multiple eligible last rows?
    last_date, last_vendor_id, last_amount = cursor.fetchone() # What if table is empty or D.N.E.?
    return Transaction(last_date, last_vendor_id, last_amount)

def get_all_transaction_messages(gmail_service, after_date=None):
    # Get all messages
    messages = list()
    gmail_query = 'from:(no.reply.alerts@chase.com) subject:(Your * transaction with *)'
    if after_date:
        gmail_query += ' after:' + after_date.strftime('%Y/%m/%d')
    logger.debug('Querying gmail with q=\'%s\'' % gmail_query)
    result = gmail_service.users().messages().list(userId='me', q=gmail_query).execute()
    
    next_page_token = result.get('nextPageToken', None)
    messages += result['messages']

    while next_page_token:
        result = gmail_service .users().messages().list(userId='me', pageToken=next_page_token, q='from:(no.reply.alerts@chase.com) subject:(Your * transaction with *)').execute()
        messages += result['messages']
        next_page_token = result.get('nextPageToken', None)

    logger.debug('Retrieved %d transaction messages', len(messages))
    return messages

def process_messages(gmail_service, messages, last_transaction):
    transactions = list()
    for message in messages:
        res = gmail_service.users().messages().get(userId='me', id=message['id']).execute()
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

                if transaction.date < last_transaction.date:
                    break

            if transaction.complete:
                if transaction != last_transaction:
                    transactions.append(transaction)
                break
        else:
            logger.warning('Could not process message with incomplete transaction data! Message id=%s', message['id'])
    return transactions

def insert_transactions(db, transactions, dry_run=True):
    # populate expenses table
    cursor = db.cursor()
    for transaction in transactions:
        sql = 'INSERT INTO expenses (datetime, vendor_id, amount) VALUES (%s, %s, %s)'
        row = (transaction.date_str, transaction.vendor_id, transaction.amount)
        cursor.execute(sql, row)

    if not dry_run:
        db.commit()

USER = 'LEE'
TIMEZONE = 'US/Eastern'
DRY_RUN = True
logger = logging.getLogger(__name__)
gmail_service = create_gmail_service()



db = get_user_db(USER)

last_transaction = get_last_transaction(db)
last_transaction_date_usertz = last_transaction.date.astimezone(pytz.timezone(TIMEZONE))
after_date = last_transaction_date_usertz - datetime.timedelta(1)
messages = get_all_transaction_messages(gmail_service, after_date=after_date)
transactions = process_messages(gmail_service, messages, last_transaction)

insert_transactions(db, transactions, DRY_RUN)

db.close()