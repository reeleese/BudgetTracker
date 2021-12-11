from model import Account

account = Account('LEE', 'leereese3@gmail.com', timezone='US/Eastern')
account.update_transactions(dry_run=True)
pass