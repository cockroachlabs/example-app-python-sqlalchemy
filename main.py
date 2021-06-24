"""This simple CRUD application performs the following operations sequentially:
    1. Initializes a SQL database and table, using the cockroach sql CLI and a .sql file.
    2. Creates 100 new accounts with randomly generated IDs and randomly-computed balance amounts.
    3. Chooses two accounts at random and takes half of the money from the first and deposits it into the second.
    4. Chooses five accounts at random and deletes them.
"""

import random
from math import floor
import uuid
import os
from sqlalchemy_cockroachdb import run_transaction
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Account

# The code below inserts new accounts.

def create_accounts(session, num):
    """Create N new accounts with random account IDs and account balances.
    """
    print("Creating new accounts...")
    new_accounts = []
    while num > 0:
        account_id = uuid.uuid4()
        account_balance = floor(random.random()*1_000_000)
        new_accounts.append(
            Account(
                id = account_id,
                balance = account_balance
            )
        )
        seen_account_ids.append(account_id)
        print("Created new account with id {0} and balance {1}.".format(account_id, account_balance))
        num = num - 1
    session.add_all(new_accounts)


def transfer_funds_randomly(session, one, two):
    """Transfer money between two accounts.
    """
    source = session.query(Account).filter(Account.id == one).first()
    dest = session.query(Account).filter(Account.id == two).first()
    print("Random account balances:\nAccount {0}: {1}\nAccount {2}: {3}".format(one, source.balance, two, dest.balance))

    amount = floor(source.balance/2)
    print("Transferring {0} from account {1} to account {2}...".format(amount, one, two))

    # Check balance of the first account.
    if source.balance < amount:
        raise "Insufficient funds in account {0}".format(one)
    else:
        source.balance -= amount
        dest.balance +=  amount

    print("Transfer complete.\nNew balances:\nAccount {0}: {1}\nAccount {2}: {3}".format(one, source.balance, two, dest.balance))



def delete_accounts(session, num):
    """Delete N existing accounts, at random.
    """
    print("Deleting existing accounts...")
    delete_ids = []
    while num > 0:
        delete_id = random.choice(seen_account_ids)
        delete_ids.append(delete_id)
        seen_account_ids.remove(delete_id)
        num = num - 1

    accounts = session.query(Account).filter(Account.id.in_(delete_ids)).all()

    for account in accounts:
        print("Deleted account {0}.".format(account.id))
        session.delete(account)

# Run the transfer inside a transaction.

if __name__ == '__main__':

    conn_string = input('Enter your node\'s connection string:\n')
    # For cockroach demo:
    # postgres://demo:<demo_password>@127.0.0.1:26257?sslmode=require
    # For CockroachCloud:
    # postgres://<username>:<password>@<globalhost>:26257/<cluster_name>.defaultdb?sslmode=verify-full&sslrootcert=<certs_dir>/<ca.crt>
    try:
        db_uri = os.path.expandvars(conn_string)
        
        print("Initializing the bank database...")
        os.system('cockroach sql --url {0} -f dbinit.sql'.format(db_uri))
        print("Database initialized.")

        psycopg_uri = db_uri.replace('postgres', 'cockroachdb').replace('26257?','26257/bank?')
        # The "cockroachdb://" prefix for the engine URL indicates that we are
        # connecting to CockroachDB using the 'cockroachdb' dialect.
        # For more information, see
        # https://github.com/cockroachdb/sqlalchemy-cockroachdb.
        engine = create_engine(psycopg_uri)
    except Exception as e:
        print('Failed to connect to database.')
        print('{0}'.format(e))

    seen_account_ids = []

    run_transaction(sessionmaker(bind=engine), lambda s: create_accounts(s, 100))

    from_id = random.choice(seen_account_ids)
    to_id = random.choice([id for id in seen_account_ids if id != from_id])

    run_transaction(sessionmaker(bind=engine), lambda s: transfer_funds_randomly(s, from_id, to_id))

    run_transaction(sessionmaker(bind=engine), lambda s: delete_accounts(s, 5))
