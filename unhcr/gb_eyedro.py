
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
import logging
import numpy as np
import pandas as pd

import psycopg2
from unhcr import constants as const
from unhcr import db
from unhcr import err_handler

mods = [["constants", "const"],["db", "db"],["err_handler", "err_handler"]]
if const.LOCAL:  # testing with local python files
    const, db, err_handler = const.import_local_libs(mods=mods)

engines = db.set_db_engines()

def get_gb_hypertables(eng):
    """
    Gets all hypertables in the "eyedro" schema that have a name like 'gb_%'.
    
    Parameters:
    end (datetime): The end date for the query.
    
    Returns:
    list of str: A list of hypertable names.
    """
    return db.sql_execute("""SELECT hypertable_name FROM timescaledb_information.hypertables 
    where hypertable_schema = 'eyedro' and hypertable_name like 'gb_%';""", eng)


def hyper_gb_gaps(hypertable_name, eng):
    """
    Collects all hypergaps in the "eyedro" schema and writes the results to a CSV file.
    
    The function first gets all hypertables in the "eyedro" schema, then runs a query on each one
    to detect gaps in the data. The results are collected in a list and then written to a CSV file.
    
    The query is a window query that calculates the difference between the current epoch and the previous
    epoch for each row in the table. If the difference is not equal to 60 (i.e., there is a gap), the row
    is included in the results.
    
    The results are written to a CSV file in the "data/gaps" directory. The file is named "eyedro_data_gaps.csv".
    """
    try:
        conn = eng.raw_connection()
        with conn.cursor() as cursor:
            query = f"""
            WITH ordered_epochs AS (
                SELECT epoch_secs, 
                    LAG(epoch_secs) OVER (ORDER BY epoch_secs) AS prev_epoch
                FROM eyedro.{hypertable_name}
            )
            SELECT '{hypertable_name}' AS hypertable, epoch_secs, prev_epoch, (epoch_secs - prev_epoch) AS diff_seconds
            FROM ordered_epochs
            WHERE (epoch_secs - prev_epoch) != 60;
            """

            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Processed hypertable {hypertable_name}, gaps found: {len(rows)}")

        conn.close()
        return rows, None

    except psycopg2.OperationalError as e:
        err = e
        print(f"Database connection error: {e}")
    except psycopg2.DatabaseError as e:
        err = e
        print(f"Database query error: {e}")
    except Exception as e:
        err = e
        print(f"Unexpected error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    return None, err

def hyper_gb_gaps_concur(eng, ht_names, src='local'):

    def process_chunk(chunk, param1):
        return [hyper_gb_gaps(item[0], eng=param1) for item in chunk]  # Apply function to each item

    # ht_names = []
    # for item in res:
    #     ht_names.append(item[0][0])
    
    num_parts = 20
    chunks = [list(chunk) for chunk in np.array_split(ht_names, num_parts)]  # Ensure list format

    # cutoff = datetime.now(timezone.utc) - timedelta(days=12)
    # epoch_cutoff = get_previous_midnight_epoch(int(cutoff.timestamp()))
    # local_engine = db.set_local_defaultdb_engine()

    with ThreadPoolExecutor(max_workers=num_parts) as executor:
        results = list(executor.map(partial(process_chunk, param1=eng), chunks))

    return results
    # Flatten results
    return [item for sublist in results for item in sublist]



def get_all_gb_gaps(src='local'):
    if src == 'local':
        eng = engines[1]
    else:
        eng = engines[0]
        ht_names, err = get_gb_hypertables(eng)
        pass
    res = hyper_gb_gaps_concur(src)
    flattened_data = list(chain.from_iterable(chain.from_iterable(res)))
    #Step 3: Convert results to DataFrame
    df = pd.DataFrame(flattened_data, columns=["hypertable", "epoch_secs", "prev_epoch", "diff_seconds"])
    df['src'] = src

    #Save results to CSV
    df.to_csv(r"E:\_UNHCR\CODE\DATA\gaps\eyedro_data_gaps.csv", index=False)

    #Print summary
    print(df.head())
    print(f"✅ Data gaps found in {len(df)} rows. Results saved to 'eyedro_data_gaps.csv'.")
