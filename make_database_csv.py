import sqlite3 as dbase

conn = dbase.connect('coupon.lite')

def make_database_csv(conn, coding = 'utf-8'):
    """
    Takes all *.csv files from directory and makes them into database
    with connection 'conn' and encoding 'utf-8'
    Assumes that dbase does not exist before creating it
    """
    import glob 
    import csv
    from os.path import splitext
    csvlist = glob.glob('*.csv')
    
    for file in csvlist:
        print('Processing %s' % file)
        with open(file, 'r', encoding = coding) as table:
            
            reader = csv.reader(table)
            columns = next(reader) 
            table_name = splitext(file)[0]
            
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE {0} ({1})".format(table_name,','.join(columns)))
            
            query = 'insert into {0}({1}) values ({2})'
            query = query.format(table_name,','.join(columns), ','.join('?' * len(columns)))

            for data in reader:
                cursor.execute(query, data)
            conn.commit()

make_database_csv(conn)