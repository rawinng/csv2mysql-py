"""
Import CSV file to MySql base on column name
"""
import sys
import MySQLdb

def clean_str(target):
    """
    Clean string
    """
    return target.strip().replace(" ", "_").replace(".", "").replace("\n", "")

def clean_sql_str(target):
    """
    Clear field string
    """
    return target.replace("'", "\\'").replace("\n", "")

def read_file(target_file):
    """
    READ filename then extract data into tuple
    """
    open_file = open(target_file, "U")
    lines = open_file.readlines()
    lines = [line.strip().split(",") for line in lines]

    cols = [clean_str(col).lower() for col in lines[0] if col != ""]
    datas = [dataline[:len(cols)] for dataline in lines[1:] if len(dataline) > len(cols)]
    #print "\ncol-size = %d\n" % len(self.cols),

    return (cols, datas)

def create_table(database, table_name, cols):
    cur = database.cursor()

    create_query_stmt = """
    CREATE TABLE %s (
        id int auto_increment PRIMARY KEY,
        %s
    )
    """ % table_name, ",".join([col + " varchar(255)" for col in cols]),
    cur.execute(create_query_stmt)
    cur.close()

def insert_data(database, table_name, cols, datas):
    """
    Insert data
    """
    cursor = database.cursor()

    strdata_stmt = []
    for data in datas:
        strdata_stmt.append("(%s)" % (",".join(["'%s'" % clean_sql_str(col) for col in data])))

    insert_stmt = """
    INSERT INTO %s ( %s )
        VALUES %s
    """ % (table_name, ",".join(cols), ",".join(strdata_stmt))

    cursor.execute(insert_stmt)
    database.commit()
    cursor.close()

def display_data(cols, datas):
    """
    Print data
    """
    for data in datas:
        for index in range(len(cols)):
            print "%s: %s" % (cols[index], data[index].strip())

        print "### end for this record ###"
####
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print """Import csv to mysql database tool develop by Rawin Ngamloet
        %s (cmd) (file)\n(cmd)\t\tcreate, insert, display\n(file)\t\tfilename""" % __file__,
        sys.exit()

    (cmd, finame) = (sys.argv[1], sys.argv[2])
    db = MySQLdb.connect(host="127.0.0.1", db="db", user="root", passwd="root")

    tab_name = finame

    if finame.find('.') > -1:
        tab_name = finame.split(".")[0]

    (cols, datas) = read_file(finame)

    if cmd == "create":
        create_table(db, tab_name, cols)
    elif cmd == "insert":
        insert_data(db, tab_name, cols, datas)
    elif cmd == "display":
        display_data(cols, datas)
    else:
        print "cmd incorrect"
