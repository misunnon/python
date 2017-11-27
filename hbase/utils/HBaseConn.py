import happybase
def hbase_conn(ip):
    connection = happybase.Connection(ip, autoconnect=False)
    connection.open()
    return connection
def open_table(connection,_table):
    table = connection.table(_table)
    return table

if __name__=="__main__":
    pass


