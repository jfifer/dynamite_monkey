#!/usr/bin/python2.6
import sys
import threading
import mysql.connector

class gateway (threading.Thread):
  def __init__(self, gw_id, gw_name, last_completed, last_max_id, dbHost, dbUser, dbPass):
    threading.Thread.__init__(self)
    self.gw_id = gw_id
    self.gw_name = gw_name
    self.last_completed = last_completed
    self.last_max_id = last_max_id
    self.dbHost = dbHost
    self.dbUser = dbUser
    self.dbPass = dbPass
    self.gw = mysql.connector.connect(user=dbUser, password=dbPass, host=dbHost, database=gw_name)

  def run(self):
    print "Gathering CDR Data from %s..." % (self.gw_name)
    cursor = self.gw.cursor()
    sql = "SELECT * FROM cdr WHERE callDate > '%s' AND id > %d" % (self.last_completed, self.last_max_id)
    #sql = "SELECT * FROM cdr WHERE callDate > DATE_SUB(NOW(), INTERVAL 2 MONTH)";
    cursor.execute(sql)
    result = cursor.fetchall()
    self.gw.close()
    print "Populating %s.local_cdr..." % (self.gw_name)
    billing = mysql.connector.connect(user='root', password='monkeyshit',
                              host='127.0.0.1',
                              database=self.gw_name)
    bCursor = billing.cursor()
    delete = "DELETE FROM %s.local_cdr WHERE callDate < DATE_SUB(NOW(), INTERVAL 2 MONTH)" % (self.gw_name)
    bCursor.execute(delete)
    for row in result:
      ins = "INSERT INTO local_cdr VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
      bCursor.execute(ins, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19]))

    self.gw = mysql.connector.connect(user="root", password="monkeyshit", host="localhost", database="dynamite_monkey")
    cursor = self.gw.cursor()
    sql = "UPDATE dynamite_monkey.gw_ids_xref SET last_max_id=(SELECT MAX(id) FROM %s.local_cdr), last_completed=NOW() WHERE gw_name='%s'" % (self.gw_name, self.gw_name)
    cursor.execute(sql);
    billing.close()
    print "%s Complete." % (self.gw_name)

def main(argv):
  cnx = mysql.connector.connect(user='root', password='monkeyshit',
                              host='127.0.0.1',
                              database='dynamite_monkey')

  cursor = cnx.cursor()
  sql = "SELECT gw_ids_xref_id, gw_name, last_completed, last_max_id, dbHost, dbUser, dbPass FROM gw_ids_xref";
  threads = []
  try:
    cursor.execute(sql)
    results = cursor.fetchall()
    cnx.close()
    for row in results:
      gw_id = row[0]
      gw_name = row[1]
      last_completed = row[2]
      last_max_id = row[3]
      dbHost = row[4]
      dbUser = row[5]
      dbPass = row[6]
      t = gateway(gw_id, gw_name, last_completed, last_max_id, dbHost, dbUser, dbPass)
      threads.append(t)
      t.start()
  except:
    print "Error: Unable to fetch Gateway Data"

  #cnx.close()

if __name__ == "__main__":
  main(sys.argv[1:])
