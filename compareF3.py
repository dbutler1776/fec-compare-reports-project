import sqlite3
import csv
import os, contextlib
import time


#in case the code blew up for some reason part-way through a previous run, remove the old database and output files
with contextlib.suppress(FileNotFoundError):
    os.remove('comparedb.sqlite')
    os.remove('Output1_Entries_Deleted.csv')
    os.remove('Output2_Entries_Added.csv')
    os.remove('Output3_Entries_Changed.csv')
    os.remove('Output4_Report1_Cleaned.csv')
    os.remove('Output5_Report2_Cleaned.csv')

#create the database file and connect to it
conn = sqlite3.connect('comparedb.sqlite')
cur = conn.cursor()

# Do some setup for the tables first
cur.executescript('''
DROP TABLE IF EXISTS Report1;
DROP TABLE IF EXISTS Report2;
DROP TABLE IF EXISTS R1notR2;
DROP TABLE IF EXISTS R2notR1;
DROP TABLE IF EXISTS Changed;
DROP TABLE IF EXISTS Temp;
DROP TABLE IF EXISTS TempReport1;
DROP TABLE IF EXISTS TempReport2;
DROP TABLE IF EXISTS ChangedTable;


CREATE TABLE Report1 (
    id                INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    sourcefile        TEXT,
    linenum           TEXT,
    transid           TEXT,
    orgname           TEXT,
    lastname          TEXT,
    firstname         TEXT,
    address1          TEXT,
    address2          TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    electioncode      TEXT,
    electionother     TEXT,
    date              TEXT,
    amount            DECIMAL(12,2),
    aggregate         DECIMAL(12,2),
    purpose           TEXT,
    employer          TEXT,
    occupation        TEXT,
    memoentry         TEXT,
    note              TEXT,
    beginningbalance  DECIMAL(12,2),
    incurredamount    DECIMAL(12,2),
    paymentamount     DECIMAL(12,2),
    balanceatclose    DECIMAL(12,2),
    disseminationdate TEXT,
    supportoppose     TEXT,
    candlastname      TEXT,
    candfirstname     TEXT,
    candoffice        TEXT,
    canddistrict      TEXT,
    candstate         TEXT,
    acctidentifier    TEXT,
    fedshare          DECIMAL(12,2),
    nonfedshare       DECIMAL(12,2),
    levinshare        DECIMAL(12,2),
    h4activity        TEXT,
    h4ad              TEXT,
    h4df              TEXT,
    h4ea              TEXT,
    h4gv              TEXT,
    h4dc              TEXT,
    h4pc              TEXT,
    h6activity        TEXT,
    h6voterreg        TEXT,
    h6gotv            TEXT,
    h6voterid         TEXT,
    h6generic         TEXT,
    onotherreport     TEXT
);

CREATE TABLE Report2 (
    id                INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    sourcefile        TEXT,
    linenum           TEXT,
    transid           TEXT,
    orgname           TEXT,
    lastname          TEXT,
    firstname         TEXT,
    address1          TEXT,
    address2          TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    electioncode      TEXT,
    electionother     TEXT,
    date              TEXT,
    amount            DECIMAL(12,2),
    aggregate         DECIMAL(12,2),
    purpose           TEXT,
    employer          TEXT,
    occupation        TEXT,
    memoentry         TEXT,
    note              TEXT,
    beginningbalance  DECIMAL(12,2),
    incurredamount    DECIMAL(12,2),
    paymentamount     DECIMAL(12,2),
    balanceatclose    DECIMAL(12,2),
    disseminationdate TEXT,
    supportoppose     TEXT,
    candlastname      TEXT,
    candfirstname     TEXT,
    candoffice        TEXT,
    canddistrict      TEXT,
    candstate         TEXT,
    acctidentifier    TEXT,
    fedshare          DECIMAL(12,2),
    nonfedshare       DECIMAL(12,2),
    levinshare        DECIMAL(12,2),
    h4activity        TEXT,
    h4ad              TEXT,
    h4df              TEXT,
    h4ea              TEXT,
    h4gv              TEXT,
    h4dc              TEXT,
    h4pc              TEXT,
    h6activity        TEXT,
    h6voterreg        TEXT,
    h6gotv            TEXT,
    h6voterid         TEXT,
    h6generic         TEXT,
    onotherreport     TEXT
);

CREATE TABLE R1notR2 (
    id                INTEGER,
    sourcefile        TEXT,
    linenum           TEXT,
    transid           TEXT,
    orgname           TEXT,
    lastname          TEXT,
    firstname         TEXT,
    address1          TEXT,
    address2          TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    electioncode      TEXT,
    electionother     TEXT,
    date              TEXT,
    amount            DECIMAL(12,2),
    aggregate         DECIMAL(12,2),
    purpose           TEXT,
    employer          TEXT,
    occupation        TEXT,
    memoentry         TEXT,
    note              TEXT,
    beginningbalance  DECIMAL(12,2),
    incurredamount    DECIMAL(12,2),
    paymentamount     DECIMAL(12,2),
    balanceatclose    DECIMAL(12,2),
    disseminationdate TEXT,
    supportoppose     TEXT,
    candlastname      TEXT,
    candfirstname     TEXT,
    candoffice        TEXT,
    canddistrict      TEXT,
    candstate         TEXT,
    acctidentifier    TEXT,
    fedshare          DECIMAL(12,2),
    nonfedshare       DECIMAL(12,2),
    levinshare        DECIMAL(12,2),
    h4activity        TEXT,
    h6activity        TEXT,
    onotherreport     TEXT
);

CREATE TABLE R2notR1 (
    id                INTEGER,
    sourcefile        TEXT,
    linenum           TEXT,
    transid           TEXT,
    orgname           TEXT,
    lastname          TEXT,
    firstname         TEXT,
    address1          TEXT,
    address2          TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    electioncode      TEXT,
    electionother     TEXT,
    date              TEXT,
    amount            DECIMAL(12,2),
    aggregate         DECIMAL(12,2),
    purpose           TEXT,
    employer          TEXT,
    occupation        TEXT,
    memoentry         TEXT,
    note              TEXT,
    beginningbalance  DECIMAL(12,2),
    incurredamount    DECIMAL(12,2),
    paymentamount     DECIMAL(12,2),
    balanceatclose    DECIMAL(12,2),
    disseminationdate TEXT,
    supportoppose     TEXT,
    candlastname      TEXT,
    candfirstname     TEXT,
    candoffice        TEXT,
    canddistrict      TEXT,
    candstate         TEXT,
    acctidentifier    TEXT,
    fedshare          DECIMAL(12,2),
    nonfedshare       DECIMAL(12,2),
    levinshare        DECIMAL(12,2),
    h4activity        TEXT,
    h6activity        TEXT,
    onotherreport     TEXT
);

CREATE TABLE Changed (
    id                INTEGER,
    sourcefile        TEXT,
    linenum           TEXT,
    transid           TEXT,
    orgname           TEXT,
    lastname          TEXT,
    firstname         TEXT,
    address1          TEXT,
    address2          TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    electioncode      TEXT,
    electionother     TEXT,
    date              TEXT,
    amount            DECIMAL(12,2),
    aggregate         DECIMAL(12,2),
    purpose           TEXT,
    employer          TEXT,
    occupation        TEXT,
    memoentry         TEXT,
    note              TEXT,
    beginningbalance  DECIMAL(12,2),
    incurredamount    DECIMAL(12,2),
    paymentamount     DECIMAL(12,2),
    balanceatclose    DECIMAL(12,2),
    disseminationdate TEXT,
    supportoppose     TEXT,
    candlastname      TEXT,
    candfirstname     TEXT,
    candoffice        TEXT,
    canddistrict      TEXT,
    candstate         TEXT,
    acctidentifier    TEXT,
    fedshare          DECIMAL(12,2),
    nonfedshare       DECIMAL(12,2),
    levinshare        DECIMAL(12,2),
    h4activity        TEXT,
    h6activity        TEXT,
    onotherreport     TEXT
);
CREATE TABLE ChangedTable (
    id                INTEGER,
    sourcefile        TEXT,
    linenum           TEXT,
    transid           TEXT,
    orgname           TEXT,
    lastname          TEXT,
    firstname         TEXT,
    address1          TEXT,
    address2          TEXT,
    city              TEXT,
    state             TEXT,
    zipcode           TEXT,
    electioncode      TEXT,
    electionother     TEXT,
    date              TEXT,
    amount            DECIMAL(12,2),
    aggregate         DECIMAL(12,2),
    purpose           TEXT,
    employer          TEXT,
    occupation        TEXT,
    memoentry         TEXT,
    note              TEXT,
    beginningbalance  DECIMAL(12,2),
    incurredamount    DECIMAL(12,2),
    paymentamount     DECIMAL(12,2),
    balanceatclose    DECIMAL(12,2),
    disseminationdate TEXT,
    supportoppose     TEXT,
    candlastname      TEXT,
    candfirstname     TEXT,
    candoffice        TEXT,
    canddistrict      TEXT,
    candstate         TEXT,
    acctidentifier    TEXT,
    fedshare          DECIMAL(12,2),
    nonfedshare       DECIMAL(12,2),
    levinshare        DECIMAL(12,2),
    h4activity        TEXT,
    h6activity        TEXT,
    onotherreport     TEXT
)
''')

#User input
print()
fname1 = input('Enter file name 1 (default is report1.csv): ')
if len(fname1) < 1: #add a try statement?
    fname1 = 'report1.csv' #this default value saves time during testing
    print(fname1)
print()
fname2 = input('Enter file name 2 (default is report2.csv): ')
if len(fname2) < 1: #add a try statement?
    fname2 = 'report2.csv' #this default value saves time during testing
    print(fname2)
print()
print('Compare all columns? (y/n)')
print('    y = compare all columns (this is the default)')
print('    n = ignore changes in the address and aggregate fields')
while True: #let's limit the user to entering only a 'y' or an 'n' here
    compareall = input('    ')
    if len(compareall) < 1:
        compareall = 'y' #this default value saves time during testing
        print('   ',compareall)
        break
    elif compareall == 'y' :
        break
    elif compareall == 'n' :
        break
    else :
        print('Input is not valid. Please enter y or n:')


#now that the user input is done, start the timer
start = time.clock()


#Set counts to zero
count1 = 0   #count of entries reviewed on all schedules of Report 1
countsa1 = 0 #count of entries on Schedule A of Report 1
countsb1 = 0 #count of entries on Schedule B of Report 1
countsd1 = 0 #count of entries on Schedule D of Report 1
countse1 = 0 #count of entries on Schedule E of Report 1
countsf1 = 0 #count of entries on Schedule F of Report 1
countsh41 = 0 #count of entries on Schedule H4 of Report 1
countsh61 = 0 #count of entries on Schedule H6 of Report 1
count2 = 0   #count of entries reviewed on all schedules of Report 2
countsa2 = 0 #count of entries on Schedule A of Report 2
countsb2 = 0 #count of entries on Schedule B of Report 2
countsd2 = 0 #count of entries on Schedule D of Report 2
countse2 = 0 #count of entries on Schedule E of Report 2
countsf2 = 0 #count of entries on Schedule F of Report 2
countsh42 = 0 #count of entries on Schedule H4 of Report 2
countsh62 = 0 #count of entries on Schedule H6 of Report 2


#FIRST REPORT
print()
print('Opening', fname1)
print('Inserting Schedule A of', fname1, 'into SQL database tables')
cur.execute('''BEGIN TRANSACTION;''')
with open(fname1) as csvfile:
    str_data = csv.reader(csvfile)
    for row in str_data:
        values = row
        linenum = values[0] #find the first value in the list
        if not linenum.startswith('SA') : continue #let's look at Schedule A first
        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
        countsa1 = countsa1 + 1 #keep track of count of entries on Schedule A
        #find the rest of the interesting data we want for this entry
        sourcefile = 'report1'
        transid = values[2]
        orgname = values[6]
        lastname = values[7]
        firstname = values[8]
        address1 = values[12]
        address2 = values[13]
        city = values[14]
        state = values[15]
        zipcode = values[16]
        electioncode = ''
        electionother = ''
        date = values[19]
        amount = values[20]
        aggregate = values[21]
        purpose = ''
        employer = values[23]
        occupation = values[24]
        memoentry = values[42]
        note = values[43]
        beginningbalance = ''
        incurredamount = ''
        paymentamount = ''
        balanceatclose = ''
        disseminationdate = ''
        supportoppose = ''
        candlastname = ''
        candfirstname = ''
        candoffice = ''
        canddistrict = ''
        candstate = ''
        acctidentifier = ''
        fedshare = ''
        nonfedshare = ''
        levinshare = ''
        h4activity = ''
        h4ad = ''
        h4df = ''
        h4ea = ''
        h4gv = ''
        h4dc = ''
        h4pc = ''
        h6activity = ''
        h6voterreg = ''
        h6gotv = ''
        h6voterid = ''
        h6generic = ''
        onotherreport = ''
        #now put this interesting data into the SQL database table
        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
        #conn.commit()
conn.commit()
#cur.execute('''COMMIT;''')
#updates user to tell them progress made so far
print ('Inserted', countsa1, 'entries from Schedule A')

print('Inserting Schedule B of', fname1, 'into SQL database tables')
cur.execute('''BEGIN TRANSACTION;''')
with open(fname1) as csvfile:
    str_data = csv.reader(csvfile)
    for row in str_data:
        values = row
        linenum = values[0] #find the first value in the list
        if not linenum.startswith('SB') : continue #now let's look at Schedule B entries
        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
        countsb1 = countsb1 + 1 #keep track of count of entries on Schedule B
        #find the rest of the interesting data we want for this entry
        sourcefile = 'report1'
        transid = values[2]
        orgname = values[6]
        lastname = values[7]
        firstname = values[8]
        address1 = values[12]
        address2 = values[13]
        city = values[14]
        state = values[15]
        zipcode = values[16]
        electioncode = values[17]
        electionother = values[18]
        date = values[19]
        amount = values[20]
        aggregate = ''
        purpose = values[22]
        employer = ''
        occupation = ''
        memoentry = values[41]
        note = values[42]
        beginningbalance = ''
        incurredamount = ''
        paymentamount = ''
        balanceatclose = ''
        disseminationdate = ''
        supportoppose = ''
        candlastname = ''
        candfirstname = ''
        candoffice = ''
        canddistrict = ''
        candstate = ''
        acctidentifier = ''
        fedshare = ''
        nonfedshare = ''
        levinshare = ''
        h4activity = ''
        h4ad = ''
        h4df = ''
        h4ea = ''
        h4gv = ''
        h4dc = ''
        h4pc = ''
        h6activity = ''
        h6voterreg = ''
        h6gotv = ''
        h6voterid = ''
        h6generic = ''
        onotherreport = ''
        #now put this interesting data into the SQL database table
        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
        #conn.commit()
conn.commit()
#cur.execute('''COMMIT;''')
#updates user to tell them progress made so far
print ('Inserted', countsb1, 'entries from Schedule B')

print('Inserting Schedule D of', fname1, 'into SQL database tables')
cur.execute('''BEGIN TRANSACTION;''')
with open(fname1) as csvfile:
    str_data = csv.reader(csvfile)
    for row in str_data:
        values = row
        linenum = values[0] #find the first value in the list
        if not linenum.startswith('SD') : continue #now let's look at Schedule D entries
        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
        countsd1 = countsd1 + 1 #keep track of count of entries on Schedule D
        #find the rest of the interesting data we want for this entry
        sourcefile = 'report1'
        transid = values[2]
        orgname = values[4]
        lastname = values[5]
        firstname = values[6]
        address1 = values[10]
        address2 = values[11]
        city = values[12]
        state = values[13]
        zipcode = values[14]
        electioncode = ''
        electionother = ''
        date = ''
        amount = ''
        aggregate = ''
        purpose = values[15]
        employer = ''
        occupation = ''
        memoentry = ''
        note = ''
        beginningbalance = values[16]
        incurredamount = values[17]
        paymentamount = values[18]
        balanceatclose = values[19]
        disseminationdate = ''
        supportoppose = ''
        candlastname = ''
        candfirstname = ''
        candoffice = ''
        canddistrict = ''
        candstate = ''
        acctidentifier = ''
        fedshare = ''
        nonfedshare = ''
        levinshare = ''
        h4activity = ''
        h4ad = ''
        h4df = ''
        h4ea = ''
        h4gv = ''
        h4dc = ''
        h4pc = ''
        h6activity = ''
        h6voterreg = ''
        h6gotv = ''
        h6voterid = ''
        h6generic = ''
        onotherreport = ''
        #now put this interesting data into the SQL database table
        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
        #conn.commit()
conn.commit()
#cur.execute('''COMMIT;''')
#updates user to tell them progress made so far
print ('Inserted', countsd1, 'entries from Schedule D')

##print('Inserting Schedule E of', fname1, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname1) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('SE') : continue #now let's look at Schedule E entries
##        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countse1 = countse1 + 1 #keep track of count of entries on Schedule E
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report1'
##        transid = values[2]
##        orgname = values[6]
##        lastname = values[7]
##        firstname = values[8]
##        address1 = values[12]
##        address2 = values[13]
##        city = values[14]
##        state = values[15]
##        zipcode = values[16]
##        electioncode = values[17]
##        electionother = values[18]
##        date = values[21]
##        amount = values[20]
##        aggregate = values[22]
##        purpose = values[23]
##        employer = ''
##        occupation = ''
##        memoentry = values[42]
##        note = values[43]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = values[19]
##        supportoppose = values[26]
##        candlastname = values[28]
##        candfirstname = values[29]
##        candoffice = values[33]
##        canddistrict = values[34]
##        candstate = values[35]
##        acctidentifier = ''
##        fedshare = ''
##        nonfedshare = ''
##        levinshare = ''
##        h4activity = ''
##        h4ad = ''
##        h4df = ''
##        h4ea = ''
##        h4gv = ''
##        h4dc = ''
##        h4pc = ''
##        h6activity = ''
##        h6voterreg = ''
##        h6gotv = ''
##        h6voterid = ''
##        h6generic = ''
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countse1, 'entries from Schedule E')
##
##print('Inserting Schedule F of', fname1, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname1) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('SF') : continue #now let's look at Schedule F entries
##        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countsf1 = countsf1 + 1 #keep track of count of entries on Schedule F
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report1'
##        transid = values[2]
##        orgname = values[16]
##        lastname = values[17]
##        firstname = values[18]
##        address1 = values[22]
##        address2 = values[23]
##        city = values[24]
##        state = values[25]
##        zipcode = values[26]
##        electioncode = ''
##        electionother = ''
##        date = values[27]
##        amount = values[28]
##        aggregate = values[29]
##        purpose = values[30]
##        employer = ''
##        occupation = ''
##        memoentry = values[42]
##        note = values[43]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = ''
##        supportoppose = ''
##        candlastname = values[34]
##        candfirstname = values[35]
##        candoffice = values[39]
##        canddistrict = values[41]
##        candstate = values[40]
##        acctidentifier = ''
##        fedshare = ''
##        nonfedshare = ''
##        levinshare = ''
##        h4activity = ''
##        h4ad = ''
##        h4df = ''
##        h4ea = ''
##        h4gv = ''
##        h4dc = ''
##        h4pc = ''
##        h6activity = ''
##        h6voterreg = ''
##        h6gotv = ''
##        h6voterid = ''
##        h6generic = ''
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countsf1, 'entries from Schedule F')
##
##print('Inserting Schedule H4 of', fname1, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname1) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('H4') : continue #now let's look at Schedule H4 entries
##        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countsh41 = countsh41 + 1 #keep track of count of entries on Schedule H4
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report1'
##        transid = values[2]
##        orgname = values[6]
##        lastname = values[7]
##        firstname = values[8]
##        address1 = values[12]
##        address2 = values[13]
##        city = values[14]
##        state = values[15]
##        zipcode = values[16]
##        electioncode = ''
##        electionother = ''
##        date = values[18]
##        amount = values[19]
##        aggregate = values[22]
##        purpose = values[23]
##        employer = ''
##        occupation = ''
##        memoentry = values[31]
##        note = values[32]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = ''
##        supportoppose = ''
##        candlastname = ''
##        candfirstname = ''
##        candoffice = ''
##        canddistrict = ''
##        candstate = ''
##        acctidentifier = values[17]
##        fedshare = values[20]
##        nonfedshare = values[21]
##        levinshare = ''
##        h4activity = ''
##        h4ad = values[25]
##        h4df = values[26]
##        h4ea = values[27]
##        h4gv = values[28]
##        h4dc = values[29]
##        h4pc = values[30]
##        h6activity = ''
##        h6voterreg = ''
##        h6gotv = ''
##        h6voterid = ''
##        h6generic = ''
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countsh41, 'entries from Schedule H4')
##
##print('Inserting Schedule H6 of', fname1, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname1) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('H6') : continue #now let's look at Schedule H6 entries
##        count1 = count1 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countsh61 = countsh61 + 1 #keep track of count of entries on Schedule H6
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report1'
##        transid = values[2]
##        orgname = values[6]
##        lastname = values[7]
##        firstname = values[8]
##        address1 = values[12]
##        address2 = values[13]
##        city = values[14]
##        state = values[15]
##        zipcode = values[16]
##        electioncode = ''
##        electionother = ''
##        date = values[18]
##        amount = values[19]
##        aggregate = values[22]
##        purpose = values[23]
##        employer = ''
##        occupation = ''
##        memoentry = values[29]
##        note = values[30]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = ''
##        supportoppose = ''
##        candlastname = ''
##        candfirstname = ''
##        candoffice = ''
##        canddistrict = ''
##        candstate = ''
##        acctidentifier = values[17]
##        fedshare = values[20]
##        nonfedshare = ''
##        levinshare = values[21]
##        h4activity = ''
##        h4ad = ''
##        h4df = ''
##        h4ea = ''
##        h4gv = ''
##        h4dc = ''
##        h4pc = ''
##        h6activity = ''
##        h6voterreg = values[25]
##        h6gotv = values[26]
##        h6voterid = values[27]
##        h6generic = values[28]
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report1 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countsh61, 'entries from Schedule H6')

#Summary AT END of all schedules
print ('Inserted', count1, 'entries total from', fname1)


#SECOND REPORT
print()
print('Opening', fname2)
print('Inserting Schedule A of', fname2, 'into SQL database tables')
cur.execute('''BEGIN TRANSACTION;''')
with open(fname2) as csvfile:
    str_data = csv.reader(csvfile)
    for row in str_data:
        values = row
        linenum = values[0] #find the first value in the list
        if not linenum.startswith('SA') : continue #let's look at Schedule A first
        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
        countsa2 = countsa2 + 1 #keep track of count of entries on Schedule A
        #find the rest of the interesting data we want for this entry
        sourcefile = 'report2'
        transid = values[2]
        orgname = values[6]
        lastname = values[7]
        firstname = values[8]
        address1 = values[12]
        address2 = values[13]
        city = values[14]
        state = values[15]
        zipcode = values[16]
        electioncode = ''
        electionother = ''
        date = values[19]
        amount = values[20]
        aggregate = values[21]
        purpose = ''
        employer = values[23]
        occupation = values[24]
        memoentry = values[42]
        note = values[43]
        beginningbalance = ''
        incurredamount = ''
        paymentamount = ''
        balanceatclose = ''
        disseminationdate = ''
        supportoppose = ''
        candlastname = ''
        candfirstname = ''
        candoffice = ''
        canddistrict = ''
        candstate = ''
        acctidentifier = ''
        fedshare = ''
        nonfedshare = ''
        levinshare = ''
        h4activity = ''
        h4ad = ''
        h4df = ''
        h4ea = ''
        h4gv = ''
        h4dc = ''
        h4pc = ''
        h6activity = ''
        h6voterreg = ''
        h6gotv = ''
        h6voterid = ''
        h6generic = ''
        onotherreport = ''
        #now put this interesting data into the SQL database table
        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
        #conn.commit()
conn.commit()
#cur.execute('''COMMIT;''')
#updates user to tell them progress made so far
print ('Inserted', countsa2, 'entries from Schedule A')

print('Inserting Schedule B of', fname2, 'into SQL database tables')
cur.execute('''BEGIN TRANSACTION;''')
with open(fname2) as csvfile:
    str_data = csv.reader(csvfile)
    for row in str_data:
        values = row
        linenum = values[0] #find the first value in the list
        if not linenum.startswith('SB') : continue #now let's look at Schedule B entries
        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
        countsb2 = countsb2 + 1 #keep track of count of entries on Schedule B
        #find the rest of the interesting data we want for this entry
        sourcefile = 'report2'
        transid = values[2]
        orgname = values[6]
        lastname = values[7]
        firstname = values[8]
        address1 = values[12]
        address2 = values[13]
        city = values[14]
        state = values[15]
        zipcode = values[16]
        electioncode = values[17]
        electionother = values[18]
        date = values[19]
        amount = values[20]
        aggregate = ''
        purpose = values[22]
        employer = ''
        occupation = ''
        memoentry = values[41]
        note = values[42]
        beginningbalance = ''
        incurredamount = ''
        paymentamount = ''
        balanceatclose = ''
        disseminationdate = ''
        supportoppose = ''
        candlastname = ''
        candfirstname = ''
        candoffice = ''
        canddistrict = ''
        candstate = ''
        acctidentifier = ''
        fedshare = ''
        nonfedshare = ''
        levinshare = ''
        h4activity = ''
        h4ad = ''
        h4df = ''
        h4ea = ''
        h4gv = ''
        h4dc = ''
        h4pc = ''
        h6activity = ''
        h6voterreg = ''
        h6gotv = ''
        h6voterid = ''
        h6generic = ''
        onotherreport = ''
        #now put this interesting data into the SQL database table
        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
        #conn.commit()
conn.commit()
#cur.execute('''COMMIT;''')
#updates user to tell them progress made so far
print ('Inserted', countsb2, 'entries from Schedule B')

print('Inserting Schedule D of', fname2, 'into SQL database tables')
cur.execute('''BEGIN TRANSACTION;''')
with open(fname2) as csvfile:
    str_data = csv.reader(csvfile)
    for row in str_data:
        values = row
        linenum = values[0] #find the first value in the list
        if not linenum.startswith('SD') : continue #now let's look at Schedule D entries
        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
        countsd2 = countsd2 + 1 #keep track of count of entries on Schedule D
        #find the rest of the interesting data we want for this entry
        sourcefile = 'report2'
        transid = values[2]
        orgname = values[4]
        lastname = values[5]
        firstname = values[6]
        address1 = values[10]
        address2 = values[11]
        city = values[12]
        state = values[13]
        zipcode = values[14]
        electioncode = ''
        electionother = ''
        date = ''
        amount = ''
        aggregate = ''
        purpose = values[15]
        employer = ''
        occupation = ''
        memoentry = ''
        note = ''
        beginningbalance = values[16]
        incurredamount = values[17]
        paymentamount = values[18]
        balanceatclose = values[19]
        disseminationdate = ''
        supportoppose = ''
        candlastname = ''
        candfirstname = ''
        candoffice = ''
        canddistrict = ''
        candstate = ''
        acctidentifier = ''
        fedshare = ''
        nonfedshare = ''
        levinshare = ''
        h4activity = ''
        h4ad = ''
        h4df = ''
        h4ea = ''
        h4gv = ''
        h4dc = ''
        h4pc = ''
        h6activity = ''
        h6voterreg = ''
        h6gotv = ''
        h6voterid = ''
        h6generic = ''
        onotherreport = ''
        #now put this interesting data into the SQL database table
        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
        #conn.commit()
conn.commit()
#cur.execute('''COMMIT;''')
#updates user to tell them progress made so far
print ('Inserted', countsd2, 'entries from Schedule D')

##print('Inserting Schedule E of', fname2, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname2) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('SE') : continue #now let's look at Schedule E entries
##        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countse2 = countse2 + 1 #keep track of count of entries on Schedule E
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report2'
##        transid = values[2]
##        orgname = values[6]
##        lastname = values[7]
##        firstname = values[8]
##        address1 = values[12]
##        address2 = values[13]
##        city = values[14]
##        state = values[15]
##        zipcode = values[16]
##        electioncode = values[17]
##        electionother = values[18]
##        date = values[21]
##        amount = values[20]
##        aggregate = values[22]
##        purpose = values[23]
##        employer = ''
##        occupation = ''
##        memoentry = values[42]
##        note = values[43]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = values[19]
##        supportoppose = values[26]
##        candlastname = values[28]
##        candfirstname = values[29]
##        candoffice = values[33]
##        canddistrict = values[34]
##        candstate = values[35]
##        acctidentifier = ''
##        fedshare = ''
##        nonfedshare = ''
##        levinshare = ''
##        h4activity = ''
##        h4ad = ''
##        h4df = ''
##        h4ea = ''
##        h4gv = ''
##        h4dc = ''
##        h4pc = ''
##        h6activity = ''
##        h6voterreg = ''
##        h6gotv = ''
##        h6voterid = ''
##        h6generic = ''
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countse2, 'entries from Schedule E')
##
##print('Inserting Schedule F of', fname2, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname2) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('SF') : continue #now let's look at Schedule F entries
##        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countsf2 = countsf2 + 1 #keep track of count of entries on Schedule F
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report2'
##        transid = values[2]
##        orgname = values[16]
##        lastname = values[17]
##        firstname = values[18]
##        address1 = values[22]
##        address2 = values[23]
##        city = values[24]
##        state = values[25]
##        zipcode = values[26]
##        electioncode = ''
##        electionother = ''
##        date = values[27]
##        amount = values[28]
##        aggregate = values[29]
##        purpose = values[30]
##        employer = ''
##        occupation = ''
##        memoentry = values[42]
##        note = values[43]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = ''
##        supportoppose = ''
##        candlastname = values[34]
##        candfirstname = values[35]
##        candoffice = values[39]
##        canddistrict = values[41]
##        candstate = values[40]
##        acctidentifier = ''
##        fedshare = ''
##        nonfedshare = ''
##        levinshare = ''
##        h4activity = ''
##        h4ad = ''
##        h4df = ''
##        h4ea = ''
##        h4gv = ''
##        h4dc = ''
##        h4pc = ''
##        h6activity = ''
##        h6voterreg = ''
##        h6gotv = ''
##        h6voterid = ''
##        h6generic = ''
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countsf2, 'entries from Schedule F')
##
##print('Inserting Schedule H4 of', fname2, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname2) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('H4') : continue #now let's look at Schedule H4 entries
##        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countsh42 = countsh42 + 1 #keep track of count of entries on Schedule H4
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report2'
##        transid = values[2]
##        orgname = values[6]
##        lastname = values[7]
##        firstname = values[8]
##        address1 = values[12]
##        address2 = values[13]
##        city = values[14]
##        state = values[15]
##        zipcode = values[16]
##        electioncode = ''
##        electionother = ''
##        date = values[18]
##        amount = values[19]
##        aggregate = values[22]
##        purpose = values[23]
##        employer = ''
##        occupation = ''
##        memoentry = values[31]
##        note = values[32]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = ''
##        supportoppose = ''
##        candlastname = ''
##        candfirstname = ''
##        candoffice = ''
##        canddistrict = ''
##        candstate = ''
##        acctidentifier = values[17]
##        fedshare = values[20]
##        nonfedshare = values[21]
##        levinshare = ''
##        h4activity = ''
##        h4ad = values[25]
##        h4df = values[26]
##        h4ea = values[27]
##        h4gv = values[28]
##        h4dc = values[29]
##        h4pc = values[30]
##        h6activity = ''
##        h6voterreg = ''
##        h6gotv = ''
##        h6voterid = ''
##        h6generic = ''
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countsh42, 'entries from Schedule H4')
##
##print('Inserting Schedule H6 of', fname2, 'into SQL database tables')
##cur.execute('''BEGIN TRANSACTION;''')
##with open(fname2) as csvfile:
##    str_data = csv.reader(csvfile)
##    for row in str_data:
##        values = row
##        linenum = values[0] #find the first value in the list
##        if not linenum.startswith('H6') : continue #now let's look at Schedule H6 entries
##        count2 = count2 + 1 #keep track of count of entries on all lines (update in every schedule)
##        countsh62 = countsh62 + 1 #keep track of count of entries on Schedule H6
##        #find the rest of the interesting data we want for this entry
##        sourcefile = 'report2'
##        transid = values[2]
##        orgname = values[6]
##        lastname = values[7]
##        firstname = values[8]
##        address1 = values[12]
##        address2 = values[13]
##        city = values[14]
##        state = values[15]
##        zipcode = values[16]
##        electioncode = ''
##        electionother = ''
##        date = values[18]
##        amount = values[19]
##        aggregate = values[22]
##        purpose = values[23]
##        employer = ''
##        occupation = ''
##        memoentry = values[29]
##        note = values[30]
##        beginningbalance = ''
##        incurredamount = ''
##        paymentamount = ''
##        balanceatclose = ''
##        disseminationdate = ''
##        supportoppose = ''
##        candlastname = ''
##        candfirstname = ''
##        candoffice = ''
##        canddistrict = ''
##        candstate = ''
##        acctidentifier = values[17]
##        fedshare = values[20]
##        nonfedshare = ''
##        levinshare = values[21]
##        h4activity = ''
##        h4ad = ''
##        h4df = ''
##        h4ea = ''
##        h4gv = ''
##        h4dc = ''
##        h4pc = ''
##        h6activity = ''
##        h6voterreg = values[25]
##        h6gotv = values[26]
##        h6voterid = values[27]
##        h6generic = values[28]
##        onotherreport = ''
##        #now put this interesting data into the SQL database table
##        cur.execute('''INSERT INTO Report2 (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport)
##            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h4ad, h4df, h4ea, h4gv, h4dc, h4pc, h6activity, h6voterreg, h6gotv, h6voterid, h6generic, onotherreport ) )
##        #conn.commit()
##conn.commit()
###cur.execute('''COMMIT;''')
###updates user to tell them progress made so far
##print ('Inserted', countsh62, 'entries from Schedule H6')

#Summary AT END of all schedules
print ('Inserted', count2, 'entries total from', fname2)



###To clarify the memoentry column, update the memoentry column values. Change the blank ones to "not memo" and the ones with "X" to "MEMO" [NEVERMIND, NOT NEEDED]
##print()
##print('Clarifying memo entries...')
###First for Report1
###Update memoentry where it is blank (not "X")
##cur.execute('''UPDATE Report1 SET memoentry = 'not memo'
##	WHERE memoentry != 'X' ''', )
##conn.commit()
###Update memoentry where it is "X"
##cur.execute('''UPDATE Report1 SET memoentry = 'MEMO'
##	WHERE memoentry = 'X' ''', )
##conn.commit()
###Now for Report2
###Update memoentry where it is blank (not "X")
##cur.execute('''UPDATE Report2 SET memoentry = 'not memo'
##	WHERE memoentry != 'X' ''', )
##conn.commit()
###Update memoentry where it is "X"
##cur.execute('''UPDATE Report2 SET memoentry = 'MEMO'
##	WHERE memoentry = 'X' ''', )
##conn.commit()



#To simplify the H4 and H6 activity types, combine the six H4 activity columns and four H6 activity columns into two columns:
#H4activity and H6activity
##print()
##print('Combining H4 and H6 activity types...')
#First for Report1
#Update H4activity where H4ad contains X
##cur.execute('''UPDATE Report1 SET H4activity = 'Administrative'
##	WHERE H4ad = 'X' ''', )
##conn.commit()
###Update H4activity where H4df contains X
##cur.execute('''UPDATE Report1 SET H4activity = 'Direct Fundraising'
##	WHERE H4df = 'X' ''', )
##conn.commit()
###Update H4activity where H4ea contains X
##cur.execute('''UPDATE Report1 SET H4activity = 'Exempt Activity'
##	WHERE H4ea = 'X' ''', )
##conn.commit()
###Update H4activity where H4gv contains X
##cur.execute('''UPDATE Report1 SET H4activity = 'Generic Voter Drive'
##	WHERE H4gv = 'X' ''', )
##conn.commit()
###Update H4activity where H4dc contains X
##cur.execute('''UPDATE Report1 SET H4activity = 'Direct Candidate Support'
##	WHERE H4dc = 'X' ''', )
##conn.commit()
###Update H4activity where H4pc contains X
##cur.execute('''UPDATE Report1 SET H4activity = 'Public Communication'
##	WHERE H4pc = 'X' ''', )
##conn.commit()
###Now for Report 2
###Update H4activity where H4ad contains X
##cur.execute('''UPDATE Report2 SET H4activity = 'Administrative'
##	WHERE H4ad = 'X' ''', )
##conn.commit()
###Update H4activity where H4df contains X
##cur.execute('''UPDATE Report2 SET H4activity = 'Direct Fundraising'
##	WHERE H4df = 'X' ''', )
##conn.commit()
###Update H4activity where H4ea contains X
##cur.execute('''UPDATE Report2 SET H4activity = 'Exempt Activity'
##	WHERE H4ea = 'X' ''', )
##conn.commit()
###Update H4activity where H4gv contains X
##cur.execute('''UPDATE Report2 SET H4activity = 'Generic Voter Drive'
##	WHERE H4gv = 'X' ''', )
##conn.commit()
###Update H4activity where H4dc contains X
##cur.execute('''UPDATE Report2 SET H4activity = 'Direct Candidate Support'
##	WHERE H4dc = 'X' ''', )
##conn.commit()
###Update H4activity where H4pc contains X
##cur.execute('''UPDATE Report2 SET H4activity = 'Public Communication'
##	WHERE H4pc = 'X' ''', )
##conn.commit()
###Again, begin with Report1
###Update H6activity where H6voterreg contains X
##cur.execute('''UPDATE Report1 SET H6activity = 'Voter Registration'
##	WHERE H6voterreg = 'X' ''', )
##conn.commit()
###Update H6activity where H6gotv contains X
##cur.execute('''UPDATE Report1 SET H6activity = 'GOTV'
##	WHERE H6gotv = 'X' ''', )
##conn.commit()
###Update H6activity where H6voterid contains X
##cur.execute('''UPDATE Report1 SET H6activity = 'Voter ID'
##	WHERE H6voterid = 'X' ''', )
##conn.commit()
###Update H6activity where H6generic contains X
##cur.execute('''UPDATE Report1 SET H6activity = 'Generic Campaign'
##	WHERE H6generic = 'X' ''', )
##conn.commit()
###And finally, with Report2
###Update H6activity where H6voterreg contains X
##cur.execute('''UPDATE Report2 SET H6activity = 'Voter Registration'
##	WHERE H6voterreg = 'X' ''', )
##conn.commit()
###Update H6activity where H6gotv contains X
##cur.execute('''UPDATE Report2 SET H6activity = 'GOTV'
##	WHERE H6gotv = 'X' ''', )
##conn.commit()
###Update H6activity where H6voterid contains X
##cur.execute('''UPDATE Report2 SET H6activity = 'Voter ID'
##	WHERE H6voterid = 'X' ''', )
##conn.commit()
###Update H6activity where H6generic contains X
##cur.execute('''UPDATE Report2 SET H6activity = 'Generic Campaign'
##	WHERE H6generic = 'X' ''', )
##conn.commit()
#remove the six unnecessary H4 headers and four unnecessary H6 headers from Report1
cur.executescript('''
DROP TABLE IF EXISTS Report1_oldcolumnheaders ;
ALTER TABLE Report1 RENAME TO Report1_oldcolumnheaders ;
CREATE TABLE Report1 (id INTEGER, sourcefile TEXT, linenum TEXT, transid TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT, disseminationdate TEXT, supportoppose TEXT, candlastname TEXT, candfirstname TEXT, candoffice TEXT, canddistrict TEXT, candstate TEXT, acctidentifier TEXT, fedshare TEXT, nonfedshare TEXT, levinshare TEXT, h4activity TEXT, h6activity TEXT, onotherreport TEXT ) ;
INSERT INTO Report1 (id, sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h6activity, onotherreport)
	SELECT id, sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h6activity, onotherreport
	FROM Report1_oldcolumnheaders ;
DROP TABLE IF EXISTS Report1_oldcolumnheaders ; ''')
conn.commit()
#and remove the six unnecessary H4 headers and four unnecessary H6 headers from Report2
cur.executescript('''
DROP TABLE IF EXISTS Report2_oldcolumnheaders ;
ALTER TABLE Report2 RENAME TO Report2_oldcolumnheaders ;
CREATE TABLE Report2 (id INTEGER, sourcefile TEXT, linenum TEXT, transid TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT, disseminationdate TEXT, supportoppose TEXT, candlastname TEXT, candfirstname TEXT, candoffice TEXT, canddistrict TEXT, candstate TEXT, acctidentifier TEXT, fedshare TEXT, nonfedshare TEXT, levinshare TEXT, h4activity TEXT, h6activity TEXT, onotherreport TEXT ) ;
INSERT INTO Report2 (id, sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h6activity, onotherreport)
	SELECT id, sourcefile, linenum, transid, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose, disseminationdate, supportoppose, candlastname, candfirstname, candoffice, canddistrict, candstate, acctidentifier, fedshare, nonfedshare, levinshare, h4activity, h6activity, onotherreport
	FROM Report2_oldcolumnheaders ;
DROP TABLE IF EXISTS Report2_oldcolumnheaders ; ''')
conn.commit()
##print('...done')




#To build lists inside of Report1 and Report2 showing which entries were removed/added...
#start by setting all the attributes in column "onotherreport" to 0
print()
print('Preparing tables showing entries added or deleted...')
cur.execute('''UPDATE Report1 SET onotherreport = 0 ''', )
conn.commit()
#Change the 0 to 1 to indicate that an entry with an identical transid appears in both Report1 and Report2
cur.execute('''UPDATE Report1 SET onotherreport = 1
	WHERE transid IN (SELECT transid FROM Report2) ''', )
conn.commit()
#this leaves an attribute of 0 for the entries which appear on Report1 but were deleted

#now, similarly for Report2, set all the attributes in column "onotherreport" to 0
cur.execute('''UPDATE Report2 SET onotherreport = 0 ''', )
conn.commit()
#Change the 0 to 1 to indicate that an entry with an identical transid appears in both Report1 and Report2
cur.execute('''UPDATE Report2 SET onotherreport = 1
	WHERE transid IN (SELECT transid FROM Report1) ''', )
conn.commit()
#this leaves an attribute of 0 for the entries which were added on Report2 (were not on Report1)

#put the entries which ONLY appear on Report1 (and not Report2) into R1notR2
cur.execute('''INSERT INTO R1notR2
	SELECT * FROM Report1 WHERE onotherreport = 0 ''', )
conn.commit()

#put the entries which ONLY appear on Report2 (and not Report1) into R2notR1
cur.execute('''INSERT INTO R2notR1
	SELECT * FROM Report2 WHERE onotherreport = 0 ''', )
conn.commit()
print('...done')



#To build list of entries which were changed/modified...
#put the entries which appear on both Report1 and Report2 (those with a "1" in the "onotherreport" column) into "Changed" table
print()
print('Preparing tables showing entries which were changed...')
cur.execute('''INSERT INTO Changed
        SELECT * FROM Report1 WHERE onotherreport = 1 ''', )
conn.commit()
cur.execute('''INSERT INTO Changed
        SELECT * FROM Report2 WHERE onotherreport = 1 ''', )
conn.commit()
#now create a column to be used in comparison
cur.execute('''ALTER TABLE Changed ADD COLUMN combinedcolumns BLOB ''', )
conn.commit()
#now concatenate most column entries and insert them in the "combinedcolumns" column
if compareall == 'y': #if user wants to compare all values, pull all values into the blob (default choice)
    cur.execute('''UPDATE Changed SET combinedcolumns = linenum || transid || orgname || lastname || firstname || address1 || address2 || city || state || zipcode || electioncode || electionother || date || amount || aggregate || purpose || employer || occupation || memoentry || note || beginningbalance || incurredamount || paymentamount || balanceatclose || disseminationdate || supportoppose || candlastname || candfirstname || candoffice || canddistrict || candstate || acctidentifier || fedshare || nonfedshare || levinshare || h4activity || h6activity ''', )
    conn.commit()
if compareall == 'n': #if user wants to ignore changes to address and aggregate fields, don't pull those columns into the blob
    cur.execute('''UPDATE Changed SET combinedcolumns = linenum || transid || orgname || lastname || firstname || electioncode || electionother || date || amount || purpose || employer || occupation || memoentry || note || beginningbalance || incurredamount || paymentamount || balanceatclose || disseminationdate || supportoppose || candlastname || candfirstname || candoffice || canddistrict || candstate || acctidentifier || fedshare || nonfedshare || levinshare || h4activity || h6activity ''', )
    conn.commit()
#now identify the rows which do NOT contain duplicate values in the "combinedcolumns" column (duplicates mean they didn't change), and replace all of the Changed table with only those rows without duplicates
cur.execute('''CREATE TABLE Temp AS SELECT *, COUNT (combinedcolumns) FROM Changed GROUP BY combinedcolumns HAVING ( COUNT(combinedcolumns) <2 ) ''', )
conn.commit()
cur.execute('''DROP TABLE Changed ''', )
conn.commit()
cur.execute('''CREATE TABLE Changed AS SELECT * FROM Temp ORDER BY transid, sourcefile ''', )
conn.commit()
cur.execute('''DROP TABLE Temp ''', )
conn.commit()
#now create temporary tables for just those rows from Report1 and a separate table for those rows from Report2
cur.execute('''CREATE TABLE TempReport1 AS SELECT * FROM Changed WHERE sourcefile='report1'  ''', )
conn.commit()
cur.execute('''CREATE TABLE TempReport2 AS SELECT * FROM Changed WHERE sourcefile='report2'  ''', )
conn.commit()

#Identify report1 values different than report2 values (for pairs sharing same transid), put into ChangedTable

#The following SQL script follows this pattern for every column:

##DROP TABLE IF EXISTS TempChangeXXXXX ;
##CREATE TABLE TempChangeXXXXX (tempchangeXXXXX_transid TEXT, tempchangeXXXXX_XXXXX TEXT) ;
##INSERT INTO TempChangeXXXXX
##SELECT  TempReport1.transid, TempReport1.XXXXX
##FROM TempReport1, TempReport2
##WHERE TempReport1.transid = TempReport2.transid AND TempReport1.XXXXX != TempReport2.XXXXX ;

#Note: add the following to every block EXCEPT the sourcefile block (because obviously the report itself is changing)
##UPDATE TempChangeXXXXX SET tempchangeXXXXX_XXXXX = 'CHANGED FROM: ' || tempchangeXXXXX_XXXXX ;

cur.executescript('''

    DROP TABLE IF EXISTS TempChangesourcefile ;
    CREATE TABLE TempChangesourcefile (tempchangesourcefile_transid TEXT, tempchangesourcefile_sourcefile TEXT) ;
    INSERT INTO TempChangesourcefile
    SELECT  TempReport1.transid, TempReport1.sourcefile
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.sourcefile != TempReport2.sourcefile ;

    DROP TABLE IF EXISTS TempChangelinenum ;
    CREATE TABLE TempChangelinenum (tempchangelinenum_transid TEXT, tempchangelinenum_linenum TEXT) ;
    INSERT INTO TempChangelinenum
    SELECT  TempReport1.transid, TempReport1.linenum
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.linenum != TempReport2.linenum ;
    UPDATE TempChangelinenum SET tempchangelinenum_linenum = 'CHANGED FROM: ' || tempchangelinenum_linenum ;

    DROP TABLE IF EXISTS TempChangeorgname ;
    CREATE TABLE TempChangeorgname (tempchangeorgname_transid TEXT, tempchangeorgname_orgname TEXT) ;
    INSERT INTO TempChangeorgname
    SELECT  TempReport1.transid, TempReport1.orgname
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.orgname != TempReport2.orgname ;
    UPDATE TempChangeorgname SET tempchangeorgname_orgname = 'CHANGED FROM: ' || tempchangeorgname_orgname ;

    DROP TABLE IF EXISTS TempChangelastname ;
    CREATE TABLE TempChangelastname (tempchangelastname_transid TEXT, tempchangelastname_lastname TEXT) ;
    INSERT INTO TempChangelastname
    SELECT  TempReport1.transid, TempReport1.lastname
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.lastname != TempReport2.lastname ;
    UPDATE TempChangelastname SET tempchangelastname_lastname = 'CHANGED FROM: ' || tempchangelastname_lastname ;

    DROP TABLE IF EXISTS TempChangefirstname ;
    CREATE TABLE TempChangefirstname (tempchangefirstname_transid TEXT, tempchangefirstname_firstname TEXT) ;
    INSERT INTO TempChangefirstname
    SELECT  TempReport1.transid, TempReport1.firstname
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.firstname != TempReport2.firstname ;
    UPDATE TempChangefirstname SET tempchangefirstname_firstname = 'CHANGED FROM: ' || tempchangefirstname_firstname ;

    DROP TABLE IF EXISTS TempChangeaddress1 ;
    CREATE TABLE TempChangeaddress1 (tempchangeaddress1_transid TEXT, tempchangeaddress1_address1 TEXT) ;
    INSERT INTO TempChangeaddress1
    SELECT  TempReport1.transid, TempReport1.address1
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.address1 != TempReport2.address1 ;
    UPDATE TempChangeaddress1 SET tempchangeaddress1_address1 = 'CHANGED FROM: ' || tempchangeaddress1_address1 ;

    DROP TABLE IF EXISTS TempChangeaddress2 ;
    CREATE TABLE TempChangeaddress2 (tempchangeaddress2_transid TEXT, tempchangeaddress2_address2 TEXT) ;
    INSERT INTO TempChangeaddress2
    SELECT  TempReport1.transid, TempReport1.address2
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.address2 != TempReport2.address2 ;
    UPDATE TempChangeaddress2 SET tempchangeaddress2_address2 = 'CHANGED FROM: ' || tempchangeaddress2_address2 ;

    DROP TABLE IF EXISTS TempChangecity ;
    CREATE TABLE TempChangecity (tempchangecity_transid TEXT, tempchangecity_city TEXT) ;
    INSERT INTO TempChangecity
    SELECT  TempReport1.transid, TempReport1.city
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.city != TempReport2.city ;
    UPDATE TempChangecity SET tempchangecity_city = 'CHANGED FROM: ' || tempchangecity_city ;

    DROP TABLE IF EXISTS TempChangestate ;
    CREATE TABLE TempChangestate (tempchangestate_transid TEXT, tempchangestate_state TEXT) ;
    INSERT INTO TempChangestate
    SELECT  TempReport1.transid, TempReport1.state
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.state != TempReport2.state ;
    UPDATE TempChangestate SET tempchangestate_state = 'CHANGED FROM: ' || tempchangestate_state ;

    DROP TABLE IF EXISTS TempChangezipcode ;
    CREATE TABLE TempChangezipcode (tempchangezipcode_transid TEXT, tempchangezipcode_zipcode TEXT);
    INSERT INTO TempChangezipcode
    SELECT  TempReport1.transid, TempReport1.zipcode
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.zipcode != TempReport2.zipcode ;
    UPDATE TempChangezipcode SET tempchangezipcode_zipcode = 'CHANGED FROM: ' || tempchangezipcode_zipcode ;

    DROP TABLE IF EXISTS TempChangeelectioncode ;
    CREATE TABLE TempChangeelectioncode (tempchangeelectioncode_transid TEXT, tempchangeelectioncode_electioncode TEXT) ;
    INSERT INTO TempChangeelectioncode
    SELECT  TempReport1.transid, TempReport1.electioncode
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.electioncode != TempReport2.electioncode ;
    UPDATE TempChangeelectioncode SET tempchangeelectioncode_electioncode = 'CHANGED FROM: ' || tempchangeelectioncode_electioncode ;

    DROP TABLE IF EXISTS TempChangeelectionother ;
    CREATE TABLE TempChangeelectionother (tempchangeelectionother_transid TEXT, tempchangeelectionother_electionother TEXT) ;
    INSERT INTO TempChangeelectionother
    SELECT  TempReport1.transid, TempReport1.electionother
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.electionother != TempReport2.electionother ;
    UPDATE TempChangeelectionother SET tempchangeelectionother_electionother = 'CHANGED FROM: ' || tempchangeelectionother_electionother ;

    DROP TABLE IF EXISTS TempChangedate ;
    CREATE TABLE TempChangedate (tempchangedate_transid TEXT, tempchangedate_date TEXT) ;
    INSERT INTO TempChangedate
    SELECT  TempReport1.transid, TempReport1.date
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.date != TempReport2.date ;
    UPDATE TempChangedate SET tempchangedate_date = 'CHANGED FROM: ' || tempchangedate_date ;

    DROP TABLE IF EXISTS TempChangeamount ;
    CREATE TABLE TempChangeamount (tempchangeamount_transid TEXT, tempchangeamount_amount TEXT) ;
    INSERT INTO TempChangeamount
    SELECT  TempReport1.transid, TempReport1.amount
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.amount != TempReport2.amount ;
    UPDATE TempChangeamount SET tempchangeamount_amount = 'CHANGED FROM: ' || tempchangeamount_amount ;

    DROP TABLE IF EXISTS TempChangeaggregate ;
    CREATE TABLE TempChangeaggregate (tempchangeaggregate_transid TEXT, tempchangeaggregate_aggregate TEXT) ;
    INSERT INTO TempChangeaggregate
    SELECT  TempReport1.transid, TempReport1.aggregate
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.aggregate != TempReport2.aggregate ;
    UPDATE TempChangeaggregate SET tempchangeaggregate_aggregate = 'CHANGED FROM: ' || tempchangeaggregate_aggregate ;

    DROP TABLE IF EXISTS TempChangepurpose ;
    CREATE TABLE TempChangepurpose (tempchangepurpose_transid TEXT, tempchangepurpose_purpose TEXT) ;
    INSERT INTO TempChangepurpose
    SELECT  TempReport1.transid, TempReport1.purpose
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.purpose != TempReport2.purpose ;
    UPDATE TempChangepurpose SET tempchangepurpose_purpose = 'CHANGED FROM: ' || tempchangepurpose_purpose ;

    DROP TABLE IF EXISTS TempChangeemployer ;
    CREATE TABLE TempChangeemployer (tempchangeemployer_transid TEXT, tempchangeemployer_employer TEXT) ;
    INSERT INTO TempChangeemployer
    SELECT  TempReport1.transid, TempReport1.employer
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.employer != TempReport2.employer ;
    UPDATE TempChangeemployer SET tempchangeemployer_employer = 'CHANGED FROM: ' || tempchangeemployer_employer ;

    DROP TABLE IF EXISTS TempChangeoccupation ;
    CREATE TABLE TempChangeoccupation (tempchangeoccupation_transid TEXT, tempchangeoccupation_occupation TEXT) ;
    INSERT INTO TempChangeoccupation
    SELECT  TempReport1.transid, TempReport1.occupation
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.occupation != TempReport2.occupation ;
    UPDATE TempChangeoccupation SET tempchangeoccupation_occupation = 'CHANGED FROM: ' || tempchangeoccupation_occupation ;

    DROP TABLE IF EXISTS TempChangememoentry ;
    CREATE TABLE TempChangememoentry (tempchangememoentry_transid TEXT, tempchangememoentry_memoentry TEXT) ;
    INSERT INTO TempChangememoentry
    SELECT  TempReport1.transid, TempReport1.memoentry
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.memoentry != TempReport2.memoentry ;
    UPDATE TempChangememoentry SET tempchangememoentry_memoentry = 'CHANGED FROM: ' || tempchangememoentry_memoentry ;

    DROP TABLE IF EXISTS TempChangenote ;
    CREATE TABLE TempChangenote (tempchangenote_transid TEXT, tempchangenote_note TEXT) ;
    INSERT INTO TempChangenote
    SELECT  TempReport1.transid, TempReport1.note
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.note != TempReport2.note ;
    UPDATE TempChangenote SET tempchangenote_note = 'CHANGED FROM: ' || tempchangenote_note ;

    DROP TABLE IF EXISTS TempChangebeginningbalance ;
    CREATE TABLE TempChangebeginningbalance (tempchangebeginningbalance_transid TEXT, tempchangebeginningbalance_beginningbalance TEXT) ;
    INSERT INTO TempChangebeginningbalance
    SELECT  TempReport1.transid, TempReport1.beginningbalance
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.beginningbalance != TempReport2.beginningbalance ;
    UPDATE TempChangebeginningbalance SET tempchangebeginningbalance_beginningbalance = 'CHANGED FROM: ' || tempchangebeginningbalance_beginningbalance ;

    DROP TABLE IF EXISTS TempChangeincurredamount ;
    CREATE TABLE TempChangeincurredamount (tempchangeincurredamount_transid TEXT, tempchangeincurredamount_incurredamount TEXT) ;
    INSERT INTO TempChangeincurredamount
    SELECT  TempReport1.transid, TempReport1.incurredamount
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.incurredamount != TempReport2.incurredamount ;
    UPDATE TempChangeincurredamount SET tempchangeincurredamount_incurredamount = 'CHANGED FROM: ' || tempchangeincurredamount_incurredamount ;

    DROP TABLE IF EXISTS TempChangepaymentamount ;
    CREATE TABLE TempChangepaymentamount (tempchangepaymentamount_transid TEXT, tempchangepaymentamount_paymentamount TEXT) ;
    INSERT INTO TempChangepaymentamount
    SELECT  TempReport1.transid, TempReport1.paymentamount
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.paymentamount != TempReport2.paymentamount ;
    UPDATE TempChangepaymentamount SET tempchangepaymentamount_paymentamount = 'CHANGED FROM: ' || tempchangepaymentamount_paymentamount ;

    DROP TABLE IF EXISTS TempChangebalanceatclose ;
    CREATE TABLE TempChangebalanceatclose (tempchangebalanceatclose_transid TEXT, tempchangebalanceatclose_balanceatclose TEXT) ;
    INSERT INTO TempChangebalanceatclose
    SELECT  TempReport1.transid, TempReport1.balanceatclose
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.balanceatclose != TempReport2.balanceatclose ;
    UPDATE TempChangebalanceatclose SET tempchangebalanceatclose_balanceatclose = 'CHANGED FROM: ' || tempchangebalanceatclose_balanceatclose ;

    DROP TABLE IF EXISTS TempChangedisseminationdate ;
    CREATE TABLE TempChangedisseminationdate (tempchangedisseminationdate_transid TEXT, tempchangedisseminationdate_disseminationdate TEXT) ;
    INSERT INTO TempChangedisseminationdate
    SELECT  TempReport1.transid, TempReport1.disseminationdate
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.disseminationdate != TempReport2.disseminationdate ;
    UPDATE TempChangedisseminationdate SET tempchangedisseminationdate_disseminationdate = 'CHANGED FROM: ' || tempchangedisseminationdate_disseminationdate ;

    DROP TABLE IF EXISTS TempChangesupportoppose ;
    CREATE TABLE TempChangesupportoppose (tempchangesupportoppose_transid TEXT, tempchangesupportoppose_supportoppose TEXT) ;
    INSERT INTO TempChangesupportoppose
    SELECT  TempReport1.transid, TempReport1.supportoppose
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.supportoppose != TempReport2.supportoppose ;
    UPDATE TempChangesupportoppose SET tempchangesupportoppose_supportoppose = 'CHANGED FROM: ' || tempchangesupportoppose_supportoppose ;

    DROP TABLE IF EXISTS TempChangecandlastname ;
    CREATE TABLE TempChangecandlastname (tempchangecandlastname_transid TEXT, tempchangecandlastname_candlastname TEXT) ;
    INSERT INTO TempChangecandlastname
    SELECT  TempReport1.transid, TempReport1.candlastname
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.candlastname != TempReport2.candlastname ;
    UPDATE TempChangecandlastname SET tempchangecandlastname_candlastname = 'CHANGED FROM: ' || tempchangecandlastname_candlastname ;

    DROP TABLE IF EXISTS TempChangecandfirstname ;
    CREATE TABLE TempChangecandfirstname (tempchangecandfirstname_transid TEXT, tempchangecandfirstname_candfirstname TEXT) ;
    INSERT INTO TempChangecandfirstname
    SELECT  TempReport1.transid, TempReport1.candfirstname
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.candfirstname != TempReport2.candfirstname ;
    UPDATE TempChangecandfirstname SET tempchangecandfirstname_candfirstname = 'CHANGED FROM: ' || tempchangecandfirstname_candfirstname ;

    DROP TABLE IF EXISTS TempChangecandoffice ;
    CREATE TABLE TempChangecandoffice (tempchangecandoffice_transid TEXT, tempchangecandoffice_candoffice TEXT) ;
    INSERT INTO TempChangecandoffice
    SELECT  TempReport1.transid, TempReport1.candoffice
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.candoffice != TempReport2.candoffice ;
    UPDATE TempChangecandoffice SET tempchangecandoffice_candoffice = 'CHANGED FROM: ' || tempchangecandoffice_candoffice ;

    DROP TABLE IF EXISTS TempChangecanddistrict ;
    CREATE TABLE TempChangecanddistrict (tempchangecanddistrict_transid TEXT, tempchangecanddistrict_canddistrict TEXT) ;
    INSERT INTO TempChangecanddistrict
    SELECT  TempReport1.transid, TempReport1.canddistrict
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.canddistrict != TempReport2.canddistrict ;
    UPDATE TempChangecanddistrict SET tempchangecanddistrict_canddistrict = 'CHANGED FROM: ' || tempchangecanddistrict_canddistrict ;

    DROP TABLE IF EXISTS TempChangecandstate ;
    CREATE TABLE TempChangecandstate (tempchangecandstate_transid TEXT, tempchangecandstate_candstate TEXT) ;
    INSERT INTO TempChangecandstate
    SELECT  TempReport1.transid, TempReport1.candstate
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.candstate != TempReport2.candstate ;
    UPDATE TempChangecandstate SET tempchangecandstate_candstate = 'CHANGED FROM: ' || tempchangecandstate_candstate ;

    DROP TABLE IF EXISTS TempChangeacctidentifier ;
    CREATE TABLE TempChangeacctidentifier (tempchangeacctidentifier_transid TEXT, tempchangeacctidentifier_acctidentifier TEXT) ;
    INSERT INTO TempChangeacctidentifier
    SELECT  TempReport1.transid, TempReport1.acctidentifier
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.acctidentifier != TempReport2.acctidentifier ;
    UPDATE TempChangeacctidentifier SET tempchangeacctidentifier_acctidentifier = 'CHANGED FROM: ' || tempchangeacctidentifier_acctidentifier ;

    DROP TABLE IF EXISTS TempChangefedshare ;
    CREATE TABLE TempChangefedshare (tempchangefedshare_transid TEXT, tempchangefedshare_fedshare TEXT) ;
    INSERT INTO TempChangefedshare
    SELECT  TempReport1.transid, TempReport1.fedshare
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.fedshare != TempReport2.fedshare ;
    UPDATE TempChangefedshare SET tempchangefedshare_fedshare = 'CHANGED FROM: ' || tempchangefedshare_fedshare ;

    DROP TABLE IF EXISTS TempChangenonfedshare ;
    CREATE TABLE TempChangenonfedshare (tempchangenonfedshare_transid TEXT, tempchangenonfedshare_nonfedshare TEXT) ;
    INSERT INTO TempChangenonfedshare
    SELECT  TempReport1.transid, TempReport1.nonfedshare
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.nonfedshare != TempReport2.nonfedshare ;
    UPDATE TempChangenonfedshare SET tempchangenonfedshare_nonfedshare = 'CHANGED FROM: ' || tempchangenonfedshare_nonfedshare ;

    DROP TABLE IF EXISTS TempChangelevinshare ;
    CREATE TABLE TempChangelevinshare (tempchangelevinshare_transid TEXT, tempchangelevinshare_levinshare TEXT) ;
    INSERT INTO TempChangelevinshare
    SELECT  TempReport1.transid, TempReport1.levinshare
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.levinshare != TempReport2.levinshare ;
    UPDATE TempChangelevinshare SET tempchangelevinshare_levinshare = 'CHANGED FROM: ' || tempchangelevinshare_levinshare ;

    DROP TABLE IF EXISTS TempChangeh4activity ;
    CREATE TABLE TempChangeh4activity (tempchangeh4activity_transid TEXT, tempchangeh4activity_h4activity TEXT) ;
    INSERT INTO TempChangeh4activity
    SELECT  TempReport1.transid, TempReport1.h4activity
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.h4activity != TempReport2.h4activity ;
    UPDATE TempChangeh4activity SET tempchangeh4activity_h4activity = 'CHANGED FROM: ' || tempchangeh4activity_h4activity ;

    DROP TABLE IF EXISTS TempChangeh6activity ;
    CREATE TABLE TempChangeh6activity (tempchangeh6activity_transid TEXT, tempchangeh6activity_h6activity TEXT) ;
    INSERT INTO TempChangeh6activity
    SELECT  TempReport1.transid, TempReport1.h6activity
    FROM TempReport1, TempReport2
    WHERE TempReport1.transid = TempReport2.transid AND TempReport1.h6activity != TempReport2.h6activity ;
    UPDATE TempChangeh6activity SET tempchangeh6activity_h6activity = 'CHANGED FROM: ' || tempchangeh6activity_h6activity ;

''')
conn.commit()

#fill in with the first column used above (only need to use this once because I am setting up the first "left join":

##DROP TABLE IF EXISTS ChangedTable;
##CREATE TABLE ChangedTable AS
##SELECT transid, tempchangeXXXXX_XXXXX
##FROM TempReport1
##LEFT JOIN TempChangeXXXXX ON TempReport1.transid = TempChangeXXXXX_transid ;

cur.executescript('''

    DROP TABLE IF EXISTS ChangedTable;
    CREATE TABLE ChangedTable AS
    SELECT transid, tempchangesourcefile_sourcefile
    FROM TempReport1
    LEFT JOIN TempChangesourcefile ON TempReport1.transid = TempChangesourcefile_transid ;

''')
conn.commit()

#continue filling in the remaining columns in the following format:

##ALTER TABLE ChangedTable ADD COLUMN tempchangeXXXXX_XXXXX TEXT ;
##UPDATE ChangedTable
##SET tempchangeXXXXX_XXXXX = (SELECT TempChangeXXXXX.tempchangeXXXXX_XXXXX
##FROM TempChangeXXXXX WHERE TempChangeXXXXX_transid = transid);

cur.executescript('''

    ALTER TABLE ChangedTable ADD COLUMN tempchangelinenum_linenum TEXT ;
    UPDATE ChangedTable
    SET tempchangelinenum_linenum = (SELECT TempChangelinenum.tempchangelinenum_linenum
    FROM TempChangelinenum WHERE TempChangelinenum_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeorgname_orgname TEXT ;
    UPDATE ChangedTable
    SET tempchangeorgname_orgname = (SELECT TempChangeorgname.tempchangeorgname_orgname
    FROM TempChangeorgname WHERE TempChangeorgname_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangelastname_lastname TEXT ;
    UPDATE ChangedTable
    SET tempchangelastname_lastname = (SELECT TempChangelastname.tempchangelastname_lastname
    FROM TempChangelastname WHERE TempChangelastname_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangefirstname_firstname TEXT ;
    UPDATE ChangedTable
    SET tempchangefirstname_firstname = (SELECT TempChangefirstname.tempchangefirstname_firstname
    FROM TempChangefirstname WHERE TempChangefirstname_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeaddress1_address1 TEXT ;
    UPDATE ChangedTable
    SET tempchangeaddress1_address1 = (SELECT TempChangeaddress1.tempchangeaddress1_address1
    FROM TempChangeaddress1 WHERE TempChangeaddress1_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeaddress2_address2 TEXT ;
    UPDATE ChangedTable
    SET tempchangeaddress2_address2 = (SELECT TempChangeaddress2.tempchangeaddress2_address2
    FROM TempChangeaddress2 WHERE TempChangeaddress2_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangecity_city TEXT ;
    UPDATE ChangedTable
    SET tempchangecity_city = (SELECT TempChangecity.tempchangecity_city
    FROM TempChangecity WHERE TempChangecity_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangestate_state TEXT ;
    UPDATE ChangedTable
    SET tempchangestate_state = (SELECT TempChangestate.tempchangestate_state
    FROM TempChangestate WHERE TempChangestate_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangezipcode_zipcode TEXT ;
    UPDATE ChangedTable
    SET tempchangezipcode_zipcode = (SELECT TempChangezipcode.tempchangezipcode_zipcode
    FROM TempChangezipcode WHERE TempChangezipcode_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeelectioncode_electioncode TEXT ;
    UPDATE ChangedTable
    SET tempchangeelectioncode_electioncode = (SELECT TempChangeelectioncode.tempchangeelectioncode_electioncode
    FROM TempChangeelectioncode WHERE TempChangeelectioncode_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeelectionother_electionother TEXT ;
    UPDATE ChangedTable
    SET tempchangeelectionother_electionother = (SELECT TempChangeelectionother.tempchangeelectionother_electionother
    FROM TempChangeelectionother WHERE TempChangeelectionother_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangedate_date TEXT ;
    UPDATE ChangedTable
    SET tempchangedate_date = (SELECT TempChangedate.tempchangedate_date
    FROM TempChangedate WHERE TempChangedate_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeamount_amount TEXT ;
    UPDATE ChangedTable
    SET tempchangeamount_amount = (SELECT TempChangeamount.tempchangeamount_amount
    FROM TempChangeamount WHERE TempChangeamount_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeaggregate_aggregate TEXT ;
    UPDATE ChangedTable
    SET tempchangeaggregate_aggregate = (SELECT TempChangeaggregate.tempchangeaggregate_aggregate
    FROM TempChangeaggregate WHERE TempChangeaggregate_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangepurpose_purpose TEXT ;
    UPDATE ChangedTable
    SET tempchangepurpose_purpose = (SELECT TempChangepurpose.tempchangepurpose_purpose
    FROM TempChangepurpose WHERE TempChangepurpose_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeemployer_employer TEXT ;
    UPDATE ChangedTable
    SET tempchangeemployer_employer = (SELECT TempChangeemployer.tempchangeemployer_employer
    FROM TempChangeemployer WHERE TempChangeemployer_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeoccupation_occupation TEXT ;
    UPDATE ChangedTable
    SET tempchangeoccupation_occupation = (SELECT TempChangeoccupation.tempchangeoccupation_occupation
    FROM TempChangeoccupation WHERE TempChangeoccupation_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangememoentry_memoentry TEXT ;
    UPDATE ChangedTable
    SET tempchangememoentry_memoentry = (SELECT TempChangememoentry.tempchangememoentry_memoentry
    FROM TempChangememoentry WHERE TempChangememoentry_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangenote_note TEXT ;
    UPDATE ChangedTable
    SET tempchangenote_note = (SELECT TempChangenote.tempchangenote_note
    FROM TempChangenote WHERE TempChangenote_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangebeginningbalance_beginningbalance TEXT ;
    UPDATE ChangedTable
    SET tempchangebeginningbalance_beginningbalance = (SELECT TempChangebeginningbalance.tempchangebeginningbalance_beginningbalance
    FROM TempChangebeginningbalance WHERE TempChangebeginningbalance_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeincurredamount_incurredamount TEXT ;
    UPDATE ChangedTable
    SET tempchangeincurredamount_incurredamount = (SELECT TempChangeincurredamount.tempchangeincurredamount_incurredamount
    FROM TempChangeincurredamount WHERE TempChangeincurredamount_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangepaymentamount_paymentamount TEXT ;
    UPDATE ChangedTable
    SET tempchangepaymentamount_paymentamount = (SELECT TempChangepaymentamount.tempchangepaymentamount_paymentamount
    FROM TempChangepaymentamount WHERE TempChangepaymentamount_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangebalanceatclose_balanceatclose TEXT ;
    UPDATE ChangedTable
    SET tempchangebalanceatclose_balanceatclose = (SELECT TempChangebalanceatclose.tempchangebalanceatclose_balanceatclose
    FROM TempChangebalanceatclose WHERE TempChangebalanceatclose_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangedisseminationdate_disseminationdate TEXT ;
    UPDATE ChangedTable
    SET tempchangedisseminationdate_disseminationdate = (SELECT TempChangedisseminationdate.tempchangedisseminationdate_disseminationdate
    FROM TempChangedisseminationdate WHERE TempChangedisseminationdate_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangesupportoppose_supportoppose TEXT ;
    UPDATE ChangedTable
    SET tempchangesupportoppose_supportoppose = (SELECT TempChangesupportoppose.tempchangesupportoppose_supportoppose
    FROM TempChangesupportoppose WHERE TempChangesupportoppose_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangecandlastname_candlastname TEXT ;
    UPDATE ChangedTable
    SET tempchangecandlastname_candlastname = (SELECT TempChangecandlastname.tempchangecandlastname_candlastname
    FROM TempChangecandlastname WHERE TempChangecandlastname_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangecandfirstname_candfirstname TEXT ;
    UPDATE ChangedTable
    SET tempchangecandfirstname_candfirstname = (SELECT TempChangecandfirstname.tempchangecandfirstname_candfirstname
    FROM TempChangecandfirstname WHERE TempChangecandfirstname_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangecandoffice_candoffice TEXT ;
    UPDATE ChangedTable
    SET tempchangecandoffice_candoffice = (SELECT TempChangecandoffice.tempchangecandoffice_candoffice
    FROM TempChangecandoffice WHERE TempChangecandoffice_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangecanddistrict_canddistrict TEXT ;
    UPDATE ChangedTable
    SET tempchangecanddistrict_canddistrict = (SELECT TempChangecanddistrict.tempchangecanddistrict_canddistrict
    FROM TempChangecanddistrict WHERE TempChangecanddistrict_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangecandstate_candstate TEXT ;
    UPDATE ChangedTable
    SET tempchangecandstate_candstate = (SELECT TempChangecandstate.tempchangecandstate_candstate
    FROM TempChangecandstate WHERE TempChangecandstate_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeacctidentifier_acctidentifier TEXT ;
    UPDATE ChangedTable
    SET tempchangeacctidentifier_acctidentifier = (SELECT TempChangeacctidentifier.tempchangeacctidentifier_acctidentifier
    FROM TempChangeacctidentifier WHERE TempChangeacctidentifier_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangefedshare_fedshare TEXT ;
    UPDATE ChangedTable
    SET tempchangefedshare_fedshare = (SELECT TempChangefedshare.tempchangefedshare_fedshare
    FROM TempChangefedshare WHERE TempChangefedshare_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangenonfedshare_nonfedshare TEXT ;
    UPDATE ChangedTable
    SET tempchangenonfedshare_nonfedshare = (SELECT TempChangenonfedshare.tempchangenonfedshare_nonfedshare
    FROM TempChangenonfedshare WHERE TempChangenonfedshare_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangelevinshare_levinshare TEXT ;
    UPDATE ChangedTable
    SET tempchangelevinshare_levinshare = (SELECT TempChangelevinshare.tempchangelevinshare_levinshare
    FROM TempChangelevinshare WHERE TempChangelevinshare_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeh4activity_h4activity TEXT ;
    UPDATE ChangedTable
    SET tempchangeh4activity_h4activity = (SELECT TempChangeh4activity.tempchangeh4activity_h4activity
    FROM TempChangeh4activity WHERE TempChangeh4activity_transid = transid);

    ALTER TABLE ChangedTable ADD COLUMN tempchangeh6activity_h6activity TEXT ;
    UPDATE ChangedTable
    SET tempchangeh6activity_h6activity = (SELECT TempChangeh6activity.tempchangeh6activity_h6activity
    FROM TempChangeh6activity WHERE TempChangeh6activity_transid = transid);

''')
conn.commit()
print('...done')



#FINAL CLEANUP BEFORE EXPORTING THE FILES TO CSV
print()
print('Preparing for export...')

#Clean up the headers on R1notR2 before exporting to CSV (drops the "id" and "onotherreport" columns)
cur.executescript('''
DROP TABLE IF EXISTS R1notR2_oldcolumnheaders ;
ALTER TABLE R1notR2 RENAME TO R1notR2_oldcolumnheaders ;
CREATE TABLE R1notR2 (transid TEXT, sourcefile TEXT, linenum TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT ) ;
INSERT INTO R1notR2 (transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose )
	SELECT transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose
	FROM R1notR2_oldcolumnheaders ;
DROP TABLE IF EXISTS R1notR2_oldcolumnheaders ; ''')
conn.commit()

#Clean up the headers on R2notR1 before exporting to CSV (drops the "id" and "onotherreport" columns)
cur.executescript('''
DROP TABLE IF EXISTS R2notR1_oldcolumnheaders ;
ALTER TABLE R2notR1 RENAME TO R2notR1_oldcolumnheaders ;
CREATE TABLE R2notR1 (transid TEXT, sourcefile TEXT, linenum TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT ) ;
INSERT INTO R2notR1 (transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose )
	SELECT transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose
	FROM R2notR1_oldcolumnheaders ;
DROP TABLE IF EXISTS R2notR1_oldcolumnheaders ; ''')
conn.commit()

#Drop Changed table, combine ChangedTable and TempReport2 and put them into the Changed table
#Make it easier to sort ChangedTable by adding a column "tempreport2linenum" to ChangedTable and filling it with the report2 linenum values for all matching transid rows
#the point of this is that I want to sort by linenum of report2 in the output file, so I can sort by tempreport2linenum before dropping that column
cur.executescript('''
INSERT INTO ChangedTable SELECT transid TEXT, sourcefile TEXT, linenum TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT, disseminationdate TEXT, supportoppose TEXT, candlastname TEXT, candfirstname TEXT, candoffice TEXT, canddistrict TEXT, candstate TEXT, acctidentifier TEXT, fedshare TEXT, nonfedshare TEXT, levinshare TEXT, h4activity TEXT, h6activity TEXT
    FROM TempReport2;
ALTER TABLE ChangedTable ADD COLUMN tempreport2linenum TEXT;
UPDATE ChangedTable
    SET tempreport2linenum = (SELECT TempReport2.linenum
    FROM TempReport2 WHERE TempReport2.transid = ChangedTable.transid); ''')
conn.commit()

cur.executescript('''
UPDATE ChangedTable SET tempreport2linenum = 'SH4' WHERE tempreport2linenum = 'H4' ;
UPDATE ChangedTable SET tempreport2linenum = 'SH6' WHERE tempreport2linenum = 'H6' ;
''')
conn.commit()

cur.executescript('''
DROP TABLE IF EXISTS Changed;
CREATE TABLE Changed AS SELECT * FROM ChangedTable
ORDER BY tempreport2linenum, transid, tempchangesourcefile_sourcefile;
''')
conn.commit()

#Clean up the headers on Changed before exporting to CSV
cur.executescript('''
DROP TABLE IF EXISTS Changed_oldcolumnheaders ;
ALTER TABLE Changed RENAME TO Changed_oldcolumnheaders ;
CREATE TABLE Changed (transid TEXT, sourcefile TEXT, linenum TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT) ;
INSERT INTO Changed (transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose)
	SELECT transid, tempchangesourcefile_sourcefile, tempchangelinenum_linenum, tempchangeorgname_orgname, tempchangelastname_lastname, tempchangefirstname_firstname, tempchangeaddress1_address1, tempchangeaddress2_address2, tempchangecity_city, tempchangestate_state, tempchangezipcode_zipcode, tempchangeelectioncode_electioncode, tempchangeelectionother_electionother, tempchangedate_date, tempchangeamount_amount, tempchangeaggregate_aggregate, tempchangepurpose_purpose, tempchangeemployer_employer, tempchangeoccupation_occupation, tempchangememoentry_memoentry, tempchangenote_note, tempchangebeginningbalance_beginningbalance, tempchangeincurredamount_incurredamount, tempchangepaymentamount_paymentamount, tempchangebalanceatclose_balanceatclose
	FROM Changed_oldcolumnheaders ;
DROP TABLE IF EXISTS Changed_oldcolumnheaders ; ''')
conn.commit()

#clean up headers of Report1 for output
cur.executescript('''
DROP TABLE IF EXISTS Report1_oldcolumnheaders ;
ALTER TABLE Report1 RENAME TO Report1_oldcolumnheaders ;
CREATE TABLE Report1 (transid TEXT, sourcefile TEXT, linenum TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT) ;
INSERT INTO Report1 (transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose)
	SELECT transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose
	FROM Report1_oldcolumnheaders ;
DROP TABLE IF EXISTS Report1_oldcolumnheaders ; ''')
conn.commit()

#clean up headers of Report2 for output
cur.executescript('''
DROP TABLE IF EXISTS Report2_oldcolumnheaders ;
ALTER TABLE Report2 RENAME TO Report2_oldcolumnheaders ;
CREATE TABLE Report2 (transid TEXT, sourcefile TEXT, linenum TEXT, orgname TEXT, lastname TEXT, firstname TEXT, address1 TEXT, address2 TEXT, city TEXT, state TEXT, zipcode TEXT, electioncode TEXT, electionother TEXT, date TEXT, amount TEXT, aggregate TEXT, purpose TEXT, employer TEXT, occupation TEXT, memoentry TEXT, note TEXT, beginningbalance TEXT, incurredamount TEXT, paymentamount TEXT, balanceatclose TEXT) ;
INSERT INTO Report2 (transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose)
	SELECT transid, sourcefile, linenum, orgname, lastname, firstname, address1, address2, city, state, zipcode, electioncode, electionother, date, amount, aggregate, purpose, employer, occupation, memoentry, note, beginningbalance, incurredamount, paymentamount, balanceatclose
	FROM Report2_oldcolumnheaders ;
DROP TABLE IF EXISTS Report2_oldcolumnheaders ; ''')
conn.commit()
print('...done')


#AND NOW EXPORT

#export the results to CSV files...
print()
print('Exporting results to output files...')
cur.execute('''SELECT * FROM R1notR2''')
columns = [i[0] for i in cur.description]
row = cur.fetchall()
with open('Output1_Entries_Deleted.csv', 'w', newline='\n') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(columns)
    a.writerows(row)
cur.execute('''SELECT * FROM R2notR1''')
columns = [i[0] for i in cur.description]
row = cur.fetchall()
with open('Output2_Entries_Added.csv', 'w', newline='\n') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(columns)
    a.writerows(row)
cur.execute('''SELECT * FROM Changed''')
columns = [i[0] for i in cur.description]
row = cur.fetchall()
with open('Output3_Entries_Changed.csv', 'w', newline='\n') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(columns)
    a.writerows(row)
cur.execute('''SELECT * FROM Report1''')
columns = [i[0] for i in cur.description]
row = cur.fetchall()
with open('Output4_Report1_Cleaned.csv', 'w', newline='\n') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(columns)
    a.writerows(row)
cur.execute('''SELECT * FROM Report2''')
columns = [i[0] for i in cur.description]
row = cur.fetchall()
with open('Output5_Report2_Cleaned.csv', 'w', newline='\n') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(columns)
    a.writerows(row)
print('...done')


#find how many entries were added, removed, and changed
cur.execute('''SELECT * FROM R1notR2 ''')
results = cur.fetchall()
lenR1notR2 = len(results)
conn.commit()
cur.execute('''SELECT * FROM R2notR1 ''')
results = cur.fetchall()
lenR2notR1 = len(results)
conn.commit()
cur.execute('''SELECT * FROM Changed ''')
results = cur.fetchall()
lenChanged = len(results)/2 #this has to be divided by two because the Changed table includes Report1 and Report2 entries
lenChanged = int(lenChanged) #make it an integer so it drops the ".0" at the end of the number when printed
conn.commit()



#LAST STEP: PROVIDE SUMMARY TO USER

#print summary of comparison
print()
print()
print('COMPARISON SUMMARY')
print()
print('Compared', fname1, 'and', fname2)
print()
print('Schedule A:  ', countsa1, ' entries on ', fname1, sep='')
print('             ', countsa2, ' entries on ', fname2, sep='')
print('Schedule B:  ', countsb1, ' entries on ', fname1, sep='')
print('             ', countsb2, ' entries on ', fname2, sep='')
print('Schedule D:  ', countsd1, ' entries on ', fname1, sep='')
print('             ', countsd2, ' entries on ', fname2, sep='')
##print('Schedule E:  ', countse1, ' entries on ', fname1, sep='')
##print('             ', countse2, ' entries on ', fname2, sep='')
##print('Schedule F:  ', countsf1, ' entries on ', fname1, sep='')
##print('             ', countsf2, ' entries on ', fname2, sep='')
##print('Schedule H4: ', countsh41, ' entries on ', fname1, sep='')
##print('             ', countsh42, ' entries on ', fname2, sep='')
##print('Schedule H6: ', countsh61, ' entries on ', fname1, sep='')
##print('             ', countsh62, ' entries on ', fname2, sep='')
print()
print('Exported results to')
print('Output1_Entries_Deleted.csv (', lenR1notR2, 'entries only on', fname1,')' )
print('Output2_Entries_Added.csv   (', lenR2notR1, 'entries only on', fname2,')' )
if compareall == 'y':
    print('Output3_Entries_Changed.csv (', lenChanged, 'entries on both reports but changed )' )
    print('Output4_Report1_Cleaned.csv ( made columns consistent )' )
    print('Output5_Report2_Cleaned.csv ( made columns consistent )' )
if compareall == 'n':
    print('Output3_Entries_Changed.csv (', lenChanged, 'entries on both reports but changed* )' )
    print('Output4_Report1_Cleaned.csv ( made columns consistent )' )
    print('Output5_Report2_Cleaned.csv ( made columns consistent )' )
    print()
    print('* Based on settings, output3 includes entries with changes in')
    print('  the address and aggregate fields only if other fields also changed.')
print()
print('Reminder: This program only reviews entries disclosed on')
print('          Schedules A, B, and D.')
print()
end = time.clock()
timeelapsed = str(round(end - start, 2))
print('Time elapsed:', timeelapsed, 'seconds')
conn.close()
