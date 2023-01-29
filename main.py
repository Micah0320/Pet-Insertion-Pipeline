#There are currently over 3 thousand pets to insert into this table
#The schema is petName, Species, Breed, DOB, Appointment
#The Appointment info is not relevant to our database.
#Instead, we Need id, animalID, breedID, name, age, gender, isAdopted.
#
#

#Imports for this
import pymysql
import datetime

def cleanup(x, islist=False):
    x = x.replace('\n', '')
    if x == r'\N':
        if islist:
            x = []
        else:
            x = None
    elif islist:
        x = x.split(',')
        if x == ['']: x = []
        return x
        
#==============================================================================
#Pets into the table based off of name
#==============================================================================

#This will only insert the names, will then update each and every row in the table with the next data
conn = pymysql.connect(host='PurrfectMatch.mysql.pythonanywhere-services.com', user='PurrfectMatch', passwd='55055245MZ', db='PurrfectMatch$zdummy')
cursor = conn.cursor()
filename = 'data/PetDatasetHSOM.csv'
f = open(filename, 'r')
f.readline() # ignore first line
sql = 'Insert Pets (name, isAdopted) values(%s, 0);'
list_indicies = 0 # indices where the value should be a list
num_fields = 6 # number of fields
for line in f:
    items = line.split(',')
    newitems = items[list_indicies]
    items = newitems
    cursor.execute(sql, (items))
    conn.commit()

cursor.close()
conn.close()

print("Done with Names")

#Password Censored
conn = pymysql.connect(host='PurrfectMatch.mysql.pythonanywhere-services.com', user='PurrfectMatch', passwd='**********', db='PurrfectMatch$zdummy')
cursor = conn.cursor()
f = open(filename, 'r')
f.readline() # ignore first line
count = 0

##############################################################################
#Updates the Pet Animal Type Next
##############################################################################
sql = "Update Pets set animal_id = %s where id = %s;"
sqlGet = "select id from Animals where type = %s;"
list_indicies = 1
f = open(filename, 'r')
f.readline() # ignore first line
demandingness = set([]) # professions as a set
count = 0
for line in f:
    items = line.split(',')
    newitems = items[list_indicies]
    items = newitems
    cursor.execute(sqlGet, (items))
    animalID = cursor.fetchall()
    cursor.execute(sql, (animalID, count + 1))
    conn.commit()
    count = count + 1

cursor.close()
conn.close()

print("Done with Animal Type")

##############################################################################
#Updates the Pet Breed Next
##############################################################################
sql = "Update Pets set breed_id = %s where id = %s;"
sqlGet = "select id from Breeds where breedName = %s;"
list_indicies = 2
conn = pymysql.connect(host='PurrfectMatch.mysql.pythonanywhere-services.com', user='PurrfectMatch', passwd='55055245MZ', db='PurrfectMatch$zdummy')
cursor = conn.cursor()
f = open(filename, 'r')
f.readline() # ignore first line
demandingness = set([]) # professions as a set
count = 0
for line in f:
    items = line.split(',')
    newitems = items[list_indicies]
    items = newitems
    cursor.execute(sqlGet, (items))
    breedID = cursor.fetchone()
    #if breedID == None:
        #continue
    cursor.execute(sql, (breedID, count + 1))
    conn.commit()
    count = count + 1


cursor.close()
conn.close()

print("Done with Breed")

##############################################################################
#Updates the Pet Gender Next
##############################################################################
sql = "Update Pets set gender = %s where id = %s;"
list_indicies = 5
conn = pymysql.connect(host='PurrfectMatch.mysql.pythonanywhere-services.com', user='PurrfectMatch', passwd='55055245MZ', db='PurrfectMatch$zdummy')
cursor = conn.cursor()

f = open(filename, 'r')
f.readline() # ignore first line
#demandingness = set([]) # professions as a set
count = 0
for line in f:
    items = line.split(',')
    newitems = items[list_indicies]
    #print(len(items))
    items = newitems
    cursor.execute(sql, (items, count + 1))
    conn.commit()
    count = count + 1

cursor.close()
conn.close()

print("Done with Gender")

##############################################################################
#Updates the Pet Age Next
##############################################################################
sql = "Update Pets set age = %s where id = %s;"
list_indicies = 3
conn = pymysql.connect(host='PurrfectMatch.mysql.pythonanywhere-services.com', user='PurrfectMatch', passwd='55055245MZ', db='PurrfectMatch$zdummy')
cursor = conn.cursor()
f = open(filename, 'r')
f.readline() # ignore first line
#demandingness = set([]) # professions as a set
count = 0
for line in f:
    items = line.split(',')
    newitems = items[list_indicies]
    items = newitems.split('/')
    #print(len(items))
    YOB = items[2]
    age = 2021 - int(YOB)
    if age > 10:
        age = age - 10
    cursor.execute(sql, (age, count + 1))
    conn.commit()
    count = count + 1

cursor.close()
conn.close()
print("Done with Age")
