import sys
import csv
import psycopg2
import math
from sklearn import linear_model
import time
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def tupleMaxValue(values):
    max = 0
    for element in values:
        if element > max:
            max = element
    max = int(max[0])
    return max

itemid = 0
file = ''

print "--------------------------"
print "1 - testsuly"
print "2 - testmagassag"
print "3 - szisztoles vernyomas"
print "4 - diasztoles vernyomas"
print "5 - vercukorszint"
print "6 - veroxigenszint"
print "--------------------------"
print "Adjon meg egy meres tipust: "
input = raw_input()

print "Minimum hany meres: "
minMeasurements = raw_input()
print "Maximum hany meres: "
maxMeasurements = raw_input()

if int(input) == 1:
    itemid = 226512
    file = 'weight.csv'
elif int(input) == 2:
    itemid = 226730
    file = 'height.csv'
elif int(input) == 3:
    itemid = 220050
    file = 'systolic.csv'
elif int(input) == 4:
    itemid = 220051
    file = 'diastolic.csv'
elif int(input) == 5:
    itemid = 1529
    file = 'bloodGlucose.csv'
elif int(input) == 6:
    itemid = 220277
    file = 'bloodOxygen.csv'

print "Script started."
print file
# connection = None
try:
    connection = psycopg2.connect(
        host="localhost", database="mimic", user="ginger", password="source32")
    cursor = connection.cursor()
    print "Connected to the database."

    with open(file, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        print "CSV file opened."
        counter = 0
        for row in reader:
            numberOfMeasurements = row['numberOfMeasurements']
            subject_id = row['subject_id']
            minValue = int(minMeasurements)
            maxValue = int(maxMeasurements)
            if int(numberOfMeasurements) >= minValue and int(numberOfMeasurements) <= maxValue:
                cursor.execute(
                    "SELECT valuenum FROM result WHERE subject_id = %s AND itemid = %s ORDER BY charttime;", (subject_id, itemid))
                selectedValues = cursor.fetchall()
                values = []
                for i in range(len(selectedValues)):
                    if selectedValues[i] == (None,):
                        values.append((0.0,))
                    else:
                        values.append(selectedValues[i])
                train = []
                for i in range(len(values)):
                    train.append(i)
                print "Subject_id: ", subject_id
                n = len(values)
                print "Darabszam: ", n
                sum = 0
                for i in values:
                    i = int(i[0])
                    sum = sum + i
                average = (float(sum) / float(n))
                print "Atlag: ", average
                temp = 0.0
                for i in values:
                    i = int(i[0])
                    temp += pow((i - average), 2)
                dispersionSquare = (float(temp)) / n
                dispersion = math.sqrt(dispersionSquare)
                print "Szoras: ", dispersion

                regr = linear_model.LinearRegression()
                regr.fit(values, train)
                prediction = regr.predict(values)
                plt.plot(range(1, n + 1, 1), values, 'ro',
                         range(1, n + 1, 1), values, 'k')

                plt.plot(prediction, values, color='blue', linewidth=3)
                plt.ylabel('Mert ertek')
                plt.xlabel('Meres sorszam')
                cursor.execute(
                    "SELECT charttime FROM result WHERE subject_id = %s AND itemid = %s ORDER BY charttime;", (subject_id, itemid))
                times = cursor.fetchall()

                level = []
                for i in range(len(times)):
                    level.append(times[i][0])

                level1 = []
                for i in range(len(level)):
                   level1.append(level[i].strftime("%Y.%m.%d %H:%M"))

                max = tupleMaxValue(values)
                print "Legnagyobb ertek: ", max

                plt.xticks(range(1, len(level1), 5), level1)
                plt.yticks(range(0, max+5, 5))
                plt.grid(True)
                plt.show()
                print "-----------------------------"
                counter += 1
                if (counter == 10):
                    break
except(Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if connection is not None:
        cursor.close()
        connection.close()
        print "Database connection closed."
    print "Script finished."
    exit()
