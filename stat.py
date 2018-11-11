import psycopg2
import csv

connection = None
print "Script started."
try:
    connection = psycopg2.connect(
        host="localhost", database="mimic", user="ginger", password="source32")
    cursor = connection.cursor()
    print "Connected to the database."

    with open('bloodOxygen.csv', mode='w') as csv_file:
        fieldnames = ['subject_id', 'numberOfMeasurements',
                      'minValue', 'maxValue', 'minDate', 'maxDate']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        print "CSV file created."

        itemid = 220277

        cursor.execute(
            "SELECT DISTINCT(subject_id) FROM result WHERE itemid = %s ORDER BY subject_id ASC;", (itemid,))
        validSubjects = cursor.fetchall()
        print(len(validSubjects))
        # from 2 to 100000
        for i in validSubjects:
            # cursor.execute(
            #     "SELECT subject_id FROM result WHERE subject_id = %s LIMIT 1;", (i,))
            # check = cursor.fetchone()
            # print "Checked: ", check

            # # if the given subject_id exists
            # if check != None:
            cursor.execute(
                "SELECT subject_id FROM result WHERE subject_id = %s AND itemid = %s;", (i, itemid))
            subject_id = cursor.fetchone()
            #print "subject_id: ", subject_id[0]
            cursor.execute(
                "SELECT COUNT(valuenum) FROM result WHERE subject_id = %s AND itemid = %s;", (i, itemid))
            numberOfMeasurements = cursor.fetchone()
            #print "numberOfMeasurements: ", int(numberOfMeasurements[0])
            cursor.execute(
                "SELECT MIN(valuenum) FROM result WHERE subject_id = %s AND itemid = %s;", (i, itemid))
            minValue = cursor.fetchone()
            #print "minValue: ", minValue[0]
            cursor.execute(
                "SELECT MAX(valuenum) FROM result WHERE subject_id = %s AND itemid = %s;", (i, itemid))
            maxValue = cursor.fetchone()
            #print "maxValue: ", maxValue[0]
            cursor.execute(
                "SELECT MIN(charttime) FROM result WHERE subject_id = %s AND itemid = %s;", (i, itemid))
            minDate = cursor.fetchone()
            #print("minDate: ", minDate[0])
            cursor.execute(
                "SELECT MAX(charttime) FROM result WHERE subject_id = %s AND itemid = %s;", (i, itemid))
            maxDate = cursor.fetchone()
            #print("maxDate: ", maxDate[0])
            writer.writerow({'subject_id': subject_id[0], 'numberOfMeasurements': numberOfMeasurements[0],
                             'minValue': minValue[0], 'maxValue': maxValue[0], 'minDate': minDate[0], 'maxDate': maxDate[0]})

            #     # if the given subject_id does not exists
            # else:
            #     print "There is no row with number ", i
            #     writer.writerow({'subject_id': i, 'numberOfMeasurements': '0',
            #                      'minValue': '0', 'maxValue': '0', 'minDate': '0', 'maxDate': '0'})
except(Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if connection is not None:
        cursor.close()
        connection.close()
        print "Database connection closed."
print "Script finished."
