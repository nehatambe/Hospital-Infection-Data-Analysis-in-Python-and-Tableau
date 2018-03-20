from builtins import ValueError


from com.hospital.data_processing.DbConnection import getConnection, closeConnection


def fetch_infection_data():
    query = "select survey_id,hospital_name,state,HCAHPS_measure_id,survey_rating from hospital_survey where HCAHPS_measure_id in('H_STAR_RATING','H_CLEAN_STAR_RATING','H_COMP_1_STAR_RATING','H_COMP_2_STAR_RATING','H_COMP_3_STAR_RATING','H_COMP_4_STAR_RATING','H_COMP_5_STAR_RATING') and survey_rating not in (0,10);"
    
    try:
        
        cursor, db = getConnection()
        print("Connection Established")
        
        cursor.execute("TRUNCATE TABLE survey_rating_data;")
        db.commit()
        
        
        cursor.execute(query);
        
        print("Processing #",cursor.rowcount, " Records...")
        cleanMeasureRating=0
        measureStarRating = 0
        firstCleanStarRating = 0
        secondCleanStarRating = 0
        thirdCleanStarRating = 0
        fourthCleanStarRating = 0
        fifthCleanStarRating = 0
        
        qSQLresults = cursor.fetchall()
        for row in qSQLresults:
            surveyProcessedId = row[0]
            hospitalName = row[1] 
            state = row[2]
            measureId = row[3] 
             
            
            if(measureId=='H_STAR_RATING'):
                measureStarRating = row[4]
                
            if(measureId=='H_CLEAN_STAR_RATING'):
                cleanMeasureRating = row[4]
                
            if(measureId=='H_COMP_1_STAR_RATING'):
                firstCleanStarRating = row[4]
            
            if(measureId=='H_COMP_2_STAR_RATING'):
                secondCleanStarRating = row[4]
            
            if(measureId=='H_COMP_3_STAR_RATING'):
                thirdCleanStarRating = row[4]
                
            if(measureId=='H_COMP_4_STAR_RATING'):
                fourthCleanStarRating = row[4]
            
            if(measureId=='H_COMP_5_STAR_RATING'):
                fifthCleanStarRating = row[4]
                                
            if(cleanMeasureRating!=0) and (measureStarRating!=0) and (firstCleanStarRating!=0) and (secondCleanStarRating!=0) and (thirdCleanStarRating!=0) and  (fourthCleanStarRating!=0) and  (fifthCleanStarRating!=0):
                cursor.execute('''INSERT into survey_rating_data (`survey_process_id`,`hospital_name`,`state`,`h_star_rating`,`h_clean_star_rating`,`h_comp_1_star_rating`,`h_comp_2_star_rating`,`h_comp_3_star_rating`,`h_comp_4_star_rating`,`h_comp_5_star_rating`)
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (surveyProcessedId, hospitalName,state , measureStarRating,cleanMeasureRating,firstCleanStarRating,secondCleanStarRating,thirdCleanStarRating,fourthCleanStarRating,fifthCleanStarRating))
                cleanMeasureRating = 0
                measureStarRating = 0
                firstCleanStarRating = 0
                secondCleanStarRating = 0
                thirdCleanStarRating = 0
                fourthCleanStarRating = 0
                fifthCleanStarRating = 0
            
            # Commit your changes in the database
            
            
    except ValueError:
        print('Error in inserting records.')
 
    finally:
        
        db.commit()
        print("Records updated successfully!")
        print("Closing connection")
        closeConnection(cursor, db)


def main():
    
    fetch_infection_data()
 
if __name__ == '__main__':
    main()
