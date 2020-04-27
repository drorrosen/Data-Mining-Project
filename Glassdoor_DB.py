import mysql.connector
import config as CFG
import pandas as pd


def create_db():
    """Creating the glassdoor database and tables"""

    # Creating the Glassdoor database
    mydb = mysql.connector.connect(
        host=CFG.HOST,
        user=CFG.USER,
        passwd=CFG.PASSWORD)

    # creating a cursor
    my_cursor = mydb.cursor()

    my_cursor.execute(f"CREATE DATABASE {CFG.DB}")

    # Connecting to our created database and creating the tables

    # Connecting to the database -
    mydb = mysql.connector.connect(
        host=CFG.HOST,
        user=CFG.USER,
        passwd=CFG.PASSWORD,
        database=CFG.DB)

    # creating a cursor
    my_cursor = mydb.cursor()

    # Creating the locations table
    my_cursor.execute("""CREATE TABLE locations (
                      id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                      location VARCHAR(100) CHARACTER SET UTF8,
                      country VARCHAR(100) CHARACTER SET UTF8 NOT NULL,
                      city VARCHAR(100) CHARACTER SET UTF8 NOT NULL,
                      longitude FLOAT ,
                      latitude FLOAT,
                      region VARCHAR(100) CHARACTER SET UTF8,
                      population INT,
                      capital VARCHAR(100) CHARACTER SET UTF8,
                      UNIQUE(country, city)
                      

  )
  """)

    # Creating the companies table
    my_cursor.execute("""CREATE TABLE companies (
                      company_name VARCHAR(100) CHARACTER SET UTF8 NOT NULL,
                      location_id INT,
                      FOREIGN KEY(location_id) REFERENCES locations(id),
                      size VARCHAR(100) CHARACTER SET UTF8,
                      founded INTEGER,
                      type VARCHAR(100) CHARACTER SET UTF8,
                      industry VARCHAR(100) CHARACTER SET UTF8,
                      sector VARCHAR(100) CHARACTER SET UTF8,
                      revenue VARCHAR(100) CHARACTER SET UTF8,
                      rating FLOAT,
                      PRIMARY KEY(company_name)
  )
  """)

    # Creating the jobs_reqs table
    my_cursor.execute("""CREATE TABLE job_reqs (
                      job_id BIGINT,
                      title VARCHAR(100) CHARACTER SET UTF8,
                      description TEXT CHARACTER SET UTF8,
                      company VARCHAR(100) CHARACTER SET UTF8,
                      FOREIGN KEY(company) REFERENCES companies(company_name),
                      scrape_date DATETIME,
                      location_id INT,
                      FOREIGN KEY(location_id) REFERENCES locations(id),
                      PRIMARY KEY(job_id)
  )
  """)
    my_cursor.execute("""CREATE TABLE skills (
                     skill_id INT AUTO_INCREMENT,
                     skill_name VARCHAR(100),
                     PRIMARY KEY(skill_id)) """)

    my_cursor.execute("""CREATE TABLE skills_in_job (
                     job_id BIGINT,
                     FOREIGN KEY(job_id) REFERENCES job_reqs(job_id),
                     skill_id INT,
                     FOREIGN KEY(skill_id) REFERENCES skills(skill_id),
                     PRIMARY KEY(job_id, skill_id)) """)

    bag_of_words = pd.read_csv('bag_of_words.csv')
    bag_of_words = bag_of_words['Skill'].str.lower().values
    df_skill_list = pd.Series(bag_of_words).reset_index().rename(columns={0: 'skill_name'})
    for i in range(len(df_skill_list)):
        row = df_skill_list.loc[i, :].tolist()
        row[0] = int(row[0])
        my_cursor.execute("""INSERT IGNORE INTO skills (
                                  skill_id, skill_name)
                                VALUES (%s, %s)""", row)
        if i % 1000 == 0:
            mydb.commit()
    mydb.commit()


def main():
    create_db()


if __name__ == "__main__":
    main()
