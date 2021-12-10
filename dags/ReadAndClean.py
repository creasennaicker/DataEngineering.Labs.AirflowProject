import pandas as pd



    def read_csv():
        df = pd.read_csv(
            r'/Users/naickercreason/dev/DataEngineering.Labs.AirflowProject/vacc CSV data/country_vaccinations.csv')
        print(df)


    def drop_cols():
        drop = df.drop(df.columns[[0, 1, 4, 5, 6, 7, 8, 9, 10, 11, 13]], axis=1)
        print(drop)

    def remove_null():
        no_null = df.dropna(subset=['total_vaccinations', 'people_vaccinated'], inplace=True)
        print(no_null)
