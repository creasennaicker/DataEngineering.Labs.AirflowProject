import pandas as pd


class covid_process:

    def read_csv():
        vac_data = pd.read_csv('country_vaccinations.csv')
        data = pd.DataFrame(vac_data)
        return data

print(read_csv())